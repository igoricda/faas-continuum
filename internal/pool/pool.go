// Package pool keeps pre-allocated container instances for each function and
// scales them on demand up to a configurable ceiling. AcquireWait is the only
// path that queues waiters: an exhausted pool fails immediately, and a
// per-function queue exists only while a provisioning call is in flight.
package pool

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// ErrPoolExhausted is returned by Acquire when every instance of a function
// is busy and the pool is already at max_instances.
var ErrPoolExhausted = errors.New("pool: todas as instâncias estão ocupadas e o pool já está no limite (max_instances)")

// ErrQueueFull is returned by AcquireWait when the per-function provisioning
// queue has reached its capacity.
var ErrQueueFull = errors.New("pool: fila de provisionamento cheia para esta função")

// Provisioner creates and terminates container instances.
type Provisioner interface {
	Provision(ctx context.Context, instanceID, imageRef string) error
	Terminate(ctx context.Context, instanceID string) error
}

// SHARD_COUNT is the default number of shards.
const SHARD_COUNT = 256

// shard holds the per-function state for a subset of functions selected by
// hash (see shardFor).
type shard struct {
	mu        sync.Mutex
	instances map[string][]instance
	pendingID map[string]string
	waiters   map[string][]chan acquireResult
	lastUsed  map[string]time.Time
}

// Manager holds the sharded per-function instance pool.
type Manager struct {
	provisioner Provisioner
	shards      []*shard
	// globalMu protects instanceByID (a presence index; busy lives only in the shard slice).
	globalMu       sync.RWMutex
	instanceByID   map[string]struct{}
	queueCap       func(functionName string) int
	coalescedTotal int64
	clockMu        sync.RWMutex
	clock          func() time.Time
}

// NewManagerWithQueueCap returns a Manager with a per-function queue capacity
// function; a nil queueCap falls back to maxInstances or the default of 32.
func NewManagerWithQueueCap(p Provisioner, queueCap func(string) int) *Manager {
	shards := make([]*shard, SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		shards[i] = &shard{
			instances: make(map[string][]instance),
			pendingID: make(map[string]string),
			waiters:   make(map[string][]chan acquireResult),
			lastUsed:  make(map[string]time.Time),
		}
	}
	return &Manager{
		provisioner:  p,
		shards:       shards,
		instanceByID: make(map[string]struct{}),
		queueCap:     queueCap,
		clock:        time.Now,
	}
}

func (m *Manager) shardFor(functionName string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(functionName))
	idx := int(h.Sum32() % uint32(len(m.shards)))
	return m.shards[idx]
}

// WithClock sets the clock used by now. It is exported so the scaler package
// can drive time deterministically in tests; production wiring leaves it
// unset to use time.Now.
func (m *Manager) WithClock(fn func() time.Time) {
	m.clockMu.Lock()
	defer m.clockMu.Unlock()
	if fn != nil {
		m.clock = fn
	} else {
		m.clock = time.Now
	}
}

func (m *Manager) now() time.Time {
	m.clockMu.RLock()
	c := m.clock
	m.clockMu.RUnlock()
	if c != nil {
		return c()
	}
	return time.Now()
}

func (m *Manager) touchOnShard(s *shard, functionName string) {
	if s.lastUsed == nil {
		s.lastUsed = make(map[string]time.Time)
	}
	n := m.now()
	s.lastUsed[functionName] = n
}

// queueCapFor returns the queue capacity for the function: the configured
// value when positive, otherwise maxInstances, otherwise 32.
func (m *Manager) queueCapFor(functionName string, maxInstances int) int {
	if m.queueCap != nil {
		if v := m.queueCap(functionName); v > 0 {
			return v
		}
	}
	if maxInstances > 0 {
		return maxInstances
	}
	return 32
}

// wakeOneLockedOnShard wakes one waiter for the function, if any. It is the
// only wakeup path and increments coalescedTotal exactly once per call. The
// caller must hold s.mu.
func (m *Manager) wakeOneLockedOnShard(s *shard, functionName string, res acquireResult) {
	list := s.waiters[functionName]
	if len(list) == 0 {
		return
	}
	ch := list[0]
	s.waiters[functionName] = list[1:]
	atomic.AddInt64(&m.coalescedTotal, 1)
	select {
	case ch <- res:
	default:
	}
}

// Count returns the number of instances tracked for the function.
func (m *Manager) Count(functionName string) int {
	s := m.shardFor(functionName)
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.instances[functionName])
}

// Instances returns the IDs of the instances tracked for the function.
func (m *Manager) Instances(functionName string) []string {
	s := m.shardFor(functionName)
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.instances[functionName]))
	for _, inst := range s.instances[functionName] {
		out = append(out, inst.id)
	}
	return out
}

func (m *Manager) QueueLen(functionName string) int {
	s := m.shardFor(functionName)
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiters[functionName])
}

func (m *Manager) CoalescedCount() int {
	return int(atomic.LoadInt64(&m.coalescedTotal))
}

// removeInstanceLocked removes an instance from the pool. It acquires the
// locks itself, in contrast with removeInstanceLockedOnShard, which requires
// the caller to hold the shard lock.
func (m *Manager) removeInstanceLocked(functionName, instanceID string) {
	s := m.shardFor(functionName)
	s.mu.Lock()
	m.removeInstanceLockedOnShard(s, functionName, instanceID)
	s.mu.Unlock()
	m.globalMu.Lock()
	delete(m.instanceByID, instanceID)
	m.globalMu.Unlock()
}

// removeInstanceLockedOnShard requires the shard lock and removes only the
// entry of the shard. The caller removes the identifier from the global
// index.
func (m *Manager) removeInstanceLockedOnShard(s *shard, functionName, instanceID string) {
	list := s.instances[functionName]
	for i, inst := range list {
		if inst.id == instanceID {
			s.instances[functionName] = append(list[:i], list[i+1:]...)
			if s.pendingID[functionName] == instanceID {
				delete(s.pendingID, functionName)
			}
			if len(s.instances[functionName]) == 0 {
				delete(s.instances, functionName)
				delete(s.pendingID, functionName)
				delete(s.lastUsed, functionName)
			}
			break
		}
	}
}
