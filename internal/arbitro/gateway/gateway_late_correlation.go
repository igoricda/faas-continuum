package gateway

import (
	"sync"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// liveRef locates the synchronous entry of an attempt in the history for
// the supersede: the keys of the segments where the record of the attempt
// wrote, the primary and the aggregated one of the fan-out, which is empty
// when the node has no layer, and the stamp that identifies the entry
// inside them.
type liveRef struct {
	segKey    string
	aggKey    string
	nodeID    string
	timestamp time.Time
	// Inheritance of the projected segment: the late report of the same
	// attempt copies these fields to land in the same segment that the
	// synchronous record used, since the key depends on the scenario.
	// The number of requests in flight, the projected load, the average
	// processor usage and the number of samples are populated by the
	// record of the attempt and inherited by the supersede of the late
	// report. The complete set captured per invocation, with the peak of
	// the processor usage, the peak of the memory, the wait of the
	// acquisition, the readiness, the function time, the depth of the
	// queue and the number of available instances, follows the same
	// path, since the wire of the late report carries none of them and
	// the inheritance is only through this reference. The observed
	// processor and memory usage follow the same path: with the
	// reference the synchronous record wins, and without it, in an
	// orphan, the values of the late report are used.
	sc                 history.ScenarioContext
	inflight           int
	projected          float64
	cpuPctAvg          float64
	cpuPctPeak         float64
	memPeakBytes       uint64
	sampleCount        int
	acquireWaitMS      int64
	readinessMS        int64
	functionMS         int64
	queueDepth         int
	instancesAvailable int
	observedCPU        float64
	observedMem        float64
	psiCpuSomeAvg10    float64
	psiMemFullAvg10    float64
}

// supersedeKeys returns the two keys of the fan-out, the primary and the
// one aggregated by layer, which is empty when the node has no layer, for
// the dual-key supersede. It is a pure extraction of the record of the late
// attempt.
func (ref liveRef) supersedeKeys() (segKey, aggKey string) {
	return ref.segKey, ref.aggKey
}

// liveAttemptIndex correlates the pair of the request identifier and the
// attempt number with the live reference. It is bounded with a drop of the
// oldest entry, since the identifier comes from an external input. The take
// consumes the entry, since a late report supersedes once, and a second
// late report of the same pair is caught by the deduplication before it
// reaches this point.
type liveAttemptIndex struct {
	mu    sync.Mutex
	m     map[lateKey]liveRef
	order []lateKey
	cap   int
}

func newLiveAttemptIndex(cap int) *liveAttemptIndex {
	if cap <= 0 {
		cap = 10000
	}
	return &liveAttemptIndex{m: make(map[lateKey]liveRef, cap), cap: cap}
}

func (x *liveAttemptIndex) note(requestID string, attemptN int, ref liveRef) {
	if requestID == "" || attemptN <= 0 {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	k := lateKey{RequestID: requestID, AttemptN: attemptN}
	if _, ok := x.m[k]; !ok {
		if len(x.m) >= x.cap {
			delete(x.m, x.order[0])
			x.order = x.order[1:]
		}
		x.order = append(x.order, k)
	}
	x.m[k] = ref
}

func (x *liveAttemptIndex) take(requestID string, attemptN int) (liveRef, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	k := lateKey{RequestID: requestID, AttemptN: attemptN}
	ref, ok := x.m[k]
	if !ok {
		return liveRef{}, false
	}
	delete(x.m, k)
	for i, old := range x.order {
		if old == k {
			x.order = append(x.order[:i], x.order[i+1:]...)
			break
		}
	}
	return ref, true
}

type lateGroupEntry struct {
	group     string
	slo       *selection.SLO
	expiresAt time.Time
}

type lateGroupMap struct {
	mu    sync.Mutex
	m     map[string]lateGroupEntry
	order []string
	cap   int
	ttl   time.Duration
	clock func() time.Time
}

func newLateGroupMap(cap int, ttl time.Duration, clock func() time.Time) *lateGroupMap {
	if cap <= 0 {
		cap = 10000
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if clock == nil {
		clock = time.Now
	}
	return &lateGroupMap{m: make(map[string]lateGroupEntry, cap), cap: cap, ttl: ttl, clock: clock}
}

// entry returns the complete entry, with the group and the objective of the
// invocation, so that the late path derives the objective with the original
// one. An expired entry is removed from the map.
func (m *lateGroupMap) entry(requestID string) (lateGroupEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.m[requestID]
	if !ok {
		return lateGroupEntry{}, false
	}
	if m.clock().After(e.expiresAt) {
		delete(m.m, requestID)
		return lateGroupEntry{}, false
	}
	return e, true
}

func (m *lateGroupMap) set(requestID, group string, slo *selection.SLO) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.m[requestID]; !exists {
		if len(m.m) >= m.cap {
			oldest := m.order[0]
			m.order = m.order[1:]
			delete(m.m, oldest)
		}
		m.order = append(m.order, requestID)
	}
	m.m[requestID] = lateGroupEntry{group: group, slo: slo, expiresAt: m.clock().Add(m.ttl)}
}
