package pool

import (
	"context"
	"fmt"
	"log"
	"strconv"
)

// EnsureMinInstances grows the pool of the function up to minInstances.
func (m *Manager) EnsureMinInstances(ctx context.Context, functionName, imageRef string, minInstances int) error {
	if minInstances < 0 {
		return fmt.Errorf("pool: minInstances não pode ser negativo, recebido %d", minInstances)
	}
	s := m.shardFor(functionName)
	s.mu.Lock()
	var pendentes []instance
	if faltando := minInstances - len(s.instances[functionName]); faltando > 0 {
		idx := m.nextIndexLockedOnShard(s, functionName)
		for i := 0; i < faltando; i++ {
			inst := instance{id: functionName + "-" + strconv.Itoa(idx+i), busy: true}
			pendentes = append(pendentes, inst)
			if s.instances == nil {
				s.instances = make(map[string][]instance)
			}
			s.instances[functionName] = append(s.instances[functionName], inst)
		}
	}
	s.mu.Unlock()
	if len(pendentes) > 0 {
		m.globalMu.Lock()
		if m.instanceByID == nil {
			m.instanceByID = make(map[string]struct{})
		}
		for _, inst := range pendentes {
			m.instanceByID[inst.id] = struct{}{}
		}
		m.globalMu.Unlock()
	}

	for i, inst := range pendentes {
		if err := m.provisioner.Provision(ctx, inst.id, imageRef); err != nil {
			// Roll back every instance that was not provisioned, this one and
			// the following, which are still marked busy. Without it the
			// pool keeps phantom slots: the count is inflated, the reaper
			// cannot collect them and the next index skips ahead. The
			// instances already provisioned, with a lower index, stay free.
			// No I/O happens under the lock, only map and slice updates.
			fantasmas := pendentes[i:]
			s.mu.Lock()
			for _, fantasma := range fantasmas {
				m.removeInstanceLockedOnShard(s, functionName, fantasma.id)
			}
			s.mu.Unlock()
			m.globalMu.Lock()
			for _, fantasma := range fantasmas {
				delete(m.instanceByID, fantasma.id)
			}
			m.globalMu.Unlock()
			return fmt.Errorf("pool: falha ao provisionar instância %q: %w", inst.id, err)
		}
		s.mu.Lock()
		for i := range s.instances[functionName] {
			if s.instances[functionName][i].id == inst.id {
				s.instances[functionName][i].busy = false
				break
			}
		}
		s.mu.Unlock()
	}
	return nil
}

// Acquire reserves a free instance. It is an alias for AcquireWait.
func (m *Manager) Acquire(ctx context.Context, functionName, imageRef string, maxInstances int) (string, error) {
	return m.AcquireWait(ctx, functionName, imageRef, maxInstances)
}

func (m *Manager) AcquireWait(ctx context.Context, functionName, imageRef string, maxInstances int) (string, error) {
	id, _, err := m.AcquireWaitCold(ctx, functionName, imageRef, maxInstances)
	return id, err
}

// AcquireWaitCold is the only path that uses the wait queue and the pending
// identifier, and the only one that distinguishes a cold start, meaning the
// instance is provisioned during the call, from a warm one.
//
// The main loop only orders the guard clauses; each stage lives in a helper
// with its own lock discipline, so the atomic reservation happens under the
// shard lock while the slow provisioning call always runs outside it.
//
// The scans over the per-function slice are linear. The global index of
// instance identifiers only records presence, without the busy flag or the
// function index, so it cannot answer whether a free slot exists and cannot
// reserve one together with the capacity check.
func (m *Manager) AcquireWaitCold(ctx context.Context, functionName, imageRef string, maxInstances int) (string, bool, error) {
	s := m.shardFor(functionName)
	for {
		if id, ok := m.tryReuseOnShard(s, functionName); ok {
			return id, false, nil
		}
		s.mu.Lock()
		if maxInstances > 0 && len(s.instances[functionName]) >= maxInstances {
			s.mu.Unlock()
			return "", false, ErrPoolExhausted
		}
		if s.pendingID[functionName] == "" {
			newInst := m.reserveSlotLockedOnShard(s, functionName)
			s.mu.Unlock()
			m.registerGlobal(newInst.id)
			return m.provisionNewSlot(ctx, s, functionName, imageRef, newInst.id)
		}
		ch, qerr := m.enqueueWaiterLockedOnShard(s, functionName, m.queueCapFor(functionName, maxInstances))
		s.mu.Unlock()
		if qerr != nil {
			return "", false, qerr
		}
		if retry, err := m.awaitWaiter(ctx, s, functionName, ch); !retry {
			return "", false, err
		}
	}
}

// tryReuseOnShard occupies a free slot when one exists and returns its
// identifier. The reservation is atomic under the shard lock, so two
// concurrent callers never take the same slot.
func (m *Manager) tryReuseOnShard(s *shard, functionName string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.instances[functionName] {
		if !s.instances[functionName][i].busy {
			s.instances[functionName][i].busy = true
			id := s.instances[functionName][i].id
			m.touchOnShard(s, functionName)
			return id, true
		}
	}
	return "", false
}

// reserveSlotLockedOnShard reserves a busy slot together with its pending
// identifier, which coalesces the waiters of a function. The caller must
// hold the shard lock, because the reservation and the capacity check must
// happen under the same lock for two concurrent callers to never exceed
// maxInstances.
func (m *Manager) reserveSlotLockedOnShard(s *shard, functionName string) instance {
	newInst := instance{id: functionName + "-" + strconv.Itoa(m.nextIndexLockedOnShard(s, functionName)), busy: true}
	if s.instances == nil {
		s.instances = make(map[string][]instance)
	}
	s.instances[functionName] = append(s.instances[functionName], newInst)
	if s.pendingID == nil {
		s.pendingID = make(map[string]string)
	}
	s.pendingID[functionName] = newInst.id
	return newInst
}

// enqueueWaiterLockedOnShard appends a waiter to the bounded per-function
// queue. The caller must hold the shard lock.
func (m *Manager) enqueueWaiterLockedOnShard(s *shard, functionName string, limit int) (chan acquireResult, error) {
	if len(s.waiters[functionName]) >= limit {
		return nil, ErrQueueFull
	}
	ch := make(chan acquireResult, 1)
	if s.waiters == nil {
		s.waiters = make(map[string][]chan acquireResult)
	}
	s.waiters[functionName] = append(s.waiters[functionName], ch)
	return ch, nil
}

// awaitWaiter waits for the signal of the in-flight provisioning on behalf of
// an enqueued waiter. It returns retry when the slot was released and the
// caller should try again, and an error when the caller should return.
//
// It takes the shard lock only in the branches that touch the queue.
func (m *Manager) awaitWaiter(ctx context.Context, s *shard, functionName string, ch chan acquireResult) (bool, error) {
	select {
	case res := <-ch:
		if res.err == nil {
			return true, nil
		}
		// Pass the signal on to the next waiter to avoid a hang.
		s.mu.Lock()
		m.wakeOneLockedOnShard(s, functionName, acquireResult{err: res.err})
		s.mu.Unlock()
		return false, res.err
	case <-ctx.Done():
		s.mu.Lock()
		m.cancelWaiterLockedOnShard(s, functionName, ch)
		s.mu.Unlock()
		return false, ctx.Err()
	}
}

// cancelWaiterLockedOnShard removes a cancelled waiter and forwards a
// buffered signal to the next one. The caller must hold the shard lock.
func (m *Manager) cancelWaiterLockedOnShard(s *shard, functionName string, ch chan acquireResult) {
	list := s.waiters[functionName]
	for i, w := range list {
		if w == ch {
			s.waiters[functionName] = append(list[:i], list[i+1:]...)
			break
		}
	}
	// A release or the end of a provisioning run can have queued a signal in
	// this channel together with the cancellation. Without forwarding it,
	// the signal would stay in the buffer of capacity one and the next
	// waiter would hang with a free slot. The forwarded value is
	// preserved: an error still fails fast through the cascade, a success
	// serves the next waiter. Only buffered channel operations and map
	// updates happen under the lock.
	select {
	case res := <-ch:
		m.wakeOneLockedOnShard(s, functionName, res)
	default:
	}
}

// registerGlobal indexes the identifier in the global presence map. It is
// unrelated to the shard lock and is always called without it.
func (m *Manager) registerGlobal(instanceID string) {
	m.globalMu.Lock()
	if m.instanceByID == nil {
		m.instanceByID = make(map[string]struct{})
	}
	m.instanceByID[instanceID] = struct{}{}
	m.globalMu.Unlock()
}

// reapGhostOnShard reverts the reservation of a slot whose provisioning
// failed: it removes the slot, clears the pending identifier and wakes one
// waiter with the original error, so the whole coalesced burst fails fast
// instead of the next caller provisioning blindly. Each waiter forwards the
// error to the next one. Only map and slice updates and the wakeup happen
// under the lock.
func (m *Manager) reapGhostOnShard(s *shard, functionName, instanceID string, cause error) error {
	provisionErr := fmt.Errorf("pool: falha ao provisionar instância %q: %w", instanceID, cause)
	s.mu.Lock()
	m.removeInstanceLockedOnShard(s, functionName, instanceID)
	delete(s.pendingID, functionName)
	m.wakeOneLockedOnShard(s, functionName, acquireResult{err: provisionErr})
	s.mu.Unlock()
	m.globalMu.Lock()
	delete(m.instanceByID, instanceID)
	m.globalMu.Unlock()
	return provisionErr
}

// completeProvisionOnShard finishes a provisioned slot: it touches the
// instance, clears the pending identifier, which ends the coalescing, and
// wakes a free waiter.
func (m *Manager) completeProvisionOnShard(s *shard, functionName string) {
	s.mu.Lock()
	m.touchOnShard(s, functionName)
	delete(s.pendingID, functionName)
	m.wakeOneLockedOnShard(s, functionName, acquireResult{})
	s.mu.Unlock()
}

// provisionNewSlot runs the slow provisioning outside any lock and settles
// the reserved slot: on success the slot is completed, on failure the ghost
// is reaped. The reservation happens under the lock in the caller, so the
// slow call never runs under it.
func (m *Manager) provisionNewSlot(ctx context.Context, s *shard, functionName, imageRef, instanceID string) (string, bool, error) {
	if err := m.provisioner.Provision(ctx, instanceID, imageRef); err != nil {
		return "", false, m.reapGhostOnShard(s, functionName, instanceID, err)
	}
	m.completeProvisionOnShard(s, functionName)
	return instanceID, true, nil
}

// Release returns an instance to the pool as free. An unknown identifier is
// ignored without panic and without changing the accounting, but it is
// logged after the lock is released, since a silent case would hide a leak
// of busy instances.
func (m *Manager) Release(functionName, instanceID string) {
	s := m.shardFor(functionName)
	s.mu.Lock()
	for i := range s.instances[functionName] {
		if s.instances[functionName][i].id == instanceID {
			s.instances[functionName][i].busy = false
			m.touchOnShard(s, functionName)
			m.wakeOneLockedOnShard(s, functionName, acquireResult{})
			s.mu.Unlock()
			m.globalMu.Lock()
			if m.instanceByID == nil {
				m.instanceByID = make(map[string]struct{})
			}
			m.instanceByID[instanceID] = struct{}{}
			m.globalMu.Unlock()
			return
		}
	}
	s.mu.Unlock()
	log.Printf("pool: Release de instância desconhecida %q para a função %q (ignorado)", instanceID, functionName)
}
