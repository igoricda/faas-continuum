package cloud

import (
	"sync"
	"time"
)

// coldState holds the live history of one function for cold-start estimation:
// the end of the last 2xx response and the number of in-flight calls.
type coldState struct {
	lastEnd  time.Time
	inflight int
}

// coldTracker estimates cold starts per function. now is injectable for
// deterministic tests and defaults to time.Now. The map is pruned to the
// current configuration on every use, so it never grows beyond the configured
// functions.
type coldTracker struct {
	mu    sync.Mutex
	now   func() time.Time
	state map[string]*coldState
}

func newColdTracker() *coldTracker {
	return &coldTracker{now: time.Now, state: map[string]*coldState{}}
}

// pruneLocked drops functions that are absent from the current configuration.
// The caller must hold the lock.
func (t *coldTracker) pruneLocked(keep map[string]FunctionEndpoint) {
	for fn := range t.state {
		if _, ok := keep[fn]; !ok {
			delete(t.state, fn)
		}
	}
}

// estimate is a pure observation with no reservation: it returns true when the
// idleness since the last 2xx response exceeds the connector reaper (the
// container was torn down) or when in-flight calls reached max_instances (no
// warm slot is free). A function never seen, or missing criteria, yields false.
func (t *coldTracker) estimate(fn string, maxInst int, reaper time.Duration, keep map[string]FunctionEndpoint) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneLocked(keep)
	return t.estimateLocked(fn, maxInst, reaper)
}

// estimateLocked applies the rule. The caller must hold the lock.
func (t *coldTracker) estimateLocked(fn string, maxInst int, reaper time.Duration) bool {
	st := t.state[fn]
	if st == nil {
		return false
	}
	if reaper > 0 && !st.lastEnd.IsZero() && t.now().Sub(st.lastEnd) > reaper {
		return true
	}
	return maxInst > 0 && st.inflight >= maxInst
}

// reserve prunes to the current configuration and counts this call as
// in-flight atomically with the cap check (check-and-act under a single lock):
// with max_instances > 0 and in-flight calls at the cap, it rejects the call
// without reserving, because the cloud would queue it instead. It returns
// whether the call was admitted and the observed in-flight count. The critical
// section is minimal and performs no I/O.
func (t *coldTracker) reserve(fn string, maxInst int, keep map[string]FunctionEndpoint) (bool, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneLocked(keep)
	st := t.state[fn]
	if st == nil {
		st = &coldState{}
		t.state[fn] = st
	}
	if maxInst > 0 && st.inflight >= maxInst {
		return false, st.inflight
	}
	st.inflight++
	return true, st.inflight
}

// release decrements the in-flight count; alive=true (a 2xx response was
// received) renews the idleness mark. Transport errors and non-2xx responses do
// not renew it: without proof of a live container the next estimate leans cold.
func (t *coldTracker) release(fn string, alive bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st := t.state[fn]; st != nil {
		if st.inflight > 0 {
			st.inflight--
		}
		if alive {
			st.lastEnd = t.now()
		}
	}
}

// parseReaper converts the connector reaper; invalid or absent yields 0, which
// disables the idle rule. Set and load already reject invalid durations, so
// this is defence in depth and never an error.
func parseReaper(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}
