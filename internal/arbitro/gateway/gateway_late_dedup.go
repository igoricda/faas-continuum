package gateway

import (
	"sync"
)

// lateSeen keeps the pairs of the request identifier and the attempt number
// that were already processed, for the deduplication. It is bounded and
// drops the oldest entry when it is full.
type lateSeen struct {
	mu    sync.Mutex
	m     map[lateKey]struct{}
	order []lateKey
	cap   int
}

type lateKey struct {
	RequestID string
	AttemptN  int
}

func newLateSeen(cap int) *lateSeen {
	if cap <= 0 {
		cap = 10000
	}
	return &lateSeen{m: make(map[lateKey]struct{}, cap), cap: cap}
}

func (s *lateSeen) markLocked(k lateKey) {
	if _, ok := s.m[k]; ok {
		return
	}
	if len(s.m) >= s.cap {
		delete(s.m, s.order[0])
		s.order = s.order[1:]
	}
	s.m[k] = struct{}{}
	s.order = append(s.order, k)
}

// checkAndMark reserves the pair under a single lock: true means it was
// already seen, so the caller counts a dropped late report and returns, and
// false means it is reserved for this caller, which goes on to the appends
// outside the lock. The reservation makes the check and the mark atomic
// without holding the lock during input or output. A failure of the append
// to the history undoes the reservation, which allows a retry.
func (s *lateSeen) checkAndMark(requestID string, attemptN int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := lateKey{RequestID: requestID, AttemptN: attemptN}
	if _, ok := s.m[k]; ok {
		return true
	}
	s.markLocked(k)
	return false
}

// unmark releases a reservation made by the check and mark. It is used on
// the error path of the append to the history, since without it a transient
// failure would become a permanent duplicate. It is only called by the
// holder of the reservation, outside any input or output.
func (s *lateSeen) unmark(requestID string, attemptN int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := lateKey{RequestID: requestID, AttemptN: attemptN}
	if _, ok := s.m[k]; !ok {
		return
	}
	delete(s.m, k)
	for i, old := range s.order {
		if old == k {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}
