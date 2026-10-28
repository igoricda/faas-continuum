package history

import (
	"time"
)

// Stats returns the count and the percentile over the successes without a cold
// start, all time, where a non-positive window disables the temporal filter.
// The total and the success rate carry the all-time negative signal, which is
// the success, the failure and the timeout, with the exceeded budget excluded,
// and the rate is always smoothed, even without a ready percentile. When there
// is no success without a cold start the result is absent, while the total and
// the rate remain valid. There is no cache.
func (h *History) Stats(key string) (count int, p95MS int64, total int, successRate float64, ok bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	entries, exists := h.m[key]
	if !exists || len(entries) == 0 {
		return 0, 0, 0, LaplaceRate(0, 0), false
	}
	acc := scanStatsLocked(entries, 0, time.Time{}, false)
	return acc.Count, acc.P95MS, acc.Total, acc.SuccessRate, acc.OK
}

// RecentSuccesses counts the successes within the window up to the instant. A
// non-positive window gives zero, which is the defensive choice, and a future
// timestamp is ignored.
func (h *History) RecentSuccesses(key string, window time.Duration, now time.Time) int {
	if window <= 0 {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	entries, exists := h.m[key]
	if !exists {
		return 0
	}
	count := 0
	for _, e := range entries {
		if classifyOutcomeBucket(e.Outcome) != bucketSuccess {
			continue
		}
		if windowFilter(e.Timestamp, now, window) {
			count++
		}
	}
	return count
}

// StatsAndRecent combines the percentile with the window and the recent
// successes under a single read lock, which avoids a race between two separate
// calls. The percentile, the count, the total, the rate and the recent count
// share the same window, and a non-positive window reads all time with a zero
// recent count. The total and the rate are valid even without a ready
// percentile, since the veto by low rate resolves the negative amnesia with a
// zero count, and an absent key gives a zero total, a neutral rate of one half
// and an absent result.
func (h *History) StatsAndRecent(key string, window time.Duration, now time.Time) (count int, p95MS int64, recent int, total int, successRate float64, ok bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	entries, exists := h.m[key]
	if !exists || len(entries) == 0 {
		return 0, 0, 0, 0, LaplaceRate(0, 0), false
	}
	acc := scanStatsLocked(entries, window, now, false)
	return acc.Count, acc.P95MS, acc.Recent, acc.Total, acc.SuccessRate, acc.OK
}

// SegmentFullStats is the extended reading of a single segment: the mixed
// aggregate, with the same semantics as the combined query, plus the solo,
// contended and cold classes consumed by the self-learning. The exact
// definitions:
//   - solo is a warm success with exactly one request in flight, and a
//     positive average processor usage also feeds the mean and the number of
//     samples of the class;
//   - contended is a warm success with exactly two requests in flight;
//   - cold is a cold start with a success;
//   - an unknown number of requests in flight and a count of three or more
//     enter no class and remain in the mixed percentile;
//   - the saturation counts the exceeded budget, the capacity rejection and
//     the admission timeout in the window, outside the total and the success
//     rate, since the total and the rate measure service reliability and the
//     saturation measures pressure;
//   - the node saturation is the subset of the saturation with the node as the
//     author, which is the capacity rejection and the admission timeout, while
//     the budget cut is a decision of the arbiter and stays out, since it is
//     the signal of the absolution floor and using it would punish the node
//     for the decision of the arbiter.
//
// The fields are ordered from the largest to the smallest for the alignment of
// eight bytes.
type SegmentFullStats struct {
	P95MS          int64
	SoloP95MS      int64
	ContP95MS      int64
	ColdMeanMS     int64
	SoloCPUPctMean float64
	SuccessRate    float64
	Count          int
	Recent         int
	Total          int
	Saturation     int
	NodeSaturation int
	SoloN          int
	ContN          int
	ColdN          int
	SoloCPUSamples int
}

// StatsFull returns the extended statistics of a single segment in a single
// linear pass under the read lock, with the classes requested. The result is
// present when the segment has any measurement of the node, that is, a positive
// total or a positive saturation, and not under the gate of the warm
// percentile, so a segment with only cold or only contended data stays present
// for the summary to show its class fields, and a segment with only saturation
// also stays present, since it is the node refusing for lack of room and
// declaring it inexistent would hide the saturation that has to remain visible.
// The recent count shares the window of the percentile and of the count, and a
// non-positive window reads all time with a zero recent count. An absent or
// empty key gives an absent result with a neutral success rate.
func (h *History) StatsFull(key string, window time.Duration, now time.Time) (SegmentFullStats, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	entries, exists := h.m[key]
	if !exists || len(entries) == 0 {
		return SegmentFullStats{SuccessRate: LaplaceRate(0, 0)}, false
	}
	acc := scanStatsLocked(entries, window, now, true)
	return SegmentFullStats{
		P95MS:          acc.P95MS,
		SoloP95MS:      acc.SoloP95MS,
		ContP95MS:      acc.ContP95MS,
		ColdMeanMS:     acc.ColdMeanMS,
		SoloCPUPctMean: acc.SoloCPUPctMean,
		SuccessRate:    acc.SuccessRate,
		Count:          acc.Count,
		Recent:         acc.Recent,
		Total:          acc.Total,
		Saturation:     acc.Saturation,
		NodeSaturation: acc.NodeSaturation,
		SoloN:          acc.SoloN,
		ContN:          acc.ContN,
		ColdN:          acc.ColdN,
		SoloCPUSamples: acc.SoloCPUSamples,
	}, acc.Total > 0 || acc.Saturation > 0
}
