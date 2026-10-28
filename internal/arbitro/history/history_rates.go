package history

import (
	"math"
)

// p95DeOrdenados returns the percentile of sorted latencies.
func p95DeOrdenados(lats []int64) int64 {
	n := len(lats)
	idx := int(math.Ceil(0.95*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return lats[idx]
}

// LaplaceRate returns the smoothed success rate, with the Laplace
// smoothing, which anchors a cold segment at one half and prevents a
// categorical zero or one with few samples. The uniform prior keeps the veto
// from biting without accumulated evidence.
func LaplaceRate(successes, total int) float64 {
	if total < 0 {
		total = 0
	}
	if successes < 0 {
		successes = 0
	}
	if successes > total {
		successes = total
	}
	return float64(successes+1) / float64(total+2)
}

// VetoByRate decides whether the segment should be vetoed for a low success
// rate even without a ready percentile, which is the negative amnesia: it
// requires at least five samples and a rate below one half. The floor of the
// total protects cold segments from a premature veto, and the ceiling of the
// rate requires a weighted majority of failures.
func VetoByRate(total int, rate float64) bool {
	return total >= 5 && rate < 0.5
}
