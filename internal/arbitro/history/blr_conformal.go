// Conformal calibration of the Bayesian linear regression, which corrects the
// parametric band. See blr_model.go for the overview. It uses only the
// standard library, without locks or input and output.
package history

import (
	"math"
	"sort"
	"time"
)

// Minimum number of samples for the conformal calibration: the pooled
// one-sided normalized correction needs at least this many scores from the
// newer half, since below that a quantile over so few points is not a
// calibration, so the correction is unavailable and the purely parametric
// band applies. The value is a safety knob that may only escalate, from 30 to
// 50 to 100, which increases the fallback to the parametric band.
const blrCalMinSamples = 30

// blrConformalQ is the split/inductive one-sided normalized conformal
// quantile (Lei 2018): q-hat = ceil((m+1)*0.9)/m-th smallest of the m
// SIGNED scores s = (lat-predict)/scale (NOT absolute residual — absolute
// would calibrate a two-sided 90% interval, whose upper edge is a 95%
// upper bound, over-wide by construction for a one-sided 90% band).
// ok=false when m < minM (never calibrate below the minimum) or m == 0.
// Pure, deterministic (sorts a copy — caller's slice untouched), O(m log m).
func blrConformalQ(scores []float64, minM int) (float64, bool) {
	m := len(scores)
	if m < minM || m == 0 {
		return 0, false
	}
	cp := make([]float64, m)
	copy(cp, scores)
	// sort.Float64s is stdlib and deterministic for a fixed multiset: the
	// k-th smallest is order-independent, so map-scan order never leaks
	// into q-hat (unlike float summation order in the fit).
	sort.Float64s(cp)
	// k = ceil(0.9*(m+1)) in integer arithmetic: ceil(9(m+1)/10).
	k := (9*(m+1) + 9) / 10
	if k < 1 {
		k = 1
	}
	if k > m {
		return 0, false
	}
	q := cp[k-1]
	if math.IsNaN(q) || math.IsInf(q, 0) {
		return 0, false
	}
	return q, true
}

// blrUsageCalQ computes the pooled Tier1 one-sided normalized conformal
// correction for one (group,layer) population: time-split the windowed
// entries at mid-window, fit a temp curve on the OLDER half through the
// existing pure seams (FitUsageCurveFromEntries for the agg prior part +
// PosteriorUsageCurve for the node-local part — the same fit the miss
// path serves), then score the NEWER-half warm-solo points with
// s = (lat - tempPredict)/tempSigma and take q-hat = blrConformalQ.
//
// Pooled per GROUP across nodes/buckets (never per-segment: calibrating
// on n<30 is lottery, not statistics); the node's own newer agg copies
// are skipped (they ride again as local primaries — no double count).
// nodeID is that local node. ok=false when the temp fit fails, the newer
// half is empty, or m < minM. Pure, deterministic, off the hot path (the
// UsageCurveFor miss path calls it once per miss; hot hits never pay).
func blrUsageCalQ(priorEntries, localEntries []Entry, nodeID string, window time.Duration, now time.Time, minM int) (float64, bool) {
	if window <= 0 || len(priorEntries)+len(localEntries) == 0 {
		return 0, false
	}
	mid := now.Add(-window / 2)
	var oldPrior, newPrior []Entry
	var oldLocal []UsagePoint
	var newLocal []Entry
	for _, e := range priorEntries {
		if e.Timestamp.After(mid) {
			newPrior = append(newPrior, e)
		} else {
			oldPrior = append(oldPrior, e)
		}
	}
	for _, e := range localEntries {
		if e.Timestamp.After(mid) {
			newLocal = append(newLocal, e)
		} else {
			oldLocal = append(oldLocal, usagePointOfEntry(e))
		}
	}
	tempPrior, tempPriorOK := FitUsageCurveFromEntries(oldPrior)
	tempCurve, ok := PosteriorUsageCurve(tempPrior, tempPriorOK, oldLocal)
	if !ok {
		return 0, false
	}
	var scores []float64
	addScores := func(entries []Entry, skipLocalNode bool) {
		for _, e := range entries {
			if skipLocalNode && e.NodeID == nodeID {
				continue
			}
			if !usageSoloEligible(e) {
				continue
			}
			x, y := usageOf(e)
			sigma, ok := blrScaleOf(tempCurve, x, y)
			if !ok {
				continue
			}
			scores = append(scores, (float64(e.ObservedLatencyMS)-tempCurve.Predict(x, y))/sigma)
		}
	}
	addScores(newPrior, true)
	addScores(newLocal, false)
	return blrConformalQ(scores, minM)
}
