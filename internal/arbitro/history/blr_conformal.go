// BLR Tier1 conformal calibration — see blr_model.go for overview.
// Pure stdlib, no locks/I-O (ADR-0007 D32).
package history

import (
	"math"
	"sort"
	"time"
)

// Tier1 calibration minimum (ADR-0007 D32): pooled one-sided normalized
// conformal correction needs at least blrCalMinSamples newer-half scores —
// below that the Beta-lottery small-n law applies (a quantile over <30
// points is a lottery ticket, not a calibration), so q-hat is unavailable
// and the pure parametric Tier0 band rules. Safety knob, pre-authorized
// escalation 30 -> 50 -> 100 (more fallback to parametric = closer to
// pre-Tier1 behavior); shipped value 30 (see calibration audit test).
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
