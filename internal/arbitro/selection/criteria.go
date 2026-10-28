// Extensible criteria of the objective, which implement the segment
// statistics and the criteria of latency and throughput and are used by both
// selectors. The default set is defined below.
package selection

import (
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// SegmentStats aggregates the statistics of a segment for the evaluation of
// the objective. The count and the percentile cover the successes without a
// cold start within the window, and the recent rate counts the successes in
// the same window, including a cold start. The total and the success rate
// carry the negative signal of the window, which is the success, the failure
// and the timeout, with the exceeded budget excluded, and the rate is always
// smoothed, which makes it valid even with a zero count for the veto.
//
// The solo, contended and cold classes widen the aggregate: solo is a warm
// success with exactly one request in flight, contended is exactly two, and
// cold is a cold start with a success. The learned factor of contention
// consumes the percentiles and the counts of the solo and contended classes,
// and an unknown number of requests in flight or one of three or more stays
// out of every class.
//
// The fields are ordered from the largest to the smallest for the alignment
// of eight bytes.
type SegmentStats struct {
	P95LatencyMS int64
	SoloP95MS    int64
	ContP95MS    int64
	ColdMeanMS   int64
	RecentRPS    float64
	SuccessRate  float64
	Count        int
	Total        int
	// The saturation counts the exceeded budget, the capacity rejection and
	// the admission timeout in the window, and it does not enter the total
	// or the success rate, since the total measures service reliability and
	// the saturation measures pressure. The selector only consumes it for
	// the absolution floor, and by subset, through the node saturation.
	Saturation int
	// The node saturation is the subset of the saturation with the node as
	// the author, which is the capacity rejection and the admission timeout.
	// It is the signal that the absolution floor consumes: a segment with a
	// lot of node saturation and no service sample is a node that is doing
	// nothing, and the veto by rate does not reach this case, since the floor
	// of the total is never reached. The exceeded budget does not enter, since
	// it is the arbiter giving up the attempt and not the node failing to
	// deliver, and using it would demote a healthy node for the decision of
	// the arbiter itself.
	NodeSaturation int
	SoloN          int
	ContN          int
	ColdN          int
}

// MinClassSamples is the minimum number of samples per class, solo and
// contended, for the learned factor of contention to apply: with any fine
// class below the minimum the residual stays inert, as the bootstrap, and
// the selection keeps the mixed percentile of the segment. It is exported
// because the summary of the history in the server consumes the same
// predicate through the effective factor, with never a hardcoded five there.
// It is also the floor of evidence of the learned queue ceiling.
const MinClassSamples = 5

// EffectiveContentionFactor computes the learned factor of contention of a
// single saturated segment, as the ratio of the contended percentile to the
// solo percentile clamped between one and eight. It is learned only and
// never receives a configured value, since the value is always derived from
// the measured classes and only the learned factor is reported, omitted when
// it is absent.
//
// The predicate is the same for the selector and for the summary: the result
// is absent when either class has fewer samples than the minimum or when
// either percentile is non-positive, and otherwise it is the clamped ratio
// and is present. The measured provenance, of about two point three times on
// the reference device, is never hardcoded, and it is only a calibration
// suggested to the operator. It is pure.
func EffectiveContentionFactor(soloP95MS, contP95MS int64, soloN, contN, minClassSamples int) (factor float64, ok bool) {
	if soloN < minClassSamples || contN < minClassSamples || soloP95MS <= 0 || contP95MS <= 0 {
		return 0, false
	}
	f := float64(contP95MS) / float64(soloP95MS)
	if f < 1 {
		f = 1
	}
	if f > 8 {
		f = 8
	}
	return f, true
}

// SLOCriterion is the extensible interface of the criteria of the objective.
//
// The five methods deviate from the interface segregation principle but form
// a cohesive unit of evaluation of an objective. Splitting them into smaller
// interfaces would fragment the contract and force the callers to depend on
// several correlated interfaces for the same life cycle, from the declaration
// to the attempt, the segment, the hard limit and the fit. The cohesion wins
// over the theoretical granularity, and a new criterion is a new
// implementation without touching the callers.
type SLOCriterion interface {
	Declared(slo *SLO) bool
	Attempt(slo *SLO, latency time.Duration, outcome string) bool
	Segment(slo *SLO, s SegmentStats) (apt bool, ok bool)
	HardLimit(n nodes.Node, slo *SLO, cfg LimitsConfig) bool
	Fit(s SegmentStats) (score float64, ok bool)
}

// LatencyCriterion evaluates an objective of latency.
type LatencyCriterion struct{}

// Declared reports whether the objective declares a positive maximum latency.
func (LatencyCriterion) Declared(slo *SLO) bool {
	if slo == nil {
		return false
	}
	return slo.MaxLatencyMS > 0
}

// Attempt evaluates an attempt: a success within the maximum latency. A nil
// objective, or an outcome other than a success, fails the criterion.
func (LatencyCriterion) Attempt(slo *SLO, latency time.Duration, outcome string) bool {
	if slo == nil {
		return false
	}
	if outcome != history.OutcomeSucesso {
		return false
	}
	return latency <= time.Duration(slo.MaxLatencyMS)*time.Millisecond
}

// Segment evaluates a segment as suitable when it has samples and its
// percentile is within the maximum latency. The count covers the successes
// without a cold start, so the minimum number of samples requires warm
// successes and never raw attempts, and a segment without a success is not
// ready and falls to the hard limit. A zero count fails without a panic, and
// a nil objective is not suitable.
func (LatencyCriterion) Segment(slo *SLO, s SegmentStats) (apt bool, ok bool) {
	if slo == nil {
		return false, false
	}
	if s.Count == 0 {
		return false, false
	}
	return s.P95LatencyMS <= int64(slo.MaxLatencyMS), true
}

// HardLimit is the bootstrap of the criterion, for an immature segment. There
// is no ceiling by layer, so it is always suitable. The limit of latency is
// the timeout of the function in the node plus the budget of the gateway.
func (LatencyCriterion) HardLimit(n nodes.Node, slo *SLO, cfg LimitsConfig) bool {
	return true
}

// Fit returns the percentile as the score, where a lower value is better,
// and reports the presence with a positive count. The percentile comes from
// the successes without a cold start, so the ordering is by the cost of a
// success.
func (LatencyCriterion) Fit(s SegmentStats) (score float64, ok bool) {
	if s.Count == 0 {
		return 0, false
	}
	return float64(s.P95LatencyMS), true
}

// ThroughputCriterion evaluates an objective of throughput. The minimum rate
// must be positive wherever it is instantiated in production, and the
// default criteria only use a window as a default for the tests.
//
// The throughput only takes part in a decision when a positive minimum rate
// is declared in the objective of that decision, with an explicit short
// circuit: the segment returns that it is not applicable without a declared
// rate, and the fit does not receive the objective, which keeps the interface
// intact, so its decoupling lives in the guard of the ordering by fit. An
// objective with latency only has zero influence of the rate on the
// suitability and on the ordering.
//
// The freshness of the latency is governed by the pressure regime and by a
// window pinned per query, since every reading point receives the window and
// the instant explicitly per call, without an implicit global default. The
// global default from the flags and the configuration is untouched.
type ThroughputCriterion struct{}

// Declared reports whether the objective declares a positive minimum rate.
func (ThroughputCriterion) Declared(slo *SLO) bool {
	if slo == nil {
		return false
	}
	return slo.MinThroughputRPS > 0
}

// Attempt is vacuously true on a success, since the latency is ignored and
// the throughput is aggregated. It does not panic when called with a nil
// objective, which the declaration already covers in the normal use.
func (ThroughputCriterion) Attempt(slo *SLO, _ time.Duration, outcome string) bool {
	return outcome == history.OutcomeSucesso
}

// Segment evaluates the suitability by throughput: it needs samples and a
// recent rate at least the minimum. A zero count fails without a panic, and a
// nil objective is not suitable.
//
// Without a declared minimum rate the method returns that it is not
// applicable instead of evaluating the rate, so that no caller, guarded or
// direct, suffers the influence of the rate in an objective with latency
// only. The consumers already filter by the declaration, and this internal
// guard makes the decoupling proof against a direct caller without changing
// any declared path, which follows the same flow as before.
func (ThroughputCriterion) Segment(slo *SLO, s SegmentStats) (apt bool, ok bool) {
	if slo == nil || slo.MinThroughputRPS <= 0 {
		return false, false
	}
	if s.Count == 0 {
		return false, false
	}
	return s.RecentRPS >= slo.MinThroughputRPS, true
}

// HardLimit is always true, since the throughput imposes no hard limit by
// ceiling of layer, and no static throughput ceiling per layer exists.
func (ThroughputCriterion) HardLimit(_ nodes.Node, _ *SLO, _ LimitsConfig) bool {
	return true
}

// Fit returns the negated recent rate as the score, so that a higher rate
// gives a lower score and therefore a better position in the ascending
// ordering, and reports the presence with a positive count.
//
// The fit does not receive the objective, which keeps the five methods of the
// interface intact, and the exclusion of the throughput in an objective with
// latency only lives entirely in the guard of the ordering by fit: without a
// declared positive minimum rate the fit is never consulted and the rate has
// zero influence on the ordering.
func (ThroughputCriterion) Fit(s SegmentStats) (score float64, ok bool) {
	if s.Count == 0 {
		return 0, false
	}
	return -s.RecentRPS, true
}

// DefaultCriteria is the default set used by the tests and by the production
// wiring, which injects the pair of latency and throughput. The global
// window of throughput is untouched, since this set does not carry a window,
// because the criterion has no window field and the window travels per call
// in the expected cost and in the queue curve. Including the throughput
// criterion here does not couple the rate to an objective with latency only,
// since every decision point filters by the declaration.
var DefaultCriteria = []SLOCriterion{
	LatencyCriterion{},
	ThroughputCriterion{},
}
