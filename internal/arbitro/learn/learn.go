// Package learn implements the statistical models used to score nodes:
// decayed latency regressions, success-rate counters, adaptive conformal
// calibration and the quantiles they rely on. It depends only on the
// standard library and performs no I/O.
package learn

import "time"

// Outcome classifies an observation for learning.
type Outcome int

const (
	// OutcomeIgnore updates no state. The result comes from an arbiter
	// decision or from saturation (budget exhausted, capacity rejection,
	// admission timeout), not from a measurement of the node.
	OutcomeIgnore Outcome = iota
	// OutcomeSuccess updates the warm and cold NIG regressions when
	// applicable, plus the Beta counter and the ACI.
	OutcomeSuccess
	// OutcomeFailure updates only the Beta counter: failures and timeouts
	// bias the latency regressor optimistically.
	OutcomeFailure
)

// Obs is one observation for the model of a (group, node) pair.
type Obs struct {
	At time.Time
	// Outcome decides which sub-models are updated.
	Outcome Outcome
	// Cold routes the observation to the cold-start NIG; LatencyMS already
	// includes the cold start.
	Cold bool
	// LatencyMS is the net latency in milliseconds, excluding the acquire
	// wait. With OutcomeSuccess, values <= 0, NaN or +Inf discard the whole
	// observation.
	LatencyMS float64
	// UseRegr false updates only the Beta counter, because the wait cannot
	// be separated from the service time (AcquireWait missing while Inflight
	// is unknown or high).
	UseRegr bool
	// M is the concurrency m (>=1). NaN, +Inf or <1 with UseRegr falls back
	// to Beta-only, since the reliability counter stays valid.
	M float64
}

// LiveState is the live state of the node or variant at decision time.
type LiveState struct {
	KFn, KNode, Servers, Alive, QueueTel int
	TelFresh                             bool
	// Cold marks a genuine cold start. The caller decides it from the
	// monitor's mark/ok state, which the learner does not see, so it must be
	// passed in.
	Cold bool
}

// Prediction is the forecast for a new request entering now.
type Prediction struct {
	// UpperMS is B (warm) = W + S_up, the calibrated upper bound of the
	// response time (service plus wait). For a cold start it is S_up of the
	// cold model (W = 0).
	UpperMS float64
	// MedianMS is the median service time under the current contention
	// (S_med).
	MedianMS float64
	// WaitMS is the fluid queueing wait W (0 for a cold start).
	WaitMS float64
	// PSucc is P_succ from the Beta counter (discounted Laplace).
	PSucc float64
	// Neff is n_eff of the active NIG, already decayed; NeffBeta = S + F.
	Neff, NeffBeta  float64
	Cold, Probation bool
}

// Prior carries profiling values for the NIG prior, keyed by group and then
// by node or layer: {group: {node_or_layer: {service_ms, sigma, cold_ms}}}.
// Zero values mean no prior: a weak intercept, and the node stays in
// probation until n_eff >= nmin.
type Prior struct {
	ServiceMS float64
	Sigma     float64
	ColdMS    float64
}

// PriorFunc resolves the prior for a (group, node) pair. A nil func always
// yields the weak prior.
type PriorFunc func(group, node string) (Prior, bool)

// Config parametrizes a Learner.
type Config struct {
	// HalfLife is the half-life of the temporal decay; <= 0 disables decay
	// (useful in tests, equivalent to an infinite half-life).
	HalfLife time.Duration
	// Risk is the target miscoverage of the ACI.
	Risk float64
	// ACIGamma is the ACI step size.
	ACIGamma float64
	// NMin is nmin; below it the model is in probation.
	NMin float64
	// Prior resolves profiling priors; nil means the weak prior.
	Prior PriorFunc
}

// withDefaults clamps the configuration to its valid domains. Fail-fast
// validation belongs to the caller; this package never aborts.
func (c Config) withDefaults() Config {
	if c.Risk <= 0 || c.Risk > 0.5 {
		c.Risk = 0.05
	}
	if c.ACIGamma <= 0 || c.ACIGamma > 0.1 {
		c.ACIGamma = 0.01
	}
	if c.NMin < 2 {
		c.NMin = 3
	}
	return c
}
