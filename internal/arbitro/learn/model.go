// Model is the learned state of one (group, node) pair: a warm and a cold
// latency regressor, a reliability counter and a conformal calibrator.
//
// The calibrator is updated before the regressor on every warm success, so
// the bound is computed with the current level and only then the
// observation enters the regressor.
package learn

import (
	"math"
	"sync"
	"time"
)

// maxPredictMS caps predictions in milliseconds (30 days). The conformal
// guarantee is about coverage, not magnitude: the clamp only removes
// values that are physically impossible and keeps +Inf from reaching the
// integer conversion of the gateway.
const maxPredictMS = 30 * 24 * 3600 * 1000.0

func clampMS(v float64) float64 {
	if math.IsNaN(v) {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v > maxPredictMS || math.IsInf(v, 0) {
		return maxPredictMS
	}
	return v
}

// Model is the learned state of one (group, node) pair. It has its own
// mutex and performs no I/O while holding it. The zero value is not usable:
// use NewModel.
type Model struct {
	mu   sync.Mutex
	warm *NIG
	cold *NIG
	beta Beta
	aci  ACI
}

// NewModel returns a model with the given priors and an initial calibration
// level equal to the configured risk.
func NewModel(warmPrior, coldPrior NIGPrior, risk float64) *Model {
	m := &Model{warm: NewNIG(warmPrior), cold: NewNIG(coldPrior)}
	m.aci = ACI{alpha: risk, set: risk > 0}
	return m
}

// Observe folds an observation into the model.
func (m *Model) Observe(o Obs, cfg Config) {
	cfg = cfg.withDefaults()
	m.mu.Lock()
	defer m.mu.Unlock()
	switch o.Outcome {
	case OutcomeIgnore:
		return
	case OutcomeFailure:
		m.beta.Observe(false, o.At, cfg.HalfLife)
		return
	case OutcomeSuccess:
		// A success with an invalid latency discards the whole observation.
		if math.IsNaN(o.LatencyMS) || math.IsInf(o.LatencyMS, 0) || o.LatencyMS <= 0 {
			return
		}
		if o.Cold {
			m.beta.Observe(true, o.At, cfg.HalfLife)
			m.cold.Observe([2]float64{1, 0}, math.Log(o.LatencyMS), o.At, cfg.HalfLife)
			return
		}
		if !o.UseRegr {
			m.beta.Observe(true, o.At, cfg.HalfLife)
			return
		}
		if math.IsNaN(o.M) || math.IsInf(o.M, 0) || o.M < 1 {
			// An invalid concurrency leaves the regressor untouched while the
			// reliability counter stays valid.
			m.beta.Observe(true, o.At, cfg.HalfLife)
			return
		}
		x := [2]float64{1, math.Log(o.M)}
		y := math.Log(o.LatencyMS)
		// The calibrator runs first, so the bound uses the current level; the
		// regressor is updated afterwards.
		_, q := m.warm.Quantile(x, 1-m.aci.Alpha(cfg.Risk), o.At, cfg.HalfLife)
		m.aci.Update(o.LatencyMS > math.Exp(q), cfg.Risk, cfg.ACIGamma)
		m.beta.Observe(true, o.At, cfg.HalfLife)
		m.warm.Observe(x, y, o.At, cfg.HalfLife)
	}
}

// Predict forecasts the calibrated upper bound B for a request that arrives
// now, given the live state: the concurrency seen by the node, the median
// service time, the fluid queueing wait W and the upper quantile S_up, with
// B = W + S_up.
//
// A cold start uses the cold regressor with no wait.
func (m *Model) Predict(now time.Time, s LiveState, cfg Config) Prediction {
	cfg = cfg.withDefaults()
	m.mu.Lock()
	defer m.mu.Unlock()
	c := s.Servers
	if c < 1 {
		c = 1
	}
	kfn := s.KFn
	if kfn < 0 {
		kfn = 0
	}
	knode := s.KNode
	if knode < 0 {
		knode = 0
	}
	psucc, neffBeta := m.beta.PSucc()
	alpha := m.aci.Alpha(cfg.Risk)
	if s.Cold {
		_, q := m.cold.Quantile([2]float64{1, 0}, 1-alpha, now, cfg.HalfLife)
		loc := m.cold.Median([2]float64{1, 0}, now, cfg.HalfLife)
		neff := m.cold.Neff()
		return Prediction{
			UpperMS: clampMS(math.Exp(q)), MedianMS: clampMS(math.Exp(loc)),
			WaitMS: 0, PSucc: psucc, Neff: neff, NeffBeta: neffBeta,
			Cold: true, Probation: neff < cfg.NMin,
		}
	}
	mnow := knode + 1
	if mnow < 1 {
		mnow = 1
	}
	x := [2]float64{1, math.Log(float64(mnow))}
	loc, q := m.warm.Quantile(x, 1-alpha, now, cfg.HalfLife)
	smed := math.Exp(loc)
	sup := math.Exp(q)
	w := 0.0
	if over := kfn + 1 - c; over > 0 {
		w = float64(over) * smed / float64(c)
	}
	neff := m.warm.Neff()
	return Prediction{
		UpperMS: clampMS(w + sup), MedianMS: clampMS(smed),
		WaitMS: clampMS(w), PSucc: psucc, Neff: neff, NeffBeta: neffBeta,
		Cold: false, Probation: neff < cfg.NMin,
	}
}

// Alpha returns the current calibration level.
func (m *Model) Alpha(risk float64) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.aci.Alpha(risk)
}
