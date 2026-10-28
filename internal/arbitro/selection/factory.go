// Factory of the canonical selector, which is the single point of construction
// of the risk selector in production and in the simulation bench.
//
// Motivation: with two selectors the wiring was duplicated in the call sites,
// with the flags, the priors, the learner, the replay, the hook, the meter and
// the statistics, and every change spread through the main command, the
// simulation and the server. The factory collapses the construction into a
// single place, and a future algorithm enters as a new builder over the ports,
// without touching the call sites, with no container, no reflection and no
// cost in the hot path, since it only runs at boot.
package selection

import (
	"math/rand"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
)

// RiskDeps aggregates the dependencies of the risk selector in a typed object,
// the same pattern as the configuration of the gateway, so that the reader of
// the call site sees the name of each dependency. The interfaces come from the
// ports of the package, and a nil value disables the dependency, the same
// semantics as the previous fields.
type RiskDeps struct {
	Mon              TelemetrySource
	Hist             HistorySource
	Inflight         InflightReader
	Layers           NodeLayerReader
	Clock            func() time.Time
	Rand             *rand.Rand
	Criteria         []SLOCriterion
	Limits           LimitsConfig
	ThroughputWindow time.Duration
	PSIVeto          PSIVetoThresholds
}

// BuildQueueSelector builds the baseline of the hard limits. In production it
// is only used as the base of the risk selector, and in the bench it is the
// hard-limit mode of the experiment.
func BuildQueueSelector(mon TelemetrySource, limits LimitsConfig) QueueSelector {
	return QueueSelector{Mon: mon, Cfg: limits}
}

// BuildRiskSelector validates the configuration, with a fail-fast on invalid
// values, loads the priors, creates the learner, rebuilds it from the history,
// wires the live observation hook and returns the selector, the statistics and
// the learner. The learner is returned for the observational dump of the node
// query, and the caller closes the clock with its own instant, as before.
func BuildRiskSelector(cfg RiskConfig, priorsPath string, d RiskDeps) (*RiskSelector, *RiskStats, *learn.Learner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, nil, nil, err
	}
	priors, err := LoadRiskPriors(priorsPath)
	if err != nil {
		return nil, nil, nil, err
	}
	layerOf := func(id string) string {
		if d.Layers == nil {
			return ""
		}
		if n, ok := d.Layers.Get(id); ok {
			return n.Layer
		}
		return ""
	}
	learner := learn.NewLearner(learn.Config{
		HalfLife: cfg.HalfLife, Risk: cfg.RiskEpsilon, ACIGamma: cfg.ACIGamma,
		NMin: cfg.NMin, Prior: priors.PriorFunc(layerOf),
	})
	ReplayRiskHistory(d.Hist, learner)
	AttachRiskObserver(d.Hist, learner)
	stats := &RiskStats{}
	sel := &RiskSelector{
		Base: BuildQueueSelector(d.Mon, d.Limits), Hist: d.Hist, Clock: d.Clock,
		Criteria: d.Criteria, ThroughputWindow: d.ThroughputWindow,
		PSIVetoThresholds: d.PSIVeto,
		Learner:           learner, Cfg: cfg,
		Rand: d.Rand, Stats: stats,
	}
	if d.Inflight != nil {
		sel.InflightOf = d.Inflight.NodeK
		sel.InflightOfFn = d.Inflight.FnK
		sel.ProjectedCostSince = d.Inflight.NodeCostSince
	}
	return sel, stats, learner, nil
}
