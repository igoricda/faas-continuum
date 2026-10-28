// Observational counters of the risk selector. They are atomic, nil safe and
// never read by any decision, and each defense against stacking has its own
// counter for the ablation.
package selection

import "sync/atomic"

// RiskStats instruments the selection. The block is omitted from the node query
// when it is not wired, which only happens in the older tests, since the
// production arbiter always wires the statistics.
type RiskStats struct {
	// The feasible count is the number of candidates in the viable set per
	// decision, summed per decision.
	Feasible atomic.Uint64
	// The probation count is the number of candidates in probation per decision.
	Probation atomic.Uint64
	// The exploration count is the number of promotions ahead of the queue.
	ExploreFront atomic.Uint64
	// The stretch exclusions count the viable candidates cut by the bound above the target without an objective.
	StretchExcluded atomic.Uint64
	// The objective exclusions count the viable candidates cut by the bound
	// above the target with an objective, and the vetoes by success probability
	// in the same regime.
	SLOExcluded atomic.Uint64
	// The ceiling exclusions count the candidates above the static admission ceiling.
	CapExcluded atomic.Uint64
	// The projected exclusions count the candidates removed by the guard of projected overload.
	ProjectedExcluded atomic.Uint64
	// The band draws count the weighted draws in a tie band.
	BandRandom atomic.Uint64
	// The empty queue count is the number of decisions with an empty queue, which go to the cloud.
	EmptyQueue atomic.Uint64
	// The cold chosen count is the number of decisions whose first choice is a cold start.
	ColdChosen atomic.Uint64
	// The bootstrap count is the number of decisions without any candidate outside the probation.
	Bootstrap atomic.Uint64
}

// RiskStatsSnapshot is the flat JSON form of the statistics.
type RiskStatsSnapshot struct {
	Feasible          uint64 `json:"feasible"`
	Probation         uint64 `json:"probation"`
	ExploreFront      uint64 `json:"explore_front"`
	StretchExcluded   uint64 `json:"stretch_excluded"`
	SLOExcluded       uint64 `json:"slo_excluded"`
	CapExcluded       uint64 `json:"cap_excluded"`
	ProjectedExcluded uint64 `json:"projected_excluded"`
	BandRandom        uint64 `json:"band_random"`
	EmptyQueue        uint64 `json:"empty_queue"`
	ColdChosen        uint64 `json:"cold_chosen"`
	Bootstrap         uint64 `json:"bootstrap"`
}

// Snapshot returns the flat reading, and a nil receiver gives an empty one.
func (s *RiskStats) Snapshot() RiskStatsSnapshot {
	if s == nil {
		return RiskStatsSnapshot{}
	}
	return RiskStatsSnapshot{
		Feasible:          s.Feasible.Load(),
		Probation:         s.Probation.Load(),
		ExploreFront:      s.ExploreFront.Load(),
		StretchExcluded:   s.StretchExcluded.Load(),
		SLOExcluded:       s.SLOExcluded.Load(),
		CapExcluded:       s.CapExcluded.Load(),
		ProjectedExcluded: s.ProjectedExcluded.Load(),
		BandRandom:        s.BandRandom.Load(),
		EmptyQueue:        s.EmptyQueue.Load(),
		ColdChosen:        s.ColdChosen.Load(),
		Bootstrap:         s.Bootstrap.Load(),
	}
}
