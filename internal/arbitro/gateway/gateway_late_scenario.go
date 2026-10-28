package gateway

import (
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// lateScenario builds the real scenario of the late report: per dimension
// the percentage of the report wins when it is present, otherwise the
// current telemetry of the node is used, and without data the value is
// unknown. The two dimensions collapse through the maximum bucket, so a
// single known dimension stands alone and none gives unknown. A negative or
// non-numeric load maps to unknown without extra validation.
func (g *Gateway) lateScenario(rep LateReport) history.ScenarioContext {
	var cpuPct, memPct float64
	var cpuOK, memOK bool
	if rep.CPUUsagePct != nil {
		cpuPct, cpuOK = *rep.CPUUsagePct, true
	}
	if rep.MemoryUsagePct != nil {
		memPct, memOK = *rep.MemoryUsagePct, true
	}
	if !cpuOK || !memOK {
		if g.mon != nil {
			if pkg, ok := g.mon.Latest(rep.NodeID); ok {
				if !cpuOK {
					cpuPct = pkg.CPUUsagePct
					cpuOK = true
				}
				if !memOK {
					memPct = pkg.MemoryUsagePct
					memOK = true
				}
			}
		}
	}
	cpuBucket, memBucket := history.BucketUnknown, history.BucketUnknown
	if cpuOK {
		cpuBucket = history.LoadBucket(cpuPct)
	}
	if memOK {
		memBucket = history.LoadBucket(memPct)
	}
	return history.ScenarioContext{LoadBucket: history.MaxBucket(cpuBucket, memBucket)}
}

// latePSI derives the pressure signals of an orphan late report, mirroring
// the scenario: per axis the average of the body of the report wins when it
// is positive, otherwise the current telemetry of the node through the same
// positive gate is used, and without data the axis stays unknown. Every
// branch fails open: an absent monitor, a miss, an absent pressure block or
// a non-positive average gives zero on that axis and never panics.
func (g *Gateway) latePSI(rep LateReport) (cpuSomeAvg10, memFullAvg10 float64) {
	if rep.PSICpuSomeAvg10 > 0 {
		cpuSomeAvg10 = rep.PSICpuSomeAvg10
	}
	if rep.PSIMemFullAvg10 > 0 {
		memFullAvg10 = rep.PSIMemFullAvg10
	}
	if cpuSomeAvg10 > 0 && memFullAvg10 > 0 {
		return cpuSomeAvg10, memFullAvg10
	}
	if g.mon != nil {
		if pkg, ok := g.mon.Latest(rep.NodeID); ok {
			if cpuSomeAvg10 <= 0 && pkg.CPUPressure != nil && pkg.CPUPressure.Avg10 > 0 {
				cpuSomeAvg10 = pkg.CPUPressure.Avg10
			}
			if memFullAvg10 <= 0 && pkg.MemoryPressureFull != nil && pkg.MemoryPressureFull.Avg10 > 0 {
				memFullAvg10 = pkg.MemoryPressureFull.Avg10
			}
		}
	}
	return cpuSomeAvg10, memFullAvg10
}

// lateObserved derives the continuous processor and memory usage of an
// orphan late report, mirroring the pressure signals and the scenario: per
// axis the percentage of the body wins when it is present, with the absent
// pointer telling an absent value from a zero, otherwise the current
// telemetry of the node is used, and without data the value is unknown.
// Every branch fails open. With a live reference the value inherited from
// the synchronous record wins and the body is ignored, which is the same
// discipline as the pressure signals.
func (g *Gateway) lateObserved(rep LateReport) (cpuPct, memPct float64) {
	var cpuOK, memOK bool
	if rep.CPUUsagePct != nil {
		cpuPct, cpuOK = *rep.CPUUsagePct, true
	}
	if rep.MemoryUsagePct != nil {
		memPct, memOK = *rep.MemoryUsagePct, true
	}
	if !cpuOK || !memOK {
		if g.mon != nil {
			if pkg, ok := g.mon.Latest(rep.NodeID); ok {
				if !cpuOK {
					cpuPct = pkg.CPUUsagePct
				}
				if !memOK {
					memPct = pkg.MemoryUsagePct
				}
			}
		}
	}
	return cpuPct, memPct
}

// classifyLateSloMet derives the objective of the late report with the same
// rule as the classification of the attempt over the real latency: a
// non-success is unmet, and a success is the conjunction of the criteria
// declared in the objective of the invocation. Without a stored objective
// no criterion is declared and a success is met, which is consistent with
// the live attempt.
func (g *Gateway) classifyLateSloMet(rep LateReport, slo *selection.SLO) bool {
	if rep.Outcome != history.OutcomeSucesso {
		return false
	}
	latency := time.Duration(rep.ObservedLatencyMS) * time.Millisecond
	for _, c := range g.criteria {
		if c == nil || !c.Declared(slo) {
			continue
		}
		if !c.Attempt(slo, latency, rep.Outcome) {
			return false
		}
	}
	return true
}
