package selection

import (
	"log"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// predictedCold decides whether a candidate is going to pay a cold start.
// That happens only when the fresh occupancy of the concrete variant
// indicates zero live instances, where a non-positive number of available
// instances is the total alive including the busy ones, and there is no mark
// of optimistic exhaustion. An absent reading, or an expired time to live,
// gives false, as does the exhaustion mark, so the absence of data never
// invents a cold start, and the mark means warm but at the ceiling and not
// without a live instance. It is pure.
//
// It is shared by the two consumers of the capacity stage: the live state of
// the risk selector and the ordering of the queue selector.
func predictedCold(fc telemetry.FunctionCapacity, exhaustedMark, ok bool) bool {
	if !ok || exhaustedMark {
		return false
	}
	return fc.InstancesAvailable <= 0
}

// projectedLoad computes the projected load of a candidate, with the same
// formula as the record in the gateway and the queries, so that the training
// and the query agree: the observed load, plus the cost in flight since the
// reception, plus the own cost. It is a shared helper of the deterministic
// shield of the canonical selector. Every reader is nil safe, so an absent
// monitor, history or meter gives a projected load of zero and therefore an
// unknown bucket.
func projectedLoad(mon TelemetrySource, hist HistorySource, projCost func(nodeID string, since time.Time) float64, group, nodeID string, window time.Duration, now time.Time) (pkg telemetry.Package, recvAt time.Time, monOK bool, projected float64, sc history.ScenarioContext) {
	if mon != nil {
		pkg, recvAt, monOK = mon.LatestAt(nodeID)
	}
	observed := 0.0
	if monOK {
		observed = pkg.CPUUsagePct
	}
	costSum := 0.0
	if projCost != nil {
		costSum = projCost(nodeID, recvAt)
	}
	// A nil history is never called, since the guard of a nil receiver of the
	// expected cost lives here, the same pattern as the other readers of
	// this helper.
	selfCost := 0.0
	if hist != nil {
		selfCost, _, _, _ = hist.ExpectedCost(group, nodeID, window, now)
	}
	projected = observed + costSum + selfCost
	if projected < 0 {
		projected = 0
	}
	sc = history.ScenarioFromProjected(pkg, monOK, projected)
	return pkg, recvAt, monOK, projected, sc
}

// capacityRead consolidates a single fresh reading of the capacity, with the
// flag of exhaustion and the presence flag, for reuse by the stages of the
// mark and of the cold start and the queue, so that both readings per
// surviving node happen in a single pass.
type capacityRead struct {
	fc   telemetry.FunctionCapacity
	mark bool
	ok   bool
}

// The capacity stage lives in another file of the package: it walks every
// variant of the category per node and only drops the candidate when all of
// them carry a fresh optimistic mark of being warm at the ceiling, so a
// genuine cold start is never dropped. The hard limit uses the same
// predicate, so the previous divergence between the two no longer exists.
// It returns the reading keyed by node identifier, for reuse in the live
// state of the risk selector and in the demotion by queue of the queue
// selector.

// filterProjectedOverloaded applies the hard guard of projected overload: it
// drops a candidate whose projected processor usage is strictly above the
// processor ceiling of the node itself, which falls back to the global
// ceiling without an override, and it reuses the flag of the maximum
// processor usage. An equal value does not drop the candidate, which matches
// the overload predicate. There is no fail-open: when every candidate is
// dropped the result is empty and the gateway answers that no node is
// available, and the cloud is not reached from an empty queue. A non-numeric
// projected load does not drop the candidate, since the comparison is false
// and it is not a fail-open of the set, and an absent projection keeps the
// candidate, which can never exceed the ceiling. There is one log line per
// exclusion.
func filterProjectedOverloaded(candidates []nodes.Node, projected map[string]float64, cfg LimitsConfig) []nodes.Node {
	filtered := make([]nodes.Node, 0, len(candidates))
	for _, n := range candidates {
		p, ok := projected[n.NodeID]
		if !ok {
			filtered = append(filtered, n)
			continue
		}
		threshold := cfg.For(n.NodeID).MaxCPUPct
		if p > threshold {
			log.Printf("arbitro: selection node=%s result=PROJECTED_OVERLOAD projected=%.2f threshold=%.2f", n.NodeID, p, threshold)
			continue
		}
		filtered = append(filtered, n)
	}
	return filtered
}
