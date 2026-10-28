// Package selection implements the algorithm of node selection of the arbiter,
// with the ranking, the overload evaluation, the queue selector and the risk
// selector, which is the canonical selector.
package selection

import (
	"encoding/json"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// LimitsConfig aggregates the configurable hard limits of the arbiter.
//
// The per-node map is the override of the three ceilings, keyed by the node
// identifier and already resolved into a complete trio by the startup of the
// arbiter: the selection never merges, it only queries the limits for the node.
// An absent or empty map means that every node is judged by the global trio,
// which is the historical behavior, identical to the one before the field
// existed.
//
// The override is deliberately restricted to the stage of the hard limits,
// which is the reactive overload filter, the projected overload and the
// overloaded status; the statistics, the prediction, the ordering and the veto
// of the selector do not consult it beyond the reactive overload filter, which
// is shared by design.
type LimitsConfig struct {
	MaxCPUPct   float64 `json:"max_cpu_pct"`
	MaxMemPct   float64 `json:"max_mem_pct"`
	MaxPSIAvg10 float64 `json:"max_psi_avg10"`
	// The per-node map goes from the node identifier to the effective trio of
	// that node. An entry never carries the map, since the resolved value is a
	// copy without it, so no recursion is possible in practice, and it is not
	// serialized, since it is born from the merge at startup.
	PerNode map[string]LimitsConfig `json:"-"`
}

// For returns the effective trio of hard limits for the node, and it is pure.
// The override of the node applies when there is an entry, and otherwise the
// global trio applies. The result never carries the map, since it is a
// degenerated copy, so chained calls are harmless, and a node without an
// override changes nothing in the behavior.
func (c LimitsConfig) For(nodeID string) LimitsConfig {
	if lc, ok := c.PerNode[nodeID]; ok {
		return LimitsConfig{MaxCPUPct: lc.MaxCPUPct, MaxMemPct: lc.MaxMemPct, MaxPSIAvg10: lc.MaxPSIAvg10}
	}
	return LimitsConfig{MaxCPUPct: c.MaxCPUPct, MaxMemPct: c.MaxMemPct, MaxPSIAvg10: c.MaxPSIAvg10}
}

// SLO is the objective of the request, accepted and propagated and never
// interpreted in this stage, while the risk selector only reads the maximum
// latency and the minimum throughput.
//
// The fields are ordered from the largest to the smallest for the alignment of
// eight bytes.
type SLO struct {
	DomainMetrics    json.RawMessage `json:"domain_metrics,omitempty"`
	MaxLatencyMS     int             `json:"max_latency_ms,omitempty"`
	MinThroughputRPS float64         `json:"min_throughput_rps,omitempty"`
}

// Selector chooses the ordered queue of candidate nodes for the function.
type Selector interface {
	Select(candidates []nodes.Node, functionName string, slo *SLO) []nodes.Node
}

// QueueSelector filters the hosting by the hosted functions and delegates to
// the ranking for the hard limits, the overload and the ordering. An empty or
// nil set of functions is eligible for any function.
//
// The capacity filter removes exhausted nodes with a fail-open, the same as the
// risk selector, since the queue selector is public and used by the gateway
// tests and leaving it without gating would reintroduce the dispatch to nodes
// without available instances. The filter uses every concrete variant of the
// category hosted by the node and only removes the candidate when all of them
// are exhausted, while the legacy key stays in the cold start and the queue
// tier. An absent monitor disables the gating.
//
// The occupancy demotion pushes a node with a fresh queue behind the idle ones
// and never removes it, so the baseline of the hard limits sees the saturation
// of the pool without depending on the aggregate processor usage, which is a
// secondary signal inside each partition.
type QueueSelector struct {
	Mon TelemetrySource
	Cfg LimitsConfig
	// The clock stamps the horizon of freshness of the capacity, and nil means the current time.
	Clock func() time.Time
}

func (q QueueSelector) now() time.Time {
	if q.Clock != nil {
		return q.Clock()
	}
	return time.Now()
}

// capacityNameForNode resolves, for the node, the concrete key of occupancy
// under the requested function, with the same precedence as the dispatch of the
// gateway: a declared quality gives the best variant, otherwise the concrete
// resolution, and a miss gives the requested name, since the dispatch also
// forwards the request verbatim in that case. A wildcard node gives the
// requested name. It is pure.
func capacityNameForNode(n nodes.Node, functionName string, reqs map[string]float64, declared bool) string {
	if declared {
		if v, _ := BestVariant(n, functionName, reqs); v != "" {
			return v
		}
		return functionName
	}
	if c, _, _, ok := ResolveConcrete(n, functionName); ok {
		return c
	}
	return functionName
}

// capacityNamesByNode builds the map from the node identifier to the concrete
// key for the candidates, computed once per selection for the parity between
// the training and the query with the dispatch.
func capacityNamesByNode(candidates []nodes.Node, functionName string, reqs map[string]float64, declared bool) map[string]string {
	out := make(map[string]string, len(candidates))
	for _, n := range candidates {
		out[n.NodeID] = capacityNameForNode(n, functionName, reqs, declared)
	}
	return out
}

// hostingFilter filters the candidates by hosting under the requested function,
// as a pure function shared by both selectors. The rule is an exact function,
// or a hosted category, or a wildcard node. The group is received ready so that
// the resolution of the group is not called twice per selection.
func hostingFilter(candidates []nodes.Node, functionName, group string) []nodes.Node {
	eligible := make([]nodes.Node, 0, len(candidates))
	for _, n := range candidates {
		ok := slices.Contains(n.Functions, functionName) || HostServesCategory(n, group) || len(n.Functions) == 0
		if !ok {
			continue
		}
		eligible = append(eligible, n)
	}
	return eligible
}

// filterHosting is the convenience entry of the hosting filter for the callers
// that do not need the group afterwards: it resolves the group once and
// delegates, keeping the historical signature used by the call sites and by the
// category tests. The risk selector uses the shared filter directly because it
// also consumes the group in the keys of the learner.
func filterHosting(candidates []nodes.Node, functionName string) []nodes.Node {
	return hostingFilter(candidates, functionName, ResolveRequestGroup(functionName, candidates))
}

// latestSnapshot takes a snapshot of the monitor for the eligible nodes, as a
// pure shared helper. An absent monitor gives an empty map.
func latestSnapshot(mon TelemetrySource, eligible []nodes.Node) map[string]telemetry.Package {
	latest := make(map[string]telemetry.Package, len(eligible))
	if mon == nil {
		return latest
	}
	for _, n := range eligible {
		if pkg, ok := mon.Latest(n.NodeID); ok {
			latest[n.NodeID] = pkg
		}
	}
	return latest
}

// applyQualityPartition applies the partition of quality, as a block shared by
// both selectors. It only runs when requirements are declared, and a parsing
// error gives the verbatim path, since the gateway validates and rejects the
// request before, while an empty qualified set gives the whole hosted set,
// since a downgrade never empties the set for a reason of quality. It also
// returns the parsed requirements and the declaration flag, which the callers
// reuse without parsing again.
func applyQualityPartition(hosted []nodes.Node, functionName string, slo *SLO) ([]nodes.Node, map[string]float64, bool) {
	reqs, qualityDeclared, _ := ParseQualityRequirements(slo)
	if qualityDeclared {
		if qualified := partitionQualified(hosted, functionName, reqs); len(qualified) > 0 {
			hosted = qualified
		}
	}
	return hosted, reqs, qualityDeclared
}

func (q QueueSelector) Select(candidates []nodes.Node, functionName string, slo *SLO) []nodes.Node {
	eligible := filterHosting(candidates, functionName)
	eligible, reqs, qualityDeclared := applyQualityPartition(eligible, functionName, slo)
	latest := latestSnapshot(q.Mon, eligible)
	now := q.now()
	nameByNode := capacityNamesByNode(eligible, functionName, reqs, qualityDeclared)
	// The capacity filter walks every variant of the category hosted by the
	// node, and a live variant keeps the candidate, since an exhausted one is
	// deprecated in the dispatch. The reading is not consumed here.
	//
	// An exhausted variant is one with a fresh optimistic mark of being warm at
	// the ceiling, the same as the self-learning, so a genuinely cold node
	// without a live instance stays eligible for the node to wake up with a cold
	// start. Previously the hard limit discarded a node without available
	// instances, and with a cold fog node the queue emptied and the request went
	// to the cloud, also cold, without trying the fog.
	variantsByNode := capacityVariantsByNode(eligible, functionName)
	eligible, _ = filterExhaustedVariantsFromMonitor(q.Mon, eligible, nameByNode, variantsByNode, now)
	// The demotion by pool occupancy pushes a node with a fresh queue behind the
	// idle ones, since every instance is busy, and never removes it, so a single
	// node is still chosen, which is the fail-open, while an unknown value or an
	// expired time to live counts as idle. The ranking runs per partition so that
	// the demotion survives the stable sort.
	queued := queuedFromMonitor(q.Mon, eligible, nameByNode, now)
	idle, busy := partitionQueued(eligible, queued)
	out := append(RankNodes(idle, latest, slo, q.Cfg), RankNodes(busy, latest, slo, q.Cfg)...)
	return out
}

// queuedFromMonitor reports, per node identifier, whether the fresh occupancy of
// the concrete variant has a queue, which means every instance is busy and there
// is a request waiting for a release or a provisioning, the signal of pool
// saturation that is primary over the aggregate processor usage. An absent
// value or an expired time to live stays out of the map, which is the fail-open,
// since an unknown value is never demoted, and an absent monitor gives nothing.
func queuedFromMonitor(mon TelemetrySource, candidates []nodes.Node, capacityNameByNode map[string]string, now time.Time) map[string]bool {
	if mon == nil {
		return nil
	}
	out := make(map[string]bool, len(candidates))
	for _, n := range candidates {
		name := capacityNameByNode[n.NodeID]
		if name == "" {
			continue
		}
		if fc, ok := mon.GetCapacity(n.NodeID, name, now); ok && fc.QueueDepth > 0 {
			out[n.NodeID] = true
		}
	}
	return out
}

// partitionQueued splits the candidates into idle, with a fresh queue of zero or
// unknown, and queued, preserving the relative order inside each partition. It
// is a demotion and never a removal, so a list with only queued candidates
// returns all of them as busy and the caller still chooses among them.
func partitionQueued(candidates []nodes.Node, queued map[string]bool) (idle, busy []nodes.Node) {
	idle = make([]nodes.Node, 0, len(candidates))
	busy = make([]nodes.Node, 0, len(candidates))
	for _, n := range candidates {
		if queued[n.NodeID] {
			busy = append(busy, n)
			continue
		}
		idle = append(idle, n)
	}
	return idle, busy
}

// Overloaded evaluates the second stage, and it is pure. Without known
// telemetry it returns false, since the absence of data is not an overload.
// The rules use a strict comparison, so equality does not overload: the
// processor usage, the memory usage and the pressure averages, of the
// processor, of the partial memory and of the full memory, compare against the
// respective ceilings.
//
// The full memory pressure is included because low processor pressure with high
// full memory pressure indicates thrashing, and the node would keep receiving
// load if only the processor pressure were observed. Absent pressure is
// ignored, which is the compatibility mode without pressure support.
func Overloaded(pkg telemetry.Package, ok bool, cfg LimitsConfig) bool {
	if !ok {
		return false
	}
	if pkg.CPUUsagePct > cfg.MaxCPUPct {
		return true
	}
	if pkg.MemoryUsagePct > cfg.MaxMemPct {
		return true
	}
	if pkg.CPUPressure != nil && pkg.CPUPressure.Avg10 > cfg.MaxPSIAvg10 {
		return true
	}
	if pkg.MemoryPressure != nil && pkg.MemoryPressure.Avg10 > cfg.MaxPSIAvg10 {
		return true
	}
	if pkg.MemoryPressureFull != nil && pkg.MemoryPressureFull.Avg10 > cfg.MaxPSIAvg10 {
		return true
	}
	return false
}

// The first stage of the ceiling by layer with the objective was removed, and
// the arbiter no longer filters the candidates by the objective. The latency
// limits are the timeout per function declared in each node, applied in the
// node itself as the minimum between that timeout and the ceiling of the late
// execution of the agent, and the budget of the gateway, which derives from the
// objective of the request and governs the slice of each attempt.
//
// The reason: the ceiling by layer required a value declared per layer in the
// arbiter and, with a loose objective, emptied the local queue and pushed the
// whole request to the cloud, in either direction of the comparison. With the
// mechanism removed, the eligibility does not depend on the objective.

// FilterExhausted filters the exhausted nodes, as the capacity stage, and it is
// pure, next to the overload filter.
//
// The readings carry the fresh occupancy per node for the resolved concrete
// variant, which is the key of the producer in the node interface, and the
// caller builds them from the capacity query: a node without data, either
// absent or unknown with an expired time to live, never enters the map and is
// never filtered. Only a node with a fresh reading and without available
// instances is filtered, where the non-positive test also covers a negative
// availability that escaped the first check, since the ingestion already clamps
// the value to zero.
//
// Fail-open: when the filtering would empty the list it returns the original
// list intact, and the gateway tries and receives the overload rejection, with
// lines only in the invocation log. The instant is the same one used in the
// capacity checks of the caller, which makes the horizon of freshness explicit
// in the signature of the stage. A nil or empty map does nothing, which is the
// pure ranking path without a monitor.
func FilterExhausted(candidates []nodes.Node, caps map[string]telemetry.FunctionCapacity, now time.Time) []nodes.Node {
	if len(caps) == 0 {
		return candidates
	}
	filtered := make([]nodes.Node, 0, len(candidates))
	for _, n := range candidates {
		if c, ok := caps[n.NodeID]; ok && c.InstancesAvailable <= 0 {
			continue
		}
		filtered = append(filtered, n)
	}
	if len(filtered) == 0 {
		return candidates
	}
	return filtered
}

// FilterOverloaded filters the overloaded nodes through the overload predicate,
// as the second stage. The threshold is that of the node itself, so a node with
// a tight override falls earlier and a node with a loose override survives
// above the global value.
func FilterOverloaded(candidates []nodes.Node, latest map[string]telemetry.Package, cfg LimitsConfig) []nodes.Node {
	filtered := make([]nodes.Node, 0, len(candidates))
	for _, n := range candidates {
		pkg, ok := latest[n.NodeID]
		if Overloaded(pkg, ok, cfg.For(n.NodeID)) {
			continue
		}
		filtered = append(filtered, n)
	}
	return filtered
}

// OrderByFit orders by the layer rank, with the edge first, then the fog and
// then the others, and then lexicographically by the declared criteria in the
// order of the criteria: for each criterion declared for the objective it takes
// the fit of the segments, and a lower score wins when both are present and
// differ, a present score wins when only one is present, and a tie passes to
// the next criterion. Without a discriminating criterion the fallback is the
// ascending processor usage, with an infinite value for the absent data, through
// a stable sort. Nil segments are treated as no data.
//
// The skip of a criterion that is not declared is where the throughput leaves
// the ordering, so an objective with latency only never consults the throughput
// fit and the recent rate has zero influence on the order, while a mixed
// objective orders by the percentile with a tie broken by the rate.
func OrderByFit(candidates []nodes.Node, latest map[string]telemetry.Package, segments map[string]SegmentStats, criteria []SLOCriterion, slo *SLO) []nodes.Node {
	sort.SliceStable(candidates, func(i, j int) bool {
		ri := layerRank(candidates[i].Layer)
		rj := layerRank(candidates[j].Layer)
		if ri != rj {
			return ri < rj
		}
		for _, c := range criteria {
			if c == nil || !c.Declared(slo) {
				continue
			}
			sI, hasI := segments[candidates[i].NodeID]
			sJ, hasJ := segments[candidates[j].NodeID]
			var scoreI, scoreJ float64
			var okI, okJ bool
			if hasI {
				scoreI, okI = c.Fit(sI)
			}
			if hasJ {
				scoreJ, okJ = c.Fit(sJ)
			}
			if okI && okJ {
				if scoreI != scoreJ {
					return scoreI < scoreJ
				}
				continue
			}
			if okI && !okJ {
				return true
			}
			if !okI && okJ {
				return false
			}
		}
		ci := cpuUsage(candidates[i].NodeID, latest)
		cj := cpuUsage(candidates[j].NodeID, latest)
		return ci < cj
	})
	return candidates
}

// RankNodes runs the stages of filtering and ordering and returns the ordered
// queue, and it is pure. It forwards the selection after filtering the rigid
// limits and then the overload, and then orders by the layer and the processor
// usage, without the history. It receives the latest telemetry by value,
// without pointers.
func RankNodes(candidates []nodes.Node, latest map[string]telemetry.Package, slo *SLO, cfg LimitsConfig) []nodes.Node {
	// The capacity stage only receives the latest snapshot, without the monitor,
	// the function and the instant needed to check the freshness, so a nil map
	// does nothing, as documented in the exhausted filter, and the queue selector
	// path is preserved.
	filteredCap := FilterExhausted(candidates, nil, time.Time{})
	filtered := FilterOverloaded(filteredCap, latest, cfg)
	return OrderByFit(filtered, latest, nil, nil, slo)
}

func layerRank(layer string) int {
	switch layer {
	case nodes.LayerBorda:
		return 0
	case nodes.LayerNevoa:
		return 1
	default:
		return 2
	}
}

func cpuUsage(nodeID string, latest map[string]telemetry.Package) float64 {
	if pkg, ok := latest[nodeID]; ok {
		return pkg.CPUUsagePct
	}
	return math.Inf(1)
}
