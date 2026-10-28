package selection

// Capacity stage per variant of the category. The previous capacity key was a
// single one per node, the alphabetically first concrete variant, so when the
// fog node hosted a processor variant and a GPU variant of the same category,
// a processor variant without a live instance dropped the whole node and the
// live GPU variant lost its chance, sending the request from the edge to the
// cloud without trying the fog.
//
// The new rule: the node only leaves the queue when every concrete variant of
// the category it hosts is exhausted under the predicate of the selector. A
// live variant keeps the node eligible, and an exhausted variant is
// deprecated in the dispatch. Without a fresh reading of some variant, or when
// the filtering would empty the list, the fail-open behavior applies.

import (
	"slices"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// capacityVariantsForNode returns the concrete capacity keys to query for the
// node under the requested function, and it is pure.
//
//   - a hosted concrete request gives the requested name;
//   - a category request gives every variant hosted by the node whose mapping
//     is equal to the requested name, in lexicographic order, which is
//     deterministic;
//   - a wildcard node, an empty request or nothing hosted gives an empty
//     result, without gating, which is the same fail-open rule of the legacy
//     filter.
func capacityVariantsForNode(n nodes.Node, functionName string) []string {
	if len(n.Functions) == 0 || functionName == "" {
		return nil
	}
	if slices.Contains(n.Functions, functionName) {
		return []string{functionName}
	}
	var variants []string
	for _, v := range n.Functions {
		if cat, ok := n.FunctionCategories[v]; ok && cat != "" && cat == functionName {
			variants = append(variants, v)
		}
	}
	if len(variants) == 0 {
		return nil
	}
	slices.Sort(variants)
	return variants
}

// capacityVariantsByNode builds the map from the node identifier to the
// concrete variants of the category, computed once per selection and reused
// in the capacity stage.
func capacityVariantsByNode(candidates []nodes.Node, functionName string) map[string][]string {
	out := make(map[string][]string, len(candidates))
	for _, n := range candidates {
		out[n.NodeID] = capacityVariantsForNode(n, functionName)
	}
	return out
}

// Exhaustion is the fresh optimistic mark per variant, which means warm at
// the ceiling. A genuinely cold node, without a live instance and with a
// minimum of zero instances, is not exhaustion, since the candidate remains
// eligible for the node to wake up with a cold start.
//
// The hard limit uses the same rule as the self-learning. It used to discard
// a node without available instances, which confused warm at the ceiling with
// no live instance: with a cold fog node the local queue emptied and the
// request jumped straight to the cloud, also cold, without trying to wake the
// fog. The reading carries the mark, and a fresh reading is the only
// requirement, since an unknown value or an expired time to live never gates.

// filterExhaustedVariantsFromMonitor is the capacity stage per variant. For
// each candidate:
//
//   - it reads the legacy key once and returns it in the readings, preserving
//     the cold start and the queue tier of the queue selector and the live
//     state of the risk selector;
//   - without a mapped variant it keeps the candidate, which is fail-open;
//   - with some variant without a fresh reading it keeps the candidate,
//     since an unknown value never gates;
//   - with some variant without a mark it keeps the candidate;
//   - it only removes the candidate when every known variant is marked.
//
// When the filtering would empty the list it returns the original list
// intact, which is the fail-open rule of the legacy filter, and an absent
// monitor does nothing.
func filterExhaustedVariantsFromMonitor(mon TelemetrySource, candidates []nodes.Node, capacityNameByNode map[string]string, variantsByNode map[string][]string, now time.Time) ([]nodes.Node, map[string]capacityRead) {
	if mon == nil {
		return candidates, nil
	}
	caps := make(map[string]capacityRead, len(candidates))
	filtered := make([]nodes.Node, 0, len(candidates))
	for _, n := range candidates {
		legacy := capacityNameByNode[n.NodeID]
		fc, mark, ok := mon.GetCapacityEx(n.NodeID, legacy, now)
		caps[n.NodeID] = capacityRead{fc: fc, mark: mark, ok: ok}

		variants := variantsByNode[n.NodeID]
		if len(variants) == 0 {
			filtered = append(filtered, n)
			continue
		}
		anyUnknown, allExhausted := false, true
		for _, v := range variants {
			cr := caps[n.NodeID]
			if v != legacy {
				f, m, o := mon.GetCapacityEx(n.NodeID, v, now)
				cr = capacityRead{fc: f, mark: m, ok: o}
			}
			if !cr.ok {
				anyUnknown = true
				continue
			}
			if !cr.mark {
				allExhausted = false
			}
		}
		if anyUnknown || !allExhausted {
			filtered = append(filtered, n)
		}
	}
	if len(filtered) == 0 {
		return candidates, caps
	}
	return filtered, caps
}

// AvailableConcrete returns the first variant of the requested category with
// a fresh and available reading in the node, in deterministic order, which is
// the concrete variant that the dispatch should prefer when the legacy
// variant is at the ceiling. Without a monitor, without a mapped variant or
// without an available variant the result is absent, and the caller keeps
// the legacy resolution identical. It is pure.
func AvailableConcrete(mon TelemetrySource, n nodes.Node, requested string, now time.Time) (string, bool) {
	if mon == nil {
		return "", false
	}
	for _, v := range capacityVariantsForNode(n, requested) {
		if fc, mark, ok := mon.GetCapacityEx(n.NodeID, v, now); ok && !mark && fc.InstancesAvailable > 0 {
			return v, true
		}
	}
	return "", false
}
