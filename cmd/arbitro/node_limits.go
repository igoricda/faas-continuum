package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/config"
)

// resolveNodeHardLimits resolves the partial map of hard limits per node into
// the complete trio that the selection consults, as a pure function, without
// input, output or clock, so that it is testable without starting the
// arbiter.
//
// The rule of the merge is that an absent field, with a zero value, inherits
// the corresponding global ceiling. Only the arbiter knows the globals, so
// the merge happens here and not in the selection package: the lookup of the
// limits is a lookup in an already resolved map, without arithmetic in the
// hot path of the selection.
//
// It returns nothing when there is no override, with no node listed, and in
// that case the map stays empty and the lookup always falls back to the
// global values, which is identical to the behavior before this field
// existed.
//
// Invalid values, negative or not finite, never reach here, since the
// validation of the configuration aborts the startup before, with the same
// fail-fast discipline.
func resolveNodeHardLimits(a config.Arbitro) map[string]selection.LimitsConfig {
	if len(a.NodeLimits) == 0 {
		return nil
	}
	out := make(map[string]selection.LimitsConfig, len(a.NodeLimits))
	for nodeID, nl := range a.NodeLimits {
		out[nodeID] = selection.LimitsConfig{
			MaxCPUPct:   firstPositive(nl.MaxCPUPct, a.MaxCPUPct),
			MaxMemPct:   firstPositive(nl.MaxMemPct, a.MaxMemPct),
			MaxPSIAvg10: firstPositive(nl.MaxPSIAvg10, a.MaxPSIAvg10),
		}
	}
	return out
}

// firstPositive materializes the rule that zero means inherit: it returns the
// override when it was declared as positive and the global value otherwise. A
// non-positive value was already rejected by the validation, so positive
// against zero is exactly the distinction between declared and inherited, and
// there is no other decision path here.
func firstPositive(override, fallback float64) float64 {
	if override > 0 {
		return override
	}
	return fallback
}

// unknownNodeLimitIDs returns, in order, the node identifiers of the
// configured limits that do not correspond to any known node, so that an
// override for an unknown node, from a typo or from a node that left the
// cluster, drops nobody and does not go unnoticed. The comparison is exact,
// without normalization, since the identifier is the literal one of the
// record, and an absent or empty set of known nodes returns every identifier.
func unknownNodeLimitIDs(perNode map[string]selection.LimitsConfig, known []nodes.Node) []string {
	if len(perNode) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(known))
	for _, n := range known {
		seen[n.NodeID] = struct{}{}
	}
	var unknown []string
	for nodeID := range perNode {
		if _, ok := seen[nodeID]; !ok {
			unknown = append(unknown, nodeID)
		}
	}
	sort.Strings(unknown)
	return unknown
}

// nodeLimitsLog formats the resolved map in a single stable line, with the
// identifiers in order, for the startup log, so that the operator can check
// at a glance which nodes deviated from the global values and by how much.
func nodeLimitsLog(perNode map[string]selection.LimitsConfig) string {
	if len(perNode) == 0 {
		return ""
	}
	ids := make([]string, 0, len(perNode))
	for nodeID := range perNode {
		ids = append(ids, nodeID)
	}
	sort.Strings(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		lc := perNode[id]
		parts = append(parts, fmt.Sprintf("%s(cpu=%.1f mem=%.1f psi=%.1f)", id, lc.MaxCPUPct, lc.MaxMemPct, lc.MaxPSIAvg10))
	}
	return strings.Join(parts, " ")
}
