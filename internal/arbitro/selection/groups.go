// Category helpers. This is the single place of the logic of the function
// categories, which maps a variant to a category: no call site duplicates
// the rule of a category, and every one of them delegates to these pure
// helpers.
//
// The comparison of categories is exact equality of bytes, case sensitive,
// without trimming or normalization. A nil map is equivalent to no category,
// which is the behavior of an older node, and an empty value never matches,
// since it is invalid data that is treated as absent and never as a match.
package selection

import (
	"slices"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// EffectiveGroup returns the category of the node for the requested name,
// and it is pure. When the requested name is a concrete variant hosted by
// the node, with a non-empty entry in the map of categories, it returns that
// category, and otherwise it returns the requested name verbatim. The result
// is relative to the node, since two nodes may map the same variant to
// different categories, and each call reflects only the node it receives.
func EffectiveGroup(requested string, n nodes.Node) string {
	if requested == "" {
		return requested
	}
	if !slices.Contains(n.Functions, requested) {
		return requested
	}
	if cat, ok := n.FunctionCategories[requested]; ok && cat != "" {
		return cat
	}
	return requested
}

// HostServesCategory reports whether the node hosts any variant of the
// category, and it is pure. It is true if and only if there is a hosted
// variant whose mapping is equal to the category by exact equality of
// bytes. An empty group never matches, and a nil map gives false.
func HostServesCategory(n nodes.Node, group string) bool {
	if group == "" {
		return false
	}
	for _, f := range n.Functions {
		if cat, ok := n.FunctionCategories[f]; ok && cat != "" && cat == group {
			return true
		}
	}
	return false
}

// ResolveConcrete resolves the requested name against a single node, and it
// is pure. The precedence is: an exact hosting wins and returns the
// requested name with its effective group; otherwise the first hosted
// variant in lexicographic order whose category matches; otherwise a
// wildcard node, with an empty or nil set of functions, forwards the
// requested name verbatim and flags it as a wildcard; otherwise it is a miss.
// An empty category value never matches.
func ResolveConcrete(n nodes.Node, requested string) (concrete, group string, wildcard bool, ok bool) {
	if requested != "" && slices.Contains(n.Functions, requested) {
		return requested, EffectiveGroup(requested, n), false, true
	}
	if requested != "" {
		sorted := slices.Clone(n.Functions)
		slices.Sort(sorted)
		for _, v := range sorted {
			if cat, mapped := n.FunctionCategories[v]; mapped && cat != "" && cat == requested {
				return v, requested, false, true
			}
		}
	}
	if len(n.Functions) == 0 {
		return requested, requested, true, true
	}
	return "", "", false, false
}

// ResolveRequestGroup is the canonical rule that maps the request to the
// group over the candidate set, and it is pure. It takes the first hit in
// increasing order of node identifier: when some candidate hosts the
// requested name as a concrete variant with a non-empty mapping, it returns
// that category; otherwise, when some candidate has a hosted variant that
// maps to the requested name, which means the requested name is a known
// category, it returns the requested name; otherwise it returns the
// requested name verbatim. With nil maps the result is always verbatim, which
// is identical to the previous filter of hosting.
func ResolveRequestGroup(requested string, candidates []nodes.Node) string {
	if requested == "" {
		return requested
	}
	ordered := slices.Clone(candidates)
	slices.SortFunc(ordered, func(a, b nodes.Node) int {
		switch {
		case a.NodeID < b.NodeID:
			return -1
		case a.NodeID > b.NodeID:
			return 1
		default:
			return 0
		}
	})
	for _, n := range ordered {
		if !slices.Contains(n.Functions, requested) {
			continue
		}
		if cat, ok := n.FunctionCategories[requested]; ok && cat != "" {
			return cat
		}
	}
	for _, n := range ordered {
		if HostServesCategory(n, requested) {
			return requested
		}
	}
	return requested
}
