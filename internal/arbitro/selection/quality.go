// Static quality matching. The quality is static per variant, declared in the
// expected metrics of the function, and therefore lives here as pure
// functions and not as a selection criterion, since the criteria are blind to
// the node and the quality is applied in the partition of the candidates and
// in the choice of the concrete variant in the gateway.
//
// When there is no match the candidates remain unchanged: no new criterion is
// added and the classification of the attempt is untouched. Without an
// objective, or without metrics of the domain, the path is verbatim, and a
// downgrade never empties the set for a reason of quality, so an empty
// ranking from overload or from the hard limit keeps the current behavior of
// the gateway, which answers 503.
package selection

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// ErrInvalidQualityRequirements indicates malformed metrics of the domain:
// a non-object, such as an array, a string or a number, or any non-numeric
// value, including mixed maps such as an object with a numeric accuracy and
// a string label. The rule is strict, since a typo fails loudly and never
// becomes a silent partial threshold. The gateway maps it to an invalid
// request, and the selection treats it as the verbatim path, without
// partitioning.
var ErrInvalidQualityRequirements = errors.New("selection: domain_metrics inválido")

// ParseQualityRequirements parses the metrics of the domain as a map of
// thresholds from metric to value, and it is pure.
//
//   - A nil objective, nil or empty metrics, where whitespace alone counts as
//     empty, or an empty object gives nothing and reports that nothing was
//     declared, which is the default routing.
//   - The declared flag is only set when the JSON is an object with at least
//     one numeric value, and JSON integers are coerced to floats by the
//     decoder, so one becomes one point zero.
//   - A non-object, or any non-numeric value, including a mixed map, gives
//     the invalid requirements error.
func ParseQualityRequirements(slo *SLO) (map[string]float64, bool, error) {
	if slo == nil || len(bytes.TrimSpace(slo.DomainMetrics)) == 0 {
		return nil, false, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(slo.DomainMetrics, &raw); err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrInvalidQualityRequirements, err)
	}
	if len(raw) == 0 {
		return nil, false, nil
	}
	reqs := make(map[string]float64, len(raw))
	for name, v := range raw {
		// Strict: only a JSON number passes, since the decoder coerces
		// integers to floats; null, a boolean, a string, an array and an
		// object all fail loudly. The pointer tells null, which is an
		// absent value without an error, from zero.
		var f *float64
		if err := json.Unmarshal(v, &f); err != nil {
			return nil, false, fmt.Errorf("%w: métrica %q não-numérica", ErrInvalidQualityRequirements, name)
		}
		if f == nil {
			return nil, false, fmt.Errorf("%w: métrica %q não-numérica", ErrInvalidQualityRequirements, name)
		}
		reqs[name] = *f
	}
	return reqs, true, nil
}

// VariantQualifies evaluates a variant against the requirements, and it is
// pure. It applies a conjunction over the restrictive metrics, which are the
// metrics of the objective with an announced expectation, since an unknown
// metric is ignored. A higher value being better requires an expectation of
// at least the requirement, and otherwise the expectation must be at most the
// requirement, since the absence of a polarity means that a lower value is
// better. An empty restrictive set qualifies.
func VariantQualifies(expected map[string]registry.ExpectedMetric, reqs map[string]float64) bool {
	for name, required := range reqs {
		exp, ok := expected[name]
		if !ok {
			continue
		}
		if exp.HigherIsBetter {
			if exp.Value < required {
				return false
			}
			continue
		}
		if exp.Value > required {
			return false
		}
	}
	return true
}

// BestVariant resolves the requested name against a single node with the
// quality applied, and it is pure. The arms, in order:
//
//   - a wildcard node, without hosted functions, forwards the requested name
//     verbatim and reports it as qualified.
//   - an exactly hosted requested name returns itself, qualified by its own
//     expectations, where an absent map announces nothing and therefore
//     qualifies.
//   - otherwise the variants of the category, in lexicographic order, where
//     the exact equality of bytes is required and an empty value never
//     matches, the same precedent as the resolution of the concrete variant:
//     the first qualified one wins, and if none qualifies the
//     lexicographically first one is returned as a downgrade, which is never
//     a miss for a reason of quality.
//   - a defensive total miss, which is unreachable since the selection
//     guarantees hosting, returns the requested name as not qualified.
func BestVariant(n nodes.Node, requested string, reqs map[string]float64) (string, bool) {
	if len(n.Functions) == 0 {
		return requested, true
	}
	if requested != "" && slices.Contains(n.Functions, requested) {
		return requested, VariantQualifies(n.FunctionExpected[requested], reqs)
	}
	var category []string
	if requested != "" {
		for _, v := range n.Functions {
			if cat, ok := n.FunctionCategories[v]; ok && cat != "" && cat == requested {
				category = append(category, v)
			}
		}
	}
	if len(category) == 0 {
		return requested, false
	}
	slices.Sort(category)
	for _, v := range category {
		if VariantQualifies(n.FunctionExpected[v], reqs) {
			return v, true
		}
	}
	return category[0], false
}

// Downgraded reports whether serving the requested name on the node is a
// downgrade, and it is pure. It receives the requirements already parsed,
// without a re-parse per target, and returns the negation of the
// qualification under the logic of the best variant.
func Downgraded(n nodes.Node, requested string, reqs map[string]float64) bool {
	_, qualified := BestVariant(n, requested, reqs)
	return !qualified
}

// partitionQualified splits the hosted variants into qualified and
// non-qualified through the best variant per node, as a pure helper of the
// quality partition of the selection. The callers skip it entirely when
// nothing was declared, which is the verbatim path, and when the qualified
// result is empty they use the whole hosted set, since a downgrade never
// empties the set for a reason of quality.
func partitionQualified(hosted []nodes.Node, functionName string, reqs map[string]float64) []nodes.Node {
	qualified := make([]nodes.Node, 0, len(hosted))
	for _, n := range hosted {
		if _, ok := BestVariant(n, functionName, reqs); ok {
			qualified = append(qualified, n)
		}
	}
	return qualified
}
