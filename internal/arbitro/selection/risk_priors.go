// Priors of profiling, from a map of the group to the node or layer to the
// service time, the dispersion and the cold start time. It is optional.
package selection

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
)

// RiskPriorEntry is the prior of a pair of group and node or layer. A zero
// value means no prior, with a weak intercept and a probation until enough
// effective samples accumulate.
type RiskPriorEntry struct {
	ServiceMS float64 `json:"service_ms"`
	Sigma     float64 `json:"sigma"`
	ColdMS    float64 `json:"cold_ms"`
}

// RiskPriors is the map from the group to the node or layer to the prior.
type RiskPriors map[string]map[string]RiskPriorEntry

// LoadRiskPriors reads the file of priors, where an empty path gives no priors
// and no error. The caller fails fast on an error and aborts the startup.
func LoadRiskPriors(path string) (RiskPriors, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("v2 priors: ler %q: %w", path, err)
	}
	var p RiskPriors
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("v2 priors: decode %q: %w", path, err)
	}
	return p, nil
}

// PriorFunc resolves the prior function: an exact match of the group and the
// node wins, and otherwise it falls back to the layer of the node, where an
// absent layer resolution keeps only the exact match. With few nodes the
// priors come from the profiling and not from sibling nodes.
func (p RiskPriors) PriorFunc(layerOf func(nodeID string) string) learn.PriorFunc {
	if len(p) == 0 {
		return nil
	}
	return func(group, node string) (learn.Prior, bool) {
		m, ok := p[group]
		if !ok {
			return learn.Prior{}, false
		}
		if e, ok := m[node]; ok {
			return learn.Prior{ServiceMS: e.ServiceMS, Sigma: e.Sigma, ColdMS: e.ColdMS}, true
		}
		if layerOf != nil {
			if layer := layerOf(node); layer != "" {
				if e, ok := m[layer]; ok {
					return learn.Prior{ServiceMS: e.ServiceMS, Sigma: e.Sigma, ColdMS: e.ColdMS}, true
				}
			}
		}
		return learn.Prior{}, false
	}
}
