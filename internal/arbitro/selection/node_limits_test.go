package selection

// Hard limits per node, configured under the node limits of the arbiter and
// resolved into the per-node map of the limits.
//
// The contract protected here is that of the overload filter: each node is
// judged by its own trio, with a silent fallback to the global values when
// there is no override. The scope is deliberately narrow, since the statistics,
// the prediction, the ordering and the veto of the self-learning never consult
// the per-node map, and the reactive overload filter is shared by both
// selection modes.

import (
	"slices"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// fixturePerNodeLimits has a global trio of eighty-five, ninety and twenty-five
// plus two partial overrides, in the format that the arbiter delivers, with the
// trio already resolved:
//   - the tight edge overrides only the processor ceiling, of sixty, and
//     inherits the memory and pressure ceilings from the global values;
//   - the loose fog overrides only the processor ceiling, of ninety-five, which
//     is above the global value on purpose.
func fixturePerNodeLimits() LimitsConfig {
	cfg := LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}
	cfg.PerNode = map[string]LimitsConfig{
		"borda-aperta": {MaxCPUPct: 60, MaxMemPct: 90, MaxPSIAvg10: 25},
		"nevoa-folga":  {MaxCPUPct: 95, MaxMemPct: 90, MaxPSIAvg10: 25},
	}
	return cfg
}

// TestLimitsConfigFor covers the resolution itself: the override wins, the
// absence falls back to the global value, and the result is a degenerate copy
// without the per-node map, so chaining the resolution cannot reintroduce an
// override of a third level.
//
// The configuration is no longer comparable with the equality operator since it
// gained the map, so the assertion compares the three scalars field by field.
func TestLimitsConfigFor(t *testing.T) {
	cfg := fixturePerNodeLimits()
	cases := []struct {
		name   string
		nodeID string
		want   LimitsConfig
	}{
		{name: "override do nó vence", nodeID: "borda-aperta", want: LimitsConfig{MaxCPUPct: 60, MaxMemPct: 90, MaxPSIAvg10: 25}},
		{name: "override folgado", nodeID: "nevoa-folga", want: LimitsConfig{MaxCPUPct: 95, MaxMemPct: 90, MaxPSIAvg10: 25}},
		{name: "nó sem override herda global", nodeID: "borda-comum", want: LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}},
		{name: "node_id vazio herda global", nodeID: "", want: LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cfg.For(tc.nodeID)
			if got.MaxCPUPct != tc.want.MaxCPUPct || got.MaxMemPct != tc.want.MaxMemPct || got.MaxPSIAvg10 != tc.want.MaxPSIAvg10 {
				t.Fatalf("For(%q) = cpu %.1f/mem %.1f/psi %.1f, want %.1f/%.1f/%.1f",
					tc.nodeID, got.MaxCPUPct, got.MaxMemPct, got.MaxPSIAvg10,
					tc.want.MaxCPUPct, tc.want.MaxMemPct, tc.want.MaxPSIAvg10)
			}
			if got.PerNode != nil {
				t.Fatalf("For(%q).PerNode = %v, want nil (cópia degenerada)", tc.nodeID, got.PerNode)
			}
		})
	}

	// Chaining: resolving the result of a resolution is innocuous, since there is
	// no map to consult, so an override never passes further down.
	if again := cfg.For("borda-aperta").For("nevoa-folga"); again.MaxCPUPct != 60 {
		t.Fatalf("For.For deveria preservar o trio do primeiro nó, got cpu=%.1f", again.MaxCPUPct)
	}
}

// TestFilterOverloadedPerNode is the boundary that owns the feature: the same
// snapshot of processor usage produces different decisions per node.
//
// The fixture discriminates in both directions, which is what makes it a real
// guard: with the global ceiling of eighty-five, the tight edge, at seventy
// against its ceiling of sixty, has to fall, since without the override it would
// survive, and the loose fog, at ninety against its ceiling of ninety-five, has
// to stay, since without the override it would be dropped. A fixture that only
// tightened a ceiling would pass the same on the code before the feature for the
// loose node.
func TestFilterOverloadedPerNode(t *testing.T) {
	cfg := fixturePerNodeLimits()
	candidates := []nodes.Node{
		{NodeID: "borda-aperta", Layer: nodes.LayerBorda},
		{NodeID: "nevoa-folga", Layer: nodes.LayerNevoa},
		{NodeID: "borda-comum", Layer: nodes.LayerBorda},
	}
	latest := map[string]telemetry.Package{
		"borda-aperta": {CPUUsagePct: 70}, // acima de 60, abaixo do global 85
		"nevoa-folga":  {CPUUsagePct: 90}, // acima do global 85, abaixo de 95
		"borda-comum":  {CPUUsagePct: 70}, // abaixo dos dois
	}
	got := FilterOverloaded(candidates, latest, cfg)
	if !slices.Equal(ids(got), []string{"nevoa-folga", "borda-comum"}) {
		t.Fatalf("FilterOverloaded per-node = %v, want [borda-comum nevoa-folga]", ids(got))
	}

	// Without the per-node map, with the pure global configuration, the same
	// snapshot drops the loose fog and keeps the tight edge, and the inversion
	// proves that the difference comes from the override and not from the ordering
	// or the stability of the filter.
	globalOnly := LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}
	got = FilterOverloaded(candidates, latest, globalOnly)
	if !slices.Equal(ids(got), []string{"borda-aperta", "borda-comum"}) {
		t.Fatalf("FilterOverloaded global = %v, want [borda-aperta borda-comum]", ids(got))
	}
}

// TestFilterOverloadedPerNodeMemPsi guarantees that the decision is not only
// about the processor: the partial override of memory and pressure also decides,
// and an axis that is not declared keeps inheriting the global value.
func TestFilterOverloadedPerNodeMemPsi(t *testing.T) {
	cfg := LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}
	cfg.PerNode = map[string]LimitsConfig{
		// Only the memory ceiling is tight, so the processor and the pressure follow the global values.
		"nevoa-mem": {MaxCPUPct: 85, MaxMemPct: 50, MaxPSIAvg10: 25},
	}
	candidates := []nodes.Node{{NodeID: "nevoa-mem", Layer: nodes.LayerNevoa}}
	latest := map[string]telemetry.Package{
		"nevoa-mem": {CPUUsagePct: 80, MemoryUsagePct: 60}, // CPU ok no global, mem acima de 50
	}
	if got := FilterOverloaded(candidates, latest, cfg); len(got) != 0 {
		t.Fatalf("mem 60 > teto 50 deveria derrubar o nó, got=%v", ids(got))
	}
	// A memory usage of forty-five passes the ceiling of the node, and a processor usage of eighty stays within the global value.
	latest["nevoa-mem"] = telemetry.Package{CPUUsagePct: 80, MemoryUsagePct: 45}
	if got := FilterOverloaded(candidates, latest, cfg); len(got) != 1 {
		t.Fatalf("mem 45 < teto 50 deveria manter o nó, got=%v", ids(got))
	}
}

// TestFilterProjectedOverloadedPerNode covers the hard guard of the projected
// overload, which compares against the processor ceiling of the node itself. The
// helper is private because only the selectors call it, and the risk of a
// regression is a threshold fixed on the global value, which is exactly what the
// fixture exercises.
func TestFilterProjectedOverloadedPerNode(t *testing.T) {
	cfg := fixturePerNodeLimits()
	candidates := []nodes.Node{
		{NodeID: "borda-aperta", Layer: nodes.LayerBorda},
		{NodeID: "nevoa-folga", Layer: nodes.LayerNevoa},
		{NodeID: "borda-comum", Layer: nodes.LayerBorda},
	}
	projected := map[string]float64{
		"borda-aperta": 70, // above the ceiling of the node and below the global one
		"nevoa-folga":  90, // below the ceiling of the node and above the global one
		"borda-comum":  70, // below both ceilings
	}
	got := filterProjectedOverloaded(candidates, projected, cfg)
	if !slices.Equal(ids(got), []string{"nevoa-folga", "borda-comum"}) {
		t.Fatalf("filterProjectedOverloaded per-node = %v, want [borda-comum nevoa-folga]", ids(got))
	}
	// An absent projection keeps the candidate, which can never exceed the ceiling.
	got = filterProjectedOverloaded(candidates, map[string]float64{}, cfg)
	if len(got) != len(candidates) {
		t.Fatalf("sem projeção deveria manter todos, got=%v", ids(got))
	}
}
