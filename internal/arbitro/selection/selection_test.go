package selection

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

var defaultLimits = LimitsConfig{
	MaxCPUPct:   85,
	MaxMemPct:   90,
	MaxPSIAvg10: 25,
}

func psiPtr(v float64) *telemetry.PressureFields {
	return &telemetry.PressureFields{Avg10: v}
}

// ---------------------------------------------------------------------------
// Overloaded
// ---------------------------------------------------------------------------

func TestOverloaded(t *testing.T) {
	cases := []struct {
		name string
		pkg  telemetry.Package
		ok   bool
		want bool
	}{
		// An unknown node is never overloaded, whatever the values.
		{name: "ok false cpu alto => false", pkg: telemetry.Package{CPUUsagePct: 100, MemoryUsagePct: 100, CPUPressure: psiPtr(100), MemoryPressure: psiPtr(100)}, ok: false, want: false},
		{name: "ok false psi alto => false", pkg: telemetry.Package{CPUPressure: psiPtr(99), MemoryPressure: psiPtr(99)}, ok: false, want: false},
		{name: "ok false vazio => false", pkg: telemetry.Package{}, ok: false, want: false},

		// A total absence of pressure, with values below the ceiling, gives false.
		{name: "todos nil abaixo limite => false", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10}, ok: true, want: false},

		// CPU isolado
		{name: "cpu isolado abaixo => false", pkg: telemetry.Package{CPUUsagePct: 50, MemoryUsagePct: 10}, ok: true, want: false},
		{name: "cpu fronteira == limite => false", pkg: telemetry.Package{CPUUsagePct: 85, MemoryUsagePct: 10}, ok: true, want: false},
		{name: "cpu isolado acima => true", pkg: telemetry.Package{CPUUsagePct: 85.1, MemoryUsagePct: 10}, ok: true, want: true},

		// Mem isolado
		{name: "mem isolado abaixo => false", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 50}, ok: true, want: false},
		{name: "mem fronteira == limite => false", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 90}, ok: true, want: false},
		{name: "mem isolado acima => true", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 90.1}, ok: true, want: true},

		// PSI cpu isolado
		{name: "psi cpu isolado abaixo => false", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, CPUPressure: psiPtr(10)}, ok: true, want: false},
		{name: "psi cpu fronteira == limite => false", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, CPUPressure: psiPtr(25)}, ok: true, want: false},
		{name: "psi cpu isolado acima => true", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, CPUPressure: psiPtr(25.1)}, ok: true, want: true},

		// PSI mem isolado
		{name: "psi mem isolado abaixo => false", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, MemoryPressure: psiPtr(10)}, ok: true, want: false},
		{name: "psi mem fronteira == limite => false", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, MemoryPressure: psiPtr(25)}, ok: true, want: false},
		{name: "psi mem isolado acima => true", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, MemoryPressure: psiPtr(25.1)}, ok: true, want: true},

		// Compatibility: one pressure absent and the other present.
		{name: "compat CPUPressure nil MemoryPressure acima => true", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, MemoryPressure: psiPtr(30)}, ok: true, want: true},
		{name: "compat MemoryPressure nil CPUPressure acima => true", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, CPUPressure: psiPtr(30)}, ok: true, want: true},
		{name: "compat CPUPressure nil MemoryPressure abaixo => false", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, MemoryPressure: psiPtr(10)}, ok: true, want: false},
		{name: "compat MemoryPressure nil CPUPressure abaixo => false", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, CPUPressure: psiPtr(10)}, ok: true, want: false},
		{name: "compat ambos nil com cpu/mem ok => false", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, CPUPressure: nil, MemoryPressure: nil}, ok: true, want: false},

		// A nil pressure is ignored even when the other field alone would be an
		// overload, and with both absent and the usage below the ceilings the node
		// is not overloaded, which is already covered above.

		// Combinados
		{name: "combinado cpu+mem acima => true", pkg: telemetry.Package{CPUUsagePct: 90, MemoryUsagePct: 95}, ok: true, want: true},
		{name: "combinado psi cpu+mem acima => true", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, CPUPressure: psiPtr(30), MemoryPressure: psiPtr(30)}, ok: true, want: true},
		{name: "combinado cpu ok psi cpu acima => true", pkg: telemetry.Package{CPUUsagePct: 10, MemoryUsagePct: 10, CPUPressure: psiPtr(30)}, ok: true, want: true},
		{name: "combinado todos acima => true", pkg: telemetry.Package{CPUUsagePct: 90, MemoryUsagePct: 95, CPUPressure: psiPtr(30), MemoryPressure: psiPtr(30)}, ok: true, want: true},

		// The strict boundary: exactly at the ceiling on every axis gives false.
		{name: "todos fronteira == limite => false", pkg: telemetry.Package{CPUUsagePct: 85, MemoryUsagePct: 90, CPUPressure: psiPtr(25), MemoryPressure: psiPtr(25)}, ok: true, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Overloaded(tc.pkg, tc.ok, defaultLimits)
			if got != tc.want {
				t.Fatalf("Overloaded pkg=%+v ok=%v got=%v want=%v", tc.pkg, tc.ok, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Hard limit, which is the bootstrap criterion of an immature segment
// ---------------------------------------------------------------------------

// There is no ceiling per layer: the bootstrap criterion is always eligible,
// independent of the layer and of the objective. The latency limit is the
// timeout per function declared by the node plus the budget of the gateway, as
// applied by the ranking.
func TestHardLimitAlwaysApt(t *testing.T) {
	c := LatencyCriterion{}
	slos := []*SLO{nil, {MaxLatencyMS: 500}, {MaxLatencyMS: 5000}, {MaxLatencyMS: 30000}, {MaxLatencyMS: 999999}}
	layers := []string{nodes.LayerBorda, nodes.LayerNevoa, "cloud", "", "outro"}
	for _, layer := range layers {
		for _, slo := range slos {
			if !c.HardLimit(nodes.Node{Layer: layer}, slo, defaultLimits) {
				t.Fatalf("HardLimit layer=%q slo=%+v = false, esperado true", layer, slo)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// RankNodes
// ---------------------------------------------------------------------------

func TestRankNodes(t *testing.T) {
	t.Run("borda antes de nevoa", func(t *testing.T) {
		candidates := []nodes.Node{
			{NodeID: "nevoa1", Layer: nodes.LayerNevoa},
			{NodeID: "borda1", Layer: nodes.LayerBorda},
		}
		latest := map[string]telemetry.Package{
			"nevoa1": {CPUUsagePct: 5},
			"borda1": {CPUUsagePct: 90}, // the higher CPU load, but the edge wins
		}
		// High limits avoid filtering by overload in the ordering test.
		cfg := LimitsConfig{MaxCPUPct: 100, MaxMemPct: 100, MaxPSIAvg10: 100}
		got := RankNodes(candidates, latest, nil, cfg)
		if len(got) != 2 || got[0].NodeID != "borda1" || got[1].NodeID != "nevoa1" {
			t.Fatalf("ordem borda antes nevoa: got %v", ids(got))
		}
	})

	t.Run("cpu asc dentro mesma camada", func(t *testing.T) {
		candidates := []nodes.Node{
			{NodeID: "b3", Layer: nodes.LayerBorda},
			{NodeID: "b1", Layer: nodes.LayerBorda},
			{NodeID: "b2", Layer: nodes.LayerBorda},
		}
		latest := map[string]telemetry.Package{
			"b1": {CPUUsagePct: 10},
			"b2": {CPUUsagePct: 30},
			"b3": {CPUUsagePct: 50},
		}
		cfg := LimitsConfig{MaxCPUPct: 100, MaxMemPct: 100, MaxPSIAvg10: 100}
		got := RankNodes(candidates, latest, nil, cfg)
		want := []string{"b1", "b2", "b3"}
		if !equalIDs(got, want) {
			t.Fatalf("cpu asc want %v got %v", want, ids(got))
		}
	})

	t.Run("sem telemetria vai pro fim Inf", func(t *testing.T) {
		candidates := []nodes.Node{
			{NodeID: "b-com", Layer: nodes.LayerBorda},
			{NodeID: "b-sem", Layer: nodes.LayerBorda},
			{NodeID: "b-com2", Layer: nodes.LayerBorda},
		}
		latest := map[string]telemetry.Package{
			"b-com":  {CPUUsagePct: 20},
			"b-com2": {CPUUsagePct: 40},
			// b-sem ausente => +Inf
		}
		cfg := LimitsConfig{MaxCPUPct: 100, MaxMemPct: 100, MaxPSIAvg10: 100}
		got := RankNodes(candidates, latest, nil, cfg)
		want := []string{"b-com", "b-com2", "b-sem"}
		if !equalIDs(got, want) {
			t.Fatalf("sem telemetria fim want %v got %v", want, ids(got))
		}
	})

	t.Run("camada desconhecida por ultimo", func(t *testing.T) {
		candidates := []nodes.Node{
			{NodeID: "x", Layer: "cloud"},
			{NodeID: "n", Layer: nodes.LayerNevoa},
			{NodeID: "b", Layer: nodes.LayerBorda},
		}
		latest := map[string]telemetry.Package{
			"x": {CPUUsagePct: 1},
			"n": {CPUUsagePct: 1},
			"b": {CPUUsagePct: 1},
		}
		cfg := LimitsConfig{MaxCPUPct: 100, MaxMemPct: 100, MaxPSIAvg10: 100}
		got := RankNodes(candidates, latest, nil, cfg)
		want := []string{"b", "n", "x"}
		if !equalIDs(got, want) {
			t.Fatalf("camada rank want %v got %v", want, ids(got))
		}
	})

	t.Run("sem filtragem por SLO (teto de camada removido)", func(t *testing.T) {
		candidates := []nodes.Node{
			{NodeID: "borda1", Layer: nodes.LayerBorda},
			{NodeID: "nevoa1", Layer: nodes.LayerNevoa},
			{NodeID: "borda2", Layer: nodes.LayerBorda},
		}
		latest := map[string]telemetry.Package{
			"borda1": {CPUUsagePct: 10},
			"nevoa1": {CPUUsagePct: 10},
			"borda2": {CPUUsagePct: 10},
		}
		// An objective of eight seconds excludes no node, so every node passes and
		// the order is only by layer and processor usage.
		slo := &SLO{MaxLatencyMS: 8000}
		got := RankNodes(candidates, latest, slo, defaultLimits)
		if len(got) != 3 || got[0].NodeID != "borda1" || got[1].NodeID != "borda2" || got[2].NodeID != "nevoa1" {
			t.Fatalf("sem filtragem por SLO want [borda1 borda2 nevoa1] got %v", ids(got))
		}
	})

	t.Run("filtragem Overloaded integrada", func(t *testing.T) {
		candidates := []nodes.Node{
			{NodeID: "b1", Layer: nodes.LayerBorda},
			{NodeID: "b2", Layer: nodes.LayerBorda},
			{NodeID: "n1", Layer: nodes.LayerNevoa},
		}
		latest := map[string]telemetry.Package{
			"b1": {CPUUsagePct: 10}, // ok
			"b2": {CPUUsagePct: 90}, // sobrecarregado (>85)
			"n1": {CPUUsagePct: 10}, // ok
		}
		got := RankNodes(candidates, latest, nil, defaultLimits)
		want := []string{"b1", "n1"}
		if !equalIDs(got, want) {
			t.Fatalf("overloaded filtragem want %v got %v", want, ids(got))
		}
	})

	t.Run("filtragem por sobrecarga (sem hardlimit de SLO)", func(t *testing.T) {
		candidates := []nodes.Node{
			{NodeID: "b1", Layer: nodes.LayerBorda},
			{NodeID: "b2", Layer: nodes.LayerBorda},
			{NodeID: "n1", Layer: nodes.LayerNevoa},
			{NodeID: "n2", Layer: nodes.LayerNevoa},
		}
		latest := map[string]telemetry.Package{
			"b1": {CPUUsagePct: 10}, // the objective no longer excludes the edge
			"b2": {CPUUsagePct: 10},
			"n1": {CPUUsagePct: 95}, // an overloaded fog is excluded
			"n2": {CPUUsagePct: 10}, // an idle fog
		}
		slo := &SLO{MaxLatencyMS: 8000}
		got := RankNodes(candidates, latest, slo, defaultLimits)
		want := []string{"b1", "b2", "n2"}
		if !equalIDs(got, want) {
			t.Fatalf("filtragem por sobrecarga want %v got %v", want, ids(got))
		}
	})

	t.Run("sem telemetria nao e sobrecarregado passa", func(t *testing.T) {
		candidates := []nodes.Node{
			{NodeID: "b1", Layer: nodes.LayerBorda},
		}
		latest := map[string]telemetry.Package{} // vazio => ok=false => Overloaded false
		got := RankNodes(candidates, latest, nil, defaultLimits)
		if len(got) != 1 || got[0].NodeID != "b1" {
			t.Fatalf("sem telemetria deveria passar want [b1] got %v", ids(got))
		}
	})

	t.Run("ordenacao com PSI sobrecarregado filtrado", func(t *testing.T) {
		candidates := []nodes.Node{
			{NodeID: "b1", Layer: nodes.LayerBorda},
			{NodeID: "b2", Layer: nodes.LayerBorda},
		}
		latest := map[string]telemetry.Package{
			"b1": {CPUUsagePct: 10, CPUPressure: psiPtr(30)}, // sobrecarregado via PSI
			"b2": {CPUUsagePct: 10},
		}
		got := RankNodes(candidates, latest, nil, defaultLimits)
		if len(got) != 1 || got[0].NodeID != "b2" {
			t.Fatalf("psi overloaded filtrado want [b2] got %v", ids(got))
		}
	})

	t.Run("entrada vazia => vazio", func(t *testing.T) {
		got := RankNodes(nil, nil, nil, defaultLimits)
		if len(got) != 0 {
			t.Fatalf("nil input want vazio got %v", ids(got))
		}
		got2 := RankNodes([]nodes.Node{}, map[string]telemetry.Package{}, nil, defaultLimits)
		if len(got2) != 0 {
			t.Fatalf("empty input want vazio got %v", ids(got2))
		}
	})

	t.Run("todos filtrados => vazio", func(t *testing.T) {
		candidates := []nodes.Node{
			{NodeID: "b1", Layer: nodes.LayerBorda},
			{NodeID: "b2", Layer: nodes.LayerBorda},
		}
		latest := map[string]telemetry.Package{
			"b1": {CPUUsagePct: 90},
			"b2": {CPUUsagePct: 90},
		}
		got := RankNodes(candidates, latest, nil, defaultLimits)
		if len(got) != 0 {
			t.Fatalf("todos sobrecarregados want vazio got %v", ids(got))
		}
	})
}

func TestRankNodes_Estabilidade(t *testing.T) {
	candidates := []nodes.Node{
		{NodeID: "b1", Layer: nodes.LayerBorda},
		{NodeID: "b2", Layer: nodes.LayerBorda},
	}
	latest := map[string]telemetry.Package{
		"b1": {CPUUsagePct: 10},
		"b2": {CPUUsagePct: 10},
	}
	cfg := LimitsConfig{MaxCPUPct: 100, MaxMemPct: 100, MaxPSIAvg10: 100}
	got := RankNodes(candidates, latest, nil, cfg)
	want := []string{"b1", "b2"}
	if !equalIDs(got, want) {
		t.Fatalf("estabilidade SliceStable want %v got %v", want, ids(got))
	}

	// The reverse order in the input prevails when the processor usage is equal.
	candidates2 := []nodes.Node{
		{NodeID: "b2", Layer: nodes.LayerBorda},
		{NodeID: "b1", Layer: nodes.LayerBorda},
	}
	got2 := RankNodes(candidates2, latest, nil, cfg)
	want2 := []string{"b2", "b1"}
	if !equalIDs(got2, want2) {
		t.Fatalf("estabilidade inversa want %v got %v", want2, ids(got2))
	}
}

func TestRankNodes_Estabilidade_SemTelemetria(t *testing.T) {
	candidates := []nodes.Node{
		{NodeID: "b1", Layer: nodes.LayerBorda},
		{NodeID: "b2", Layer: nodes.LayerBorda},
	}
	latest := map[string]telemetry.Package{} // ambos +Inf
	cfg := LimitsConfig{MaxCPUPct: 100, MaxMemPct: 100, MaxPSIAvg10: 100}
	got := RankNodes(candidates, latest, nil, cfg)
	want := []string{"b1", "b2"}
	if !equalIDs(got, want) {
		t.Fatalf("estabilidade sem telemetria want %v got %v", want, ids(got))
	}
}

// helpers

func ids(nodes []nodes.Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.NodeID
	}
	return out
}

func equalIDs(got []nodes.Node, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i, id := range want {
		if got[i].NodeID != id {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Helpers shared by the tests of the package, which cover the queue selector,
// the risk selector and the pure functions. The previous selector was retired,
// and the tests of the behavior that was exclusive to it were removed with it.
// ---------------------------------------------------------------------------

func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

// nodeIDs returns only the identifiers, and it is duplicated from the gateway
// tests on purpose, since tests of different packages do not share identifiers.
func nodeIDs(ns []nodes.Node) []string {
	ids := make([]string, len(ns))
	for i, n := range ns {
		ids[i] = n.NodeID
	}
	return ids
}

func scBucket(bucket string) history.ScenarioContext {
	return history.ScenarioContext{LoadBucket: bucket}
}
