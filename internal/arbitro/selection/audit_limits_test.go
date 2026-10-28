package selection

// Regressions of the queue selector with capacity, of the negative availability
// and of the full memory pressure: the pure ranking stays without a monitor, but
// the public queue selector does not reintroduce a send to an exhausted node, a
// negative value never evades the filter, and a thrashing node is vetoed.

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func auditLimitsCfg() LimitsConfig {
	return LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}
}

func TestOverloadedMemoryPressureFull(t *testing.T) {
	cfg := auditLimitsCfg()
	// A low processor pressure with a high full memory pressure, which is thrashing, means overload.
	pkg := telemetry.Package{
		CPUUsagePct:        10,
		MemoryUsagePct:     20,
		MemoryPressure:     &telemetry.PressureFields{Avg10: 5},
		MemoryPressureFull: &telemetry.PressureFields{Avg10: 40},
	}
	if !Overloaded(pkg, true, cfg) {
		t.Fatal("Overloaded(full=40) = false, esperado true (AUD-18: full entra no cálculo)")
	}
	// The full pressure exactly at the ceiling is not an overload, since the comparison is strict.
	pkg.MemoryPressureFull = &telemetry.PressureFields{Avg10: 25}
	if Overloaded(pkg, true, cfg) {
		t.Fatal("Overloaded(full==teto) = true, esperado false (> estrito)")
	}
	// An absent full pressure, from an older node, stays ignored, which preserves the compatibility.
	pkg.MemoryPressureFull = nil
	if Overloaded(pkg, true, cfg) {
		t.Fatal("Overloaded(full=nil) = true, esperado false (modo compatibilidade)")
	}
}

func TestFilterExhaustedNegativeAvail(t *testing.T) {
	now := time.Now()
	cands := []nodes.Node{{NodeID: "n1"}, {NodeID: "n2"}}
	// A negative availability filters as exhausted, while before only zero filtered.
	got := FilterExhausted(cands, map[string]telemetry.FunctionCapacity{"n1": {InstancesAvailable: -1}}, now)
	if len(got) != 1 || got[0].NodeID != "n2" {
		t.Fatalf("FilterExhausted(avail=-1) = %v, esperado só n2", got)
	}
}

func TestQueueSelectorFiltersExhausted(t *testing.T) {
	now := time.Now()
	mon := monitor.NewMonitor()
	n1 := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h1", Functions: []string{"sleep"}}
	n2 := nodes.Node{NodeID: "n2", Layer: nodes.LayerBorda, Host: "h2", Functions: []string{"sleep"}}
	// The queue selector filters by a fresh optimistic mark, as the risk selector
	// does. A genuinely cold node, with no available instance and without a mark, is
	// not filtered, so a cold fog stays eligible for a cold start instead of
	// emptying the queue to the cloud.
	mon.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 0}, now)
	sel := QueueSelector{Mon: mon, Cfg: auditLimitsCfg(), Clock: func() time.Time { return now }}
	if got := sel.Select([]nodes.Node{n1, n2}, "sleep", nil); len(got) != 2 {
		t.Fatalf("QueueSelector cold sem marca = %v, esperado 2 (não filtra)", got)
	}
	mon.MarkExhausted("n1", "sleep", now)
	got := sel.Select([]nodes.Node{n1, n2}, "sleep", nil)
	if len(got) != 1 || got[0].NodeID != "n2" {
		t.Fatalf("QueueSelector com n1 marcado = %v, esperado só n2", got)
	}
	// Fail-open: tudo marcado => lista original intacta (gateway tenta e
	// recebe 503 NODE_OVERLOADED em vez de 503 vazio silencioso).
	mon.MarkExhausted("n2", "sleep", now)
	got = sel.Select([]nodes.Node{n1, n2}, "sleep", nil)
	if len(got) != 2 {
		t.Fatalf("QueueSelector tudo-marcado = %v, esperado fail-open com 2", got)
	}
	// Sem dado de capacidade => sem gating (desconhecido nunca filtra).
	selFresh := QueueSelector{Mon: monitor.NewMonitor(), Cfg: auditLimitsCfg(), Clock: func() time.Time { return now }}
	got = selFresh.Select([]nodes.Node{n1, n2}, "sleep", nil)
	if len(got) != 2 {
		t.Fatalf("QueueSelector sem capacity = %v, esperado 2 (sem gating)", got)
	}
}
