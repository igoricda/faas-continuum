package selection

// Wiring of the full pressure veto in the selection: the pure veto, with the
// default thresholds, lives in the loop per node of the risk selector, in the
// documented order of the projected guard followed by the pressure veto, with the
// fail-open of the selector itself, which restores the set only when the veto
// emptied it. A monitor without pressure gives no pressure veto, which preserves
// the compatibility, and an invalid threshold is an error in the validation,
// which is the fail-fast of the wiring.
//
// The previous selector had the same veto inside the fail-open of the monotonic
// veto, and with its retirement the wiring is tested in the risk selector, the
// single canonical selector.

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// vetoMonitor builds the monitor with an explicit pressure per node, with the
// processor and the full memory averages, and a zero reading without the fields
// means the compatibility mode, without pressure.
func vetoMonitor(now time.Time, psi map[string][2]float64, usage map[string]float64) *monitor.Monitor {
	mon := monitor.NewMonitor()
	for id, p := range psi {
		pkg := telemetry.Package{NodeID: id, CPUUsagePct: usage[id], MemoryUsagePct: 5}
		if p[0] > 0 || p[1] > 0 {
			pkg.CPUPressure = psiPtr(p[0])
			pkg.MemoryPressureFull = psiPtr(p[1])
		}
		mon.UpdateAt(pkg, now)
	}
	return mon
}

func vetoNodes(ids ...string) []nodes.Node {
	out := make([]nodes.Node, 0, len(ids))
	for i, id := range ids {
		out = append(out, nodes.Node{NodeID: id, Layer: nodes.LayerBorda, Host: "h" + string(rune('1'+i))})
	}
	return out
}

func TestPSIVetoWiring(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	fn := "myfn"

	base := func(mon *monitor.Monitor, h *history.History) *RiskSelector {
		cfg := DefaultRiskConfig()
		return &RiskSelector{
			Base:             QueueSelector{Mon: mon, Cfg: defaultLimits},
			Hist:             h,
			Criteria:         nil, // nil => DefaultCriteria (sem SLO declarado => vacuamente apto)
			ThroughputWindow: 60 * time.Second,
			Clock:            fixedClock(now),
			Learner:          learn.NewLearner(cfg.LearnConfig()),
			Cfg:              cfg,
		}
	}

	t.Run("mem-full-acima-do-limiar-veta-observavel", func(t *testing.T) {
		// The sick node has a low processor usage, which would win the bootstrap, but
		// its full memory pressure exceeds the threshold, so it is vetoed, while the
		// healthy node passes alone, without a revival, since the eligible list does
		// not become empty. With loose thresholds the veto disappears and the sick
		// node leads again, which is an observable proof that the veto decided.
		mon := vetoMonitor(now,
			map[string][2]float64{"sick": {5, 20}, "healthy": {5, 5}},
			map[string]float64{"sick": 10, "healthy": 50})
		h, _ := history.NewHistory("", 500)
		out := base(mon, h).Select(vetoNodes("sick", "healthy"), fn, nil)
		if ids := nodeIDs(out); len(ids) != 1 || ids[0] != "healthy" {
			t.Fatalf("mem_full=20 deveria vetar sick, obteve %v", ids)
		}

		loose := base(mon, h)
		loose.PSIVetoThresholds = PSIVetoThresholds{CPUSomeAvg10: 100, MemFullAvg10: 100}
		out = loose.Select(vetoNodes("sick", "healthy"), fn, nil)
		if ids := nodeIDs(out); len(ids) != 2 || ids[0] != "sick" {
			t.Fatalf("limiares frouxos deveriam liberar sick em primeiro, obteve %v", ids)
		}
	})

	t.Run("veto-sozinho-revive-via-fail-open", func(t *testing.T) {
		// A single node vetoed by the pressure never empties the list alone: the
		// fail-open of the risk selector restores the vetoed set when the eligible
		// list became empty only because of the veto, so the node leaves through the
		// bootstrap.
		mon := vetoMonitor(now,
			map[string][2]float64{"sick": {5, 20}},
			map[string]float64{"sick": 10})
		h, _ := history.NewHistory("", 500)
		out := base(mon, h).Select(vetoNodes("sick"), fn, nil)
		if ids := nodeIDs(out); len(ids) != 1 || ids[0] != "sick" {
			t.Fatalf("fail-open deveria resgatar sick vetado, obteve %v", ids)
		}
	})

	t.Run("monitor-sem-psi-zero-vetos", func(t *testing.T) {
		// Compat: telemetria sem campos PSI (modo antigo) => PSIVetoFull
		// fail-open => nenhum veto; o de CPU menor lidera normalmente.
		mon := vetoMonitor(now,
			map[string][2]float64{"a": {0, 0}, "b": {0, 0}},
			map[string]float64{"a": 10, "b": 50})
		h, _ := history.NewHistory("", 500)
		out := base(mon, h).Select(vetoNodes("a", "b"), fn, nil)
		if ids := nodeIDs(out); len(ids) != 2 || ids[0] != "a" {
			t.Fatalf("sem PSI ambos deveriam passar com a primeiro, obteve %v", ids)
		}
	})

	t.Run("zero-means-default-explicit-verbatim", func(t *testing.T) {
		if got := effectivePSIVetoOf(PSIVetoThresholds{}); got != DefaultPSIVetoThresholds() {
			t.Fatalf("zero deveria resolver para defaults, obteve %+v", got)
		}
		explicit := PSIVetoThresholds{CPUSomeAvg10: 30, MemFullAvg10: 15}
		if got := effectivePSIVetoOf(explicit); got != explicit {
			t.Fatalf("explícito deveria passar verbatim, obteve %+v", got)
		}
	})

	t.Run("validate-fail-fast", func(t *testing.T) {
		// The single reconciled rule, with a positive value required at every layer:
		// the defaults pass, and any threshold at or below zero fails the validation
		// of the configuration, which is the same predicate of the pressure veto.
		if err := config.Defaults().Validate(); err != nil {
			t.Fatalf("Defaults().Validate() = %v, want nil (defaults 28/12)", err)
		}
		for _, th := range []PSIVetoThresholds{{0, 12}, {28, 0}, {-1, 12}, {28, -5}} {
			if err := ValidatePSIVetoThresholds(th); err == nil {
				t.Fatalf("ValidatePSIVetoThresholds(%+v) = nil, want erro", th)
			}
			bad := config.Defaults()
			bad.Arbitro.PSIVetoCPUSomeAvg10 = th.CPUSomeAvg10
			bad.Arbitro.PSIVetoMemFullAvg10 = th.MemFullAvg10
			if err := bad.Validate(); err == nil {
				t.Fatalf("config.Validate() com psi-veto %+v = nil, want erro", th)
			}
		}
		// An overlay by flag or by environment reaches the same validation, so a
		// negative flag parses correctly and fails the validation, which aborts the
		// startup on the same predicate.
		loaded, err := config.Load("/nonexistent-task12.jsonc", map[string]string{"psi-veto-cpu": "-3"})
		if err != nil {
			t.Fatalf("Load com flag negativa: %v", err)
		}
		if err := loaded.Validate(); err == nil {
			t.Fatalf("Validate após flag psi-veto-cpu=-3 = nil, want erro")
		}
	})
}
