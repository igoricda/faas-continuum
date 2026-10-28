package selection

// Wiring do veto full PSI no Select (todo 12): o veto puro de psi_veto.go
// (defaults T_cpu=28, T_full=12) vive no loop por-nó do RiskSelector na
// ordem documentada projected-guard → PSI-veto, com fail-open do próprio
// seletor (restaura o conjunto SÓ se ele esvaziou por causa do veto).
// Monitor sem PSI => zero vetos PSI (compat). Limiar inválido => erro no
// Validate (fail-fast no wiring).
//
// O v1 (SelfLearningSelector) tinha o mesmo veto encaixado no fail-open do
// veto monotônico; com a aposentadoria do v1 (ADR-0012) o wiring passa a
// ser testado no RiskSelector, único seletor canônico.

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

// vetoMonitor monta o Monitor com PSI explícito por nó (cpu_some, mem_full);
// (0,0) sem campos => modo compatibilidade (sem PSI).
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
		// sick: CPU baixa (venceria o bootstrap) mas mem_full=20 > T_full=12
		// => vetado; healthy (CPU 50, PSI baixo) passa sozinho — sem revival,
		// pois aptList não esvazia. Com limiares frouxos o veto some e sick
		// volta a liderar: prova observável de que o veto (e não outra via)
		// decidiu.
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
		// Nó único vetado pelo PSI nunca esvazia sozinho: o fail-open do
		// RiskSelector restaura o conjunto psiVetoed quando a lista apta
		// esvaziou SÓ por causa do veto (§7.1) — o nó sai por bootstrap.
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
		// Regra única reconciliada (>0 em todas as camadas): defaults passam,
		// qualquer braço <=0 falha no config.Validate (o mesmo que
		// ValidatePSIVetoThresholds rejeita).
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
		// Overlay por flag/env chega ao mesmo Validate: flag negativa =>
		// Load ok (parse) mas Validate falha (main.go ainda aborta Fatal
		// antes, via validateStartupFlags, no mesmo predicado <=0).
		loaded, err := config.Load("/nonexistent-task12.jsonc", map[string]string{"psi-veto-cpu": "-3"})
		if err != nil {
			t.Fatalf("Load com flag negativa: %v", err)
		}
		if err := loaded.Validate(); err == nil {
			t.Fatalf("Validate após flag psi-veto-cpu=-3 = nil, want erro")
		}
	})
}
