// Fixtures numéricas de decisão (SELETOR-V2 §13): sem aleatoriedade
// (band=0, explore=0, Rand nil), tudo warm, nenhuma requisição termina
// entre decisões (k só cresce no nó escolhido).
//
// Cenário: borda c=2, S=1,0s; névoa c=4, S=0,8s; W = max(0,k+1−c)·S/c;
// stretch=2; queue_allowance=1.0 (teto: borda 4, névoa 8).
//
// Seed determinístico (sem RNG): 500 sucessos idênticos por nó (M=1).
// O S_up aprendido excede S_med em ~2% (t-quantil com n=500), o que
// preserva todas as margens da tabela EXCETO o empate idealizado da
// 8ª decisão sem SLO... (ver TestRiskFixture_NoSLO). A linha com SLO
// diverge em 1 decisão pelo mesmo motivo (ver TestRiskFixture_SLO).
package selection

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

var fixtureTime = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

type fixtureGauge struct {
	k map[string]int
}

func (g *fixtureGauge) nodeK(id string) int   { return g.k["n\x00"+id] }
func (g *fixtureGauge) fnK(id, fn string) int { return g.k["f\x00"+id+"\x00"+fn] }
func (g *fixtureGauge) inc(id, fn string) {
	g.k["n\x00"+id]++
	g.k["f\x00"+id+"\x00"+fn]++
}

type fixtureWorld struct {
	sel   *RiskSelector
	gauge *fixtureGauge
	borda nodes.Node
	nevoa nodes.Node
	group string
}

func newFixtureWorld(t *testing.T, cfg RiskConfig) *fixtureWorld {
	t.Helper()
	mon := monitor.NewMonitor()
	borda := nodes.Node{NodeID: "borda", Layer: nodes.LayerBorda, Host: "h1", Functions: []string{"fn"}}
	nevoa := nodes.Node{NodeID: "nevoa", Layer: nodes.LayerNevoa, Host: "h2", Functions: []string{"fn"}}
	mon.UpdateCapacity("borda", "fn", telemetry.FunctionCapacity{MaxInstances: 2, InstancesAvailable: 2}, fixtureTime)
	mon.UpdateCapacity("nevoa", "fn", telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 4}, fixtureTime)
	g := &fixtureGauge{k: map[string]int{}}
	learner := learn.NewLearner(cfg.LearnConfig())
	group := ResolveRequestGroup("fn", []nodes.Node{borda, nevoa})
	// Seed idêntico e determinístico: S=1,0s na borda, S=0,8s na névoa.
	for i := 0; i < 500; i++ {
		learner.Observe(group, "borda", learn.Obs{At: fixtureTime, Outcome: learn.OutcomeSuccess, LatencyMS: 1000, UseRegr: true, M: 1})
		learner.Observe(group, "nevoa", learn.Obs{At: fixtureTime, Outcome: learn.OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: 1})
	}
	sel := &RiskSelector{
		Base:         QueueSelector{Mon: mon, Cfg: LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}},
		Clock:        func() time.Time { return fixtureTime },
		InflightOf:   g.nodeK,
		InflightOfFn: g.fnK,
		Learner:      learner,
		Cfg:          cfg,
		Stats:        &RiskStats{},
	}
	return &fixtureWorld{sel: sel, gauge: g, borda: borda, nevoa: nevoa, group: group}
}

func (w *fixtureWorld) decide(slo *SLO) string {
	out := w.sel.Select([]nodes.Node{w.borda, w.nevoa}, "fn", slo)
	if len(out) == 0 {
		return "∅"
	}
	first := out[0].NodeID
	if first == "borda" {
		return "B"
	}
	if first == "nevoa" {
		return "N"
	}
	return "?"
}

func (w *fixtureWorld) run(slo *SLO, n int) string {
	got := ""
	for i := 0; i < n; i++ {
		d := w.decide(slo)
		got += d + " "
		if d == "B" {
			w.gauge.inc("borda", "fn")
		} else if d == "N" {
			w.gauge.inc("nevoa", "fn")
		}
	}
	return got
}

func fixtureCfg() RiskConfig {
	cfg := DefaultRiskConfig()
	cfg.Band = 0
	cfg.Explore = 0
	return cfg
}

// TestRiskFixture_NoSLO: Sem SLO, stretch=2 => B B B N N N N B N N.
func TestRiskFixture_NoSLO(t *testing.T) {
	w := newFixtureWorld(t, fixtureCfg())
	if got, want := w.run(nil, 10), "B B B N N N N B N N "; got != want {
		t.Fatalf("sequência = %q, want %q", got, want)
	}
}

// TestRiskFixture_SLO: SLO T=1,2s, stretch_slo=0.
//
// DIVERGÊNCIA da tabela idealizada (§13): a tabela dá
// "B B N N N N N N ∅ ∅" com B_nevoa = T = 1,2s exatos na 8ª decisão.
// Num modelo aprendido, S_up > S_med estritamente (t-quantil com
// alpha < 0.5), então B_nevoa = 0,4·S_med + S_up > 1,2s e a 8ª decisão
// já transborda para a nuvem: "B B N N N N N ∅ ∅ ∅". O regime é o
// mesmo (saturação => nuvem, uma decisão antes); registrar no ADR-0012.
func TestRiskFixture_SLO(t *testing.T) {
	w := newFixtureWorld(t, fixtureCfg())
	slo := &SLO{MaxLatencyMS: 1200}
	if got, want := w.run(slo, 10), "B B N N N N N ∅ ∅ ∅ "; got != want {
		t.Fatalf("sequência = %q, want %q", got, want)
	}
	if got := w.sel.Stats.EmptyQueue.Load(); got != 3 {
		t.Fatalf("empty_queue = %d, want 3", got)
	}
}

// TestRiskFixture_NoStretchNoCap: Sem SLO, stretch=∞, sem teto => B ×10
// (documenta o "tudo na borda" que o RiskSelector corrige com stretch finito).
func TestRiskFixture_NoStretchNoCap(t *testing.T) {
	cfg := fixtureCfg()
	cfg.Stretch = 1e9
	cfg.QueueAllowance = 1e9
	w := newFixtureWorld(t, cfg)
	if got, want := w.run(nil, 10), "B B B B B B B B B B "; got != want {
		t.Fatalf("sequência = %q, want %q", got, want)
	}
}
