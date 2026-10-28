// Numerical fixtures of the decision, without randomness, with a zero band, no
// exploration and no random generator, with every node warm, and with no request
// finishing between decisions, so the number of requests in flight only grows on
// the chosen node.
//
// Scenario: the edge with a capacity of two and a service time of one second,
// the fog with a capacity of four and a service time of eight tenths of a
// second, the wait of the excess over the capacity scaled by the service time,
// a stretch of two and a queue allowance of one, which gives a ceiling of four
// on the edge and of eight on the fog.
//
// The seed is deterministic, without a random generator: five hundred identical
// successes per node. The learned upper bound of the service time exceeds the
// measured value by about two percent, from the quantile with five hundred
// samples, which preserves every margin of the table except the idealized tie of
// the eighth decision without an objective. The row with an objective diverges in
// one decision for the same reason.
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
	// The seed is identical and deterministic, with one second on the edge and eight tenths of a second on the fog.
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

// TestRiskFixture_SLO covers the table with an objective of twelve hundred
// milliseconds and without a stretch on the objective.
//
// The idealized table has the fog exactly at the objective on the eighth
// decision, but in a learned model the upper bound of the service time is
// strictly above the measured value, since the quantile is taken below one half,
// so the bound exceeds the objective and the eighth decision already overflows to
// the cloud. The regime is the same, with saturation leading to the cloud one
// decision earlier, which is recorded in the documentation of the selector.
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
