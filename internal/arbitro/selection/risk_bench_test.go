// Benchmarks of the selection in the canonical selector, with four nodes and a
// seeded learner. A benchmark only measures, without an assertion on the order.
package selection

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func benchNodes4() []nodes.Node {
	var out []nodes.Node
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		out = append(out, nodes.Node{NodeID: id, Layer: nodes.LayerBorda, Host: "h", Functions: []string{"fn"}})
	}
	return out
}

func benchMonitor4(now time.Time) *monitor.Monitor {
	mon := monitor.NewMonitor()
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		mon.UpdateCapacity(id, "fn", telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 4}, now)
	}
	return mon
}

func BenchmarkSelectRisk(b *testing.B) {
	now := time.Now()
	mon := benchMonitor4(now)
	cfg := DefaultRiskConfig()
	learner := learn.NewLearner(cfg.LearnConfig())
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		for i := 0; i < 50; i++ {
			learner.Observe("fn", id, learn.Obs{At: now, Outcome: learn.OutcomeSuccess, LatencyMS: 500, UseRegr: true, M: 1})
		}
	}
	sel := &RiskSelector{
		Base:    QueueSelector{Mon: mon, Cfg: LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}},
		Clock:   func() time.Time { return now },
		Learner: learner,
		Cfg:     cfg,
		Stats:   &RiskStats{},
	}
	cands := benchNodes4()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sel.Select(cands, "fn", nil)
	}
}
