package selection

import (
	"slices"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// TestQueueSelectorQueueDepthDemotion proves the demotion by occupancy of the
// pool in the hard-limit path: the edge with a fresh positive queue depth enters
// behind the idle fog even when it would be preferred by layer, a node with a
// queue alone stays eligible, since the demotion never removes the node, and an
// unknown capacity or an expired reading counts as idle, which is the fail-open.
func TestQueueSelectorQueueDepthDemotion(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	edge := nodes.Node{NodeID: "edge", Layer: nodes.LayerBorda, Host: "h1", Functions: []string{"lpr"}}
	fog := nodes.Node{NodeID: "fog", Layer: nodes.LayerNevoa, Host: "h2", Functions: []string{"lpr"}}
	slo := &SLO{MaxLatencyMS: 5000}

	t.Run("fila fresca demove borda atrás de névoa ociosa", func(t *testing.T) {
		mon := monitor.NewMonitor()
		mon.UpdateCapacity("edge", "lpr", telemetry.FunctionCapacity{MaxInstances: 0, InstancesAvailable: 3, QueueDepth: 2}, now)
		mon.UpdateCapacity("fog", "lpr", telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 2, QueueDepth: 0}, now)
		sel := QueueSelector{Mon: mon, Cfg: defaultLimits, Clock: fixedClock(now)}
		out := sel.Select([]nodes.Node{edge, fog}, "lpr", slo)
		if got := nodeIDs(out); !slices.Equal(got, []string{"fog", "edge"}) {
			t.Fatalf("fila=%v want [fog edge] (borda com QueueDepth=2 demovida)", got)
		}
	})

	t.Run("fila sozinha não remove o nó", func(t *testing.T) {
		mon := monitor.NewMonitor()
		mon.UpdateCapacity("edge", "lpr", telemetry.FunctionCapacity{MaxInstances: 0, InstancesAvailable: 3, QueueDepth: 2}, now)
		sel := QueueSelector{Mon: mon, Cfg: defaultLimits, Clock: fixedClock(now)}
		out := sel.Select([]nodes.Node{edge}, "lpr", slo)
		if got := nodeIDs(out); !slices.Equal(got, []string{"edge"}) {
			t.Fatalf("fila=%v want [edge] (demotion, nunca remoção)", got)
		}
	})

	t.Run("capacidade desconhecida conta como ociosa", func(t *testing.T) {
		mon := monitor.NewMonitor()
		mon.UpdateCapacity("fog", "lpr", telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 2, QueueDepth: 1}, now)
		sel := QueueSelector{Mon: mon, Cfg: defaultLimits, Clock: fixedClock(now)}
		out := sel.Select([]nodes.Node{edge, fog}, "lpr", slo)
		if got := nodeIDs(out); !slices.Equal(got, []string{"edge", "fog"}) {
			t.Fatalf("fila=%v want [edge fog] (desconhecido é fail-open)", got)
		}
	})
}
