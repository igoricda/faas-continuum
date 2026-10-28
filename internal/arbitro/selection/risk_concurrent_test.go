// Concurrency of the selector: sixty-four goroutines running a thousand
// selections each, with a simulated reservation and release. The fresh number of
// requests in flight and the band come first, and the atomic reservation exists
// only because this test fails without it.
package selection

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

type herdGauge struct {
	mu  sync.Mutex
	k   map[string]int
	max map[string]int
}

func newHerdGauge() *herdGauge {
	return &herdGauge{k: map[string]int{}, max: map[string]int{}}
}

func (g *herdGauge) nodeK(id string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.k["n\x00"+id]
}

func (g *herdGauge) fnK(id, fn string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.k["f\x00"+id+"\x00"+fn]
}

// reserve incrementa e anota o pico; release decrementa.
func (g *herdGauge) reserve(id, fn string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.k["n\x00"+id]++
	g.k["f\x00"+id+"\x00"+fn]++
	for _, key := range []string{"n\x00" + id, "f\x00" + id + "\x00" + fn} {
		if g.k[key] > g.max[key] {
			g.max[key] = g.k[key]
		}
	}
}

func (g *herdGauge) release(id, fn string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.k["n\x00"+id]--
	g.k["f\x00"+id+"\x00"+fn]--
}

func TestConcurrentSelect_NoHerd(t *testing.T) {
	mon := monitor.NewMonitor()
	now := time.Now()
	var cands []nodes.Node
	ids := []string{"n1", "n2", "n3", "n4"}
	for _, id := range ids {
		cands = append(cands, nodes.Node{NodeID: id, Layer: nodes.LayerBorda, Host: "h", Functions: []string{"fn"}})
		mon.UpdateCapacity(id, "fn", telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 4}, now)
	}
	g := newHerdGauge()
	cfg := DefaultRiskConfig()
	learner := learn.NewLearner(cfg.LearnConfig())
	for _, id := range ids {
		for i := 0; i < 20; i++ {
			learner.Observe("fn", id, learn.Obs{At: now, Outcome: learn.OutcomeSuccess, LatencyMS: 500, UseRegr: true, M: 1})
		}
	}
	sel := &RiskSelector{
		Base:         QueueSelector{Mon: mon, Cfg: LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}},
		Clock:        func() time.Time { return now },
		InflightOf:   g.nodeK,
		InflightOfFn: g.fnK,
		Learner:      learner,
		Cfg:          cfg,
		Rand:         rand.New(rand.NewSource(1)),
		Stats:        &RiskStats{},
	}
	var firstCount [4]atomic.Int64
	var emptyCount atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				out := sel.Select(cands, "fn", nil)
				if len(out) == 0 {
					emptyCount.Add(1)
					continue
				}
				for j, id := range ids {
					if out[0].NodeID == id {
						firstCount[j].Add(1)
					}
				}
				// Simulated reservation and release, which holds the node briefly.
				g.reserve(out[0].NodeID, "fn")
				g.release(out[0].NodeID, "fn")
			}
		}()
	}
	wg.Wait()
	total := int64(0)
	maxShare := 0.0
	for j := range ids {
		c := firstCount[j].Load()
		total += c
		if share := float64(c) / 64000; share > maxShare {
			maxShare = share
		}
	}
	if total != 64000 {
		t.Fatalf("primeiras escolhas = %d, want 64000 (empty=%d)", total, emptyCount.Load())
	}
	if maxShare > 0.70 {
		t.Fatalf("maior fatia = %.2f, want < 0.70 (rebanho entre equivalentes)", maxShare)
	}
	t.Logf("fatia máxima = %.3f (4 equivalentes, banda 0.10)", maxShare)
	// Gauge zerado no fim (Dec simulado 1:1 com a reserva).
	for _, id := range ids {
		if g.nodeK(id) != 0 || g.fnK(id, "fn") != 0 {
			t.Fatalf("gauge %s = %d/%d, want 0/0", id, g.nodeK(id), g.fnK(id, "fn"))
		}
	}
}
