package selection

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// seedQueueFaixa records the evidence of a single band of the number of
// requests in flight for the pair of group and node: the successes are warm
// deliveries, the failures are timeouts and the saturations are cuts and
// refusals, which stay outside the delivery rate.
func seedQueueFaixa(t *testing.T, h *history.History, group, nodeID string, now time.Time, k, successes, failures, saturations int) {
	t.Helper()
	for i := 0; i < successes; i++ {
		seedQueueEntrySel(t, h, group, nodeID, now, history.OutcomeSucesso, k)
	}
	for i := 0; i < failures; i++ {
		seedQueueEntrySel(t, h, group, nodeID, now, history.OutcomeTimeout, k)
	}
	for i := 0; i < saturations; i++ {
		seedQueueEntrySel(t, h, group, nodeID, now, history.OutcomeOrcamentoExcedido, k)
	}
}

func seedQueueEntrySel(t *testing.T, h *history.History, group, nodeID string, ts time.Time, outcome string, k int) {
	t.Helper()
	if err := h.AppendWithGroup(history.Entry{
		FunctionName:      "detect",
		NodeID:            nodeID,
		Layer:             nodes.LayerBorda,
		Timestamp:         ts,
		ScenarioContext:   history.ScenarioContext{LoadBucket: "0-10%"},
		ObservedLatencyMS: 100,
		Outcome:           outcome,
		Inflight:          k,
	}, group); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
}

// TestPredictMaxQueueK fixes the policy of the learned queue ceiling: the first
// band with enough evidence and a majority of failures, judged by the veto over
// the smoothed rate, defines the ceiling one below the band; a band without
// evidence does not prove a limit, saturation does not enter the rate, and
// without a bad band the selector invents no ceiling. This contract calibrates
// the ceiling of the gate, since without it the limit would become a fixed
// number. The logic is the shared function that the canonical risk selector
// exposes through the prediction port.
func TestPredictMaxQueueK(t *testing.T) {
	now := time.Date(2026, 10, 2, 21, 0, 0, 0, time.UTC)
	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h1"}
	sel := func(h *history.History) (int, bool) {
		return maxQueueKForNode(h, "detect", node, now)
	}
	newHist := func(t *testing.T) *history.History {
		t.Helper()
		h, err := history.NewHistory("", 500)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		return h
	}

	t.Run("colapso em 3-5 define teto 2 (borda)", func(t *testing.T) {
		h := newHist(t)
		seedQueueFaixa(t, h, "detect", "n1", now, 1, 10, 0, 0)
		seedQueueFaixa(t, h, "detect", "n1", now, 2, 5, 1, 0)
		seedQueueFaixa(t, h, "detect", "n1", now, 4, 6, 13, 0)
		if k, ok := sel(h); !ok || k != 2 {
			t.Fatalf("PredictMaxQueueK = (%d, %v); want (2, true)", k, ok)
		}
	})

	t.Run("sem vao ruim e fail-open (nevoa)", func(t *testing.T) {
		h := newHist(t)
		seedQueueFaixa(t, h, "detect", "n1", now, 1, 10, 0, 0)
		seedQueueFaixa(t, h, "detect", "n1", now, 2, 5, 1, 0)
		seedQueueFaixa(t, h, "detect", "n1", now, 4, 6, 0, 20) // a saturation is not a delivery failure
		seedQueueFaixa(t, h, "detect", "n1", now, 30, 9, 1, 0)
		if k, ok := sel(h); ok || k != 0 {
			t.Fatalf("PredictMaxQueueK = (%d, %v); want (0, false)", k, ok)
		}
	})

	t.Run("evidencia insuficiente nao define teto", func(t *testing.T) {
		h := newHist(t)
		seedQueueFaixa(t, h, "detect", "n1", now, 4, 1, 3, 0) // 4 < MinClassSamples
		if k, ok := sel(h); ok || k != 0 {
			t.Fatalf("PredictMaxQueueK = (%d, %v); want (0, false)", k, ok)
		}
	})

	t.Run("colapso ja em k=1 define teto 1", func(t *testing.T) {
		h := newHist(t)
		seedQueueFaixa(t, h, "detect", "n1", now, 1, 1, 5, 0)
		if k, ok := sel(h); !ok || k != 1 {
			t.Fatalf("PredictMaxQueueK = (%d, %v); want (1, true)", k, ok)
		}
	})

	t.Run("historico vazio e fail-open", func(t *testing.T) {
		h := newHist(t)
		if k, ok := sel(h); ok || k != 0 {
			t.Fatalf("PredictMaxQueueK = (%d, %v); want (0, false)", k, ok)
		}
	})
}
