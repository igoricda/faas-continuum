package gateway

// Test of the keying by group in the recording path: the recording of an attempt
// stores the statistics under the resolved group, which shares the curve between
// the variants, while the entry preserves the request. The recording of a cloud
// attempt never touches the history, which keeps the exclusion.

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func TestRecordAttemptGroupKeying(t *testing.T) {
	h, err := history.NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Clock: fixedClock(now)})

	node := nodes.Node{NodeID: "borda-1"}
	slo := &selection.SLO{MaxLatencyMS: 500}
	// Two variants share the resolved group, since the request of the tiny variant maps to the category.
	gw.recordAttempt(node, 5*time.Millisecond, nil, slo, "alpr-yolo-tiny", "alpr-yolo", nil, true, "", 0, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})
	gw.recordAttempt(node, 7*time.Millisecond, nil, slo, "alpr-yolo-full", "alpr-yolo", nil, true, "", 0, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})

	sc := history.ScenarioFrom(telemetry.Package{}, false)
	groupKey := history.SegmentKey("alpr-yolo", "borda-1", sc)
	count, _, _, _, ok := h.Stats(groupKey)
	if !ok || count != 2 {
		t.Fatalf("Stats(%q) = (%d,%v), esperado (2,true): variantes partilham a chave de grupo", groupKey, count, ok)
	}
	// The entry preserves the request in each line.
	keys := h.KeysForNode("borda-1")
	if len(keys) != 1 {
		t.Fatalf("KeysForNode = %d chaves, esperado 1", len(keys))
	}
}

func TestRecordCloudAttemptNeverTouchesHistory(t *testing.T) {
	h, err := history.NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Clock: fixedClock(now)})

	slo := &selection.SLO{MaxLatencyMS: 500}
	gw.recordCloudAttempt(100*time.Millisecond, nil, slo, "fn-a", nil)
	if keys := h.Keys(); len(keys) != 0 {
		t.Fatalf("History tem %d chaves após cloud attempt, esperado 0 (nuvem nunca gravada)", len(keys))
	}
}
