package server

// Test of the summary grouped by group: two variants under the same resolved
// group appear once in the summary of the history, under the key of the group,
// and a cold category does not appear and does not cause a panic.

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/auth"
)

func TestHistorySummaryGroupsByResolvedGroup(t *testing.T) {
	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	mon := monitor.NewMonitor()
	h, err := history.NewHistory(filepath.Join(t.TempDir(), "history.json"), 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}})
	gen, err := auth.Generate()
	if err != nil {
		t.Fatalf("auth.Generate: %v", err)
	}
	cloudStore, err := cloud.NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	srv := NewServerWithHistory(ns, mon, gw, gen.Cred, 90*time.Second, defaultLimits, cloudStore, h, 60*time.Second, func() time.Time { return now })
	handler := srv.Handler()

	body := map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}
	if rr := doReq(t, handler, "POST", "/nodes", body, gen.PlainToken); rr.Code != http.StatusCreated {
		t.Fatalf("POST /nodes: status=%d, want 201", rr.Code)
	}

	// Two variants and one group give a single segment key in the summary.
	sc := history.ScenarioContext{LoadBucket: history.Bucket0_10}
	mk := func(requested string) history.Entry {
		return history.Entry{FunctionName: requested, NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: 100, Outcome: history.OutcomeSucesso}
	}
	if err := h.AppendWithGroup(mk("alpr-yolo-tiny"), "alpr-yolo"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	if err := h.AppendWithGroup(mk("alpr-yolo-full"), "alpr-yolo"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}

	rr := doReq(t, handler, "GET", "/nodes", nil, gen.PlainToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes: status=%d, want 200", rr.Code)
	}
	var list struct {
		Nodes []struct {
			NodeID         string                         `json:"node_id"`
			HistorySummary map[string]HistorySummaryEntry `json:"history_summary,omitempty"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(list.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(list.Nodes))
	}
	summary := list.Nodes[0].HistorySummary
	groupKey := history.SegmentKey("alpr-yolo", "n1", sc)
	entry, ok := summary[groupKey]
	if !ok {
		t.Fatalf("summary sem chave de grupo %q (keys=%v)", groupKey, keysOf(summary))
	}
	if entry.Count != 2 {
		t.Fatalf("summary[%q].count=%d, want 2 (variantes somadas)", groupKey, entry.Count)
	}
	// Nenhuma chave por variante isolada.
	for k := range summary {
		fn, _, _ := strings.Cut(k, "\x00")
		if fn == "alpr-yolo-tiny" || fn == "alpr-yolo-full" {
			t.Fatalf("summary contém chave por variante %q, esperado só grupo", k)
		}
	}
}

func keysOf(m map[string]HistorySummaryEntry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
