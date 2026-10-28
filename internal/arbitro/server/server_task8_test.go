package server

// function-category-routing, todo 8: audit gap-fill tests.
//
// Todo 2 already wires FunctionCategories into listEntry (omitempty) and
// todo 6 already keys history + summary by resolved group with the
// coexistence comment on historySummaryForNode. What was missing was a
// single GET asserting BOTH surfaces together in one entry, plus explicit
// hist-set-but-window<=0 / hist-nil-but-window>0 panic-free coverage
// (the F21 table only covers hist==nil&&window==0).

import (
	"encoding/json"
	"net/http"
	"path/filepath"
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

// GET shows categories map AND group-keyed summary in the same entry.
func TestListNodesCategoriesAndGroupSummary(t *testing.T) {
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

	body := map[string]any{
		"node_id":   "n1",
		"layer":     "borda",
		"host":      "10.0.0.1:8080",
		"functions": []string{"alpr-yolo-tiny", "alpr-yolo-full"},
		"function_categories": map[string]string{
			"alpr-yolo-tiny": "alpr-yolo",
			"alpr-yolo-full": "alpr-yolo",
		},
	}
	if rr := doReq(t, handler, "POST", "/nodes", body, gen.PlainToken); rr.Code != http.StatusCreated {
		t.Fatalf("POST /nodes: status=%d, want 201 body=%s", rr.Code, rr.Body.String())
	}

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
		t.Fatalf("GET /nodes: status=%d, want 200 body=%s", rr.Code, rr.Body.String())
	}
	var list struct {
		Nodes []struct {
			NodeID             string                         `json:"node_id"`
			FunctionCategories map[string]string              `json:"function_categories"`
			HistorySummary     map[string]HistorySummaryEntry `json:"history_summary,omitempty"`
		} `json:"nodes"`
		Raw []json.RawMessage `json:"-"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, rr.Body.String())
	}
	if len(list.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1 body=%s", len(list.Nodes), rr.Body.String())
	}
	entry := list.Nodes[0]
	if entry.FunctionCategories["alpr-yolo-tiny"] != "alpr-yolo" ||
		entry.FunctionCategories["alpr-yolo-full"] != "alpr-yolo" {
		t.Fatalf("function_categories mismatch: %+v", entry.FunctionCategories)
	}
	groupKey := history.SegmentKey("alpr-yolo", "n1", sc)
	sum, ok := entry.HistorySummary[groupKey]
	if !ok {
		t.Fatalf("history_summary sem chave de grupo %q (keys=%v)", groupKey, keysOf(entry.HistorySummary))
	}
	if sum.Count != 2 {
		t.Fatalf("history_summary[%q].count=%d, want 2", groupKey, sum.Count)
	}
}

// hist set + window<=0, and hist nil + window>0: 200, no history_summary
// key, no panic (normalization lives in NewServerWithHistory).
func TestListNodesHistoryViewDegenerateNoPanic(t *testing.T) {
	h, err := history.NewHistory(filepath.Join(t.TempDir(), "history.json"), 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	cases := []struct {
		name   string
		hist   *history.History
		window time.Duration
	}{
		{"hist set, window 0", h, 0},
		{"hist set, window negative", h, -time.Second},
		{"hist nil, window positive", nil, 60 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns, err := nodes.NewNodeStore("")
			if err != nil {
				t.Fatalf("NewNodeStore: %v", err)
			}
			mon := monitor.NewMonitor()
			gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}})
			gen, err := auth.Generate()
			if err != nil {
				t.Fatalf("auth.Generate: %v", err)
			}
			cloudStore, err := cloud.NewCloudStore("")
			if err != nil {
				t.Fatalf("NewCloudStore: %v", err)
			}
			srv := NewServerWithHistory(ns, mon, gw, gen.Cred, 90*time.Second, defaultLimits, cloudStore, tc.hist, tc.window, time.Now)
			handler := srv.Handler()

			body := map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}
			if rr := doReq(t, handler, "POST", "/nodes", body, gen.PlainToken); rr.Code != http.StatusCreated {
				t.Fatalf("POST /nodes: status=%d, want 201", rr.Code)
			}
			rr := doReq(t, handler, "GET", "/nodes", nil, gen.PlainToken)
			if rr.Code != http.StatusOK {
				t.Fatalf("GET /nodes: status=%d, want 200", rr.Code)
			}
			var list struct {
				Nodes []json.RawMessage `json:"nodes"`
			}
			if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if len(list.Nodes) != 1 {
				t.Fatalf("nodes = %d, want 1", len(list.Nodes))
			}
			var entry map[string]any
			if err := json.Unmarshal(list.Nodes[0], &entry); err != nil {
				t.Fatalf("unmarshal entry: %v", err)
			}
			if _, has := entry["history_summary"]; has {
				t.Fatalf("history_summary deve ser omitido (%s), got %v", tc.name, entry)
			}
		})
	}
}
