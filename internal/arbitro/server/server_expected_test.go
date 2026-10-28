package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// POST with valid function_expected_metrics persists and GET echoes them.
func TestRegisterNodeFunctionExpected(t *testing.T) {
	srv, token, ns, _ := newTestServerEnv(t)
	h := srv.Handler()

	body := map[string]any{
		"node_id":   "n1",
		"layer":     "borda",
		"host":      "h:1",
		"functions": []string{"alpr-yolo-tiny"},
		"function_expected_metrics": map[string]map[string]any{
			"alpr-yolo-tiny": {"accuracy": map[string]any{"value": 0.9, "higher_is_better": true}},
		},
	}
	rr := doReq(t, h, "POST", "/nodes", body, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("POST status=%d want 201 body=%s", rr.Code, rr.Body.String())
	}
	var created nodes.Node
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created: %v body=%s", err, rr.Body.String())
	}
	want := map[string]map[string]registry.ExpectedMetric{
		"alpr-yolo-tiny": {"accuracy": {Value: 0.9, HigherIsBetter: true}},
	}
	if len(created.FunctionExpected) != 1 || created.FunctionExpected["alpr-yolo-tiny"]["accuracy"] != want["alpr-yolo-tiny"]["accuracy"] {
		t.Fatalf("created expected mismatch: %+v", created.FunctionExpected)
	}
	// Persisted in store.
	stored, ok := ns.Get("n1")
	if !ok {
		t.Fatalf("node n1 not in store")
	}
	if len(stored.FunctionExpected) != 1 || stored.FunctionExpected["alpr-yolo-tiny"]["accuracy"] != want["alpr-yolo-tiny"]["accuracy"] {
		t.Fatalf("stored expected mismatch: %+v", stored.FunctionExpected)
	}
	// GET echoes them.
	rr = doReq(t, h, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status=%d want 200 body=%s", rr.Code, rr.Body.String())
	}
	var list struct {
		Nodes []struct {
			NodeID           string                                        `json:"node_id"`
			FunctionExpected map[string]map[string]registry.ExpectedMetric `json:"function_expected_metrics"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal list: %v body=%s", err, rr.Body.String())
	}
	if len(list.Nodes) != 1 {
		t.Fatalf("nodes len=%d want 1 body=%s", len(list.Nodes), rr.Body.String())
	}
	if len(list.Nodes[0].FunctionExpected) != 1 || list.Nodes[0].FunctionExpected["alpr-yolo-tiny"]["accuracy"] != want["alpr-yolo-tiny"]["accuracy"] {
		t.Fatalf("list expected mismatch: %+v", list.Nodes[0].FunctionExpected)
	}
}

// POST with outer key not in Functions → 400 INVALID_NODE.
func TestRegisterNodeInvalidFunctionExpected(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	body := map[string]any{
		"node_id":   "n1",
		"layer":     "borda",
		"host":      "h:1",
		"functions": []string{"alpr-yolo-tiny"},
		"function_expected_metrics": map[string]map[string]any{
			"ghost": {"accuracy": map[string]any{"value": 0.9}},
		},
	}
	rr := doReq(t, h, "POST", "/nodes", body, token)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rr.Code, rr.Body.String())
	}
	var er gateway.ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
		t.Fatalf("unmarshal er: %v", err)
	}
	if er.Code != "INVALID_NODE" {
		t.Fatalf("code=%q want INVALID_NODE body=%s", er.Code, rr.Body.String())
	}
}

// POST without the key → nil map, 201 unchanged, GET omits the key.
func TestListNodesFunctionExpectedOmitted(t *testing.T) {
	srv, token, ns, _ := newTestServerEnv(t)
	h := srv.Handler()

	body := map[string]any{
		"node_id": "n1", "layer": "borda", "host": "h:1",
		"functions": []string{"hello"},
	}
	rr := doReq(t, h, "POST", "/nodes", body, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("POST status=%d want 201 body=%s", rr.Code, rr.Body.String())
	}
	stored, ok := ns.Get("n1")
	if !ok {
		t.Fatalf("node n1 not in store")
	}
	if stored.FunctionExpected != nil {
		t.Fatalf("stored expected=%v want nil", stored.FunctionExpected)
	}
	rr = doReq(t, h, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status=%d want 200", rr.Code)
	}
	var list struct {
		Nodes []json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(list.Nodes) != 1 {
		t.Fatalf("nodes len=%d want 1", len(list.Nodes))
	}
	var entry map[string]any
	if err := json.Unmarshal(list.Nodes[0], &entry); err != nil {
		t.Fatalf("unmarshal entry: %v", err)
	}
	if _, has := entry["function_expected_metrics"]; has {
		t.Fatalf("function_expected_metrics should be omitted, got %v", entry)
	}
}
