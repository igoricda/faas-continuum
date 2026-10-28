package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// POST with valid categories persists and GET echoes them.
func TestRegisterNodeFunctionCategories(t *testing.T) {
	srv, token, ns, _ := newTestServerEnv(t)
	h := srv.Handler()

	body := map[string]any{
		"node_id":   "n1",
		"layer":     "borda",
		"host":      "h:1",
		"functions": []string{"alpr-yolo-tiny"},
		"function_categories": map[string]string{
			"alpr-yolo-tiny": "alpr-yolo",
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
	if created.FunctionCategories["alpr-yolo-tiny"] != "alpr-yolo" {
		t.Fatalf("created categories mismatch: %+v", created.FunctionCategories)
	}
	// Persisted in store.
	stored, ok := ns.Get("n1")
	if !ok {
		t.Fatalf("node n1 not in store")
	}
	if stored.FunctionCategories["alpr-yolo-tiny"] != "alpr-yolo" {
		t.Fatalf("stored categories mismatch: %+v", stored.FunctionCategories)
	}
	// GET echoes them.
	rr = doReq(t, h, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status=%d want 200 body=%s", rr.Code, rr.Body.String())
	}
	var list struct {
		Nodes []struct {
			NodeID             string            `json:"node_id"`
			FunctionCategories map[string]string `json:"function_categories"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal list: %v body=%s", err, rr.Body.String())
	}
	if len(list.Nodes) != 1 {
		t.Fatalf("nodes len=%d want 1 body=%s", len(list.Nodes), rr.Body.String())
	}
	if list.Nodes[0].FunctionCategories["alpr-yolo-tiny"] != "alpr-yolo" {
		t.Fatalf("list categories mismatch: %+v", list.Nodes[0].FunctionCategories)
	}
}

// POST with invalid map → 400 INVALID_NODE.
func TestRegisterNodeInvalidFunctionCategories(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	cases := []map[string]any{
		// key not in functions
		{
			"node_id":   "n1",
			"layer":     "borda",
			"host":      "h:1",
			"functions": []string{"alpr-yolo-tiny"},
			"function_categories": map[string]string{
				"ghost": "alpr-yolo",
			},
		},
		// empty value
		{
			"node_id":   "n2",
			"layer":     "borda",
			"host":      "h:1",
			"functions": []string{"alpr-yolo-tiny"},
			"function_categories": map[string]string{
				"alpr-yolo-tiny": "",
			},
		},
		// empty key
		{
			"node_id":   "n3",
			"layer":     "borda",
			"host":      "h:1",
			"functions": []string{"alpr-yolo-tiny"},
			"function_categories": map[string]string{
				"": "alpr-yolo",
			},
		},
	}
	for i, body := range cases {
		rr := doReq(t, h, "POST", "/nodes", body, token)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("case %d status=%d want 400 body=%s", i, rr.Code, rr.Body.String())
		}
		var er gateway.ErrorResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
			t.Fatalf("case %d unmarshal er: %v", i, err)
		}
		if er.Code != "INVALID_NODE" {
			t.Fatalf("case %d code=%q want INVALID_NODE body=%s", i, er.Code, rr.Body.String())
		}
	}
}

// POST without the key → nil map, 201 unchanged.
func TestListNodesFunctionCategoriesOmitted(t *testing.T) {
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
	var created nodes.Node
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	if created.FunctionCategories != nil {
		t.Fatalf("created categories=%v want nil", created.FunctionCategories)
	}
	stored, ok := ns.Get("n1")
	if !ok {
		t.Fatalf("node n1 not in store")
	}
	if stored.FunctionCategories != nil {
		t.Fatalf("stored categories=%v want nil", stored.FunctionCategories)
	}
	// GET entry must omit the key entirely.
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
	if _, has := entry["function_categories"]; has {
		t.Fatalf("function_categories should be omitted, got %v", entry)
	}
}

// Unknown fields stay silently ignored (no DisallowUnknownFields).
func TestRegisterNodeUnknownFieldsIgnored(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	rr := doReq(t, h, "POST", "/nodes",
		`{"node_id":"n1","layer":"borda","host":"h:1","bogus_field":123}`,
		token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("POST with unknown field status=%d want 201 body=%s", rr.Code, rr.Body.String())
	}
}
