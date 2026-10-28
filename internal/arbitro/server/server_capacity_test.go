package server

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

func decodeCapacityList(t *testing.T, body []byte) []listEntry {
	t.Helper()
	var list struct {
		Nodes []listEntry `json:"nodes"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("unmarshal list: %v body=%s", err, body)
	}
	return list.Nodes
}

func TestRegisterCapacityRoundTrip(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	body := map[string]any{
		"node_id": "cap-1", "layer": "borda", "host": "10.0.0.1:8080",
		"functions":     []string{"sleep", "other"},
		"max_instances": map[string]int{"sleep": 2, "other": 0},
	}
	rr := doReq(t, h, "POST", "/nodes", body, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("POST /nodes status=%d want 201 body=%s", rr.Code, rr.Body.String())
	}
	var created nodes.Node
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	if !reflect.DeepEqual(created.FunctionCapacity, map[string]int{"sleep": 2, "other": 0}) {
		t.Fatalf("created FunctionCapacity=%v, want map[sleep:2 other:0]", created.FunctionCapacity)
	}

	rr = doReq(t, h, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes status=%d want 200 body=%s", rr.Code, rr.Body.String())
	}
	entries := decodeCapacityList(t, rr.Body.Bytes())
	if len(entries) != 1 {
		t.Fatalf("nodes len=%d want 1", len(entries))
	}
	if !reflect.DeepEqual(entries[0].FunctionCapacity, map[string]int{"sleep": 2, "other": 0}) {
		t.Fatalf("GET FunctionCapacity=%v, want igualdade exata com o POST", entries[0].FunctionCapacity)
	}
	// Chave JSON normativa presente no GET.
	var raw struct {
		Nodes []map[string]any `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	mi, ok := raw.Nodes[0]["max_instances"].(map[string]any)
	if !ok {
		t.Fatalf("max_instances ausente no GET /nodes: %v", raw.Nodes[0])
	}
	if mi["sleep"] != float64(2) {
		t.Fatalf("max_instances.sleep=%v want 2", mi["sleep"])
	}
}

func TestRegisterCapacityNegative400(t *testing.T) {
	srv, token, ns, _ := newTestServerEnv(t)
	h := srv.Handler()

	// Estado prévio válido.
	rr := doReq(t, h, "POST", "/nodes", map[string]any{
		"node_id": "cap-neg", "layer": "borda", "host": "10.0.0.1:8080",
		"functions":     []string{"sleep"},
		"max_instances": map[string]int{"sleep": 2},
	}, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("setup POST status=%d want 201 body=%s", rr.Code, rr.Body.String())
	}

	rr = doReq(t, h, "POST", "/nodes", map[string]any{
		"node_id": "cap-neg", "layer": "borda", "host": "10.0.0.1:8080",
		"functions":     []string{"sleep"},
		"max_instances": map[string]int{"sleep": -1},
	}, token)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("negativo status=%d want 400 body=%s", rr.Code, rr.Body.String())
	}
	var er struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
		t.Fatalf("unmarshal er: %v", err)
	}
	if er.Code != "INVALID_NODE" {
		t.Fatalf("code=%q want INVALID_NODE", er.Code)
	}
	// Store inalterado.
	got, ok := ns.Get("cap-neg")
	if !ok {
		t.Fatalf("nó sumiu após POST rejeitado")
	}
	if !reflect.DeepEqual(got.FunctionCapacity, map[string]int{"sleep": 2}) {
		t.Fatalf("store alterado após 400: %v", got.FunctionCapacity)
	}
}

func TestRegisterCapacityZeroAndAbsent(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	rr := doReq(t, h, "POST", "/nodes", map[string]any{
		"node_id": "cap-zero", "layer": "borda", "host": "10.0.0.1:8080",
		"functions":     []string{"sleep"},
		"max_instances": map[string]int{"sleep": 0},
	}, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("zero POST status=%d want 201 body=%s", rr.Code, rr.Body.String())
	}

	rr = doReq(t, h, "POST", "/nodes", map[string]any{
		"node_id": "cap-absent", "layer": "nevoa", "host": "10.0.0.2:8080",
		"functions": []string{"sleep"},
	}, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("ausente POST status=%d want 201 body=%s", rr.Code, rr.Body.String())
	}

	rr = doReq(t, h, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status=%d want 200", rr.Code)
	}
	entries := decodeCapacityList(t, rr.Body.Bytes())
	byID := map[string]listEntry{}
	for _, e := range entries {
		byID[e.NodeID] = e
	}
	if !reflect.DeepEqual(byID["cap-zero"].FunctionCapacity, map[string]int{"sleep": 0}) {
		t.Fatalf("zero FunctionCapacity=%v, want map[sleep:0] (sem teto)", byID["cap-zero"].FunctionCapacity)
	}
	if byID["cap-absent"].FunctionCapacity != nil {
		t.Fatalf("ausente FunctionCapacity=%v, want nil (sem teto)", byID["cap-absent"].FunctionCapacity)
	}
}

func TestRegisterCapacityEmptyFunctions(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"sem functions", map[string]any{
			"node_id": "cap-e1", "layer": "borda", "host": "10.0.0.1:8080",
			"max_instances": map[string]int{"sleep": 2},
		}},
		{"functions vazia", map[string]any{
			"node_id": "cap-e2", "layer": "borda", "host": "10.0.0.2:8080",
			"functions":     []string{},
			"max_instances": map[string]int{"sleep": 2},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := doReq(t, h, "POST", "/nodes", tc.body, token)
			if rr.Code != http.StatusCreated {
				t.Fatalf("POST status=%d want 201 body=%s", rr.Code, rr.Body.String())
			}
			var created nodes.Node
			if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !reflect.DeepEqual(created.FunctionCapacity, map[string]int{"sleep": 2}) {
				t.Fatalf("FunctionCapacity=%v, want map[sleep:2] (ignorada, sem gating)", created.FunctionCapacity)
			}
		})
	}
}
