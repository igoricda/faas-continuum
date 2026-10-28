package server

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// Ingestion of the function timeout in the post of a node: the round trip between
// the post and the listing with the normative key, and the rejection of a key
// outside the function list and of a value outside the valid range, without
// changing the store.
//
// The wire of the ingestion is protected, since it is the single source of the
// hard deadline. The credible regressions are a field ignored by the request of
// the registration, which loses the announcement, and a validation that is not
// connected, which persists a degenerate ceiling. It mirrors the tests of the
// maximum of instances without duplicating them, since the key and the rule are
// distinct.
func TestRegisterTimeoutRoundTrip(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	body := map[string]any{
		"node_id": "tm-1", "layer": "borda", "host": "10.0.0.1:8080",
		"functions":           []string{"sleep", "other"},
		"function_timeout_ms": map[string]int{"sleep": 15000, "other": 90000},
	}
	rr := doReq(t, h, "POST", "/nodes", body, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("POST /nodes status=%d want 201 body=%s", rr.Code, rr.Body.String())
	}
	var created nodes.Node
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created: %v", err)
	}
	if !reflect.DeepEqual(created.FunctionTimeoutMS, map[string]int{"sleep": 15000, "other": 90000}) {
		t.Fatalf("created FunctionTimeoutMS=%v, want map[sleep:15000 other:90000]", created.FunctionTimeoutMS)
	}

	rr = doReq(t, h, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes status=%d want 200 body=%s", rr.Code, rr.Body.String())
	}
	entries := decodeCapacityList(t, rr.Body.Bytes())
	if len(entries) != 1 {
		t.Fatalf("nodes len=%d want 1", len(entries))
	}
	if !reflect.DeepEqual(entries[0].FunctionTimeoutMS, map[string]int{"sleep": 15000, "other": 90000}) {
		t.Fatalf("GET FunctionTimeoutMS=%v, want igualdade exata com o POST", entries[0].FunctionTimeoutMS)
	}
	var raw struct {
		Nodes []map[string]any `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	tm, ok := raw.Nodes[0]["function_timeout_ms"].(map[string]any)
	if !ok {
		t.Fatalf("function_timeout_ms ausente no GET /nodes: %v", raw.Nodes[0])
	}
	if tm["sleep"] != float64(15000) {
		t.Fatalf("function_timeout_ms.sleep=%v want 15000", tm["sleep"])
	}
}

func TestRegisterTimeoutInvalid400(t *testing.T) {
	srv, token, ns, _ := newTestServerEnv(t)
	h := srv.Handler()

	rr := doReq(t, h, "POST", "/nodes", map[string]any{
		"node_id": "tm-neg", "layer": "borda", "host": "10.0.0.1:8080",
		"functions":           []string{"sleep"},
		"function_timeout_ms": map[string]int{"sleep": 15000},
	}, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("setup POST status=%d want 201 body=%s", rr.Code, rr.Body.String())
	}

	cases := []struct {
		name string
		body map[string]any
	}{
		{"chave fora de functions", map[string]any{
			"node_id": "tm-neg", "layer": "borda", "host": "10.0.0.1:8080",
			"functions":           []string{"sleep"},
			"function_timeout_ms": map[string]int{"alpr": 15000},
		}},
		{"valor zero", map[string]any{
			"node_id": "tm-neg", "layer": "borda", "host": "10.0.0.1:8080",
			"functions":           []string{"sleep"},
			"function_timeout_ms": map[string]int{"sleep": 0},
		}},
		{"valor acima de 24h", map[string]any{
			"node_id": "tm-neg", "layer": "borda", "host": "10.0.0.1:8080",
			"functions":           []string{"sleep"},
			"function_timeout_ms": map[string]int{"sleep": 86400001},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := doReq(t, h, "POST", "/nodes", tc.body, token)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status=%d want 400 body=%s", rr.Code, rr.Body.String())
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
		})
	}
	got, ok := ns.Get("tm-neg")
	if !ok {
		t.Fatalf("nó sumiu após POSTs rejeitados")
	}
	if !reflect.DeepEqual(got.FunctionTimeoutMS, map[string]int{"sleep": 15000}) {
		t.Fatalf("store alterado após 400: %v", got.FunctionTimeoutMS)
	}
}
