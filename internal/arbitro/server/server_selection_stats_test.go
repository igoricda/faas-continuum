package server

// Regression of the wire: the block of the selection statistics was the
// observational instrumentation of the previous selector, which was retired and
// removed with it. The field must not come back to the listing of the nodes,
// since the canonical observational block is the one of the selection risk.

import (
	"net/http"
	"strings"
	"testing"
)

func listNodesRaw(t *testing.T, srv *Server, token string) string {
	t.Helper()
	h := srv.Handler()
	body := map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}
	if rr := doReq(t, h, "POST", "/nodes", body, token); rr.Code != http.StatusCreated {
		t.Fatalf("POST /nodes: status=%d, want 201 (%s)", rr.Code, rr.Body.String())
	}
	rr := doReq(t, h, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes: status=%d, want 200 (%s)", rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}

func TestServerSelectionStatsRetired(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	raw := listNodesRaw(t, srv, token)
	if strings.Contains(raw, "selection_stats") {
		t.Fatalf("selection_stats aposentado (ADR-0012) não pode aparecer: %s", raw)
	}
}
