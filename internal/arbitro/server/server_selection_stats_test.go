package server

// Regressão de wire (ADR-0012): o bloco `selection_stats` era a
// instrumentação observacional do SelfLearningSelector (v1) — aposentado e
// removido junto com o seletor. O campo NÃO pode voltar ao GET /nodes: o
// observacional canônico é o `selection_risk*` (ver
// server_selection_risk_test.go).

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
