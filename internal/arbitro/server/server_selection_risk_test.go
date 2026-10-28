// Observational exposure of the selection risk in the listing of the nodes: the
// fields are omitted without the wiring and present with it.
package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

func listNodesRawRisk(t *testing.T, srv *Server, token string) string {
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

func TestServerSelectionRiskOmittedWhenUnwired(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	raw := listNodesRawRisk(t, srv, token)
	if strings.Contains(raw, "selection_risk") {
		t.Fatalf("selection_risk* presente sem fiação: %s", raw)
	}
}

func TestServerSelectionRiskIncludedWhenWired(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	st := &selection.RiskStats{}
	st.Feasible.Add(5)
	dump := []learn.ModelDump{{Group: "g", Node: "n1", MedianMS: 800, UpperMS: 900, Neff: 10, Alpha: 0.05, PSucc: 0.99, NeffBeta: 10}}
	if got := srv.SetSelectionRisk(st, func() []learn.ModelDump { return dump }); got != srv {
		t.Fatalf("SetSelectionRisk não devolveu o próprio *Server")
	}
	raw := listNodesRawRisk(t, srv, token)
	if !strings.Contains(raw, "selection_risk_stats") {
		t.Fatalf("selection_risk_stats ausente com fiação: %s", raw)
	}
	var decoded struct {
		Nodes []struct {
			SelectionRiskStats *selection.RiskStatsSnapshot `json:"selection_risk_stats"`
			SelectionRisk      []learn.ModelDump            `json:"selection_risk"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Nodes) != 1 || decoded.Nodes[0].SelectionRiskStats == nil {
		t.Fatalf("nodes=%+v", decoded.Nodes)
	}
	if got := decoded.Nodes[0].SelectionRiskStats.Feasible; got != 5 {
		t.Fatalf("feasible=%d want 5", got)
	}
	if len(decoded.Nodes[0].SelectionRisk) != 1 || decoded.Nodes[0].SelectionRisk[0].Node != "n1" {
		t.Fatalf("selection_risk=%+v", decoded.Nodes[0].SelectionRisk)
	}
}
