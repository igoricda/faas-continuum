package server

// Regression group of the gateway and of the server: the dedicated limiter of
// the late report, the injectable clock in the server and the application of
// the status, of the deduplication and of the rejection without a gateway in
// the late report.

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/auth"
)

// mkLateServer builds a server with the tokens of the node and a gateway with a history.
func mkLateServer(t *testing.T) (*Server, http.Handler, string) {
	t.Helper()
	ns, _ := nodes.NewNodeStore("")
	mon := monitor.NewMonitor()
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, History: hist})
	gen, err := auth.Generate()
	if err != nil {
		t.Fatalf("auth.Generate: %v", err)
	}
	srv := NewServerWithHistory(ns, mon, gw, gen.Cred, 90*time.Second, defaultLimits, nil, hist, 0, nil)
	tokens, err := nodes.NewNodeTokenStore("")
	if err != nil {
		t.Fatal(err)
	}
	srv.SetNodeTokens(tokens)
	plain, err := tokens.Issue("node1")
	if err != nil {
		t.Fatal(err)
	}
	return srv, srv.Handler(), plain
}

func mkLateReport() gateway.LateReport {
	return gateway.LateReport{
		RequestID:         "req-aud40",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "node1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 1200,
		StartedAt:         time.Now().Add(-2 * time.Second).Format(time.RFC3339Nano),
	}
}

// Scenario of the statuses: the applied report gives an accepted answer, the duplicate gives the already reported one and the report without a gateway is rejected.
func TestAuditFix_LateReportStatuses(t *testing.T) {
	_, h, plain := mkLateServer(t)

	rr := doReq(t, h, http.MethodPost, "/internal/late-report", mkLateReport(), plain)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("aplicado status=%d want 202; body=%s", rr.Code, rr.Body.String())
	}
	rr = doReq(t, h, http.MethodPost, "/internal/late-report", mkLateReport(), plain)
	if rr.Code != http.StatusAlreadyReported {
		t.Fatalf("dedup status=%d want 208; body=%s", rr.Code, rr.Body.String())
	}

	// Sem gateway configurado: 503 honesto em vez de "202 vazio".
	bare := &Server{}
	rr = doReq(t, bare.Handler(), http.MethodPost, "/internal/late-report", mkLateReport(), "")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("sem gateway status=%d want 503; body=%s", rr.Code, rr.Body.String())
	}
}

// Scenario of the dedicated limiter: exhausting the limiter of the nodes cannot
// bring down the late report, since it has its own quota.
func TestAuditFix_LateLimiterIsolatedFromNodeLimiter(t *testing.T) {
	_, h, plain := mkLateServer(t)

	teleBody := map[string]any{"node_id": "node1"}
	for i := 0; i < 600; i++ {
		rr := doReq(t, h, http.MethodPost, "/nodes/node1/telemetry", teleBody, "")
		if rr.Code == http.StatusTooManyRequests {
			t.Fatalf("telemetria %d já com 429 (limiter menor que 600?)", i)
		}
	}
	rr := doReq(t, h, http.MethodPost, "/nodes/node1/telemetry", teleBody, "")
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("telemetria 601ª status=%d want 429", rr.Code)
	}
	// The late report is still accepted, since the limiter is dedicated and not contaminated.
	rr = doReq(t, h, http.MethodPost, "/internal/late-report", mkLateReport(), plain)
	if rr.Code == http.StatusTooManyRequests {
		t.Fatalf("late-report com 429 após esgotar limiter de nós (cota compartilhada?)")
	}
	if rr.Code != http.StatusAccepted {
		t.Fatalf("late-report status=%d want 202; body=%s", rr.Code, rr.Body.String())
	}
}

// Scenario of the injectable clock, in the server: the clock governs the derived
// status, so with a clock ten minutes in the future a node that was just
// registered appears unavailable.
func TestAuditFix_ServerClockDrivesDerivedStatus(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	mon := monitor.NewMonitor()
	gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}})
	gen, _ := auth.Generate()
	future := time.Now().Add(10 * time.Minute)
	srv := NewServerWithHistory(ns, mon, gw, gen.Cred, 90*time.Second, defaultLimits, nil, nil, 0, func() time.Time { return future })
	if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h1"}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	rr := doReq(t, srv.Handler(), http.MethodGet, "/nodes", nil, gen.PlainToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes status=%d want 200", rr.Code)
	}
	if got := rr.Body.String(); !strings.Contains(got, `"status":"indisponivel"`) {
		t.Fatalf("status sob clock futuro: %s (esperado indisponivel)", got)
	}
}
