package server

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

// ---------------------------------------------------------------------------
// POST /internal/late-report — válido, duplicado, 401, oversize
// ---------------------------------------------------------------------------

func TestLateReportEndpoint(t *testing.T) {
	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatal(err)
	}
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
	h := srv.Handler()

	rep := gateway.LateReport{
		RequestID:         "req-1",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "node1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 3200,
		StartedAt:         time.Now().Add(-2 * time.Second).Format(time.RFC3339Nano),
	}
	key := history.SegmentKey("f", "node1", history.ScenarioContext{LoadBucket: history.BucketUnknown})

	rr := doReq(t, h, http.MethodPost, "/internal/late-report", rep, plain)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("valid report status=%d want 202; body=%s", rr.Code, rr.Body.String())
	}
	count, _, _, _, _, ok := hist.StatsAndRecent(key, 60*time.Second, time.Now())
	if !ok || count != 1 {
		t.Fatalf("sink count=%d want 1 after valid report", count)
	}

	rep.ObservedLatencyMS = 9999
	rr = doReq(t, h, http.MethodPost, "/internal/late-report", rep, plain)
	if rr.Code != http.StatusAlreadyReported {
		t.Fatalf("duplicate report status=%d want 208; body=%s", rr.Code, rr.Body.String())
	}
	count, _, _, _, _, ok = hist.StatsAndRecent(key, 60*time.Second, time.Now())
	if !ok || count != 1 {
		t.Fatalf("sink count=%d want 1 after duplicate", count)
	}

	rr = doReq(t, h, http.MethodPost, "/internal/late-report", rep, "wrong-token")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("bad auth status=%d want 401; body=%s", rr.Code, rr.Body.String())
	}

	rr = doReq(t, h, http.MethodPost, "/internal/late-report", []byte(strings.Repeat("a", 65*1024)), plain)
	if rr.Code != http.StatusRequestEntityTooLarge && rr.Code != http.StatusBadRequest {
		t.Fatalf("oversize status=%d want 413 or 400; body=%s", rr.Code, rr.Body.String())
	}
}
