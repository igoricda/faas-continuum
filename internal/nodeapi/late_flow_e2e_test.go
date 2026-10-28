package nodeapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	arbiter "github.com/igoricda/faas-continuum/internal/arbitro/server"

	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/auth"
)

// ---------------------------------------------------------------------------
// Helpers: end-to-end environment with a real node and a real arbiter
// ---------------------------------------------------------------------------

type lateFlowEnv struct {
	nodeTS *httptest.Server
	arbTS  *httptest.Server
	gw     *gateway.Gateway
	hist   *history.History
	invTok string
}

func newLateFlowEnv(t *testing.T, backend *fakeInstanceBackend, nodeID string, cs *cloud.CloudStore) *lateFlowEnv {
	t.Helper()
	nodeTS, nodeSrv, mgmtToken := newLateTestServer(t, backend)
	resp := doRegister(t, nodeTS, mgmtToken, "f", registerRequest{ImageRef: "img:1.0", MinInstances: 1, TimeoutMS: 5000, LateReportable: true})
	_, invTok := mustRegister(t, resp)

	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = ns.Upsert(nodes.Node{NodeID: nodeID, Layer: nodes.LayerBorda, Host: strings.TrimPrefix(nodeTS.URL, "http://"), Functions: []string{"f"}})
	tokens, err := nodes.NewNodeTokenStore("")
	if err != nil {
		t.Fatal(err)
	}
	nodePlain, err := tokens.Issue(nodeID)
	if err != nil {
		t.Fatal(err)
	}

	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	mon := monitor.NewMonitor()
	limits := selection.LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}
	gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: limits}, History: hist, RemoteExecutor: cloud.NewConnector(cs)})
	arbGen, err := auth.Generate()
	if err != nil {
		t.Fatalf("falha ao gerar credencial de teste: %v", err)
	}
	arb := arbiter.NewServerWithHistory(ns, mon, gw, arbGen.Cred, 90*time.Second, limits, cs, hist, 60*time.Second, time.Now)
	arb.SetNodeTokens(tokens)
	arbTS := httptest.NewServer(arb.Handler())

	nodeSrv.ConfigureLateReports(arbTS.URL+"/internal/late-report", nodePlain, nodeID, 1024, 0)
	t.Cleanup(func() { nodeTS.Close(); arbTS.Close() })
	return &lateFlowEnv{nodeTS: nodeTS, arbTS: arbTS, gw: gw, hist: hist, invTok: invTok}
}

func postInvoke(t *testing.T, url, invTok, body string) (int, string, time.Duration) {
	t.Helper()
	creq, _ := http.NewRequest(http.MethodPost, url+"/invoke", bytes.NewReader([]byte(body)))
	creq.Header.Set("Content-Type", "application/json")
	creq.Header.Set(auth.DefaultHeaderName, invTok)
	start := time.Now()
	cresp, err := http.DefaultClient.Do(creq)
	if err != nil {
		t.Fatal(err)
	}
	latency := time.Since(start)
	cbody, _ := io.ReadAll(cresp.Body)
	cresp.Body.Close()
	if !json.Valid(cbody) {
		t.Fatalf("resposta do cliente não é JSON único: %q", string(cbody))
	}
	return cresp.StatusCode, string(cbody), latency
}

func waitLateReceived(t *testing.T, gw *gateway.Gateway) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		_, lateReceived, _, _ := gw.Counters()
		if lateReceived >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("late report não chegou ao History em 8s")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Scenario A: a slow function exceeds the objective. The client receives a
// gateway timeout close to the objective, and the late report carries the
// real cost. A timeout of the arbiter is not the internal error of the
// node.
// ---------------------------------------------------------------------------

func TestLateFlowEndToEnd(t *testing.T) {
	env := newLateFlowEnv(t, newFakeInstanceBackend(t, slowHandler), "node-slow", nil)

	status, body, latency := postInvoke(t, env.arbTS.URL, env.invTok, `{"function":"f","slo":{"max_latency_ms":300},"input":{}}`)
	if status != http.StatusGatewayTimeout {
		t.Fatalf("cliente status = %d, esperado %d (corpo: %s)", status, http.StatusGatewayTimeout, body)
	}
	if !strings.Contains(body, "NODE_INVOCATION_TIMEOUT") {
		t.Fatalf("cliente code inesperado: %s", body)
	}
	if latency > 1500*time.Millisecond {
		t.Fatalf("cliente servido em %v, acima de SLO+ε (300ms+1200ms)", latency)
	}
	t.Logf("cliente 504 em %v (SLO 300ms)", latency)

	waitLateReceived(t, env.gw)
	key := history.SegmentKey("f", "node-slow", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	count, p95, recent, _, _, ok := env.hist.StatsAndRecent(key, 60*time.Second, time.Now())
	if !ok {
		t.Fatal("History sem entrada após late recebido")
	}
	// The live attempt, whose budget was exceeded, is not counted and
	// never reaches the history, while the successful late report counts
	// once, so there is no double counting. The counters and the
	// percentile only consider the successes, so the latency of the
	// late report is the real one and not the truncated one.
	if count != 1 {
		t.Errorf("history count = %d, esperado 1 (live superseded + late sucesso)", count)
	}
	if p95 < 400 || p95 > 2000 {
		t.Errorf("history p95 = %d, esperado ~500 (custo real do lento)", p95)
	}
	if recent != 1 {
		t.Errorf("history recent = %d, esperado 1 (só o late é sucesso)", recent)
	}

	attempts, lateReceived, _, _ := env.gw.Counters()
	if attempts != 1 {
		t.Errorf("attempts_total = %d, esperado 1", attempts)
	}
	if lateReceived != 1 {
		t.Errorf("late_received_total = %d, esperado 1", lateReceived)
	}
}

// ---------------------------------------------------------------------------
// Scenario B: a fast failure and the cloud fallback. The client receives a
// success and the late report covers the error.
// ---------------------------------------------------------------------------

func TestLateFlowFallback(t *testing.T) {
	cloudTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"cloud"}`))
	}))
	defer cloudTS.Close()
	cs, err := cloud.NewCloudStore("")
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.Set(cloud.CloudFallbackConfig{EndpointURL: cloudTS.URL, InputContentType: "application/json", AuthScheme: "none"}); err != nil {
		t.Fatal(err)
	}

	env := newLateFlowEnv(t, newFakeInstanceBackend(t, errorHandler), "node-fail", cs)

	status, body, _ := postInvoke(t, env.arbTS.URL, env.invTok, `{"function":"f","slo":{"max_latency_ms":2000},"input":{}}`)
	if status != http.StatusOK {
		t.Fatalf("cliente status = %d, esperado %d (corpo: %s)", status, http.StatusOK, body)
	}
	if !strings.Contains(body, `"cloud_fallback":true`) {
		t.Fatalf("cliente sem cloud_fallback:true: %s", body)
	}

	waitLateReceived(t, env.gw)
	key := history.SegmentKey("f", "node-fail", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	// The live failure only reaches the invocation log, with no
	// performance measurement, and the failed late report is
	// recorded. The fan-out per layer, the primary and the aggregated
	// one, gives two keys, but the counters and the percentile require
	// a success, so the segment stays not ready.
	if len(env.hist.Keys()) != 2 {
		t.Errorf("history keys = %d, esperado 2 (late falha com fan-out de layer)", len(env.hist.Keys()))
	}
	count, _, recent, _, _, ok := env.hist.StatsAndRecent(key, 60*time.Second, time.Now())
	if ok || count != 0 {
		t.Errorf("history count = %d ok=%v, esperado (0,false) sem sucesso", count, ok)
	}
	if recent != 0 {
		t.Errorf("history recent = %d, esperado 0 (nenhum sucesso)", recent)
	}
	attempts, lateReceived, _, _ := env.gw.Counters()
	if attempts != 1 || lateReceived != 1 {
		t.Errorf("counters attempts = %d lateReceived = %d, esperado 1/1", attempts, lateReceived)
	}
}
