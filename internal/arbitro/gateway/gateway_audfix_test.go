package gateway

// Regression group of the gateway: the arbiter timeout against the function
// timeout, the liveness against the success, the absence of attempts mapping
// to a timeout, the visible fan-out, the injectable clock in the handler, the
// fail-fast of a bad request, the retryability of the root cause in the cloud
// and the ceiling of the invocation log.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// Scenario of the arbiter timeout: a short objective exhausts the overall budget
// in the middle of the attempt, so the error classifies as an exceeded budget,
// the answer is a retryable timeout and not a bad gateway attributed to the
// node, and the history records the failure without latency, so the guard by
// prediction and the veto by rate see the saturation.
func TestAuditFix_BudgetTimeoutVsFunctionTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte(`{"result":"slow"}`))
	}))
	t.Cleanup(slow.Close)
	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slow.URL), Functions: []string{"f"}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, History: h})
	gw.SetInvocationLog(l)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"f","slo":{"max_latency_ms":50},"input":{}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NODE_INVOCATION_TIMEOUT" || !er.Retryable {
		t.Fatalf("code=%q retryable=%v want NODE_INVOCATION_TIMEOUT/true", er.Code, er.Retryable)
	}
	if got := invLogOutcomes(t, logPath); len(got) != 1 || got[0] != history.OutcomeOrcamentoExcedido {
		t.Fatalf("invLog outcomes=%v, esperado [orcamento_excedido]", got)
	}
	keys := h.Keys()
	if len(keys) != 2 {
		t.Fatalf("history tem %d chaves após budget-exceeded, esperado 2 (primária + agregada)", len(keys))
	}
	count, p95, total, rate, ok := h.Stats(keys[0])
	if ok || count != 0 || p95 != 0 {
		t.Fatalf("p95/Count contaminados por orcamento_excedido: count=%d p95=%d ok=%v", count, p95, ok)
	}
	// The arbiter giving up is a saturation and not the node failing to deliver,
	// so it stays out of the service rate, since otherwise the veto by rate would
	// ban the node that the arbiter did not wait for, which happened in the
	// measured campaign with the fastest node vetoed after ninety-seven samples
	// without a success.
	if total != 0 || rate != history.LaplaceRate(0, 0) {
		t.Fatalf("total=%d rate=%v, esperado (0,%v): corte de orçamento é saturação", total, rate, history.LaplaceRate(0, 0))
	}
	if history.VetoByRate(total, rate) {
		t.Fatal("corte de orçamento vetou o nó: o veto só pode ver falha de serviço")
	}
	if full, okFull := h.StatsFull(keys[0], time.Hour, time.Now()); !okFull || full.Saturation != 1 {
		t.Fatalf("Saturation=(%d,%v), esperado (1,true)", full.Saturation, okFull)
	}
}

// Scenario of the absence of attempts: a context already cancelled before the
// first attempt has to map to a retryable answer of no node available, and never
// to the stale internal error of the initial failure.
func TestAuditFix_ZeroAttemptsMapsTo504(t *testing.T) {
	gw, _, _ := recordabilityFixture(t, http.StatusInternalServerError, func() []byte {
		return []byte(`{"code":"X"}`)
	})
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	cancelled, cancelFn := context.WithCancel(req.Context())
	cancelFn()
	req = req.WithContext(cancelled)
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NO_NODE_AVAILABLE" || !er.Retryable {
		t.Fatalf("code=%q retryable=%v want NO_NODE_AVAILABLE/true", er.Code, er.Retryable)
	}
}

// Scenario of the liveness: a live node that answers a continuous rejection
// keeps its last seen instant updated, since without the touch it would age
// until it became unavailable and would cause a flapping of the membership.
func TestAuditFix_TouchOnOverloadResponse(t *testing.T) {
	gw, _, _ := recordabilityFixture(t, http.StatusServiceUnavailable, func() []byte {
		return []byte(`{"code":"POOL_EXHAUSTED"}`)
	})
	before, ok := gw.nodes.Get("n1")
	if !ok {
		t.Fatal("nó n1 ausente")
	}
	time.Sleep(20 * time.Millisecond)
	rec := invokeRecordability(t, gw)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	after, ok := gw.nodes.Get("n1")
	if !ok {
		t.Fatal("nó n1 sumiu")
	}
	if !after.LastSeen.After(before.LastSeen) {
		t.Fatalf("LastSeen não avançou após 503: before=%v after=%v", before.LastSeen, after.LastSeen)
	}
}

// Scenario of the fan-out: a queue of three with a ceiling of one, so the cut
// appears in the counter of cuts and only one attempt happens.
func TestAuditFix_FanoutCutCounted(t *testing.T) {
	var hits int32
	mk := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&hits, 1)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"code":"boom"}`))
		}))
	}
	s1, s2, s3 := mk(), mk(), mk()
	t.Cleanup(s1.Close)
	t.Cleanup(s2.Close)
	t.Cleanup(s3.Close)
	ns, _ := nodes.NewNodeStore("")
	for i, s := range []*httptest.Server{s1, s2, s3} {
		id := []string{"n1", "n2", "n3"}[i]
		if _, err := ns.Upsert(nodes.Node{NodeID: id, Layer: nodes.LayerBorda, Host: hostFromURL(t, s.URL)}); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, MaxRaceFanout: 1})

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502 body=%s", rec.Code, rec.Body.String())
	}
	if got := gw.FanoutCuts(); got != 1 {
		t.Fatalf("FanoutCuts=%d want 1", got)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("tentativas=%d want 1 (fila truncada)", got)
	}
}

// Scenario of the bad request: a rejection of the node is deterministic, so the
// fail-fast is immediate, as for an unauthorized answer, with a single attempt.
func TestAuditFix_BadRequestFailFast(t *testing.T) {
	var hits int32
	mk := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&hits, 1)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":"INVALID_BODY"}`))
		}))
	}
	s1, s2 := mk(), mk()
	t.Cleanup(s1.Close)
	t.Cleanup(s2.Close)
	ns, _ := nodes.NewNodeStore("")
	for i, s := range []*httptest.Server{s1, s2} {
		id := []string{"n1", "n2"}[i]
		if _, err := ns.Upsert(nodes.Node{NodeID: id, Layer: nodes.LayerBorda, Host: hostFromURL(t, s.URL)}); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}
	h, _ := history.NewHistory("", 100)
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, History: h})

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NODE_BAD_REQUEST" || er.Retryable {
		t.Fatalf("code=%q retryable=%v want NODE_BAD_REQUEST/false", er.Code, er.Retryable)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("tentativas=%d want 1 (fail-fast, sem failover)", got)
	}
	if len(h.Keys()) != 0 {
		t.Fatalf("history tem %d chaves após 400, esperado 0", len(h.Keys()))
	}
}

// Scenario of the cloud: the failure of the fallback inherits the retryability
// of the root cause of the queue, so an overload stays retryable and a definitive
// rejection stays definitive.
func TestAuditFix_CloudPreservesRootRetryability(t *testing.T) {
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("cloud down"))
	}))
	t.Cleanup(cloudSrv.Close)
	for _, tc := range []struct {
		name       string
		nodeStatus int
		nodeBody   string
		wantRetry  bool
	}{
		{"sobrecarga-retryable", http.StatusServiceUnavailable, `{"code":"POOL_EXHAUSTED"}`, true},
		{"nao-encontrada-definitiva", http.StatusNotFound, `{"code":"FUNCTION_NOT_FOUND"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.nodeStatus)
				_, _ = w.Write([]byte(tc.nodeBody))
			}))
			t.Cleanup(node.Close)
			ns, _ := nodes.NewNodeStore("")
			if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, node.URL), Functions: []string{"f"}}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			mon := monitor.NewMonitor()
			gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL)})

			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"f","input":{}}`))
			rec := httptest.NewRecorder()
			gw.Handle(rec, req)

			if rec.Code != http.StatusBadGateway {
				t.Fatalf("status=%d want 502 body=%s", rec.Code, rec.Body.String())
			}
			er := decodeErrResp(t, rec)
			if er.Code != "CLOUD_FALLBACK_FAILED" || er.Retryable != tc.wantRetry {
				t.Fatalf("code=%q retryable=%v want CLOUD_FALLBACK_FAILED/%v", er.Code, er.Retryable, tc.wantRetry)
			}
		})
	}
}

// Scenario of the injectable clock: a clock that advances fifty milliseconds per
// call makes the latency of the attempt, which is the difference between two
// consecutive readings without the wall clock in between, exactly fifty milliseconds.
func TestAuditFix_AttemptLatencyFromInjectedClock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	t.Cleanup(srv.Close)
	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	base := time.Now()
	var n int64
	stepClock := func() time.Time {
		n++
		return base.Add(time.Duration(n) * 50 * time.Millisecond)
	}
	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, Clock: stepClock})
	gw.SetInvocationLog(l)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var r InvocationRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &r); err != nil {
		t.Fatalf("unmarshal invLog: %v", err)
	}
	if r.ObservedLatencyMS != 50 {
		t.Fatalf("ObservedLatencyMS=%d want 50 (clock injetável, sem wall)", r.ObservedLatencyMS)
	}
}

// Scenario of the invocation log: a blob of execution metrics of about one
// hundred kibibytes becomes a truncated stub with the original size, so the
// line stays valid JSON and limited to sixty-four kibibytes.
func TestAuditFix_InvocationLogMetricsCap(t *testing.T) {
	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	big := json.RawMessage(`{"blob":"` + strings.Repeat("x", 100*1024) + `"}`)
	if err := l.Append(InvocationRecord{ExecutionMetrics: big}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var r InvocationRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &r); err != nil {
		t.Fatalf("linha invLog inválida após truncate: %v", err)
	}
	var stub struct {
		Truncated     bool `json:"truncated"`
		OriginalBytes int  `json:"original_bytes"`
	}
	if err := json.Unmarshal(r.ExecutionMetrics, &stub); err != nil {
		t.Fatalf("metrics não é stub: %v", err)
	}
	if !stub.Truncated || stub.OriginalBytes != len(big) {
		t.Fatalf("stub=%+v want truncated/original=%d", stub, len(big))
	}
	if len(data) > 70*1024 {
		t.Fatalf("linha com %d bytes, esperado ~64KiB", len(data))
	}
	// Under the ceiling it passes verbatim.
	small := json.RawMessage(`{"a":1}`)
	if got := truncateMetrics(small); string(got) != string(small) {
		t.Fatalf("metrics pequena alterada: %s", got)
	}
}
