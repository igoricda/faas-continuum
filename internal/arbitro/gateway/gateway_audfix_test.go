package gateway

// Regressão do grupo gateway da auditoria 2026-09-17 (adendo ADR-0008
// 2026-09-17): AUD-22 (árbitro-timeout vs função-timeout), AUD-23
// (liveness≠sucesso), AUD-26 (zero-tentativas→504), AUD-27 (fanout
// visível), AUD-29 (clock injetável no Handle), AUD-38 (400 fail-fast),
// AUD-39 (retryability da causa raiz no cloud), AUD-42 (teto do invLog).
// Cada teste ancora um cenário do documento de auditoria.

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

// Cenário AUD-22: SLO curto esgota o overall no meio da tentativa — o
// erro deve classificar como orcamento_excedido, a resposta deve ser 504
// retryable (não 502 atribuído ao nó) e, desde a emenda 2026-09-24, o
// History registra a falha SEM LATÊNCIA (Total=1, p95 intocado) para o
// crivo por previsão e o veto por taxa enxergarem a saturação.
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
	// O árbitro desistir NA HORA (orcamento_excedido) é saturação, não o nó
	// deixando de entregar: fica fora da taxa de serviço, senão o veto por
	// taxa bania o nó que o árbitro não esperou (D1 — campanha arb4-*, em que
	// o nó mais rápido ficou vetado com 97 amostras e 0 acertos).
	// contagem por segmento em BRUTOS/arbitro/history.json: 61 + 20 + 16 nos
	// buckets 0-10% / 25-50% / 50-75%, todos `orcamento_excedido`).
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

// Cenário AUD-26: contexto já cancelado antes da 1ª tentativa — zero
// tentativas deve mapear para 504 NO_NODE_AVAILABLE retryable, nunca o
// 502 stale "internal error" do failure inicial.
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

// Cenário AUD-23: nó vivo que responde 503 contínuo deve ter LastSeen
// atualizado (liveness) — sem Touch ele envelheceria até indisponivel e
// causaria flapping de membership.
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

// Cenário AUD-27: fila de 3 com maxRaceFanout=1 — o corte aparece no
// contador FanoutCuts e só 1 tentativa acontece.
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

// Cenário AUD-38: 400 do nó é determinístico — fail-fast imediato sem
// failover para o próximo (como o 401), com 1 única tentativa total.
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

// Cenário AUD-39: CLOUD_FALLBACK_FAILED herda retryable da causa raiz da
// fila — sobrecarga retryable continua retryable; 404 definitivo continua
// definitivo.
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

// Cenário AUD-29: clock que avança 50ms por chamada — a latência da
// tentativa (duas leituras consecutivas do clock, sem wall no meio)
// deve ser exatamente 50ms, determinística.
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

// Cenário AUD-42: execution_metrics de ~100KiB vira stub truncado com o
// tamanho original — a linha continua JSON válido e limitada a ~64KiB.
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
	// Dentro do teto passa verbatim.
	small := json.RawMessage(`{"a":1}`)
	if got := truncateMetrics(small); string(got) != string(small) {
		t.Fatalf("metrics pequena alterada: %s", got)
	}
}
