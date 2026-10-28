package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// Regressão do todo 3 (selflearning-consistency-fixes, C1/D16): fila vazia
// (nenhum candidato: sem nó ativo/hospedeiro, todos sobrecarregados/projetados,
// inaptos ou filtrados por marca) DEVE alcançar o fallback de nuvem quando ele
// está configurado. Sem nuvem, o desfecho 503 NO_NODE_AVAILABLE permanece
// byte-idêntico ao caminho legado (inclusive recordNoNode).

// newEmptyQueueInvLog abre um invocation log isolado e devolve o logger + path.
func newEmptyQueueInvLog(t *testing.T) (*InvocationLog, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "invocations.jsonl")
	l, err := NewInvocationLog(path)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	return l, path
}

// readEmptyQueueInvRecords fecha o log e devolve as linhas decodificadas.
func readEmptyQueueInvRecords(t *testing.T, l *InvocationLog, path string) []InvocationRecord {
	t.Helper()
	if err := l.Close(); err != nil {
		t.Fatalf("Close invLog: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile invLog: %v", err)
	}
	var recs []InvocationRecord
	for _, ln := range splitLines(t, raw) {
		var r InvocationRecord
		if err := json.Unmarshal(ln, &r); err != nil {
			t.Fatalf("linha inválida %q: %v", ln, err)
		}
		recs = append(recs, r)
	}
	return recs
}

func newCloudOKServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cloud":"ok"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

type emptyQueueCloudResp struct {
	Result           json.RawMessage `json:"result"`
	ExecutionMetrics struct {
		CloudFallback bool `json:"cloud_fallback"`
	} `json:"execution_metrics"`
}

// (a) fila vazia + nuvem configurada + Forward 2xx => 200 cloud_fallback:true,
// e recordNoNode NUNCA é chamado quando a nuvem serviu a resposta.
func TestGateway_EmptyQueueCloudFallback(t *testing.T) {
	cloudSrv := newCloudOKServer(t)

	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:          ns,
		Monitor:        monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
	})
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	var resp emptyQueueCloudResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v body=%s", err, rec.Body.String())
	}
	if !resp.ExecutionMetrics.CloudFallback {
		t.Fatalf("cloud_fallback=false want true body=%s", rec.Body.String())
	}

	for _, r := range readEmptyQueueInvRecords(t, il, path) {
		if r.NodeID == noNodeAvailableID {
			t.Fatalf("recordNoNode chamado mesmo com a nuvem servindo a resposta")
		}
	}
}

// (b) fila vazia + SEM nuvem => 503 byte-idêntico (status+body+retryable) e
// recordNoNode como antes. Fixture pinada: mudanças futuras não podem alterar
// silenciosamente o envelope do caminho legado.
func TestGateway_EmptyQueueNoCloud503ByteIdentical(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	gw := newTestGateway(ns)
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	const wantBody = `{"code":"NO_NODE_AVAILABLE","message":"nenhum nó disponível para função \"fn\"","retryable":true,"fallback_applied":false}`
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content-type=%q want application/json", got)
	}
	if rec.Body.String() != wantBody {
		t.Fatalf("body divergiu do fixture byte-idêntico:\n got=%q\nwant=%q", rec.Body.String(), wantBody)
	}

	recs := readEmptyQueueInvRecords(t, il, path)
	if len(recs) != 1 {
		t.Fatalf("invLog lines=%d want 1 (recordNoNode)", len(recs))
	}
	if recs[0].NodeID != noNodeAvailableID || recs[0].Outcome != "falha" || recs[0].SloMet {
		t.Fatalf("linha no-cloud = %+v, want node=%s outcome=falha slo_met=false", recs[0], noNodeAvailableID)
	}
}

// (c) todos os candidatos excluídos por sobrecarga (projetada/observada) +
// nuvem => nuvem; os nós sobrecarregados NÃO são tocados.
func TestGateway_AllOverloadedCloudFallback(t *testing.T) {
	var nodeHits int32
	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nodeHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node":true}`))
	}))
	t.Cleanup(nodeSrv.Close)
	cloudSrv := newCloudOKServer(t)

	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "over", Layer: nodes.LayerBorda, Host: hostFromURL(t, nodeSrv.URL)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	mon := monitor.NewMonitor()
	// CPU 95 > max_cpu_pct 85: excluído tanto por FilterOverloaded quanto
	// pelo hard guard PROJETADO (projected = 95 + 0 + 0 > 85) do
	// RiskSelector — candidato esvazia e a fila chega vazia.
	mon.Update(newPkg("over", 95))

	now := time.Now()
	risk := &selection.RiskSelector{
		Base:             selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		Hist:             nil,
		ThroughputWindow: 60 * time.Second,
		Criteria:         selection.DefaultCriteria,
		Clock:            fixedClock(now),
		Learner:          learn.NewLearner(selection.DefaultRiskConfig().LearnConfig()),
		Cfg:              selection.DefaultRiskConfig(),
	}
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:          ns,
		Monitor:        mon,
		Selector:       risk,
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
		MinSamples:     1,
		Criteria:       selection.DefaultCriteria,
		Clock:          time.Now,
	})
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	var resp emptyQueueCloudResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v body=%s", err, rec.Body.String())
	}
	if !resp.ExecutionMetrics.CloudFallback {
		t.Fatalf("cloud_fallback=false want true body=%s", rec.Body.String())
	}
	if got := atomic.LoadInt32(&nodeHits); got != 0 {
		t.Fatalf("nó sobrecarregado foi contatado: hits=%d want 0", got)
	}
	for _, r := range readEmptyQueueInvRecords(t, il, path) {
		if r.NodeID == noNodeAvailableID {
			t.Fatalf("recordNoNode chamado mesmo com a nuvem servindo a resposta")
		}
	}
}

// (d) fila vazia + nuvem configurada mas falhando => 502 CLOUD_FALLBACK_FAILED
// preservando retryable=true (herdado da causa raiz NO_NODE_AVAILABLE).
func TestGateway_EmptyQueueCloudFailure(t *testing.T) {
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("cloud down"))
	}))
	t.Cleanup(cloudSrv.Close)

	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:          ns,
		Monitor:        monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
	})
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "CLOUD_FALLBACK_FAILED" || !er.Retryable || !er.FallbackApplied {
		t.Fatalf("code=%q retryable=%v fallback=%v want CLOUD_FALLBACK_FAILED/true/true", er.Code, er.Retryable, er.FallbackApplied)
	}
	for _, r := range readEmptyQueueInvRecords(t, il, path) {
		if r.NodeID == noNodeAvailableID {
			t.Fatalf("recordNoNode chamado mesmo na tentativa de nuvem")
		}
	}
}

// (e) SLO minúsculo: orçamento expira ANTES da chamada de nuvem
// (remaining<=0) => o 503 NO_NODE_AVAILABLE é escrito, a nuvem NÃO é chamada
// e recordNoNode NÃO é executado (o caminho de nuvem respondeu).
func TestGateway_EmptyQueueCloudBudgetExpired(t *testing.T) {
	var cloudCalls int32
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&cloudCalls, 1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"cloud":"ok"}`))
	}))
	t.Cleanup(cloudSrv.Close)

	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:          ns,
		Monitor:        monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
	})
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","slo":{"max_latency_ms":1},"input":{"a":1}}`))
	// Deadline do pai já vencido garante remaining<=0 de forma determinística
	// (o overall continua sendo o SLO minúsculo de 1ms).
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	req = req.WithContext(expired)

	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NO_NODE_AVAILABLE" || !er.Retryable || er.FallbackApplied {
		t.Fatalf("code=%q retryable=%v fallback=%v want NO_NODE_AVAILABLE/true/false", er.Code, er.Retryable, er.FallbackApplied)
	}
	if got := atomic.LoadInt32(&cloudCalls); got != 0 {
		t.Fatalf("nuvem chamada com remaining<=0: calls=%d want 0", got)
	}
	if recs := readEmptyQueueInvRecords(t, il, path); len(recs) != 0 {
		t.Fatalf("invLog lines=%d want 0 (nem nuvem nem recordNoNode no caminho remaining<=0)", len(recs))
	}
}
