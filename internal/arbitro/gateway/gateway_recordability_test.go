package gateway

// Regressão do grupo History/recordability (ADR-0007, AUD-01/AUD-21/AUD-30):
// cada classe sem medida real de performance vai só para o invLog, nunca
// para o History; p95/Count refletem custo de sucesso. Cada teste abaixo
// ancora um cenário do documento de auditoria.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// recordabilityFixture monta gateway com History + invLog para os cenários
// abaixo. Devolve o gateway, o histórico e o caminho do JSONL.
func recordabilityFixture(t *testing.T, nodeStatus int, nodeBody func() []byte) (*Gateway, *history.History, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if nodeStatus >= 200 && nodeStatus < 300 {
			w.Header().Set("Content-Type", "application/json")
		} else {
			w.WriteHeader(nodeStatus)
		}
		_, _ = w.Write(nodeBody())
	}))
	t.Cleanup(srv.Close)
	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
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
	now := time.Now().UTC().Truncate(time.Millisecond)
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, History: h, Clock: fixedClock(now)})
	gw.SetInvocationLog(l)
	return gw, h, logPath
}

// invokeRecordability executa um invoke e devolve o status gravado.
func invokeRecordability(t *testing.T, gw *Gateway) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	return rec
}

// invLogOutcomes lê os outcomes registrados no JSONL.
func invLogOutcomes(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile invLog: %v", err)
	}
	var out []string
	for _, ln := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(ln) == "" {
			continue
		}
		var r InvocationRecord
		if err := json.Unmarshal([]byte(ln), &r); err != nil {
			t.Fatalf("linha invLog inválida: %v", err)
		}
		out = append(out, r.Outcome)
	}
	return out
}

// Cenário AUD-01 (401): token errado/expirado (~1ms) não pode poluir
// Count/p95 — fail-fast com History vazio e 1 linha falha no invLog.
func TestRecordability401StaysOutOfHistory(t *testing.T) {
	gw, h, logPath := recordabilityFixture(t, http.StatusUnauthorized, func() []byte {
		return []byte(`{"code":"UNAUTHORIZED"}`)
	})
	rec := invokeRecordability(t, gw)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401 body=%s", rec.Code, rec.Body.String())
	}
	if len(h.Keys()) != 0 {
		t.Fatalf("history tem %d chaves após 401, esperado 0", len(h.Keys()))
	}
	if got := invLogOutcomes(t, logPath); len(got) != 1 || got[0] != history.OutcomeFalha {
		t.Fatalf("invLog outcomes=%v, esperado [falha]", got)
	}
}

// Cenário AUD-01 (404): mapa obsoleto num único nó esgota a fila —
// FUNCTION_NOT_FOUND final sem treinar o History; invLog guarda a falha.
func TestRecordability404StaysOutOfHistory(t *testing.T) {
	gw, h, logPath := recordabilityFixture(t, http.StatusNotFound, func() []byte {
		return []byte(`{"code":"FUNCTION_NOT_FOUND"}`)
	})
	rec := invokeRecordability(t, gw)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want 404 body=%s", rec.Code, rec.Body.String())
	}
	if er := decodeErrResp(t, rec); er.Code != "FUNCTION_NOT_FOUND" {
		t.Fatalf("code=%q want FUNCTION_NOT_FOUND", er.Code)
	}
	if len(h.Keys()) != 0 {
		t.Fatalf("history tem %d chaves após 404, esperado 0", len(h.Keys()))
	}
	if got := invLogOutcomes(t, logPath); len(got) != 1 || got[0] != history.OutcomeFalha {
		t.Fatalf("invLog outcomes=%v, esperado [falha]", got)
	}
}

// Cenário AUD-01 (framing): 2xx com corpo acima de 8 MiB não é medida do
// nó — History vazio, invLog com a falha; contrato segue 502 NODE_ERROR.
func TestRecordabilityResponseTooLargeStaysOutOfHistory(t *testing.T) {
	big := make([]byte, (8<<20)+1)
	for i := range big {
		big[i] = 'x'
	}
	gw, h, logPath := recordabilityFixture(t, http.StatusOK, func() []byte { return big })
	rec := invokeRecordability(t, gw)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502 body=%s", rec.Code, rec.Body.String())
	}
	if er := decodeErrResp(t, rec); er.Code != "NODE_ERROR" {
		t.Fatalf("code=%q want NODE_ERROR", er.Code)
	}
	if len(h.Keys()) != 0 {
		t.Fatalf("history tem %d chaves após response_too_large, esperado 0", len(h.Keys()))
	}
	if got := invLogOutcomes(t, logPath); len(got) != 1 || got[0] != history.OutcomeFalha {
		t.Fatalf("invLog outcomes=%v, esperado [falha]", got)
	}
}

// Cenário AUD-01 (framing): 2xx com corpo não-JSON não é medida do nó —
// History vazio, invLog com a falha.
func TestRecordabilityInvalidJSONStaysOutOfHistory(t *testing.T) {
	gw, h, logPath := recordabilityFixture(t, http.StatusOK, func() []byte {
		return []byte(`{não-json`)
	})
	rec := invokeRecordability(t, gw)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502 body=%s", rec.Code, rec.Body.String())
	}
	if len(h.Keys()) != 0 {
		t.Fatalf("history tem %d chaves após invalid_json, esperado 0", len(h.Keys()))
	}
	if got := invLogOutcomes(t, logPath); len(got) != 1 || got[0] != history.OutcomeFalha {
		t.Fatalf("invLog outcomes=%v, esperado [falha]", got)
	}
}

// Cenário AUD-01 (falha genérica): 500 rápido (5ms) não pode abaixar o p95
// — nó doente pareceria rápido. History vazio; contrato segue 502.
func TestRecordabilityNode500StaysOutOfHistory(t *testing.T) {
	gw, h, logPath := recordabilityFixture(t, http.StatusInternalServerError, func() []byte {
		return []byte(`oops`)
	})
	rec := invokeRecordability(t, gw)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502 body=%s", rec.Code, rec.Body.String())
	}
	if len(h.Keys()) != 0 {
		t.Fatalf("history tem %d chaves após 500, esperado 0", len(h.Keys()))
	}
	if got := invLogOutcomes(t, logPath); len(got) != 1 || got[0] != history.OutcomeFalha {
		t.Fatalf("invLog outcomes=%v, esperado [falha]", got)
	}
}

// Cenário AUD-21 (timeout legítimo): 504 real do nó treina o History com
// outcome timeout (contrato 504 NODE_INVOCATION_TIMEOUT preservado) — mas
// fora do p95/Count, que refletem custo de sucesso.
func TestRecordabilityNode504TrainsHistoryAsTimeout(t *testing.T) {
	gw, h, logPath := recordabilityFixture(t, http.StatusGatewayTimeout, func() []byte {
		return []byte(`{"code":"INVOCATION_TIMEOUT"}`)
	})
	rec := invokeRecordability(t, gw)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504 body=%s", rec.Code, rec.Body.String())
	}
	if er := decodeErrResp(t, rec); er.Code != "NODE_INVOCATION_TIMEOUT" {
		t.Fatalf("code=%q want NODE_INVOCATION_TIMEOUT", er.Code)
	}
	if len(h.Keys()) != 2 {
		t.Fatalf("history tem %d chaves após 504, esperado 2 (timeout treina primária + agregada)", len(h.Keys()))
	}
	if got := invLogOutcomes(t, logPath); len(got) != 1 || got[0] != history.OutcomeTimeout {
		t.Fatalf("invLog outcomes=%v, esperado [timeout]", got)
	}
}

// Emenda 2026-09-24 (AUD-01 revisitado): rejeição de capacidade (503
// POOL_EXHAUSTED) passa a treinar o History como FALHA SEM LATÊNCIA —
// Total/SuccessRate/VetoByRate enxergam a saturação da borda; p95/Count
// seguem só com sucesso. O contrato HTTP continua 503 NODE_OVERLOADED.
// Rejeição de capacidade é SATURAÇÃO, não falta de confiabilidade: o nó está
// recusando corretamente o que não dá conta (plano-sinal-saturacao-selflearning
// D1). O contrato é que a tentativa continue GRAVADA e fora do p95 — e que
// NÃO derrube a taxa de serviço, porque foi exatamente isso que fez o veto
// banir o nó mais rápido da campanha arb4-*.
func TestRecordabilityCapacityTrainsHistoryAsSaturation(t *testing.T) {
	gw, h, logPath := recordabilityFixture(t, http.StatusServiceUnavailable, func() []byte {
		return []byte(`{"code":"POOL_EXHAUSTED"}`)
	})
	rec := invokeRecordability(t, gw)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	keys := h.Keys()
	if len(keys) != 2 {
		t.Fatalf("history tem %d chaves após 503, esperado 2 (primária + agregada)", len(keys))
	}
	count, p95, total, rate, ok := h.Stats(keys[0])
	if ok || count != 0 || p95 != 0 {
		t.Fatalf("p95/Count contaminados por rejeição de capacidade: count=%d p95=%d ok=%v", count, p95, ok)
	}
	if total != 0 || rate != history.LaplaceRate(0, 0) {
		t.Fatalf("total=%d rate=%v, esperado (0,%v): rejeição é saturação, não falha de serviço", total, rate, history.LaplaceRate(0, 0))
	}
	if history.VetoByRate(total, rate) {
		t.Fatal("rejeição de capacidade vetou o nó: o veto só pode ver falha de serviço")
	}
	if full, okFull := h.StatsFull(keys[0], time.Hour, time.Now()); !okFull || full.Saturation != 1 {
		t.Fatalf("Saturation=%d ok=%v, esperado 1: a recusa precisa ficar visível", full.Saturation, okFull)
	}
	if got := invLogOutcomes(t, logPath); len(got) != 1 || got[0] != history.OutcomeRejeicaoCapacidade {
		t.Fatalf("invLog outcomes=%v, esperado [rejeicao_capacidade]", got)
	}
}

// Cenário AUD-30: rejeição sem nó disponível grava falha com slo_met=false
// (fora do domínio de tentativa real, latência 0 não é SLO cumprido).
func TestRecordNoNodeWritesSloMetFalse(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	now := time.Now().UTC().Truncate(time.Millisecond)
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, Clock: fixedClock(now)})
	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gw.SetInvocationLog(l)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1},"slo":{"max_latency_ms":500}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile invLog: %v", err)
	}
	var r InvocationRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &r); err != nil {
		t.Fatalf("linha invLog inválida: %v", err)
	}
	if r.Outcome != history.OutcomeFalha {
		t.Fatalf("outcome=%q want falha", r.Outcome)
	}
	if r.SloMet {
		t.Fatalf("slo_met=true want false (sem tentativa real não há SLO cumprido)")
	}
	if r.ObservedLatencyMS != 0 {
		t.Fatalf("observed_latency_ms=%d want 0", r.ObservedLatencyMS)
	}
}
