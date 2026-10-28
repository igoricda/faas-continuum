package gateway

// Regression group of the recordability: each class without a real measurement of
// performance goes only to the invocation log and never to the history, while the
// percentile and the count reflect the cost of a success. Each test below anchors
// one scenario.

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

// recordabilityFixture builds a gateway with a history and a log for the scenarios
// below, and returns the gateway, the history and the path of the log.
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

// invokeRecordability runs an invocation and returns the recorded status.
func invokeRecordability(t *testing.T, gw *Gateway) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	return rec
}

// invLogOutcomes reads the outcomes recorded in the log.
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

// An unauthorized answer, from a wrong or expired token, takes about a millisecond
// and cannot pollute the count or the percentile, so the fail-fast leaves the
// history empty and writes one failed line in the log.
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

// A stale map in a single node exhausts the queue, so the final not found answer
// does not train the history while the log keeps the failure.
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

// A success with a body above the ceiling is not a measurement of the node, so the
// history stays empty and the log keeps the failure, while the contract stays a bad
// gateway attributed to the node.
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

// A success with a non-JSON body is not a measurement of the node, so the history
// stays empty and the log keeps the failure.
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

// A fast generic failure of a few milliseconds cannot lower the percentile,
// since a sick node would look fast. The history stays empty and the contract stays a bad gateway.
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

// A legitimate timeout trains the history with a timeout outcome, which keeps
// the contract of the node timeout, but stays outside the percentile and the
// count, which reflect the cost of a success.
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

// A capacity rejection, of an exhausted pool, trains the history as a failure
// without latency, so the total, the success rate and the veto by rate see the
// saturation of the edge, while the percentile and the count keep only the
// successes. The HTTP contract stays a rejection marked as an overload.
// The rejection is a saturation and not a lack of reliability, since the node is
// correctly refusing what it cannot handle. The attempt stays recorded and
// outside the percentile, and it must not lower the service rate, since that is
// what made the veto ban the fastest node of the campaign.
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

// Scenario of the absence of a node: a rejection without an available node
// records a failure with an unmet objective, since outside the domain of a real attempt a zero latency is not a met objective.
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
