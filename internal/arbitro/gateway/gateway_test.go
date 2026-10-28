package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// defaultLimits espelha selection_test.defaultLimits (mesmos tetos da Sprint 6);
// duplicado aqui porque testes de pacotes distintos não compartilham identificadores.
var defaultLimits = selection.LimitsConfig{
	MaxCPUPct:   85,
	MaxMemPct:   90,
	MaxPSIAvg10: 25,
}

// fixedClock congela o tempo (duplicado de selection_test de propósito).
func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

func hostFromURL(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url %q: %v", raw, err)
	}
	return u.Host
}

func newTestGateway(ns *nodes.NodeStore) *Gateway {
	mon := monitor.NewMonitor()
	return NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}})
}

func decodeErrResp(t *testing.T, rec *httptest.ResponseRecorder) ErrorResponse {
	t.Helper()
	var er ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &er); err != nil {
		t.Fatalf("decode ErrorResponse: %v body=%q", err, rec.Body.String())
	}
	return er
}

func TestGateway_HappyPath(t *testing.T) {
	// Nó fake que captura o corpo enviado pelo Gateway.
	var capturedBody []byte
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		body, _ := json.Marshal(map[string]any{"ok": true})
		// Verifica que Gateway encaminhou slo+input
		raw, _ := json.Marshal(r.Body)
		_ = raw
		b, _ := readAll(r)
		capturedBody = b
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	// Captura verbatim do nó
	expectedNodeBody := json.RawMessage(`{"result":"hello"}`)

	// Recria server que retorna verbatim body não envolto
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		b, _ := readAll(r)
		capturedBody = b
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(expectedNodeBody)
	}))
	defer srv2.Close()
	host2 := hostFromURL(t, srv2.URL)
	ns2, _ := nodes.NewNodeStore("")
	_, _ = ns2.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: host2})
	nBefore, _ := ns2.Get("n1")
	before := nBefore
	gw2 := newTestGateway(ns2)

	slo := `{"max_latency_ms":5000}`
	input := `{"x":1}`
	reqBody := `{"function":"myfunc","slo":` + slo + `,"input":` + input + `}`
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	time.Sleep(2 * time.Millisecond)
	gw2.Handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content-type=%q want application/json", rec.Header().Get("Content-Type"))
	}
	if !bytes.Equal(rec.Body.Bytes(), expectedNodeBody) {
		t.Fatalf("body verbatim mismatch: got %q want %q", rec.Body.String(), string(expectedNodeBody))
	}
	if capturedPath != "/function/myfunc/invoke" {
		t.Fatalf("path=%q want /function/myfunc/invoke", capturedPath)
	}
	// Verifica que input foi encaminhado (slo removido do wire por todo 6).
	var fwd map[string]json.RawMessage
	if err := json.Unmarshal(capturedBody, &fwd); err != nil {
		t.Fatalf("forwarded body not json: %v captured=%q", err, string(capturedBody))
	}
	if _, ok := fwd["input"]; !ok {
		t.Fatalf("forwarded missing input: %s", string(capturedBody))
	}
	if !json.Valid(fwd["input"]) {
		t.Fatalf("forwarded input not json")
	}
	if _, ok := fwd["slo"]; ok {
		t.Fatalf("forwarded unexpected slo: %s", string(capturedBody))
	}

	after, _ := ns2.Get("n1")
	if !after.LastSeen.After(before.LastSeen) {
		t.Fatalf("last_seen not updated: before=%v after=%v", before.LastSeen, after.LastSeen)
	}
	_ = srv
}

func readAll(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	return buf.Bytes(), err
}

func TestGateway_NodeUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	host := hostFromURL(t, srv.URL)
	srv.Close() // fecha antes do invoke

	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: host})
	gw := newTestGateway(ns)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NODE_UNAVAILABLE" {
		t.Fatalf("code=%q want NODE_UNAVAILABLE", er.Code)
	}
	if !er.Retryable {
		t.Fatalf("retryable false want true")
	}
	if er.FallbackApplied {
		t.Fatalf("fallback true want false")
	}
}

func TestGateway_Node404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"FUNCTION_NOT_FOUND"}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)})
	gw := newTestGateway(ns)
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want 404", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "FUNCTION_NOT_FOUND" {
		t.Fatalf("code=%q want FUNCTION_NOT_FOUND", er.Code)
	}
	if er.Retryable {
		t.Fatalf("retryable true want false")
	}
}

func TestGateway_Node503Overloaded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"POOL_EXHAUSTED"}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)})
	gw := newTestGateway(ns)
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NODE_OVERLOADED" {
		t.Fatalf("code=%q want NODE_OVERLOADED", er.Code)
	}
	if !er.Retryable {
		t.Fatalf("retryable false want true")
	}
}

func TestGateway_SLOTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":1}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)})
	gw := newTestGateway(ns)
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","slo":{"max_latency_ms":50},"input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	// Orçamento esgotado no meio da tentativa (AUD-22, adendo ADR-0008
	// 2026-09-17): árbitro-timeout => 504, não 502 atribuído ao nó.
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NODE_INVOCATION_TIMEOUT" {
		t.Fatalf("code=%q want NODE_INVOCATION_TIMEOUT (budget do árbitro)", er.Code)
	}
	if !er.Retryable {
		t.Fatalf("retryable false want true")
	}
}

func TestGateway_EmptyQueue(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	// Nenhum nó registrado -> fila vazia
	gw := newTestGateway(ns)
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NO_NODE_AVAILABLE" {
		t.Fatalf("code=%q want NO_NODE_AVAILABLE", er.Code)
	}
	if !er.Retryable {
		t.Fatalf("retryable false want true")
	}

	// Também fila vazia por filtro Functions
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	ns2, _ := nodes.NewNodeStore("")
	_, _ = ns2.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL), Functions: []string{"other"}})
	gw2 := newTestGateway(ns2)
	req2 := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec2 := httptest.NewRecorder()
	gw2.Handle(rec2, req2)
	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("filtered fila vazia status=%d want 503", rec2.Code)
	}
	er2 := decodeErrResp(t, rec2)
	if er2.Code != "NO_NODE_AVAILABLE" {
		t.Fatalf("filtered code=%q want NO_NODE_AVAILABLE", er2.Code)
	}
}

func TestGateway_InvalidBodyNoInput(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	gw := newTestGateway(ns)
	cases := []string{
		`{"function":"fn"}`,
		`{"function":"","input":{"a":1}}`,
		`not-json`,
		`{"function":"fn","input":null}`,
	}
	for _, body := range cases {
		req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(body))
		rec := httptest.NewRecorder()
		gw.Handle(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %q: status=%d want 400", body, rec.Code)
		}
		er := decodeErrResp(t, rec)
		if er.Code != "INVALID_REQUEST" {
			t.Fatalf("body %q: code=%q want INVALID_REQUEST", body, er.Code)
		}
	}
}

func TestGateway_Node2xxLargeBody(t *testing.T) {
	large := bytes.Repeat([]byte("a"), 9*1024*1024)
	// Wrap as JSON array to make it valid JSON but oversized
	// Use a JSON string that is 9MiB when encoded; simplest: make a JSON object with a big string field
	bigJSON := make([]byte, 0, 9*1024*1024+20)
	bigJSON = append(bigJSON, `{"data":"`...)
	bigJSON = append(bigJSON, large...)
	bigJSON = append(bigJSON, `"}`...)
	_ = large

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write(bigJSON)
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)})
	gw := newTestGateway(ns)
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("large body status=%d want 502", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NODE_ERROR" {
		t.Fatalf("large body code=%q want NODE_ERROR", er.Code)
	}
	// last_seen NÃO deve ter sido atualizado em erro
	// Não estritamente necessário, mas garante no-touch em falha
}

func TestGateway_Node2xxNonJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`not-json`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)})
	gw := newTestGateway(ns)
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("non-json status=%d want 502", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NODE_ERROR" {
		t.Fatalf("non-json code=%q want NODE_ERROR", er.Code)
	}
}

func TestGateway_Node504(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGatewayTimeout)
		_, _ = w.Write([]byte(`{"code":"INVOCATION_TIMEOUT"}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)})
	gw := newTestGateway(ns)
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NODE_INVOCATION_TIMEOUT" {
		t.Fatalf("code=%q want NODE_INVOCATION_TIMEOUT", er.Code)
	}
	if !er.Retryable {
		t.Fatalf("retryable false want true")
	}
}

func TestGateway_Node500Generic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`oops`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)})
	gw := newTestGateway(ns)
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NODE_ERROR" {
		t.Fatalf("code=%q want NODE_ERROR", er.Code)
	}
}

func TestGateway_PayloadTooLarge(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	gw := newTestGateway(ns)
	big := bytes.Repeat([]byte("a"), 9*1024*1024)
	body := `{"function":"fn","input":"` + string(big) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(body))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d want 413", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "PAYLOAD_TOO_LARGE" {
		t.Fatalf("code=%q want PAYLOAD_TOO_LARGE", er.Code)
	}
}

func TestQueueSelector(t *testing.T) {
	// Helpers
	mon := monitor.NewMonitor()
	limits := defaultLimits

	// Filtra por Functions: só n2 hospeda "fn"
	sel := selection.QueueSelector{Mon: mon, Cfg: limits}
	nds := []nodes.Node{
		{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h1", Functions: []string{"other"}},
		{NodeID: "n2", Layer: nodes.LayerBorda, Host: "h2", Functions: []string{"fn"}},
	}
	out := sel.Select(nds, "fn", nil)
	if len(out) != 1 || out[0].NodeID != "n2" {
		t.Fatalf("filter Functions: got %v want [n2]", out)
	}

	// Lista vazia e nil elegível para qualquer função
	nds2 := []nodes.Node{
		{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h1", Functions: []string{}},
		{NodeID: "n2", Layer: nodes.LayerBorda, Host: "h2", Functions: nil},
		{NodeID: "n3", Layer: nodes.LayerBorda, Host: "h3"},
	}
	out2 := sel.Select(nds2, "any", nil)
	if len(out2) != 3 {
		t.Fatalf("empty/nil Functions eligible: got %v want 3", out2)
	}

	// Delega ordenação a RankNodes: borda antes de nevoa
	nds3 := []nodes.Node{
		{NodeID: "nevoa1", Layer: nodes.LayerNevoa, Host: "h1"},
		{NodeID: "borda1", Layer: nodes.LayerBorda, Host: "h2"},
	}
	out3 := sel.Select(nds3, "fn", nil)
	if len(out3) != 2 || out3[0].NodeID != "borda1" || out3[1].NodeID != "nevoa1" {
		t.Fatalf("rank borda before nevoa: got %v", out3)
	}

	// Delega ordenação por cpu dentro da mesma camada: usa Monitor.Latest
	mon2 := monitor.NewMonitor()
	mon2.Update(newPkg("b1", 30))
	mon2.Update(newPkg("b2", 10))
	mon2.Update(newPkg("b3", 20))
	sel2 := selection.QueueSelector{Mon: mon2, Cfg: limits}
	nds4 := []nodes.Node{
		{NodeID: "b1", Layer: nodes.LayerBorda, Host: "h1"},
		{NodeID: "b2", Layer: nodes.LayerBorda, Host: "h2"},
		{NodeID: "b3", Layer: nodes.LayerBorda, Host: "h3"},
	}
	out4 := sel2.Select(nds4, "fn", nil)
	if len(out4) != 3 || out4[0].NodeID != "b2" || out4[1].NodeID != "b3" || out4[2].NodeID != "b1" {
		t.Fatalf("cpu ordering: got %v want [b2 b3 b1]", nodeIDs(out4))
	}

	// Ausente no Monitor => tratado como +Inf (fica ao final)
	mon3 := monitor.NewMonitor()
	mon3.Update(newPkg("b1", 5))
	sel3 := selection.QueueSelector{Mon: mon3, Cfg: limits}
	nds5 := []nodes.Node{
		{NodeID: "b1", Layer: nodes.LayerBorda, Host: "h1"},
		{NodeID: "b2", Layer: nodes.LayerBorda, Host: "h2"},
	}
	out5 := sel3.Select(nds5, "fn", nil)
	if len(out5) != 2 || out5[0].NodeID != "b1" || out5[1].NodeID != "b2" {
		t.Fatalf("missing telemetry +Inf at end: got %v", nodeIDs(out5))
	}

	// Não filtra DerivedStatus aqui: nó stale permanece se ativo por Functions
	sel4 := selection.QueueSelector{Mon: mon, Cfg: limits}
	nds6 := []nodes.Node{
		{NodeID: "stale", Layer: nodes.LayerBorda, Host: "h1", LastSeen: time.Now().Add(-10 * time.Minute)},
	}
	out6 := sel4.Select(nds6, "fn", nil)
	if len(out6) != 1 {
		t.Fatalf("DerivedStatus not filtered in selector: got %v want 1", out6)
	}

	// Hard-limit removido (2026-09-28) e sobrecarga delegada a RankNodes:
	// sobrecarregado filtrado; SLO NÃO filtra por camada.
	mon4 := monitor.NewMonitor()
	mon4.Update(newPkg("over", 95)) // cpu 95 > 85 => overloaded
	sel5 := selection.QueueSelector{Mon: mon4, Cfg: limits}
	nds7 := []nodes.Node{
		{NodeID: "over", Layer: nodes.LayerBorda, Host: "h1"},
		{NodeID: "ok", Layer: nodes.LayerBorda, Host: "h2"},
	}
	out7 := sel5.Select(nds7, "fn", nil)
	if len(out7) != 1 || out7[0].NodeID != "ok" {
		t.Fatalf("overloaded filtered: got %v want [ok]", nodeIDs(out7))
	}
	slo := &selection.SLO{MaxLatencyMS: 8000}
	nds8 := []nodes.Node{
		{NodeID: "borda1", Layer: nodes.LayerBorda, Host: "h1"},
		{NodeID: "nevoa1", Layer: nodes.LayerNevoa, Host: "h2"},
	}
	out8 := sel.Select(nds8, "fn", slo)
	if len(out8) != 2 || out8[0].NodeID != "borda1" || out8[1].NodeID != "nevoa1" {
		t.Fatalf("SLO não deve filtrar por camada: got %v want [borda1 nevoa1]", nodeIDs(out8))
	}
}

// helpers for TestQueueSelector
func newPkg(nodeID string, cpu float64) telemetryPackageAlias {
	// local alias to avoid import cycle naming; use telemetry.Package directly via helper
	return telemetryPackageAlias{NodeID: nodeID, CPUUsagePct: cpu}
}

type telemetryPackageAlias = telemetry.Package

func nodeIDs(nds []nodes.Node) []string {
	ids := make([]string, len(nds))
	for i, n := range nds {
		ids[i] = n.NodeID
	}
	return ids
}

func TestGateway_StaleFiltering(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	// Cria nó stale manualmente (LastSeen 5 min atrás)
	stale := nodes.Node{NodeID: "stale", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL), LastSeen: time.Now().Add(-5 * time.Minute)}
	// Injeta direto no map (sem Upsert que sobrescreve LastSeen)
	ns2 := ns
	// Use hack via Upsert then manipulate: we need to access private field.
	// Instead, test via time: Upsert cria LastSeen=now, então fica ativo; filtra não deve descartar.
	// Para stale, vamos criar gateway com staleAfter curto e fazer sleep.
	gw := newTestGateway(ns)
	gw.staleAfter = 10 * time.Millisecond
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)})
	time.Sleep(20 * time.Millisecond)
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("stale filtered status=%d want 503", rec.Code)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NO_NODE_AVAILABLE" {
		t.Fatalf("code=%q want NO_NODE_AVAILABLE", er.Code)
	}
	_ = stale
	_ = ns2
}

func TestGateway_ContextCancelBeforeFirstAttempt(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	// Register a node so the queue is non-empty and we enter the attempt loop.
	host := "127.0.0.1:8080"
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: host})
	gw := newTestGateway(ns)

	// Create a canceled context so the loop breaks on first iteration.
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	// Zero tentativas por contexto cancelado (AUD-26, adendo ADR-0008
	// 2026-09-17): 504 NO_NODE_AVAILABLE retryable, nunca o 502 stale do
	// failure inicial.
	if rec.Code == 0 {
		t.Fatalf("status=0: expected a non-zero status when context is canceled before first attempt")
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NO_NODE_AVAILABLE" {
		t.Fatalf("code=%q want NO_NODE_AVAILABLE", er.Code)
	}
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("rec.Code=%d want %d (http.StatusGatewayTimeout)", rec.Code, http.StatusGatewayTimeout)
	}
	if !er.Retryable {
		t.Fatalf("retryable false want true")
	}
	if er.FallbackApplied {
		t.Fatalf("fallback true want false")
	}
}

func TestGateway_CustomStaleAfter(t *testing.T) {
	// Testa que staleAfter personalizado é passado corretamente para o Gateway
	// quando usando o RiskSelector com Base contendo QueueSelector.
	now := time.Now()
	mon := monitor.NewMonitor()
	cfg := selection.LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}
	queueSel := selection.QueueSelector{Mon: mon, Cfg: cfg}
	riskSel := &selection.RiskSelector{Base: queueSel, Clock: fixedClock(now), Learner: learn.NewLearner(selection.DefaultRiskConfig().LearnConfig()), Cfg: selection.DefaultRiskConfig()}
	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: riskSel, StaleAfter: 5 * time.Minute, MinSamples: 1, Criteria: selection.DefaultCriteria, Clock: time.Now})
	if gw.staleAfter != 5*time.Minute {
		t.Fatalf("expected staleAfter=5m, got %v", gw.staleAfter)
	}
	// Teste com valor zero (deve cair para defaultStaleAfter)
	ns2, _ := nodes.NewNodeStore("")
	gw2 := NewGatewayWithHistory(GatewayConfig{Nodes: ns2, Monitor: mon, Selector: riskSel, MinSamples: 1, Criteria: selection.DefaultCriteria, Clock: time.Now})
	if gw2.staleAfter != defaultStaleAfter {
		t.Fatalf("expected staleAfter=defaultStaleAfter when 0, got %v", gw2.staleAfter)
	}
}

// ---------------------------------------------------------------------------
// gateway_classify_test.go — classify logic
// ---------------------------------------------------------------------------

func TestClassifyAttempt(t *testing.T) {
	// Helper to compare outcome/sloMet
	tests := []struct {
		name     string
		err      error
		latency  time.Duration
		slo      *selection.SLO
		criteria []selection.SLOCriterion
		wantOut  string
		wantMet  bool
	}{
		{
			name:     "sucesso latency dentro do SLO 800",
			err:      nil,
			latency:  100 * time.Millisecond,
			slo:      &selection.SLO{MaxLatencyMS: 800},
			criteria: selection.DefaultCriteria,
			wantOut:  history.OutcomeSucesso,
			wantMet:  true,
		},
		{
			name:     "sucesso latency acima do SLO 800",
			err:      nil,
			latency:  900 * time.Millisecond,
			slo:      &selection.SLO{MaxLatencyMS: 800},
			criteria: selection.DefaultCriteria,
			wantOut:  history.OutcomeSucesso,
			wantMet:  false,
		},
		{
			name:     "só vazão vacuamente verdadeira",
			err:      nil,
			latency:  5000 * time.Millisecond,
			slo:      &selection.SLO{MinThroughputRPS: 2},
			criteria: selection.DefaultCriteria,
			wantOut:  history.OutcomeSucesso,
			wantMet:  true,
		},
		{
			name:     "timeout com SLO generoso => sloMet false",
			err:      context.DeadlineExceeded,
			latency:  100 * time.Millisecond,
			slo:      &selection.SLO{MaxLatencyMS: 5000, MinThroughputRPS: 2},
			criteria: selection.DefaultCriteria,
			wantOut:  history.OutcomeTimeout,
			wantMet:  false,
		},
		{
			name:     "ambas dimensões latency estourada => false",
			err:      nil,
			latency:  900 * time.Millisecond,
			slo:      &selection.SLO{MaxLatencyMS: 800, MinThroughputRPS: 2},
			criteria: selection.DefaultCriteria,
			wantOut:  history.OutcomeSucesso,
			wantMet:  false,
		},
		{
			name:     "criteria nil => vacuamente verdadeiro se sucesso",
			err:      nil,
			latency:  100 * time.Millisecond,
			slo:      &selection.SLO{MaxLatencyMS: 800},
			criteria: nil,
			wantOut:  history.OutcomeSucesso,
			wantMet:  true,
		},
		{
			name:     "criteria nil com latency estourada ainda true (sem critérios)",
			err:      nil,
			latency:  900 * time.Millisecond,
			slo:      &selection.SLO{MaxLatencyMS: 800},
			criteria: nil,
			wantOut:  history.OutcomeSucesso,
			wantMet:  true,
		},
		{
			name:     "falha genérica => sloMet false",
			err:      errors.New("connection refused"),
			latency:  100 * time.Millisecond,
			slo:      &selection.SLO{MaxLatencyMS: 800},
			criteria: selection.DefaultCriteria,
			wantOut:  history.OutcomeFalha,
			wantMet:  false,
		},
		{
			name:     "timeout ancorado no status (sentinela 504) => timeout",
			err:      fmt.Errorf("%w: nó %q timeout na invocação", ErrAttemptTimeout, "n1"),
			latency:  100 * time.Millisecond,
			slo:      &selection.SLO{MaxLatencyMS: 5000},
			criteria: selection.DefaultCriteria,
			wantOut:  history.OutcomeTimeout,
			wantMet:  false,
		},
		{
			// Cenário AUD-21: porta "15040" contém "504" mas não é timeout —
			// sem o sentinela do status real, o outcome é falha.
			name:     "substring 504 solta (porta 15040) => falha",
			err:      errors.New(`falha ao contatar nó "n1": dial tcp 127.0.0.1:15040: connection refused`),
			latency:  100 * time.Millisecond,
			slo:      &selection.SLO{MaxLatencyMS: 5000},
			criteria: selection.DefaultCriteria,
			wantOut:  history.OutcomeFalha,
			wantMet:  false,
		},
		{
			name:     "context.Canceled => timeout",
			err:      context.Canceled,
			latency:  50 * time.Millisecond,
			slo:      &selection.SLO{MaxLatencyMS: 800},
			criteria: selection.DefaultCriteria,
			wantOut:  history.OutcomeTimeout,
			wantMet:  false,
		},
		{
			name:     "slo nil com sucesso => sloMet true (nenhum declarado)",
			err:      nil,
			latency:  100 * time.Millisecond,
			slo:      nil,
			criteria: selection.DefaultCriteria,
			wantOut:  history.OutcomeSucesso,
			wantMet:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, met := classifyAttempt(tc.err, tc.latency, tc.slo, tc.criteria)
			if out != tc.wantOut {
				t.Fatalf("outcome=%q want %q", out, tc.wantOut)
			}
			if met != tc.wantMet {
				t.Fatalf("sloMet=%v want %v", met, tc.wantMet)
			}
		})
	}
}

func TestClassifyOutcome(t *testing.T) {
	if got := classifyOutcome(nil); got != history.OutcomeSucesso {
		t.Fatalf("nil => %q want sucesso", got)
	}
	if got := classifyOutcome(context.DeadlineExceeded); got != history.OutcomeTimeout {
		t.Fatalf("DeadlineExceeded => %q want timeout", got)
	}
	if got := classifyOutcome(errors.New("fail")); got != history.OutcomeFalha {
		t.Fatalf("generic err => %q want falha", got)
	}
	if got := classifyOutcome(errors.New("504 Gateway Timeout")); got != history.OutcomeTimeout {
		t.Fatalf("504 => %q want timeout", got)
	}
	// Cenário AUD-21: timeout ancorado no sentinela do status real (sem
	// substring); porta "15040" contém "504" mas classifica como falha.
	if got := classifyOutcome(fmt.Errorf("%w: status real", ErrAttemptTimeout)); got != history.OutcomeTimeout {
		t.Fatalf("sentinela => %q want timeout", got)
	}
	if got := classifyOutcome(errors.New("dial tcp 127.0.0.1:15040: connection refused")); got != history.OutcomeFalha {
		t.Fatalf("porta 15040 => %q want falha", got)
	}
}

func TestNewGatewayWithHistoryDefaults(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	mon := monitor.NewMonitor()
	sel := selection.QueueSelector{Mon: mon, Cfg: defaultLimits}
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: sel})
	if gw.clock == nil {
		t.Fatalf("clock nil want time.Now")
	}
	if gw.criteria == nil {
		t.Fatalf("criteria nil want selection.DefaultCriteria")
	}
	if len(gw.criteria) != len(selection.DefaultCriteria) {
		t.Fatalf("criteria len=%d want %d", len(gw.criteria), len(selection.DefaultCriteria))
	}
	// clock injectable determinístico
	fixed := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	gw2 := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: sel, Criteria: selection.DefaultCriteria, Clock: func() time.Time { return fixed }})
	if !gw2.clock().Equal(fixed) {
		t.Fatalf("clock not injected")
	}
	// NewGateway preserva assinatura 5 args
	gw3 := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: sel})
	if gw3 == nil {
		t.Fatalf("NewGateway returned nil")
	}
	if gw3.hist != nil || gw3.minSamples != 0 {
		t.Fatalf("NewGateway defaults hist=%v minSamples=%d want nil/0", gw3.hist, gw3.minSamples)
	}
}

// ---------------------------------------------------------------------------
// gateway_race_test.go — races
// ---------------------------------------------------------------------------

// raceDefaultLimits espelha selection_test.raceDefaultLimits para os testes de
// corrida (mesmos tetos conservadores da Sprint 6).
var raceDefaultLimits = selection.LimitsConfig{
	MaxCPUPct:   85,
	MaxMemPct:   90,
	MaxPSIAvg10: 25,
}

// TestGatewayRace_Handle_List_Upsert_Concurrent testa data races entre (ver
// Handle em gateway_handle.go e List/Upsert em ../nodes/nodes.go):
// - Gateway.Handle (lê nós via List(), usa buffer do pool para o corpo)
// - NodeStore.List (chamado por Handle, devolve fatia nova)
// - NodeStore.Upsert (escritas concorrentes no registro de nós)
//
// Valida que:
// 1. A cópia antes do Put em gateway_handle.go:72 evita aliasing do buffer do pool
// 2. O filtro all[:0] em gateway_handle.go:36 é seguro (List devolve fatia nova)
// 3. Upsert concorrente não corrompe a iteração de List nem a seleção de nós
func TestGatewayRace_Handle_List_Upsert_Concurrent(t *testing.T) {
	// Servidor de nó lento que segura conexões para maximizar a sobreposição
	var nodeReqCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nodeReqCount.Add(1)
		// Segura a conexão brevemente para aumentar a janela de concorrência
		time.Sleep(5 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")

	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}

	// Pre-populate with one node
	if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: host}); err != nil {
		t.Fatalf("Upsert initial: %v", err)
	}

	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: raceDefaultLimits}})

	const numHandleCalls = 50
	const numUpsertCalls = 50

	var wg sync.WaitGroup
	wg.Add(numHandleCalls + numUpsertCalls)

	handleErrors := make([]error, numHandleCalls)
	upsertErrors := make([]error, numUpsertCalls)

	// Concurrent Handle calls
	for i := 0; i < numHandleCalls; i++ {
		i := i
		go func() {
			defer wg.Done()
			reqBody := `{"function":"testfn","input":{"x":1}}`
			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			// Add Authorization to test passthrough under race
			req.Header.Set("Authorization", "race-token-"+string(rune('A'+i%26)))

			rec := httptest.NewRecorder()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req = req.WithContext(ctx)

			gw.Handle(rec, req)

			// Accept 200, 502, 503, 504 as valid responses under race
			// (node may be removed by Upsert, or context may timeout)
			if rec.Code != http.StatusOK &&
				rec.Code != http.StatusBadGateway &&
				rec.Code != http.StatusServiceUnavailable &&
				rec.Code != http.StatusGatewayTimeout {
				handleErrors[i] = &unexpectedStatusError{code: rec.Code, body: rec.Body.String()}
			}
		}()
	}

	// Concurrent Upsert calls (add/remove nodes)
	for i := 0; i < numUpsertCalls; i++ {
		i := i
		go func() {
			defer wg.Done()
			nodeID := "race-node-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i%10))
			layer := nodes.LayerBorda
			if i%2 == 0 {
				layer = nodes.LayerNevoa
			}
			n := nodes.Node{
				NodeID:       nodeID,
				Layer:        layer,
				Host:         host,
				Functions:    []string{"testfn"},
				MinInstances: 1,
				PSISupported: true,
			}
			// Mix of Upsert and Delete
			if i%3 == 0 {
				// Delete some nodes
				if err := ns.Delete(nodeID); err != nil && !strings.Contains(err.Error(), "não encontrado") {
					upsertErrors[i] = err
				}
			} else {
				if _, err := ns.Upsert(n); err != nil {
					upsertErrors[i] = err
				}
			}
			// Also call List directly to race with Handle's List
			_ = ns.List()
		}()
	}

	wg.Wait()

	// Check for errors (excluding expected "not found" on delete)
	for i, err := range handleErrors {
		if err != nil {
			t.Errorf("Handle[%d] error: %v", i, err)
		}
	}
	for i, err := range upsertErrors {
		if err != nil {
			t.Errorf("Upsert[%d] error: %v", i, err)
		}
	}

	// Verify node registry is still consistent
	finalNodes := ns.List()
	if len(finalNodes) == 0 {
		t.Log("All nodes removed during race — acceptable")
	} else {
		t.Logf("Final node count: %d", len(finalNodes))
		for _, n := range finalNodes {
			if n.NodeID == "" || n.Host == "" || n.Layer == "" {
				t.Errorf("Corrupt node in registry: %+v", n)
			}
		}
	}

	t.Logf("Total node requests handled: %d", nodeReqCount.Load())
}

// TestGatewayRace_PooledBufferAliasing specifically targets the buffer pool
// aliasing scenario: concurrent Handle calls that encode request bodies
// into the same pooled buffer must not corrupt each other's bodyBytes.
// TestGatewayRace_PooledBufferAliasing prova o contrato de aliasing do
// pool de buffers (PROJECT §2, adendo edge §7: "copy obrigatório antes de
// Put"): 100 Handle concorrentes, cada um com input DISTINTO, e o nó
// devolve o corpo que RECEBEU. Só assim uma corruption de bodyBytes
// (o buffer do pool reusado por outra goroutine antes do envio) fica
// visível — a versão anterior ecoava o header Authorization, que não tem
// relação com o buffer, então o teste passaria mesmo com o bug presente.
func TestGatewayRace_PooledBufferAliasing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		got, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var fwd struct {
			Input json.RawMessage `json:"input"`
		}
		if err := json.Unmarshal(got, &fwd); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]string{"echo": string(fwd.Input)}
		data, _ := json.Marshal(resp)
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")

	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: host}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: raceDefaultLimits}})

	const numCalls = 100
	var wg sync.WaitGroup
	wg.Add(numCalls)

	results := make([]string, numCalls)
	errors := make([]error, numCalls)

	for i := 0; i < numCalls; i++ {
		i := i
		go func() {
			defer wg.Done()
			token := "token-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26))
			// Input ÚNICO por chamada: se o bodyBytes de uma goroutine for
			// sobrescrito por outra antes do envio, o nó devolve outro
			// payload e a comparação abaixo acusa. Com input repetido
			// (i%10) a corruption seria invisível.
			want := fmt.Sprintf(`{"data":"call-%d"}`, i)
			reqBody := fmt.Sprintf(`{"function":"echo","input":%s}`, want)
			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", token)

			rec := httptest.NewRecorder()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req = req.WithContext(ctx)

			gw.Handle(rec, req)

			if rec.Code == http.StatusOK {
				var resp map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					errors[i] = err
					return
				}
				// O eco é o INPUT que o nó recebeu. Divergir = o
				// bodyBytes desta chamada foi sobrescrito por outra
				// goroutine antes do envio (o bug de aliasing).
				if resp["echo"] != want {
					errors[i] = &bodyCorruptionError{expected: want, got: resp["echo"]}
					return
				}
				results[i] = want
			} else {
				// Non-200 sob race é aceitável (nó ocupado, etc.); a
				// ausência de erro de corruption é o que este teste mede.
				results[i] = "status-" + string(rune('0'+rec.Code/100))
			}
		}()
	}

	wg.Wait()

	// Check for body corruption errors
	corruptionCount := 0
	for i, err := range errors {
		if err != nil {
			if _, ok := err.(*bodyCorruptionError); ok {
				corruptionCount++
				t.Errorf("Call[%d] body corruption: %v", i, err)
			} else {
				t.Errorf("Call[%d] error: %v", i, err)
			}
		}
	}

	if corruptionCount > 0 {
		t.Fatalf("DETECTED %d body corruption(s) — pooled buffer aliasing bug!", corruptionCount)
	}

	t.Logf("All %d concurrent Handle calls completed without body corruption", numCalls)
}

// Helper error types for detailed reporting
type unexpectedStatusError struct {
	code int
	body string
}

func (e *unexpectedStatusError) Error() string {
	return "unexpected status " + string(rune('0'+e.code/100)) + "xx: " + e.body
}

type bodyCorruptionError struct {
	expected string
	got      string
}

func (e *bodyCorruptionError) Error() string {
	return "body corruption: input esperado=" + e.expected + " recebido=" + e.got
}

// (TestGatewayRace_ListDuringUpsert e o tipo corruptNodeError foram
// removidos: o corpo do teste exercitava apenas nodes.NodeStore (List vs
// Upsert vs Delete, 1000 iterações) sem tocar num Gateway — vírgula no
// nome. O owner do contrato de concorrência do store é
// internal/arbitro/nodes/nodes_test.go:TestNodeStore_UpsertConcorrenteIsNew;
// e a afirmação que importa aqui — o registro do gateway não corrompe
// durante List/Upsert concorrentes — já é de
// TestGatewayRace_Handle_List_Upsert_Concurrent, que roda Handle de verdade
// contra as mutações do store.)

// ---------------------------------------------------------------------------
// gateway_auth_test.go — 401 passthrough
// ---------------------------------------------------------------------------

// TestGateway_401PassThrough cobre o repasse de auth (correcao-seguranca,
// todo 8, I2): o Authorization do cliente é encaminhado verbatim ao nó e,
// com 401 do nó, o gateway responde 401 UNAUTHORIZED após exatamente 1
// tentativa — credencial errada não se cura com failover.
func TestGateway_401PassThrough(t *testing.T) {
	var attempts int64
	var fwdAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&attempts, 1)
		fwdAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&attempts, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv2.Close()

	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
		t.Fatal(err)
	}
	if _, err := ns.Upsert(nodes.Node{NodeID: "n2", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv2.URL)}); err != nil {
		t.Fatal(err)
	}
	gw := newTestGateway(ns)

	reqBody := `{"function":"f","input":{"x":1}}`
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "token-da-funcao")
	rec := httptest.NewRecorder()

	gw.Handle(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401 body=%s", rec.Code, rec.Body.String())
	}
	if got := atomic.LoadInt64(&attempts); got != 1 {
		t.Fatalf("tentativas=%d want 1 (sem failover em 401)", got)
	}
	if fwdAuth != "token-da-funcao" {
		t.Errorf("Authorization repassado=%q want verbatim", fwdAuth)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "UNAUTHORIZED" {
		t.Errorf("code=%q want UNAUTHORIZED", er.Code)
	}
	if er.Retryable {
		t.Errorf("retryable=true para 401; credencial errada não se cura com retry")
	}
}

// ---------------------------------------------------------------------------
// invocation_log_test.go — invocation log
// ---------------------------------------------------------------------------

func TestInvocationLogDisabledNilIsNoOp(t *testing.T) {
	l, err := NewInvocationLog("")
	if err != nil {
		t.Fatalf("NewInvocationLog() erro = %v", err)
	}
	if l != nil {
		t.Fatalf("NewInvocationLog(\"\") = %v, want nil", l)
	}
	var nilLog *InvocationLog
	if err := nilLog.Append(InvocationRecord{Entry: history.Entry{FunctionName: "fn"}}); err != nil {
		t.Fatalf("Append em logger nil erro = %v", err)
	}
	if err := nilLog.Close(); err != nil {
		t.Fatalf("Close em logger nil erro = %v", err)
	}
}

func TestInvocationLogAppendJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invocations.jsonl")
	l, err := NewInvocationLog(path)
	if err != nil {
		t.Fatalf("NewInvocationLog() erro = %v", err)
	}
	defer l.Close()

	ts := time.Now().UTC().Truncate(time.Millisecond)
	slo := &selection.SLO{MaxLatencyMS: 500, MinThroughputRPS: 10}
	metrics := json.RawMessage(`{"response_time_ms":12,"cold_start":true,"instance_id":"i-1","phases":{"acquire_wait_ms":1,"readiness_ms":2,"function_ms":9,"overhead_ms":0},"resource_profile":{"cpu_pct_avg":3.5,"cpu_pct_peak":9.1,"mem_peak_bytes":1048576,"sample_count":4}}`)
	want := []InvocationRecord{
		{Entry: history.Entry{Timestamp: ts, FunctionName: "fn-a", NodeID: "borda-1", Outcome: history.OutcomeSucesso, ObservedLatencyMS: 12, SloMet: true}, SLO: slo, ExecutionMetrics: metrics},
		{Entry: history.Entry{Timestamp: ts, FunctionName: "fn-a", NodeID: cloudFallbackNodeID, Outcome: history.OutcomeSucesso, ObservedLatencyMS: 300, SloMet: true, InstanceID: cloudFallbackNodeID}, SLO: slo},
		{Entry: history.Entry{Timestamp: ts, FunctionName: "fn-a", NodeID: noNodeAvailableID, Outcome: history.OutcomeFalha, ObservedLatencyMS: 0, SloMet: false}, SLO: slo},
	}
	for _, r := range want {
		if err := l.Append(r); err != nil {
			t.Fatalf("Append() erro = %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() erro = %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() erro = %v", err)
	}
	lines := splitLines(t, raw)
	if len(lines) != len(want) {
		t.Fatalf("linhas = %d, want %d", len(lines), len(want))
	}
	for i, ln := range lines {
		var got InvocationRecord
		if err := json.Unmarshal(ln, &got); err != nil {
			t.Fatalf("linha %d não é JSON válido: %v", i, err)
		}
		if got.FunctionName != want[i].FunctionName || got.NodeID != want[i].NodeID || got.Outcome != want[i].Outcome {
			t.Errorf("linha %d = %+v, want %+v", i, got, want[i])
		}
		if got.SLO == nil || got.SLO.MaxLatencyMS != 500 || got.SLO.MinThroughputRPS != 10 {
			t.Errorf("linha %d slo = %+v, want max_latency 500 + min_rps 10", i, got.SLO)
		}
	}
	// Linha 0 carrega o bloco de métricas verbatim; demais omitem o campo.
	var first map[string]any
	if err := json.Unmarshal(lines[0], &first); err != nil {
		t.Fatal(err)
	}
	em, ok := first["execution_metrics"].(map[string]any)
	if !ok {
		t.Fatalf("linha 0 sem execution_metrics: %v", first)
	}
	if em["cold_start"] != true || em["instance_id"] != "i-1" {
		t.Errorf("execution_metrics = %v, want cold_start=true instance i-1", em)
	}
	if _, ok := em["resource_profile"].(map[string]any); !ok {
		t.Errorf("execution_metrics sem resource_profile: %v", em)
	}
	var cloud map[string]any
	if err := json.Unmarshal(lines[1], &cloud); err != nil {
		t.Fatal(err)
	}
	if _, present := cloud["execution_metrics"]; present {
		t.Errorf("linha 1 deveria omitir execution_metrics: %v", cloud)
	}
}

func TestGatewayRecordsToInvocationLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invocations.jsonl")
	l, err := NewInvocationLog(path)
	if err != nil {
		t.Fatalf("NewInvocationLog() erro = %v", err)
	}
	defer l.Close()

	gw := NewGatewayWithHistory(GatewayConfig{Clock: time.Now})
	gw.SetInvocationLog(l)

	slo := &selection.SLO{MaxLatencyMS: 500}
	nodeBody := []byte(`{"result":{"ok":true},"execution_metrics":{"response_time_ms":12,"cold_start":false,"instance_id":"i-9","phases":{"acquire_wait_ms":0,"readiness_ms":0,"function_ms":12,"overhead_ms":0}}}`)
	gw.recordAttempt(nodes.Node{NodeID: "borda-1"}, 5*time.Millisecond, nil, slo, "fn-a", "fn-a", executionMetricsOf(nodeBody), true, "", 0, attemptCapture{})
	cloudMetrics, _ := json.Marshal(gatewayCloudMetrics{ResponseTimeMS: 100, InstanceID: cloudFallbackNodeID, CloudFallback: true})
	gw.recordCloudAttempt(100*time.Millisecond, nil, slo, "fn-a", cloudMetrics)
	gw.recordNoNode("fn-a", slo)

	if err := l.Close(); err != nil {
		t.Fatalf("Close() erro = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() erro = %v", err)
	}
	lines := splitLines(t, raw)
	if len(lines) != 3 {
		t.Fatalf("linhas = %d, want 3", len(lines))
	}
	var recs []InvocationRecord
	for _, ln := range lines {
		var r InvocationRecord
		if err := json.Unmarshal(ln, &r); err != nil {
			t.Fatalf("linha inválida: %v", err)
		}
		recs = append(recs, r)
	}
	if recs[0].NodeID != "borda-1" || recs[1].NodeID != cloudFallbackNodeID || recs[2].NodeID != noNodeAvailableID {
		t.Errorf("node_ids = %v", []string{recs[0].NodeID, recs[1].NodeID, recs[2].NodeID})
	}
	for i, r := range recs {
		if r.SLO == nil || r.SLO.MaxLatencyMS != 500 {
			t.Errorf("linha %d slo = %+v, want max_latency 500", i, r.SLO)
		}
	}
	if string(recs[0].ExecutionMetrics) == "" || !json.Valid(recs[0].ExecutionMetrics) {
		t.Errorf("linha 0 sem execution_metrics válido")
	}
	var m0 map[string]any
	_ = json.Unmarshal(recs[0].ExecutionMetrics, &m0)
	if m0["instance_id"] != "i-9" {
		t.Errorf("linha 0 metrics = %v, want instance i-9", m0)
	}
	var m1 map[string]any
	_ = json.Unmarshal(recs[1].ExecutionMetrics, &m1)
	if m1["cloud_fallback"] != true {
		t.Errorf("linha 1 metrics = %v, want cloud_fallback=true", m1)
	}
	if recs[2].ExecutionMetrics != nil {
		t.Errorf("linha 2 deveria omitir execution_metrics")
	}
}

func TestExecutionMetricsOf(t *testing.T) {
	if got := executionMetricsOf([]byte(`{"result":1}`)); got != nil {
		t.Errorf("sem bloco => %s, want nil", got)
	}
	if got := executionMetricsOf([]byte(`{inválido`)); got != nil {
		t.Errorf("corpo inválido => %s, want nil", got)
	}
	got := executionMetricsOf([]byte(`{"execution_metrics":{"cold_start":true}}`))
	if string(got) != `{"cold_start":true}` {
		t.Errorf("got = %s", got)
	}
}

func splitLines(t *testing.T, raw []byte) [][]byte {
	t.Helper()
	var out [][]byte
	start := 0
	for i, b := range raw {
		if b == '\n' {
			out = append(out, raw[start:i])
			start = i + 1
		}
	}
	if start < len(raw) {
		t.Fatalf("arquivo não termina com newline (bytes finais perdidos?)")
	}
	return out
}
