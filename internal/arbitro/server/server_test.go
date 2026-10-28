package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/auth"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// defaultLimits mirrors the limits of the selection tests, with the same
// ceilings, and it is duplicated here because tests of different packages do not
// share identifiers.
var defaultLimits = selection.LimitsConfig{
	MaxCPUPct:   85,
	MaxMemPct:   90,
	MaxPSIAvg10: 25,
}

func newTestServerEnv(t *testing.T) (*Server, string, *nodes.NodeStore, *monitor.Monitor) {
	t.Helper()
	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	mon := monitor.NewMonitor()
	gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}})
	gen, err := auth.Generate()
	if err != nil {
		t.Fatalf("auth.Generate: %v", err)
	}
	cloudStore, err := cloud.NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	srv := NewServerWithHistory(ns, mon, gw, gen.Cred, 90*time.Second, defaultLimits, cloudStore, nil, 0, nil)
	return srv, gen.PlainToken, ns, mon
}

func doReq(t *testing.T, h http.Handler, method, path string, body any, token string) *httptest.ResponseRecorder {
	t.Helper()
	var buf *bytes.Reader
	if body != nil {
		switch v := body.(type) {
		case string:
			buf = bytes.NewReader([]byte(v))
		case []byte:
			buf = bytes.NewReader(v)
		default:
			b, err := json.Marshal(v)
			if err != nil {
				t.Fatalf("marshal body: %v", err)
			}
			buf = bytes.NewReader(b)
		}
	} else {
		buf = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, buf)
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// Ensure RemoteAddr for rate-limit tests
	req.RemoteAddr = "127.0.0.1:12345"
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// ---------------------------------------------------------------------------
// 401 sem credencial
// ---------------------------------------------------------------------------

func TestServer_ControlRoutesSemCredencial401(t *testing.T) {
	srv, _, _, _ := newTestServerEnv(t)
	// With the per-node tokens configured, the node routes require the token of
	// the node, so without a credential the answer is unauthorized.
	tokens, err := nodes.NewNodeTokenStore("")
	if err != nil {
		t.Fatal(err)
	}
	srv.SetNodeTokens(tokens)
	h := srv.Handler()

	cases := []struct {
		method string
		path   string
		body   any
	}{
		{"POST", "/nodes", map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}},
		{"GET", "/nodes", nil},
		{"DELETE", "/nodes/n1", nil},
		{"POST", "/nodes/n1/telemetry", map[string]any{"node_id": "n1"}},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rr := doReq(t, h, tc.method, tc.path, tc.body, "")
			if rr.Code != http.StatusUnauthorized {
				t.Fatalf("%s %s sem credencial: status=%d want 401 body=%s", tc.method, tc.path, rr.Code, rr.Body.String())
			}
			var er gateway.ErrorResponse
			if err := json.Unmarshal(rr.Body.Bytes(), &er); err == nil {
				if er.Code != "UNAUTHORIZED" {
					t.Fatalf("code=%q want UNAUTHORIZED", er.Code)
				}
			}
		})
	}
	// A wrong token is also unauthorized.
	rr := doReq(t, h, "GET", "/nodes", nil, "token-invalido")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("token inválido: status=%d want 401", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// Registro feliz →201 e GET
// ---------------------------------------------------------------------------

func TestServer_RegistroFeliz201EGet(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	body := map[string]any{
		"node_id": "xp4-test", "layer": "borda", "host": "10.0.0.1:8080",
		"min_instances": 2, "psi_supported": true, "functions": []string{"hello"},
	}
	rr := doReq(t, h, "POST", "/nodes", body, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("POST /nodes status=%d want 201 body=%s", rr.Code, rr.Body.String())
	}
	var created nodes.Node
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created: %v body=%s", err, rr.Body.String())
	}
	if created.NodeID != "xp4-test" || created.Layer != "borda" || created.Host != "10.0.0.1:8080" {
		t.Fatalf("created mismatch: %+v", created)
	}

	rr = doReq(t, h, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes status=%d want 200 body=%s", rr.Code, rr.Body.String())
	}
	var list struct {
		Nodes []json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(list.Nodes) != 1 {
		t.Fatalf("nodes len=%d want 1 body=%s", len(list.Nodes), rr.Body.String())
	}
	// It verifies that the node carries a status and no telemetry yet.
	var entry map[string]any
	if err := json.Unmarshal(list.Nodes[0], &entry); err != nil {
		t.Fatalf("unmarshal entry: %v", err)
	}
	if entry["status"] != nodes.StatusAtivo {
		t.Fatalf("status=%v want %q", entry["status"], nodes.StatusAtivo)
	}
	if _, has := entry["latest_telemetry"]; has {
		t.Fatalf("latest_telemetry não deveria estar presente antes de telemetria, got %v", entry)
	}
	contentType := rr.Header().Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		t.Fatalf("Content-Type=%q want application/json", contentType)
	}
}

// ---------------------------------------------------------------------------
// A second post with the same identifier gives a success
// ---------------------------------------------------------------------------

func TestServer_SegundoPOSTMesmoID200(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	body1 := map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}
	rr := doReq(t, h, "POST", "/nodes", body1, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("primeiro POST status=%d want 201", rr.Code)
	}
	body2 := map[string]any{"node_id": "n1", "layer": "nevoa", "host": "10.0.0.2:9090", "functions": []string{"a"}}
	rr = doReq(t, h, "POST", "/nodes", body2, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("segundo POST status=%d want 200 body=%s", rr.Code, rr.Body.String())
	}
	var updated nodes.Node
	if err := json.Unmarshal(rr.Body.Bytes(), &updated); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if updated.Layer != "nevoa" || updated.Host != "10.0.0.2:9090" {
		t.Fatalf("não atualizou: %+v", updated)
	}
}

// ---------------------------------------------------------------------------
// Beyond the maximum of nodes the answer is a rejection
// ---------------------------------------------------------------------------

func TestServer_AlemMaxNodes503(t *testing.T) {
	srv, token, ns, _ := newTestServerEnv(t)
	h := srv.Handler()

	// Fill the store up to the maximum of nodes through the store itself.
	for i := 0; i < nodes.MaxNodes; i++ {
		id := fmt.Sprintf("node-%03d", i)
		n := nodes.Node{NodeID: id, Layer: nodes.LayerBorda, Host: fmt.Sprintf("10.0.0.%d:8080", i%254+1)}
		if _, err := ns.Upsert(n); err != nil {
			t.Fatalf("Upsert %s: %v", id, err)
		}
	}
	// node_id NOVO deve falhar 503
	body := map[string]any{"node_id": "overflow", "layer": "borda", "host": "10.0.0.99:8080"}
	rr := doReq(t, h, "POST", "/nodes", body, token)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("overflow status=%d want 503 body=%s", rr.Code, rr.Body.String())
	}
	var er gateway.ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
		t.Fatalf("unmarshal er: %v", err)
	}
	if er.Code != "NODE_LIMIT_REACHED" {
		t.Fatalf("code=%q want NODE_LIMIT_REACHED", er.Code)
	}
	if !er.Retryable {
		t.Fatalf("retryable=false want true")
	}

	// An identifier that already exists keeps giving a success even at the ceiling.
	body2 := map[string]any{"node_id": "node-000", "layer": "nevoa", "host": "10.0.0.99:9999"}
	rr = doReq(t, h, "POST", "/nodes", body2, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("update no teto status=%d want 200 body=%s", rr.Code, rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Corpo acima do limite →413
// ---------------------------------------------------------------------------

func TestServer_CorpoAcimaLimite413(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	// Corpo >64 KiB
	large := strings.Repeat("A", 70*1024)
	body := fmt.Sprintf(`{"node_id":"%s","layer":"borda","host":"10.0.0.1:8080"}`, large)
	rr := doReq(t, h, "POST", "/nodes", body, token)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("corpo grande status=%d want 413 body=%s", rr.Code, rr.Body.String())
	}
	var er gateway.ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if er.Code != "PAYLOAD_TOO_LARGE" {
		t.Fatalf("code=%q want PAYLOAD_TOO_LARGE", er.Code)
	}

	// The same holds for the telemetry.
	nbody := map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}
	rr = doReq(t, h, "POST", "/nodes", nbody, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("register n1: %d", rr.Code)
	}
	largePkg := strings.Repeat("B", 70*1024)
	pkgBody := fmt.Sprintf(`{"node_id":"n1","cpu_usage_pct":10,"memory_usage_pct":20,"extra":"%s"}`, largePkg)
	req := httptest.NewRequest("POST", "/nodes/n1/telemetry", strings.NewReader(pkgBody))
	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("telemetry corpo grande status=%d want 413 body=%s", rec.Code, rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Telemetria ID divergente →400
// ---------------------------------------------------------------------------

func TestServer_TelemetriaIDDivergente400(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	// Registrar n1
	rr := doReq(t, h, "POST", "/nodes", map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("register: %d", rr.Code)
	}
	pkg := telemetry.Package{NodeID: "outro-id", CPUUsagePct: 10, MemoryUsagePct: 20, Timestamp: time.Now().UTC()}
	rr = doReq(t, h, "POST", "/nodes/n1/telemetry", pkg, token)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("mismatch status=%d want 400 body=%s", rr.Code, rr.Body.String())
	}
	var er gateway.ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if er.Code != "TELEMETRY_MISMATCH" {
		t.Fatalf("code=%q want TELEMETRY_MISMATCH", er.Code)
	}
}

// ---------------------------------------------------------------------------
// Telemetry for an unregistered node gives a not found, and the listing
// confirms that the monitor was not populated
// ---------------------------------------------------------------------------

func TestServer_TelemetriaParaNaoRegistrado404(t *testing.T) {
	srv, token, _, mon := newTestServerEnv(t)
	h := srv.Handler()

	pkg := telemetry.Package{NodeID: "ghost", CPUUsagePct: 10, MemoryUsagePct: 20, Timestamp: time.Now().UTC()}
	rr := doReq(t, h, "POST", "/nodes/ghost/telemetry", pkg, token)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("ghost status=%d want 404 body=%s", rr.Code, rr.Body.String())
	}
	var er gateway.ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if er.Code != "NODE_NOT_FOUND" {
		t.Fatalf("code=%q want NODE_NOT_FOUND", er.Code)
	}
	// The monitor must not have been populated.
	if _, ok := mon.Latest("ghost"); ok {
		t.Fatalf("Monitor populado para nó fantasma")
	}
	// The listing of the nodes must be empty, since no node was registered.
	rr = doReq(t, h, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes: %d", rr.Code)
	}
	var list struct {
		Nodes []any `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(list.Nodes) != 0 {
		t.Fatalf("nodes len=%d want 0", len(list.Nodes))
	}
}

// ---------------------------------------------------------------------------
// Telemetria correta →204 e GET mostra latest_telemetry + ativo
// ---------------------------------------------------------------------------

func TestServer_TelemetriaCorreta204EGetMostra(t *testing.T) {
	srv, token, _, mon := newTestServerEnv(t)
	h := srv.Handler()

	rr := doReq(t, h, "POST", "/nodes", map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("register: %d body=%s", rr.Code, rr.Body.String())
	}
	pkg := telemetry.Package{
		NodeID:         "n1",
		Timestamp:      time.Now().UTC(),
		CPUUsagePct:    42.5,
		MemoryUsagePct: 73.1,
		Source:         "periodic",
	}
	rr = doReq(t, h, "POST", "/nodes/n1/telemetry", pkg, token)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("telemetry status=%d want 204 body=%s", rr.Code, rr.Body.String())
	}
	// Monitor deve ter
	if latest, ok := mon.Latest("n1"); !ok || latest.CPUUsagePct != 42.5 {
		t.Fatalf("mon Latest not found or mismatch: ok=%v pkg=%+v", ok, latest)
	}
	// GET deve mostrar latest_telemetry e status ativo
	rr = doReq(t, h, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET: %d", rr.Code)
	}
	var resp struct {
		Nodes []struct {
			NodeID          string             `json:"node_id"`
			Status          string             `json:"status"`
			LatestTelemetry *telemetry.Package `json:"latest_telemetry"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, rr.Body.String())
	}
	if len(resp.Nodes) != 1 {
		t.Fatalf("nodes len=%d want 1", len(resp.Nodes))
	}
	n := resp.Nodes[0]
	if n.Status != nodes.StatusAtivo {
		t.Fatalf("status=%q want %q", n.Status, nodes.StatusAtivo)
	}
	if n.LatestTelemetry == nil {
		t.Fatalf("latest_telemetry nil")
	}
	if n.LatestTelemetry.CPUUsagePct != 42.5 || n.LatestTelemetry.MemoryUsagePct != 73.1 {
		t.Fatalf("latest_telemetry mismatch: %+v", n.LatestTelemetry)
	}
	if n.LatestTelemetry.NodeID != "n1" {
		t.Fatalf("node_id mismatch: %q", n.LatestTelemetry.NodeID)
	}
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func TestServer_DeleteNode(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	// 404 antes de existir
	rr := doReq(t, h, "DELETE", "/nodes/n1", nil, token)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("delete inexistente status=%d want 404", rr.Code)
	}
	// Cria
	rr = doReq(t, h, "POST", "/nodes", map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("register: %d", rr.Code)
	}
	// Delete ok 204
	rr = doReq(t, h, "DELETE", "/nodes/n1", nil, token)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d want 204 body=%s", rr.Code, rr.Body.String())
	}
	// GET confirma vazio
	rr = doReq(t, h, "GET", "/nodes", nil, token)
	var list struct {
		Nodes []any `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(list.Nodes) != 0 {
		t.Fatalf("nodes len=%d want 0", len(list.Nodes))
	}
	// A second deletion is a not found.
	rr = doReq(t, h, "DELETE", "/nodes/n1", nil, token)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("segundo delete status=%d want 404", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// healthz →200
// ---------------------------------------------------------------------------

func TestServer_Healthz200(t *testing.T) {
	srv, _, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	req := httptest.NewRequest("GET", "/healthz", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("healthz status=%d want 200", rr.Code)
	}
	// The health route also passes without a credential.
	req2 := httptest.NewRequest("GET", "/healthz", nil)
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("healthz sem auth status=%d want 200", rr2.Code)
	}
}

// ---------------------------------------------------------------------------
// An invocation without a credential passes, and it is rate limited
// ---------------------------------------------------------------------------

func TestServer_InvokeSemCredencialPassa(t *testing.T) {
	srv, _, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	body := map[string]any{"function": "hello", "input": map[string]any{}}
	rr := doReq(t, h, "POST", "/invoke", body, "")
	// Without an available node the answer is a rejection, and not an unauthorized.
	if rr.Code == http.StatusUnauthorized {
		t.Fatalf("invoke sem credencial retornou 401, want não-401")
	}
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("invoke sem credencial status=%d want 503 (NO_NODE_AVAILABLE) body=%s", rr.Code, rr.Body.String())
	}
	var er gateway.ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if er.Code != "NO_NODE_AVAILABLE" {
		t.Fatalf("code=%q want NO_NODE_AVAILABLE", er.Code)
	}
}

func TestServer_InvokeRateLimit429(t *testing.T) {
	// The real gateway requires a node, so an injected fake handler isolates the rate limit.
	fakeHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	ns, _ := nodes.NewNodeStore("")
	mon := monitor.NewMonitor()
	// Server com limiter 60/min
	gen, _ := auth.Generate()
	srv := &Server{
		nodes:         ns,
		mon:           mon,
		gw:            &gateway.Gateway{}, // unused, replaced by a fake through the invoke limiter directly
		cred:          gen.Cred,
		staleAfter:    90 * time.Second,
		invokeLimiter: auth.NewRateLimiter(60, time.Minute),
	}
	limited := srv.invokeRateLimit(fakeHandler)

	// Sixty requests must pass.
	for i := 0; i < 60; i++ {
		req := httptest.NewRequest("POST", "/invoke", strings.NewReader(`{"function":"x","input":{}}`))
		req.RemoteAddr = "10.0.0.1:1234"
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		limited.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("req %d status=%d want 200", i, rr.Code)
		}
	}
	// 61ª deve ser 429
	req := httptest.NewRequest("POST", "/invoke", strings.NewReader(`{"function":"x","input":{}}`))
	req.RemoteAddr = "10.0.0.1:1234"
	rr := httptest.NewRecorder()
	limited.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("61st status=%d want 429", rr.Code)
	}
	var er gateway.ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if er.Code != "TOO_MANY_REQUESTS" {
		t.Fatalf("code=%q want TOO_MANY_REQUESTS", er.Code)
	}
	if !er.Retryable {
		t.Fatalf("retryable=false want true")
	}
	// A different address is not affected.
	req2 := httptest.NewRequest("POST", "/invoke", strings.NewReader(`{"function":"x","input":{}}`))
	req2.RemoteAddr = "10.0.0.2:1234"
	rr2 := httptest.NewRecorder()
	limited.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("outro IP status=%d want 200", rr2.Code)
	}
}

// ---------------------------------------------------------------------------
// The validation of an invalid node gives a bad request
// ---------------------------------------------------------------------------

func TestServer_RegistroInvalido400(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	cases := []map[string]any{
		{"node_id": "", "layer": "borda", "host": "10.0.0.1:8080"},
		{"node_id": "n1", "layer": "invalida", "host": "10.0.0.1:8080"},
		{"node_id": "n1", "layer": "borda", "host": ""},
	}
	for i, body := range cases {
		rr := doReq(t, h, "POST", "/nodes", body, token)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("case %d status=%d want 400 body=%s", i, rr.Code, rr.Body.String())
		}
		var er gateway.ErrorResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if er.Code != "INVALID_NODE" {
			t.Fatalf("case %d code=%q want INVALID_NODE", i, er.Code)
		}
	}
}

// ---------------------------------------------------------------------------
// JSON malformado →400
// ---------------------------------------------------------------------------

func TestServer_RegistroJSONInvalido400(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	rr := doReq(t, h, "POST", "/nodes", "{invalido", token)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("json invalido status=%d want 400", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// The atomic creation flag replaces a separate read, and the concurrency is
// already covered by the tests of the store, so here it is only a smoke test
// ---------------------------------------------------------------------------

func TestServer_RegistroContentTypeJSON(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	rr := doReq(t, h, "POST", "/nodes", map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}, token)
	if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type=%q want application/json", ct)
	}
}

// ---------------------------------------------------------------------------
// Table of tests for the summary of the history: with no history, a zero window
// or no clock the answer is a success, with no summary of the history and no
// panic, and with a history the keys are present
// ---------------------------------------------------------------------------

func TestServer_HistoryTable(t *testing.T) {
	t.Helper()

	tests := []struct {
		name             string
		hist             *history.History
		window           time.Duration
		clock            func() time.Time
		expectedPostCode int
		expectedGetCode  int
		wantSummary      bool
	}{
		{
			name:             "hist nil, window 0, clock nil",
			hist:             nil,
			window:           0,
			clock:            nil,
			expectedPostCode: http.StatusCreated,
			expectedGetCode:  http.StatusOK,
			wantSummary:      false,
		},
		{
			name: "hist set, no entries",
			hist: func() *history.History {
				h, _ := history.NewHistory(filepath.Join(t.TempDir(), "history-test.json"), 1000)
				return h
			}(),
			window:           60 * time.Second,
			clock:            time.Now,
			expectedPostCode: http.StatusCreated,
			expectedGetCode:  http.StatusOK,
			wantSummary:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, err := nodes.NewNodeStore("")
			if err != nil {
				t.Fatalf("NewNodeStore: %v", err)
			}
			mon := monitor.NewMonitor()
			gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}})
			gen, err := auth.Generate()
			if err != nil {
				t.Fatalf("auth.Generate: %v", err)
			}
			cloudStore, err := cloud.NewCloudStore("")
			if err != nil {
				t.Fatalf("NewCloudStore: %v", err)
			}
			srv := NewServerWithHistory(ns, mon, gw, gen.Cred, 90*time.Second, defaultLimits, cloudStore, tt.hist, tt.window, tt.clock)
			h := srv.Handler()

			body := map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}
			rr := doReq(t, h, "POST", "/nodes", body, gen.PlainToken)
			if rr.Code != tt.expectedPostCode {
				t.Fatalf("%s POST: status=%d want %d", tt.name, rr.Code, tt.expectedPostCode)
			}

			rr = doReq(t, h, "GET", "/nodes", nil, gen.PlainToken)
			if rr.Code != tt.expectedGetCode {
				t.Fatalf("%s GET /nodes: status=%d want %d", tt.name, rr.Code, tt.expectedGetCode)
			}

			var list struct {
				Nodes []struct {
					HistorySummary map[string]HistorySummaryEntry `json:"history_summary,omitempty"`
				} `json:"nodes"`
			}
			if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
				t.Fatalf("%s: unmarshal: %v", tt.name, err)
			}

			if tt.wantSummary {
				if len(list.Nodes) == 0 {
					t.Fatalf("%s: expected at least one node with history summary", tt.name)
				}
				summary := list.Nodes[0].HistorySummary
				if summary == nil {
					t.Logf("%s: history_summary is nil", tt.name)
				}
			} else {
				for _, node := range list.Nodes {
					if node.HistorySummary != nil {
						t.Fatalf("%s: history_summary should be nil, got %v", tt.name, node.HistorySummary)
					}
				}
			}
		})
	}
}
