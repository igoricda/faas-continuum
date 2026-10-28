package gateway

// Testes de resolve-then-forward por categoria (function-category-routing,
// todo 7): o pedido (categoria ou concreto) é traduzido UMA vez por nó via
// selection.ResolveConcrete para o concreto encaminhado na URL, enquanto
// TODOS os recordAttempt registram sob o grupo resolvido uma vez por
// invoke; 404 em concreto obsoleto continua para o próximo nó, 503 nomeia
// o pedido e 401 não faz failover.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func newCategoryTestGateway(ns *nodes.NodeStore, h *history.History) *Gateway {
	mon := monitor.NewMonitor()
	return NewGatewayWithHistory(GatewayConfig{
		Nodes:    ns,
		Monitor:  mon,
		Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		History:  h,
	})
}

func categoryPairStore(t *testing.T, srv1, srv2 *httptest.Server) *nodes.NodeStore {
	t.Helper()
	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := ns.Upsert(nodes.Node{
		NodeID: "cat-n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv1.URL),
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo"},
	}); err != nil {
		t.Fatalf("Upsert cat-n1: %v", err)
	}
	if _, err := ns.Upsert(nodes.Node{
		NodeID: "cat-n2", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv2.URL),
		Functions:          []string{"alpr-yolo-full"},
		FunctionCategories: map[string]string{"alpr-yolo-full": "alpr-yolo"},
	}); err != nil {
		t.Fatalf("Upsert cat-n2: %v", err)
	}
	return ns
}

func invokeFunction(t *testing.T, gw *Gateway, function string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"`+function+`","input":{"x":1}}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	return rec
}

func TestGateway_CategoryPerNodeConcreteURLs(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	mk := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			paths = append(paths, r.URL.Path)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"result":"ok"}`))
		}))
	}
	srv1 := mk()
	defer srv1.Close()
	srv2 := mk()
	defer srv2.Close()

	h, err := history.NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	gw := newCategoryTestGateway(categoryPairStore(t, srv1, srv2), h)

	rec := invokeFunction(t, gw, "alpr-yolo")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}

	// O pedido de categoria chega a um concreto; a URL leva o concreto.
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 {
		t.Fatalf("paths=%v want exatamente 1 tentativa (primeiro nó 200)", paths)
	}
	if paths[0] != "/function/alpr-yolo-tiny/invoke" && paths[0] != "/function/alpr-yolo-full/invoke" {
		t.Fatalf("path=%q want /function/<concreto-da-categoria>/invoke", paths[0])
	}

	// Histórico chaveado pelo grupo, nunca pelo concreto.
	for _, k := range h.Keys() {
		if !strings.Contains(k, "alpr-yolo") {
			t.Fatalf("history key %q sem o grupo alpr-yolo", k)
		}
		if strings.Contains(k, "tiny") || strings.Contains(k, "full") {
			t.Fatalf("history key %q vazou nome concreto (esperado só o grupo)", k)
		}
	}
	if len(h.Keys()) == 0 {
		t.Fatalf("nenhuma chave de histórico registrada sob o grupo")
	}
	sc := history.ScenarioFrom(telemetry.Package{}, false)
	served := "cat-n1"
	if paths[0] == "/function/alpr-yolo-full/invoke" {
		served = "cat-n2"
	}
	if count, _, _, _, ok := h.Stats(history.SegmentKey("alpr-yolo", served, sc)); !ok || count != 1 {
		t.Fatalf("Stats(grupo, %s) = (%d,%v), esperado (1,true)", served, count, ok)
	}
}

func TestGateway_CategoryWildcardForwardsRequested(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer srv.Close()

	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	// Nó wildcard: sem Functions, serve tudo — encaminha o pedido verbatim.
	if _, err := ns.Upsert(nodes.Node{NodeID: "wild-1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	h, _ := history.NewHistory("", 1000)
	rec := invokeFunction(t, newCategoryTestGateway(ns, h), "alpr-yolo")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if gotPath != "/function/alpr-yolo/invoke" {
		t.Fatalf("path=%q want /function/alpr-yolo/invoke (wildcard verbatim)", gotPath)
	}
}

func TestGateway_CategoryStale404Continues(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var first int64
	mk := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			paths = append(paths, r.URL.Path)
			mu.Unlock()
			// Mapa obsoleto no primeiro contato: 404 = stale → continua.
			if atomic.CompareAndSwapInt64(&first, 0, 1) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"code":"FUNCTION_NOT_FOUND"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"result":"ok"}`))
		}))
	}
	srv1 := mk()
	defer srv1.Close()
	srv2 := mk()
	defer srv2.Close()

	h, _ := history.NewHistory("", 1000)
	rec := invokeFunction(t, newCategoryTestGateway(categoryPairStore(t, srv1, srv2), h), "alpr-yolo")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (404 obsoleto continua) body=%s", rec.Code, rec.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 2 {
		t.Fatalf("paths=%v want 2 tentativas (404 → próximo nó)", paths)
	}
	want := map[string]bool{
		"/function/alpr-yolo-tiny/invoke": true,
		"/function/alpr-yolo-full/invoke": true,
	}
	for _, p := range paths {
		if !want[p] {
			t.Fatalf("path=%q fora dos concretos da categoria", p)
		}
		delete(want, p)
	}
	if len(want) != 0 {
		t.Fatalf("concretos não visitados: %v", want)
	}
}

func TestGateway_CategoryUnknownNamesRequested503(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer srv.Close()

	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := ns.Upsert(nodes.Node{
		NodeID: "cat-n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL),
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo"},
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	rec := invokeFunction(t, newCategoryTestGateway(ns, nil), "categoria-inexistente-zzz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NO_NODE_AVAILABLE" {
		t.Fatalf("code=%q want NO_NODE_AVAILABLE", er.Code)
	}
	if !strings.Contains(er.Message, "categoria-inexistente-zzz") {
		t.Fatalf("message=%q não nomeia o pedido", er.Message)
	}
}

func TestGateway_Category401NoFailover(t *testing.T) {
	var attempts int64
	mk := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&attempts, 1)
			w.WriteHeader(http.StatusUnauthorized)
		}))
	}
	srv1 := mk()
	defer srv1.Close()
	srv2 := mk()
	defer srv2.Close()

	gw := newCategoryTestGateway(categoryPairStore(t, srv1, srv2), nil)
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"alpr-yolo","input":{"x":1}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "token-errado")
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401 body=%s", rec.Code, rec.Body.String())
	}
	if got := atomic.LoadInt64(&attempts); got != 1 {
		t.Fatalf("tentativas=%d want 1 (401 categoria: sem failover)", got)
	}
	er := decodeErrResp(t, rec)
	if er.Code != "UNAUTHORIZED" {
		t.Fatalf("code=%q want UNAUTHORIZED", er.Code)
	}
}
