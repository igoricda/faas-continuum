// End-to-end de categoria (function-category-routing, todo 11): a ordem da
// fila preserva o ranking do seletor (borda→névoa→outro, CPU asc) quando o
// pedido é uma categoria, e a resolução para o concreto por nó não reordena
// a fila. Complementa gateway_category_test.go (todo 7) que já cobre URLs
// concretas, recordAttempt por grupo, 503 e 401.
package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// A fila de uma categoria preserva a ordem do ranking (borda antes de névoa)
// e a resolução por concreto não a reordena: o primeiro nó (borda, concreto
// tiny) atende antes do segundo (névoa, concreto full) mesmo quando o pedido
// é o nome da categoria.
func TestGateway_CategoryQueueOrderPreservedFromRanking(t *testing.T) {
	var mu sync.Mutex
	var order []string
	mk := func(id string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			order = append(order, id+":"+r.URL.Path)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"result":"ok"}`))
		}))
	}
	bordaSrv := mk("borda")
	defer bordaSrv.Close()
	nevoaSrv := mk("nevoa")
	defer nevoaSrv.Close()

	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := ns.Upsert(nodes.Node{
		NodeID:             "borda",
		Layer:              nodes.LayerBorda,
		Host:               hostFromURL(t, bordaSrv.URL),
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo"},
	}); err != nil {
		t.Fatalf("Upsert borda: %v", err)
	}
	if _, err := ns.Upsert(nodes.Node{
		NodeID:             "nevoa",
		Layer:              nodes.LayerNevoa,
		Host:               hostFromURL(t, nevoaSrv.URL),
		Functions:          []string{"alpr-yolo-full"},
		FunctionCategories: map[string]string{"alpr-yolo-full": "alpr-yolo"},
	}); err != nil {
		t.Fatalf("Upsert nevoa: %v", err)
	}

	h, _ := history.NewHistory("", 1000)
	rec := invokeFunction(t, newCategoryTestGateway(ns, h), "alpr-yolo")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}

	mu.Lock()
	defer mu.Unlock()
	// Borda vem primeiro no ranking; o primeiro 200 encerra a fila.
	if len(order) != 1 {
		t.Fatalf("order=%v want exatamente 1 tentativa (primeiro nó 200)", order)
	}
	if !strings.HasPrefix(order[0], "borda:") {
		t.Fatalf("primeiro nó = %q, want borda (ranking preservado)", order[0])
	}
	if order[0] != "borda:/function/alpr-yolo-tiny/invoke" {
		t.Fatalf("borda deve receber o concreto tiny, got %q", order[0])
	}
}

// A fila de categoria mantém o ranking quando o primeiro nó falha (404
// obsoleto): borda falha, névoa atende em seguida — ordem preservada.
func TestGateway_CategoryRankingPreservedAcrossFailover(t *testing.T) {
	var mu sync.Mutex
	var order []string
	mkBorda := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		order = append(order, "borda:"+r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"FUNCTION_NOT_FOUND"}`))
	}))
	defer mkBorda.Close()
	mkNevoa := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		order = append(order, "nevoa:"+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer mkNevoa.Close()

	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := ns.Upsert(nodes.Node{
		NodeID:             "borda",
		Layer:              nodes.LayerBorda,
		Host:               hostFromURL(t, mkBorda.URL),
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo"},
	}); err != nil {
		t.Fatalf("Upsert borda: %v", err)
	}
	if _, err := ns.Upsert(nodes.Node{
		NodeID:             "nevoa",
		Layer:              nodes.LayerNevoa,
		Host:               hostFromURL(t, mkNevoa.URL),
		Functions:          []string{"alpr-yolo-full"},
		FunctionCategories: map[string]string{"alpr-yolo-full": "alpr-yolo"},
	}); err != nil {
		t.Fatalf("Upsert nevoa: %v", err)
	}

	h, _ := history.NewHistory("", 1000)
	rec := invokeFunction(t, newCategoryTestGateway(ns, h), "alpr-yolo")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (404 obsoleto continua) body=%s", rec.Code, rec.Body.String())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 {
		t.Fatalf("order=%v want 2 tentativas (borda 404 → névoa 200)", order)
	}
	if !strings.HasPrefix(order[0], "borda:") || !strings.HasPrefix(order[1], "nevoa:") {
		t.Fatalf("ordem do ranking não preservada no failover: %v", order)
	}
	// Registro de histórico sob o grupo, nunca o concreto.
	for _, k := range h.Keys() {
		if strings.Contains(k, "tiny") || strings.Contains(k, "full") {
			t.Fatalf("history key %q vazou concreto", k)
		}
	}
}
