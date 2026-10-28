package gateway

// Keying de capacidade pelo CONCRETO resolvido (selflearning-consistency-fixes,
// todo 6, C2/D17): o produtor (nodeapi) reporta ocupação pela variante física
// (fn.Name), então a marca otimista de rejeição de capacidade precisa usar a
// MESMA chave concreta resolvida no dispatch — nunca o nome pedido (que pode
// ser uma categoria). Em HEAD a marca cai no nome pedido.

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

func TestGatewayCapacityRejectionMarksConcrete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"POOL_EXHAUSTED","message":"pool busy"}`))
	}))
	defer srv.Close()

	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := ns.Upsert(nodes.Node{
		NodeID:             "cat-n1",
		Layer:              nodes.LayerBorda,
		Host:               hostFromURL(t, srv.URL),
		Functions:          []string{"cat-v1"},
		FunctionCategories: map[string]string{"cat-v1": "cat"},
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	mon := monitor.NewMonitor()
	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:    ns,
		Monitor:  mon,
		Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		History:  h,
		Clock:    fixedClock(now),
	})
	rec := invokeFunction(t, gw, "cat")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}

	if _, ok := mon.GetCapacity("cat-n1", "cat-v1", now); !ok {
		t.Fatal("capacidade do CONCRETO cat-v1 não marcada (marca ficou no nome pedido `cat`)")
	}
	if _, ok := mon.GetCapacity("cat-n1", "cat", now); ok {
		t.Fatal("nome PEDIDO `cat` marcado; esperado só o concreto cat-v1")
	}
}
