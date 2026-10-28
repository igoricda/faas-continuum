package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// TestGateway_TetoDeFilaAprendido covers the effect of the learned queue
// ceiling on the dispatch: with the edge already queued, since its rank of the
// requests in flight plus one is above the learned ceiling, the attempt does
// not even start and the request moves to the fog. Within the ceiling the edge
// is attempted normally, and without a learned ceiling the gate keeps the
// projection and nothing is skipped. It is the regression of the collapse
// measured in the ramp, where a queue of thirty-nine piled up on the fragile
// node, which without the skip by capacity is buried by the burst again.
func TestGateway_TetoDeFilaAprendido(t *testing.T) {
	cases := []struct {
		name       string
		queueCap   map[string]int
		inflightA  int
		wantACalls int32
		wantBCalls int32
		wantSkips  int64
	}{
		{"rank acima do teto pula a borda", map[string]int{"a": 1}, 1, 0, 1, 1},
		{"rank dentro do teto tenta a borda", map[string]int{"a": 2}, 1, 1, 0, 0},
		{"sem teto aprendido nao pula", nil, 5, 1, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var aCalls, bCalls int32
			fastA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&aCalls, 1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"result":"a"}`))
			}))
			defer fastA.Close()
			fastB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&bCalls, 1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"result":"b"}`))
			}))
			defer fastB.Close()

			ns, _ := nodes.NewNodeStore("")
			nodeA := nodes.Node{NodeID: "a", Layer: nodes.LayerBorda, Host: hostFromURL(t, fastA.URL), Functions: []string{"f"}}
			nodeB := nodes.Node{NodeID: "b", Layer: nodes.LayerNevoa, Host: hostFromURL(t, fastB.URL), Functions: []string{"f"}}
			_, _ = ns.Upsert(nodeA)
			_, _ = ns.Upsert(nodeB)

			gauge := NewInflightGauge()
			for i := 0; i < tc.inflightA; i++ {
				gauge.Inc("a", "f", 0, time.Now())
			}
			gw := NewGatewayWithHistory(GatewayConfig{
				Nodes:   ns,
				Monitor: monitor.NewMonitor(),
				Selector: queueFakeSelector{
					queue:     []nodes.Node{nodeA, nodeB},
					predicted: map[string]int64{"a": 50, "b": 50},
					queueCap:  tc.queueCap,
				},
				DefaultBudget: 800 * time.Millisecond,
			}).WithInflight(gauge)

			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"f","input":{}}`))
			rec := httptest.NewRecorder()
			gw.Handle(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d want 200; body=%s", rec.Code, rec.Body.String())
			}
			wantBody := `"a"`
			if tc.wantBCalls == 1 {
				wantBody = `"b"`
			}
			if !strings.Contains(rec.Body.String(), wantBody) {
				t.Fatalf("corpo=%q; want conter %s", rec.Body.String(), wantBody)
			}
			if got := atomic.LoadInt32(&aCalls); got != tc.wantACalls {
				t.Fatalf("nó a chamado %d vez(es), want %d", got, tc.wantACalls)
			}
			if got := atomic.LoadInt32(&bCalls); got != tc.wantBCalls {
				t.Fatalf("nó b chamado %d vez(es), want %d", got, tc.wantBCalls)
			}
			if got := gw.QueueCapSkips(); got != tc.wantSkips {
				t.Fatalf("QueueCapSkips=%d, want %d", got, tc.wantSkips)
			}
		})
	}
}
