package gateway

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// benchGateway builds a gateway with a test node that answers success
// immediately. The default log is silenced, since the handler logs two lines per
// invocation and the output of the benchmark would be only noise.
func benchGateway(b *testing.B) *Gateway {
	b.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(contentTypeHeader, applicationJSON)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	b.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	ns, err := nodes.NewNodeStore("")
	if err != nil {
		b.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := ns.Upsert(nodes.Node{NodeID: "bench-node", Layer: nodes.LayerBorda, Host: host}); err != nil {
		b.Fatalf("Upsert: %v", err)
	}
	mon := monitor.NewMonitor()
	return NewGatewayWithHistory(GatewayConfig{
		Nodes:    ns,
		Monitor:  mon,
		Selector: selection.QueueSelector{Mon: mon, Cfg: raceDefaultLimits},
	})
}

// BenchmarkGatewayHandle measures the complete invocation path of the arbiter,
// with the decoding, the listing, the selection, the encoding, the call to the
// node and the liveness update, against an instant local node, which is the
// ceiling of the hot path without a real network.
func BenchmarkGatewayHandle(b *testing.B) {
	gw := benchGateway(b)
	prev := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(prev) })

	reqBody := `{"function":"echo","input":{"q":1}}`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", applicationJSON)
		req.Header.Set("Authorization", "bench-token")
		rec := httptest.NewRecorder()
		gw.Handle(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("Handle: status %d body %s", rec.Code, rec.Body.String())
		}
	}
}

// BenchmarkGatewayHandleDecodeOnly measures the cost of the decoding and of the
// validation of the request, without an answer of a node, which isolates the
// cost of the parsing from the cost of the network. The ghost node stays stale
// and fails the dial immediately, which empties the single-element queue with a
// bad gateway and without touching a real network.
func BenchmarkGatewayHandleDecodeOnly(b *testing.B) {
	gw := benchGateway(b)
	// The node is removed, so the selection returns an empty queue and the answer
	// is written immediately after the decoding and the validation.
	if _, err := gw.nodes.Upsert(nodes.Node{NodeID: "ghost", Layer: nodes.LayerBorda, Host: "127.0.0.1:1"}); err != nil {
		b.Fatalf("Upsert ghost: %v", err)
	}
	if err := gw.nodes.Delete("bench-node"); err != nil {
		b.Fatalf("Delete bench-node: %v", err)
	}
	// The ghost node stays stale, so its derived status is not active and the queue is empty.
	time.Sleep(10 * time.Millisecond)

	prev := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(prev) })

	reqBody := `{"function":"echo","input":{"q":1}}`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		gw.Handle(rec, req)
		if rec.Code != http.StatusBadGateway {
			b.Fatalf("expected 502 after queue exhaustion, got %d", rec.Code)
		}
	}
}
