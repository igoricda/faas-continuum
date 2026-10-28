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

// benchGateway monta um Gateway com um nó httptest que responde 200
// imediatamente. O log padrão é silenciado: Handle loga duas linhas por
// invoke (OK + RACE_WINNER) e o output do bench seria só ruído.
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

// BenchmarkGatewayHandle mede o caminho de invocação completo do Árbitro
// (decode + List + Select + encode + client.Do + Touch) contra um nó local
// instantâneo: é o teto do hot path do gateway sem rede real.
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

// BenchmarkGatewayHandleDecodeOnly mede o custo de decode+validação do
// pedido, sem resposta de nó: isola o custo do parse (frontend do Handle)
// do custo de rede. O nó "ghost" fica stale e falha o dial imediatamente,
// esgotando a fila de 1 elemento com 502 sem tocar em rede real.
func BenchmarkGatewayHandleDecodeOnly(b *testing.B) {
	gw := benchGateway(b)
	// Remove o nó: Select retorna fila vazia => 503 imediatamente após
	// decode/validação/late-group, sem client.Do.
	if _, err := gw.nodes.Upsert(nodes.Node{NodeID: "ghost", Layer: nodes.LayerBorda, Host: "127.0.0.1:1"}); err != nil {
		b.Fatalf("Upsert ghost: %v", err)
	}
	if err := gw.nodes.Delete("bench-node"); err != nil {
		b.Fatalf("Delete bench-node: %v", err)
	}
	// Nó "ghost" fica stale: DerivedStatus != ativo => fila vazia.
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
