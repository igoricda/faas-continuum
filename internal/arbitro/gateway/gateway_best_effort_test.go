package gateway

// Unitários do Passo 4.3 (adendo 2026-09-30): pendencySet (arm/settle/
// cancel/drain, Dec exatamente 1x, settled nunca bloqueia) e o ramo
// assíncrono dentro da fatia (anunciado-rápido == síncrono, sem armamento).
//
// Gate test-audit: protegem D2 (Dec adiado, 1x) e o armamento (registro
// orcamento_excedido + contador + join no drain). Regressão crível: Dec
// perdido no drain (gauge vaza), Dec duplo (contagem negativa) ou armamento
// espúrio em resposta rápida. Sem cobertura existente (código novo). Sem
// seam novo (tipos de produção + httptest, sem export extra).
import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

func announcedNode(t *testing.T, srvURL string, timeoutMS int) nodes.Node {
	t.Helper()
	return nodes.Node{
		NodeID:            "n1",
		Layer:             nodes.LayerBorda,
		Host:              hostFromURL(t, srvURL),
		Functions:         []string{"fn"},
		FunctionTimeoutMS: map[string]int{"fn": timeoutMS},
	}
}

func invokeRecorder(t *testing.T, gw *Gateway, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	return rec
}

// Anunciado que responde dentro da fatia segue o caminho síncrono: 200 sem
// header tardio, sem armamento, sem sobra no gauge.
func TestAsyncFastEqualsSync(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	mon := monitor.NewMonitor()
	ig := NewInflightGauge()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}}).WithInflight(ig)

	rec := invokeRecorder(t, gw, `{"function":"fn","input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Faas-Late") != "" {
		t.Fatalf("X-Faas-Late=%q want ausente (resposta dentro da fatia)", rec.Header().Get("X-Faas-Late"))
	}
	if armed, _, _, _ := gw.BestEffortCounters(); armed != 0 {
		t.Fatalf("armed=%d want 0 (sem estouro de fatia)", armed)
	}
	if got := ig.NodeK("n1"); got != 0 {
		t.Fatalf("inflight=%d want 0 (Dec no desfecho síncrono)", got)
	}
}

// Endereçamento por CATEGORIA (função pedida "detect", registrada como
// yolo26n-coco): o anúncio D1 é por nome de função (concreto) e o opt-in tem
// de resolver a categoria antes do lookup — regressão: lookup pela função
// pedida não acha o anúncio e o invoke cai no caminho síncrono (a fatia corta
// a tentativa em vez de entregá-la tarde).
func TestBestEffortCategoryAddressing(t *testing.T) {
	srv := lateSuccessServer(t, 300*time.Millisecond, `{"result":"categoria"}`)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{
		NodeID:             "n1",
		Layer:              nodes.LayerBorda,
		Host:               hostFromURL(t, srv.URL),
		Functions:          []string{"yolo26n-coco"},
		FunctionCategories: map[string]string{"yolo26n-coco": "detect"},
		FunctionTimeoutMS:  map[string]int{"yolo26n-coco": 15000},
	})
	gw := waitTestGateway(t, ns, "")

	// SLO 100ms: fatia ~50ms < 300ms da resposta => pendência armada.
	rec := invokeRecorder(t, gw, `{"function":"detect","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != `{"result":"categoria"}` {
		t.Fatalf("body=%q want o corpo da borda", rec.Body.String())
	}
	if got := rec.Header().Get("X-Faas-Late"); got != "true" {
		t.Fatalf("X-Faas-Late=%q want true (a categoria deve achar o anúncio do concreto)", got)
	}
	if armed, served, _, _ := gw.BestEffortCounters(); armed != 1 || served != 1 {
		t.Fatalf("armed=%d served=%d want 1/1 (categoria armou e entregou tarde)", armed, served)
	}
}

// Estouro da fatia arma a pendência: orcamento_excedido no invLog, contador,
// gauge zerado no retorno (drain executou o Dec adiado) e cliente com o
// failure (sem fase de espera ainda — Passo 4.4).
func TestArmOnSliceExpiry(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer srv.Close()
	defer close(release)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	mon := monitor.NewMonitor()
	ig := NewInflightGauge()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}}).WithInflight(ig)
	logPath := t.TempDir() + "/inv.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	defer l.Close()
	gw.SetInvocationLog(l)

	// SLO curto força a fatia a estourar com o nó bloqueado.
	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504 body=%s", rec.Code, rec.Body.String())
	}
	if armed, _, _, _ := gw.BestEffortCounters(); armed != 1 {
		t.Fatalf("armed=%d want 1", armed)
	}
	// O armamento gravou orcamento_excedido; o drain (pós-resposta, 2026-10-01/2)
	// supersede com o desfecho REAL (timeout no cap) e só então executa o Dec.
	deadline := time.Now().Add(2 * time.Second)
	for ig.NodeK("n1") != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("inflight=%d want 0 após o desfecho (Dec adiado vazou)", ig.NodeK("n1"))
		}
		time.Sleep(10 * time.Millisecond)
	}
	recs := readInvLogLines(t, logPath)
	if len(recs) != 2 || recs[0].Outcome != "orcamento_excedido" || recs[1].Outcome != "timeout" {
		t.Fatalf("invLog=%+v want [orcamento_excedido, timeout real do cap]", recs)
	}
}

// D7: teto de pendências = maxRaceFanout — além do teto, arm recusa.
func TestPendencySetMaxFanout(t *testing.T) {
	set := newPendencySet(4, 1)
	first := &pendingAttempt{done: make(chan struct{})}
	if !set.arm(first) {
		t.Fatalf("primeiro arm recusado com teto 1")
	}
	second := &pendingAttempt{done: make(chan struct{})}
	if set.arm(second) {
		t.Fatalf("segundo arm aceito com teto 1 (D7 violado)")
	}
	if got := len(set.armedList()); got != 1 {
		t.Fatalf("armadas=%d want 1", got)
	}
}

// D7: ramo sem armamento corta no overall (ou na desconexão), nunca no cap do
// nó — sem o corte, uma tentativa que a espera não entregará seguraria o laço
// até o cap anunciado (validação permite até 24h).
func TestD7RejectCutsAtOverallNotCap(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release: // trava até o árbitro cortar; release libera o Close
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	ns, _ := nodes.NewNodeStore("")
	node := announcedNode(t, srv.URL, 15000)
	_, _ = ns.Upsert(node)
	gw := newTestGateway(ns)

	// Conjunto cheio: a armação é recusada (mesmo ramo do teste do teto).
	set := newPendencySet(2, 1)
	if !set.arm(&pendingAttempt{}) {
		t.Fatalf("primeira armação deveria caber no teto")
	}
	overall := 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), overall)
	defer cancel()
	sh := gatewayDispatchShared{
		ctx:       ctx,
		clientCtx: context.Background(),
		overall:   overall,
		start:     gw.now(),
		requestID: "req-d7",
		bodyBytes: []byte(`{"input":{"a":1}}`),
		queue:     []nodes.Node{node, node},
		pend:      set,
	}
	var captured attemptCapture
	start := time.Now()
	f, responded := gw.dispatchAsyncAttempt(httptest.NewRecorder(), InvocationRequest{Function: "fn"}, sh, node, 0, gw.now(), "fn", false, "", 0, 15*time.Second, &captured)
	elapsed := time.Since(start)
	if responded {
		t.Fatalf("responded=true: o ramo sem armamento não escreve")
	}
	if f.httpStatus != http.StatusGatewayTimeout || f.code != "NODE_INVOCATION_TIMEOUT" {
		t.Fatalf("failure=%+v want 504 NODE_INVOCATION_TIMEOUT (corte no overall)", f)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("retorno em %v: segurou até o cap do nó em vez do overall %v", elapsed, overall)
	}
}

// Drain NÃO cancela mais (2026-10-01/2): espera a órfã concluir sozinha,
// assenta o desfecho real (aprendizado — supersede do orcamento_excedido, sem
// entrega) e executa o Dec exatamente 1x, mesmo com drain duplo. A cancelada
// (desconexão do cliente) é contada, sem assentamento. Regressões críveis: o
// drain voltar a cancelar (perde a amostra e o container quente), vazar o Dec
// ou reassentar a Entry no drain duplo.
func TestPendencySetDrainSettlesOrphanOnce(t *testing.T) {
	gw, node, now, logPath, pa, out := settleFixture(t, "req-drain")
	var decs atomic.Int64
	set := newPendencySet(4, 3)
	pa.done = make(chan struct{})
	pa.dec = func() { decs.Add(1) }
	pa.cancel = func() {}
	pa.outcome = out
	if !set.arm(pa) {
		t.Fatalf("arm recusado")
	}
	// O "nó" conclui sozinho (sem cancelamento nosso): a órfã segue até o
	// fim, como no dispatch real.
	close(pa.done)
	gw.drainPendencies(set)
	gw.drainPendencies(set) // idempotente: não Dec 2x nem reassenta.
	if got := decs.Load(); got != 1 {
		t.Fatalf("decs=%d want exatamente 1", got)
	}
	recs := readInvLogLines(t, logPath)
	if len(recs) != 2 || recs[1].Outcome != "sucesso" || recs[1].LateDelivered {
		t.Fatalf("invLog=%+v want [orcamento_excedido, sucesso late_delivered=false]", recs)
	}
	if recs[1].ObservedLatencyMS != 300 {
		t.Fatalf("latência=%d want 300 (desfecho real, não a fatia)", recs[1].ObservedLatencyMS)
	}
	// Cancelada (cliente sumiu): contada em cancelled, sem Entry nova.
	paCancel := &pendingAttempt{
		target: node, function: "fn", group: "fn",
		requestID: "req-drain", attemptN: 2,
		attemptStart: now, cap: 15 * time.Second,
		done:    make(chan struct{}),
		dec:     func() { decs.Add(1) },
		cancel:  func() {},
		outcome: pendingOutcome{transportErr: context.Canceled, ctxErr: context.Canceled, latency: 10 * time.Millisecond},
	}
	if !set.arm(paCancel) {
		t.Fatalf("arm da cancelada recusado")
	}
	close(paCancel.done)
	gw.drainPendencies(set)
	if _, _, _, cancelled := gw.BestEffortCounters(); cancelled != 1 {
		t.Fatalf("cancelled=%d want 1 (desconexão contada)", cancelled)
	}
	if recs2 := readInvLogLines(t, logPath); len(recs2) != 2 {
		t.Fatalf("linhas=%d want 2 (cancelada não assenta)", len(recs2))
	}
}
