package gateway

// Unit tests of the set of pending reports, with the arming, the settlement, the
// cancellation and the drain, a single decrement, and a settled report that never
// blocks, plus the asynchronous branch within the slice, where an announced node
// that answers fast behaves as the synchronous path without arming.
//
// They protect the deferred single decrement and the arming, which records the
// exceeded budget, increments the counter and joins in the drain. The credible
// regressions are a lost decrement in the drain, which leaks the meter, a double
// decrement, which makes the count negative, and a spurious arming on a fast
// answer.
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

// An announced node that answers within the slice follows the synchronous path:
// a success without the late header, without arming and without a leftover in the
// meter.
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

// Addressing by category: the requested function is a category and the
// registered one is a concrete variant, so the announcement of the function
// timeout is per concrete name and the opt-in has to resolve the category before
// the lookup. Otherwise the lookup by the requested function does not find the
// announcement and the invocation falls into the synchronous path, which cuts
// the attempt instead of delivering it late.
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

	// An objective of one hundred milliseconds gives a slice of about fifty, which is below the response of three hundred, so the pending report is armed.
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

// The overflow of the slice arms the pending report: an exceeded budget in the
// log, the counter, the meter back to zero on the return, since the drain ran the
// deferred decrement, and the client with the failure, without the stage of wait.
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

	// A short objective forces the slice to overflow with the node blocked.
	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504 body=%s", rec.Code, rec.Body.String())
	}
	if armed, _, _, _ := gw.BestEffortCounters(); armed != 1 {
		t.Fatalf("armed=%d want 1", armed)
	}
	// The arming recorded an exceeded budget, and the drain after the answer supersedes
	// it with the real outcome, which is the timeout at the ceiling, and only then runs the decrement.
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

// The ceiling of pending reports is the fan-out of the race, so beyond it the arming is refused.
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

// The branch without arming cuts at the overall budget or at the disconnection,
// and never at the ceiling of the node, since without the cut an attempt that
// the wait will not deliver would hold the loop up to the announced ceiling.
func TestD7RejectCutsAtOverallNotCap(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release: // the request stays blocked until the arbiter cuts it, and the release frees the close
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	ns, _ := nodes.NewNodeStore("")
	node := announcedNode(t, srv.URL, 15000)
	_, _ = ns.Upsert(node)
	gw := newTestGateway(ns)

	// The set is full, so the arming is refused, which is the same branch of the test of the ceiling.
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

// The drain no longer cancels: it waits for the orphan to conclude alone,
// settles the real outcome, which feeds the learning and supersedes the exceeded
// A client disconnection is counted, without a settlement. The credible
// regressions are the drain cancelling again, which loses the sample and the
// warm container, a leaked decrement, and a double settlement in a double drain.
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
	// The node concludes alone, without a cancellation from the arbiter, so the
	// orphan runs to the end as in the real dispatch.
	close(pa.done)
	gw.drainPendencies(set)
	gw.drainPendencies(set) // idempotent, without a double decrement and without a second settlement
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
