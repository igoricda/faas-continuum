// Tests of the observation of the armed pending reports during the loop, behind
// the flag of the gateway.
package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// fixedQueueSelector returns a fixed queue, with a deterministic order that does
// not depend on the ranking. It only implements the selection, without the
// prediction ports, so the slice is half of the remaining budget.
type fixedQueueSelector struct{ queue []nodes.Node }

func (s fixedQueueSelector) Select([]nodes.Node, string, *selection.SLO) []nodes.Node {
	return s.queue
}

func announcedNodeID(t *testing.T, id, srvURL string, timeoutMS int) nodes.Node {
	t.Helper()
	return nodes.Node{
		NodeID:            id,
		Layer:             nodes.LayerBorda,
		Host:              hostFromURL(t, srvURL),
		Functions:         []string{"fn"},
		FunctionTimeoutMS: map[string]int{"fn": timeoutMS},
	}
}

func statusServer(t *testing.T, delay time.Duration, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(delay):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func observeTestGateway(t *testing.T, ns *nodes.NodeStore, logPath string, sel selection.Selector, observeArmed bool) (*Gateway, *InflightGauge) {
	t.Helper()
	h, err := history.NewHistory("", 500)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	mon := monitor.NewMonitor()
	ig := NewInflightGauge()
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: mon, Selector: sel, History: h,
		ObserveArmed: observeArmed,
	}).WithInflight(ig)
	if !gw.observeArmed != !observeArmed {
		t.Fatalf("fiação ObserveArmed: got %v want %v", gw.observeArmed, observeArmed)
	}
	if logPath != "" {
		l, err := NewInvocationLog(logPath)
		if err != nil {
			t.Fatalf("NewInvocationLog: %v", err)
		}
		t.Cleanup(func() { _ = l.Close() })
		gw.SetInvocationLog(l)
	}
	return gw, ig
}

func waitForLines(t *testing.T, path string, n int, timeout time.Duration) []InvocationRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		lines := readInvLogLines(t, path)
		if len(lines) >= n {
			return lines
		}
		if time.Now().After(deadline) {
			t.Fatalf("linhas=%d após %s, want >=%d", len(lines), timeout, n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitForGaugeZero(t *testing.T, ig *InflightGauge, fn string, ids []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		zero := true
		for _, id := range ids {
			if ig.NodeK(id) != 0 || ig.FnK(id, fn) != 0 {
				zero = false
			}
		}
		if zero {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gauge não zerou em %s", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestRace_ArmedWinsWhileNextInSlice: the first node takes one and two tenths of
// a second with a slice of one, and the second takes three with a loose slice. The
// client receives the first in about one and two tenths, with the late header, the
// second is settled by the drain, the history receives two entries and the meter
// returns to zero.
func TestRace_ArmedWinsWhileNextInSlice(t *testing.T) {
	srvA := statusServer(t, 1200*time.Millisecond, 200, `{"result":"A"}`)
	srvB := statusServer(t, 3000*time.Millisecond, 200, `{"result":"B"}`)
	ns, _ := nodes.NewNodeStore("")
	nA := announcedNodeID(t, "nA", srvA.URL, 15000)
	nB := announcedNodeID(t, "nB", srvB.URL, 15000)
	_, _ = ns.Upsert(nA)
	_, _ = ns.Upsert(nB)
	logPath := t.TempDir() + "/inv.jsonl"
	gw, ig := observeTestGateway(t, ns, logPath, fixedQueueSelector{[]nodes.Node{nA, nB}}, true)

	start := time.Now()
	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":2000},"input":{"a":1}}`)
	elapsed := time.Since(start)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != `{"result":"A"}` {
		t.Fatalf("body=%q want A (primeira resposta com sucesso vence)", rec.Body.String())
	}
	if got := rec.Header().Get("X-Faas-Late"); got != "true" {
		t.Fatalf("X-Faas-Late=%q want true", got)
	}
	if elapsed >= 1800*time.Millisecond {
		t.Fatalf("latência=%s want < 1.8s (armada entregue no laço, sem esperar a fatia de B)", elapsed)
	}
	if got := gw.ObserveArmedWins(); got != 1 {
		t.Fatalf("armed_wins=%d want 1", got)
	}
	lines := waitForLines(t, logPath, 3, 10*time.Second)
	rid := lines[0].RequestID
	if rid == "" {
		t.Fatalf("request_id vazio")
	}
	var attempts []int
	delivered := 0
	for _, ln := range lines {
		if ln.RequestID != rid {
			t.Fatalf("request_id=%q want %q", ln.RequestID, rid)
		}
		attempts = append(attempts, ln.AttemptN)
		if ln.Delivered {
			delivered++
			if ln.AttemptN != 1 {
				t.Fatalf("delivered attempt_n=%d want 1 (A venceu)", ln.AttemptN)
			}
		}
	}
	if delivered != 1 {
		t.Fatalf("delivered=%d want exatamente 1", delivered)
	}
	waitForGaugeZero(t, ig, "fn", []string{"nA", "nB"}, 10*time.Second)
	now := time.Now()
	for _, id := range []string{"nA", "nB"} {
		fs, ok := gw.hist.StatsFull(history.SegmentKey("fn", id, history.ScenarioContext{LoadBucket: history.BucketUnknown}), time.Hour, now)
		if !ok || fs.Count != 1 {
			t.Fatalf("nó %s: count=%d ok=%v, want 1 sucesso no History", id, fs.Count, ok)
		}
	}
}

// TestRace_ArmedWinsLegacyOff: the same scenario with the flag disabled still
// delivers the first node, through the wait for the pending reports, but only
// after the slice of the second, of about two seconds.
func TestRace_ArmedWinsLegacyOff(t *testing.T) {
	srvA := statusServer(t, 1200*time.Millisecond, 200, `{"result":"A"}`)
	srvB := statusServer(t, 3000*time.Millisecond, 200, `{"result":"B"}`)
	ns, _ := nodes.NewNodeStore("")
	nA := announcedNodeID(t, "nA", srvA.URL, 15000)
	nB := announcedNodeID(t, "nB", srvB.URL, 15000)
	_, _ = ns.Upsert(nA)
	_, _ = ns.Upsert(nB)
	logPath := t.TempDir() + "/inv.jsonl"
	gw, ig := observeTestGateway(t, ns, logPath, fixedQueueSelector{[]nodes.Node{nA, nB}}, false)

	start := time.Now()
	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":2000},"input":{"a":1}}`)
	elapsed := time.Since(start)
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"result":"A"}` {
		t.Fatalf("status=%d body=%s want 200 + A", rec.Code, rec.Body.String())
	}
	if elapsed < 1800*time.Millisecond {
		t.Fatalf("latência=%s want >= 1.8s sem o G1 (espera a fatia de B)", elapsed)
	}
	if got := gw.ObserveArmedWins(); got != 0 {
		t.Fatalf("armed_wins=%d want 0 com flag off", got)
	}
	waitForLines(t, logPath, 3, 10*time.Second)
	waitForGaugeZero(t, ig, "fn", []string{"nA", "nB"}, 10*time.Second)
}

// TestRace_CurrentWinsUnchanged: the current attempt concludes first, with a
// behavior identical to the previous one, with and without the flag.
func TestRace_CurrentWinsUnchanged(t *testing.T) {
	for _, observe := range []bool{false, true} {
		srvA := statusServer(t, 50*time.Millisecond, 200, `{"result":"A"}`)
		srvB := statusServer(t, 3000*time.Millisecond, 200, `{"result":"B"}`)
		ns, _ := nodes.NewNodeStore("")
		nA := announcedNodeID(t, "nA", srvA.URL, 15000)
		nB := announcedNodeID(t, "nB", srvB.URL, 15000)
		_, _ = ns.Upsert(nA)
		_, _ = ns.Upsert(nB)
		logPath := t.TempDir() + "/inv.jsonl"
		gw, ig := observeTestGateway(t, ns, logPath, fixedQueueSelector{[]nodes.Node{nA, nB}}, observe)

		rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":5000},"input":{"a":1}}`)
		if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"result":"A"}` {
			t.Fatalf("observe=%v: status=%d body=%s want 200 + A", observe, rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("X-Faas-Late"); got != "" {
			t.Fatalf("observe=%v: X-Faas-Late=%q want ausente (vitória na fatia)", observe, got)
		}
		if got := gw.ObserveArmedWins(); got != 0 {
			t.Fatalf("observe=%v: armed_wins=%d want 0", observe, got)
		}
		lines := waitForLines(t, logPath, 1, 5*time.Second)
		if len(lines) != 1 || !lines[0].Delivered || lines[0].Armed || lines[0].AttemptN != 1 {
			t.Fatalf("observe=%v: linhas=%+v want 1 entregue sem arma", observe, lines)
		}
		if armed, _, _, _ := gw.BestEffortCounters(); armed != 0 {
			t.Fatalf("observe=%v: armed=%d want 0 (B nunca tentado)", observe, armed)
		}
		waitForGaugeZero(t, ig, "fn", []string{"nA", "nB"}, 5*time.Second)
	}
}

// TestRace_ArmedFailsThenCurrentWins: the armed report fails during the slice of
// the current attempt, so the observation settles the failure inside the loop and
// the current attempt wins. The order of the lines with the observation is the
// arming, the failure and the victory, and without it the failure is settled in
// the drain, after the victory.
func TestRace_ArmedFailsThenCurrentWins(t *testing.T) {
	for _, tc := range []struct {
		observe bool
		want    []string
	}{
		{true, []string{"orcamento_excedido", "falha", "sucesso"}},
		{false, []string{"orcamento_excedido", "sucesso", "falha"}},
	} {
		srvA := statusServer(t, 1100*time.Millisecond, 500, `erro`)
		srvB := statusServer(t, 500*time.Millisecond, 200, `{"result":"B"}`)
		ns, _ := nodes.NewNodeStore("")
		nA := announcedNodeID(t, "nA", srvA.URL, 15000)
		nB := announcedNodeID(t, "nB", srvB.URL, 15000)
		_, _ = ns.Upsert(nA)
		_, _ = ns.Upsert(nB)
		logPath := t.TempDir() + "/inv.jsonl"
		gw, ig := observeTestGateway(t, ns, logPath, fixedQueueSelector{[]nodes.Node{nA, nB}}, tc.observe)

		rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":2000},"input":{"a":1}}`)
		if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"result":"B"}` {
			t.Fatalf("observe=%v: status=%d body=%s want 200 + B", tc.observe, rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("X-Faas-Late"); got != "" {
			t.Fatalf("observe=%v: X-Faas-Late=%q want ausente (B venceu na fatia)", tc.observe, got)
		}
		lines := waitForLines(t, logPath, 3, 10*time.Second)
		var outcomes []string
		for _, ln := range lines {
			outcomes = append(outcomes, ln.Outcome)
		}
		for i := range tc.want {
			if outcomes[i] != tc.want[i] {
				t.Fatalf("observe=%v: outcomes=%v want %v", tc.observe, outcomes, tc.want)
			}
		}
		delivered := 0
		for _, ln := range lines {
			if ln.Delivered {
				delivered++
				if ln.AttemptN != 2 {
					t.Fatalf("observe=%v: delivered attempt_n=%d want 2 (B)", tc.observe, ln.AttemptN)
				}
			}
		}
		if delivered != 1 {
			t.Fatalf("observe=%v: delivered=%d want 1", tc.observe, delivered)
		}
		waitForGaugeZero(t, ig, "fn", []string{"nA", "nB"}, 10*time.Second)
	}
}

// TestRace_NoGaugeLeakNoGoroutineLeak runs several invocations with a race and
// checks that the meter returns to zero and that no goroutine leaks.
func TestRace_NoGaugeLeakNoGoroutineLeak(t *testing.T) {
	srvA := statusServer(t, 300*time.Millisecond, 200, `{"result":"A"}`)
	srvB := statusServer(t, 100*time.Millisecond, 200, `{"result":"B"}`)
	ns, _ := nodes.NewNodeStore("")
	nA := announcedNodeID(t, "nA", srvA.URL, 5000)
	nB := announcedNodeID(t, "nB", srvB.URL, 5000)
	_, _ = ns.Upsert(nA)
	_, _ = ns.Upsert(nB)
	logPath := t.TempDir() + "/inv.jsonl"
	gw, ig := observeTestGateway(t, ns, logPath, fixedQueueSelector{[]nodes.Node{nA, nB}}, true)

	// Aquecimento + assentamento total antes da baseline.
	invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":400},"input":{"a":1}}`)
	waitForGaugeZero(t, ig, "fn", []string{"nA", "nB"}, 10*time.Second)
	base := runtime.NumGoroutine()
	const n = 10
	for i := 0; i < n; i++ {
		rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":400},"input":{"a":1}}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("invoke %d: status=%d", i, rec.Code)
		}
	}
	waitForGaugeZero(t, ig, "fn", []string{"nA", "nB"}, 15*time.Second)
	deadline := time.Now().Add(15 * time.Second)
	for runtime.NumGoroutine() > base+4 {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines=%d baseline=%d (vazamento?)", runtime.NumGoroutine(), base)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestWaitPendencies_RemainingExcludesConcluded: the wait counts only the armed
// reports that did not conclude, so one consumed inside the loop does not hold
// the wait until the hard deadline.
func TestWaitPendencies_RemainingExcludesConcluded(t *testing.T) {
	now := time.Now()
	h, err := history.NewHistory("", 500)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	ns, _ := nodes.NewNodeStore("")
	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
	_, _ = ns.Upsert(node)
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: mon,
		Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		History:  h,
	})
	l, err := NewInvocationLog(t.TempDir() + "/inv.jsonl")
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gw.SetInvocationLog(l)

	pend := newPendencySet(2, 3)
	// The first report is consumed inside the loop, concluded, with the message already taken.
	done1 := make(chan struct{})
	close(done1)
	pa1 := &pendingAttempt{
		target: node, function: "fn", concrete: "fn", group: "fn",
		requestID: "req-r", attemptN: 1, attemptStart: now, cap: 20 * time.Second,
		done: done1, settled: pend.settled,
		cancel: func() {}, stopClient: func() bool { return false },
		dec: func() {},
	}
	pa1.armed.Store(true)
	pa1.concluded.Store(true)
	if !pend.arm(pa1) {
		t.Fatalf("arm pa1 recusado")
	}
	// PA2: conclui com sucesso em 100ms.
	done2 := make(chan struct{})
	pa2 := &pendingAttempt{
		target: node, function: "fn", concrete: "fn", group: "fn",
		requestID: "req-r", attemptN: 2, attemptStart: now, cap: 20 * time.Second,
		done: done2, settled: pend.settled,
		cancel: func() {}, stopClient: func() bool { return false },
		dec: func() {},
	}
	pa2.armed.Store(true)
	if !pend.arm(pa2) {
		t.Fatalf("arm pa2 recusado")
	}
	var decCalls atomic.Int64
	pa2.dec = func() { decCalls.Add(1) }
	go func() {
		time.Sleep(100 * time.Millisecond)
		pa2.outcome = pendingOutcome{
			hasResponse: true,
			res:         attemptResult{status: 200, body: []byte(`{"result":"ok"}`)},
			latency:     100 * time.Millisecond,
		}
		close(done2)
		pend.settled <- pa2
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sh := gatewayDispatchShared{
		ctx: ctx, clientCtx: context.Background(),
		overall: 10 * time.Second, start: now,
		requestID: "req-r", group: "fn", pend: pend,
	}
	w := httptest.NewRecorder()
	req := InvocationRequest{Function: "fn"}
	seed := gatewayFailure{httpStatus: http.StatusBadGateway, code: "SEED", msg: "seed", retryable: true}
	start := time.Now()
	_, responded := gw.waitPendencies(w, req, sh, pend, seed)
	elapsed := time.Since(start)
	if !responded {
		t.Fatalf("responded=false want true (PA2 vence)")
	}
	if elapsed >= 5*time.Second {
		t.Fatalf("espera=%s want < 5s (PA1 concluída não conta no remaining)", elapsed)
	}
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"result":"ok"}` {
		t.Fatalf("status=%d body=%s want 200 + ok", w.Code, w.Body.String())
	}
}
