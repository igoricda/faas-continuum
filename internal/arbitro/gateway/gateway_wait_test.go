package gateway

// Tests of the stage of wait: the late delivery of the first success, the winner
// cancelling the loser, a disconnection aborting without writing, the hard
// deadline falling through to the cloud or to the failure, a non-success settling
// and continuing, and the arithmetic of the hard deadline.
//
// Each case protects a distinct branch of the wait for the pending reports.
// The credible regressions are delivering the second success, not cancelling the
// loser, which leaks the meter, writing after a disconnection, and swapping the
// maximum for the minimum in the hard deadline, which would kill the window of
// the cloud without an objective.
import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

func waitTestGateway(t *testing.T, ns *nodes.NodeStore, logPath string) *Gateway {
	t.Helper()
	mon := monitor.NewMonitor()
	ig := NewInflightGauge()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}}).WithInflight(ig)
	if logPath != "" {
		l, err := NewInvocationLog(logPath)
		if err != nil {
			t.Fatalf("NewInvocationLog: %v", err)
		}
		t.Cleanup(func() { _ = l.Close() })
		gw.SetInvocationLog(l)
	}
	return gw
}

func readInvLogLines(t *testing.T, path string) []InvocationRecord {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var out []InvocationRecord
	for _, ln := range splitLines(t, raw) {
		var r InvocationRecord
		if err := json.Unmarshal(ln, &r); err != nil {
			t.Fatalf("linha inválida: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// The edge exceeds the slice and concludes later with a success, so the client
// receives the body with a success and the entry settles as a success outside the
// objective.
func TestWaitDeliversLateSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"tardio"}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	logPath := t.TempDir() + "/inv.jsonl"
	gw := waitTestGateway(t, ns, logPath)

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != `{"result":"tardio"}` {
		t.Fatalf("body=%q want o corpo da borda", rec.Body.String())
	}
	recs := readInvLogLines(t, logPath)
	if len(recs) != 2 {
		t.Fatalf("linhas=%d want 2 (orcamento + sucesso tardio)", len(recs))
	}
	if recs[0].Outcome != history.OutcomeOrcamentoExcedido {
		t.Fatalf("linha 0 outcome=%q want orcamento_excedido", recs[0].Outcome)
	}
	last := recs[1]
	if last.Outcome != history.OutcomeSucesso || last.SloMet {
		t.Fatalf("última outcome=%q slo_met=%v want sucesso/false", last.Outcome, last.SloMet)
	}
	if !last.LateDelivered {
		t.Fatalf("última late_delivered=false want true (entrega tardia)")
	}
	if last.ObservedLatencyMS < 250 {
		t.Fatalf("latência=%d want ~300 (real, não a fatia)", last.ObservedLatencyMS)
	}
}

// The fog answers within the slice while the edge is a pending report, so the fog
// wins, the edge is cancelled and the meter of the requests in flight returns to
// zero.
func TestWaitFirstSuccessWins(t *testing.T) {
	releaseSlow := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-releaseSlow:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"result":"lento"}`))
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	defer close(releaseSlow)
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"rapido"}`))
	}))
	defer fast.Close()
	ns, _ := nodes.NewNodeStore("")
	// Stable order by node identifier: the slow node arms first and the fast one wins in the loop.
	_, _ = ns.Upsert(nodes.Node{NodeID: "a-slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slow.URL), Functions: []string{"fn"}, FunctionTimeoutMS: map[string]int{"fn": 15000}})
	_, _ = ns.Upsert(nodes.Node{NodeID: "b-fast", Layer: nodes.LayerBorda, Host: hostFromURL(t, fast.URL), Functions: []string{"fn"}, FunctionTimeoutMS: map[string]int{"fn": 15000}})
	logPath := t.TempDir() + "/inv.jsonl"
	gw := waitTestGateway(t, ns, logPath)

	// An objective of two hundred milliseconds gives slices of about one hundred,
	// which is a loose margin for the fast node to answer within its own slice even
	// under a loaded continuous integration.
	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":200},"input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != `{"result":"rapido"}` {
		t.Fatalf("body=%q want o corpo da vencedora", rec.Body.String())
	}
	if armed, _, _, _ := gw.BestEffortCounters(); armed != 1 {
		t.Fatalf("armed=%d want 1 (a-slow armou)", armed)
	}
	recs := readInvLogLines(t, logPath)
	var slowBudget, fastOK bool
	for _, r := range recs {
		if r.NodeID == "a-slow" && r.Outcome == history.OutcomeOrcamentoExcedido {
			slowBudget = true
		}
		if r.NodeID == "b-fast" && r.Outcome == history.OutcomeSucesso {
			fastOK = true
		}
		if r.NodeID == "a-slow" && r.Outcome == history.OutcomeSucesso {
			t.Fatalf("a-slow cancelada não deveria assentar sucesso")
		}
	}
	if !slowBudget || !fastOK {
		t.Fatalf("linhas sem orcamento(a-slow)=%v e sucesso(b-fast)=%v: %+v", slowBudget, fastOK, recs)
	}
}

// The client disappears during the wait, so the handler aborts without writing,
// without a panic and without leaking a goroutine, and the race detector confirms
// the join.
func TestWaitClientDisconnect(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			_, _ = w.Write([]byte(`{}`))
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	logPath := t.TempDir() + "/inv.jsonl"
	gw := waitTestGateway(t, ns, logPath)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		gw.Handle(rec, req)
	}()
	// Aguarda o armamento (fatia ~100ms) e desconecta no meio da espera.
	time.Sleep(250 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatalf("Handle não retornou após a desconexão (travou na espera)")
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("retorno em %v: esperava abortar na desconexão, não no cap", elapsed)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("body=%q want vazio (abortou sem escrever)", rec.Body.String())
	}
	recs := readInvLogLines(t, logPath)
	if len(recs) == 0 || recs[len(recs)-1].Outcome != history.OutcomeOrcamentoExcedido {
		t.Fatalf("última linha=%+v want orcamento_excedido (sem assentamento)", recs)
	}
}

// An expired hard deadline falls through to the cloud or to the failure, without
// waiting for the ceiling of the node. The orphan stays alive, since nothing
// cancels it, and the drain settles it with the real outcome after the answer.
func TestWaitHardExpiryFallsThrough(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			_, _ = w.Write([]byte(`{}`))
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 400))
	logPath := t.TempDir() + "/inv.jsonl"
	gw := waitTestGateway(t, ns, logPath)

	start := time.Now()
	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":200},"input":{"a":1}}`)
	elapsed := time.Since(start)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504 body=%s", rec.Code, rec.Body.String())
	}
	// The hard deadline is the maximum between two hundred and about four hundred milliseconds, far below the five seconds of the blocked node.
	if elapsed > 3*time.Second {
		t.Fatalf("espera=%v: prazo rígido não cortou", elapsed)
	}
	// The orphan stays alive, and the drain after the answer settles it with the real
	// outcome, which is the timeout at the ceiling of the node, so the arming and the
	// outcome coexist in the log and the timed out answer above is the final failure.
	deadline := time.Now().Add(2 * time.Second)
	for {
		raw, _ := os.ReadFile(logPath)
		if strings.Contains(string(raw), `"outcome":"timeout"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("invLog sem o desfecho real (timeout) do cap: %s", string(raw))
		}
		time.Sleep(20 * time.Millisecond)
	}
	recs := readInvLogLines(t, logPath)
	var armado, timeout bool
	for _, r := range recs {
		if r.NodeID == "n1" && r.Outcome == history.OutcomeOrcamentoExcedido {
			armado = true
		}
		if r.NodeID == "n1" && r.Outcome == history.OutcomeTimeout {
			timeout = true
		}
	}
	if !armado || !timeout {
		t.Fatalf("invLog=%+v want armamento e desfecho real (timeout) depois do drain", recs)
	}
}

// A late non-success settles the real entry, which supersedes the previous one, and
// the wait continues, so without a success it falls through to the cloud or to the failure with the real class in the log.
func TestWaitLateFailureSettles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"POOL_EXHAUSTED"}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	logPath := t.TempDir() + "/inv.jsonl"
	gw := waitTestGateway(t, ns, logPath)

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 (último assentado, não o budget) body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NODE_OVERLOADED" {
		t.Fatalf("code=%q want NODE_OVERLOADED", er.Code)
	}
	recs := readInvLogLines(t, logPath)
	if len(recs) != 2 {
		t.Fatalf("linhas=%d want 2 (orcamento + rejeição real)", len(recs))
	}
	if recs[1].Outcome != history.OutcomeRejeicaoCapacidade {
		t.Fatalf("última outcome=%q want rejeicao_capacidade (supersede do orcamento)", recs[1].Outcome)
	}
	if recs[1].LateDelivered {
		t.Fatalf("late_delivered=true em não-sucesso")
	}
}

// The hard deadline is the maximum between the overall budget and the start of the attempt plus the ceiling, and without pending reports it is the overall budget.
func TestHardDeadlineMath(t *testing.T) {
	start := time.Now()
	armed := []*pendingAttempt{
		{attemptStart: start.Add(1 * time.Second), cap: 10 * time.Second},
		{attemptStart: start.Add(2 * time.Second), cap: 3 * time.Second},
	}
	if got := hardDeadline(5*time.Second, start, armed); !got.Equal(start.Add(11 * time.Second)) {
		t.Fatalf("hard=%v want start+11s (fim natural da 1ª)", got)
	}
	if got := hardDeadline(30*time.Second, start, armed); !got.Equal(start.Add(30 * time.Second)) {
		t.Fatalf("hard=%v want start+30s (overall domina)", got)
	}
	if got := hardDeadline(5*time.Second, start, nil); !got.Equal(start.Add(5 * time.Second)) {
		t.Fatalf("hard=%v want start+5s (sem pendências)", got)
	}
}
