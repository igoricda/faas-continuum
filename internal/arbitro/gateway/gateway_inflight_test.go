package gateway

// Cost-weighted meter of the requests in flight and the unified plumbing of
// the projected load: the balance of the meter under a concurrent handler, the
// isolation of two functions in the same node, since the count is per node,
// the cost per node and function with the reception-time filter, the
// projection without a double count in the case of a single request in
// flight, and the decrement on a panic and on every error path.

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func TestInflightGaugeIncDecBalance(t *testing.T) {
	g := NewInflightGauge()
	now := time.Unix(1_700_000_000, 0)

	if k := g.Inc("n1", "f1", 1.5, now); k != 1 {
		t.Fatalf("Inc#1 k=%d, want 1", k)
	}
	if k := g.Inc("n1", "f1", 1.5, now); k != 2 {
		t.Fatalf("Inc#2 k=%d, want 2 (node-level)", k)
	}
	if g.NodeK("n1") != 2 || g.FnK("n1", "f1") != 2 {
		t.Fatalf("contagens = node %d fn %d, want 2/2", g.NodeK("n1"), g.FnK("n1", "f1"))
	}
	g.Dec("n1", "f1", now)
	if g.NodeK("n1") != 1 || g.FnK("n1", "f1") != 1 {
		t.Fatalf("após Dec = node %d fn %d, want 1/1", g.NodeK("n1"), g.FnK("n1", "f1"))
	}
	g.Dec("n1", "f1", now)
	if g.NodeK("n1") != 0 || g.FnK("n1", "f1") != 0 {
		t.Fatalf("após 2 Dec = node %d fn %d, want 0/0", g.NodeK("n1"), g.FnK("n1", "f1"))
	}
	// Deletion on zero: the three collections return to empty, bounded by the
	// live concurrency and not by the history.
	if len(g.node) != 0 || len(g.fn) != 0 || len(g.entries) != 0 {
		t.Fatalf("mapas não zeraram: node=%d fn=%d entries=%d", len(g.node), len(g.fn), len(g.entries))
	}
	// A decrement below zero does nothing, since the value is never negative.
	g.Dec("n1", "f1", now)
	if g.NodeK("n1") != 0 {
		t.Fatalf("Dec extra levou NodeK a %d, want 0", g.NodeK("n1"))
	}
}

// TestInflightGaugeDecIdentityNewerFirst proves that the decrement removes the
// entry by the identity of the dispatch, which is the instant stamped at the
// increase, and not by first in first out: with two entries of different costs
// in the same pair of node and function, the newer one finishes first and the
// survivor has to be the older one, so the cost since the reception reflects
// five and not one. Under the previous first-in-first-out logic the decrement
// would remove the older entry and the cost would be one, and this test fails
// on that behavior.
func TestInflightGaugeDecIdentityNewerFirst(t *testing.T) {
	g := NewInflightGauge()
	t1 := time.Unix(1_700_000_000, 0)
	t2 := t1.Add(time.Second)
	g.Inc("n1", "f1", 5.0, t1) // mais ANTIGA, custo 5
	g.Inc("n1", "f1", 1.0, t2) // mais NOVA, custo 1

	// The newer entry finishes first.
	g.Dec("n1", "f1", t2)

	if g.FnK("n1", "f1") != 1 || g.NodeK("n1") != 1 {
		t.Fatalf("após Dec = node %d fn %d, want 1/1", g.NodeK("n1"), g.FnK("n1", "f1"))
	}
	if got := g.NodeCostSince("n1", time.Time{}); got != 5.0 {
		t.Fatalf("NodeCostSince=%v, want 5 (custo da entry ANTIGA sobrevivente; FIFO deixaria 1)", got)
	}
	// The older entry stays visible for a reception instant before it...
	if got := g.NodeCostSince("n1", t1.Add(-time.Millisecond)); got != 5.0 {
		t.Fatalf("NodeCostSince(antes de t1)=%v, want 5", got)
	}
	// ...and disappears for a reception instant equal to it, since the filter is strictly later.
	if got := g.NodeCostSince("n1", t1); got != 0.0 {
		t.Fatalf("NodeCostSince(t1)=%v, want 0 (t1 não é estritamente posterior a t1)", got)
	}
}

// TestInflightGaugeDecUnmatchedDrainsOldest proves the documented fallback: an
// instant that matches no entry, from a clock collision or from an entry that
// was already removed, drains the oldest one, so the key never leaks a stale
// count and the call never panics.
func TestInflightGaugeDecUnmatchedDrainsOldest(t *testing.T) {
	g := NewInflightGauge()
	t1 := time.Unix(1_700_000_000, 0)
	t2 := t1.Add(time.Second)
	g.Inc("n1", "f1", 5.0, t1)
	g.Inc("n1", "f1", 1.0, t2)

	// No match means that the oldest entry is removed, of cost five.
	g.Dec("n1", "f1", t2.Add(time.Hour))

	if g.FnK("n1", "f1") != 1 || g.NodeK("n1") != 1 {
		t.Fatalf("após Dec unmatched = node %d fn %d, want 1/1", g.NodeK("n1"), g.FnK("n1", "f1"))
	}
	if got := g.NodeCostSince("n1", time.Time{}); got != 1.0 {
		t.Fatalf("NodeCostSince=%v, want 1 (a mais antiga/custo 5 foi drenada)", got)
	}
	// Draining to empty deletes the key, so no empty slice is leaked.
	g.Dec("n1", "f1", t2)
	if g.FnK("n1", "f1") != 0 || g.NodeK("n1") != 0 || len(g.entries) != 0 {
		t.Fatalf("chave não zerou: node=%d fn=%d entries=%d", g.NodeK("n1"), g.FnK("n1", "f1"), len(g.entries))
	}
}

func TestInflightGaugeTwoFunctionIsolation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	g := NewInflightGauge()
	k1 := g.Inc("n1", "f1", 1, now)
	k2 := g.Inc("n1", "f2", 2, now)
	if k1 != 1 {
		t.Fatalf("k1=%d, want 1", k1)
	}
	if k2 != 2 {
		t.Fatalf("k2=%d, want 2 (NODE-level, não fn-level)", k2)
	}
	if g.FnK("n1", "f1") != 1 || g.FnK("n1", "f2") != 1 {
		t.Fatalf("FnK = (%d,%d), want (1,1): duas funções isoladas", g.FnK("n1", "f1"), g.FnK("n1", "f2"))
	}

	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Clock: fixedClock(now)})
	gw.WithInflight(g)
	logPath := t.TempDir() + "/inv.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gw.SetInvocationLog(l)

	// The entry recorded with the count captured at the dispatch carries the count of the node.
	gw.recordAttempt(nodes.Node{NodeID: "n1"}, time.Millisecond, nil, nil, "f2", "f2", nil, true, "", 0, attemptCapture{inflight: k2})
	recs := readInvRecords(t, logPath)
	if len(recs) != 1 {
		t.Fatalf("invLog = %d linhas, want 1", len(recs))
	}
	if got := recs[0].Entry.Inflight; got != g.NodeK("n1") || got != 2 {
		t.Fatalf("Entry.Inflight=%d, want NodeK=%d (2)", got, g.NodeK("n1"))
	}
}

func TestInflightGaugeNodeCostSinceMixedFunction(t *testing.T) {
	g := NewInflightGauge()
	t0 := time.Unix(1_700_000_000, 0)
	t1 := t0.Add(time.Second)
	t2 := t0.Add(2 * time.Second)
	g.Inc("n1", "f1", 1.0, t1)
	g.Inc("n1", "f2", 2.0, t2)
	g.Inc("n2", "f1", 5.0, t2)

	// A zero instant sums every live entry of the node in every function.
	if got := g.NodeCostSince("n1", time.Time{}); got != 3.0 {
		t.Fatalf("NodeCostSince(n1, zero)=%v, want 3 (f1+f2 mixed-function)", got)
	}
	// Reception-time filter: only entries strictly after the instant.
	if got := g.NodeCostSince("n1", t1); got != 2.0 {
		t.Fatalf("NodeCostSince(n1, t1)=%v, want 2 (só a entry de t2)", got)
	}
	if got := g.NodeCostSince("n1", t2); got != 0.0 {
		t.Fatalf("NodeCostSince(n1, t2)=%v, want 0 (não estritamente posterior)", got)
	}
	// Isolation per node: the prefix of the node identifier does not match a neighbor.
	if got := g.NodeCostSince("n2", t1); got != 5.0 {
		t.Fatalf("NodeCostSince(n2, t1)=%v, want 5", got)
	}
	if got := g.NodeCostSince("n1", t1); got == 0 || g.NodeCostSince("n2", t1) == 0 {
		t.Fatal("custos de nós distintos não podem se misturar")
	}
}

func TestInflightGaugeNilSafe(t *testing.T) {
	var g *InflightGauge
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("gauge nil panicou: %v", r)
		}
	}()
	if k := g.Inc("n", "f", 1, time.Now()); k != 0 {
		t.Fatalf("nil Inc=%d, want 0", k)
	}
	g.Dec("n", "f", time.Now())
	if g.NodeK("n") != 0 || g.FnK("n", "f") != 0 || g.NodeCostSince("n", time.Time{}) != 0 {
		t.Fatalf("nil accessors != 0")
	}
}

func TestHandleInflightGaugeBalanceParallel(t *testing.T) {
	const n = 8
	release := make(chan struct{})
	entered := make(chan struct{}, n)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"ok":true}}`))
	}))
	defer srv.Close()

	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	mon := monitor.NewMonitor()
	g := NewInflightGauge()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}}).WithInflight(g)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{}}`))
			gw.Handle(httptest.NewRecorder(), req)
		}()
	}
	for i := 0; i < n; i++ {
		<-entered
	}
	if got := g.NodeK("n1"); got != n {
		t.Fatalf("pico do gauge=%d, want %d (N Handles em voo)", got, n)
	}
	close(release)
	wg.Wait()
	if got := g.NodeK("n1"); got != 0 {
		t.Fatalf("gauge após drain=%d, want 0", got)
	}
	if len(g.node) != 0 || len(g.fn) != 0 || len(g.entries) != 0 {
		t.Fatalf("mapas vazaram: node=%d fn=%d entries=%d", len(g.node), len(g.fn), len(g.entries))
	}
}

func TestHandleProjectedOneInFlightNoDoubleCount(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"ok":true}}`))
	}))
	defer srv.Close()

	ns, _ := nodes.NewNodeStore("")
	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}
	if _, err := ns.Upsert(node); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	group := selection.ResolveRequestGroup("fn", []nodes.Node{node})
	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	mon := monitor.NewMonitor()
	// Observed telemetry, with a processor usage of thirty and a memory usage of twenty, received before the dispatch.
	mon.UpdateAt(telemetry.Package{NodeID: "n1", CPUUsagePct: 30, MemoryUsagePct: 20}, now.Add(-time.Second))

	g := NewInflightGauge()
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:            ns,
		Monitor:          mon,
		Selector:         selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		History:          h,
		Clock:            fixedClock(now),
		ThroughputWindow: time.Minute,
	}).WithInflight(g)
	logPath := t.TempDir() + "/inv.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gw.SetInvocationLog(l)

	// Seed a single solo sample with a known processor usage, so the expected
	// cost is the own cost. The telemetry block carries a positive pressure on
	// both axes, so the sample passes the solo gate, and without it the own cost
	// would be zero and the projection would be thirty.
	seed := json.RawMessage(`{"cold_start":false,"resource_profile":{"cpu_pct_avg":12.0,"sample_count":5},"telemetry":{"cpu_pressure":{"avg10":12.5},"memory_pressure_full":{"avg10":8.25}}}`)
	gw.recordAttempt(node, time.Millisecond, nil, nil, "fn", group, seed, true, "", 0, attemptCapture{sc: history.ScenarioContext{LoadBucket: history.BucketUnknown}, inflight: 1})

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200 body=%s", rec.Code, rec.Body.String())
	}

	recs := readInvRecords(t, logPath)
	if len(recs) != 2 {
		t.Fatalf("invLog=%d linhas, want 2 (seed + handle)", len(recs))
	}
	entry := recs[1].Entry

	// The own cost is twelve and the cost in flight is zero, so the projection is
	// thirty plus twelve, which is forty-two, with the own cost counted once.
	const wantProjected = 42.0
	if math.Abs(entry.ProjectedLoadPct-wantProjected) > 1e-9 {
		t.Fatalf("ProjectedLoadPct=%v, want %v (observed 30 + ownCost 12, sem dupla-contagem)", entry.ProjectedLoadPct, wantProjected)
	}
	if entry.Inflight != 1 {
		t.Fatalf("Inflight=%d, want 1", entry.Inflight)
	}
	expectedSc := history.ScenarioFromProjected(telemetry.Package{CPUUsagePct: 30, MemoryUsagePct: 20}, true, wantProjected)
	if entry.ScenarioContext != expectedSc {
		t.Fatalf("ScenarioContext=%+v, want %+v", entry.ScenarioContext, expectedSc)
	}
	key := history.SegmentKey(group, "n1", expectedSc)
	if _, _, _, _, _, ok := h.StatsAndRecent(key, time.Minute, now); !ok {
		t.Fatalf("segmento %q ausente: SegmentKey não bate com ScenarioFromProjected", key)
	}
	if g.NodeK("n1") != 0 {
		t.Fatalf("gauge após Handle=%d, want 0", g.NodeK("n1"))
	}
}

// panicWriter is a response writer whose write panics, which exercises the
// decrement through the deferred call inside the closure of the attempt. The
// panic happens after the increase and the recording of the attempt, so the
// meter has to drain anyway.
type panicWriter struct{}

func (panicWriter) Header() http.Header       { return http.Header{} }
func (panicWriter) WriteHeader(int)           {}
func (panicWriter) Write([]byte) (int, error) { panic("panicWriter: boom") }

func TestHandlePanicDecrementsGauge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"ok":true}}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	mon := monitor.NewMonitor()
	g := NewInflightGauge()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}}).WithInflight(g)

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("esperava panic do panicWriter")
			}
		}()
		req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{}}`))
		gw.Handle(panicWriter{}, req)
	}()

	if got := g.NodeK("n1"); got != 0 {
		t.Fatalf("gauge após panic=%d, want 0 (defer Dec dentro da closure)", got)
	}
}

func TestHandleErrorPathsDrainGauge(t *testing.T) {
	cases := []struct {
		name       string
		host       string
		serverCode int
		hijack     bool
		budget     time.Duration
		slow       bool
	}{
		{name: "newrequest-error", host: "bad host"},
		{name: "transport-error", hijack: true},
		{name: "status-503", serverCode: http.StatusServiceUnavailable},
		{name: "budget-cancel", serverCode: http.StatusOK, budget: 30 * time.Millisecond, slow: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host := tc.host
			if tc.serverCode != 0 || tc.hijack {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if tc.hijack {
						// Close the connection without an HTTP answer, which is a
						// deterministic and fast transport error, without depending on a
						// refused port or on the name resolution of the environment.
						hj, ok := w.(http.Hijacker)
						if !ok {
							return
						}
						conn, _, err := hj.Hijack()
						if err == nil {
							_ = conn.Close()
						}
						return
					}
					if tc.slow {
						time.Sleep(150 * time.Millisecond)
					}
					w.WriteHeader(tc.serverCode)
					_, _ = w.Write([]byte(`{"result":{"ok":true}}`))
				}))
				defer srv.Close()
				host = hostFromURL(t, srv.URL)
			}
			ns, _ := nodes.NewNodeStore("")
			if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: host}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			mon := monitor.NewMonitor()
			g := NewInflightGauge()
			gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, DefaultBudget: tc.budget}).WithInflight(g)

			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{}}`))
			gw.Handle(httptest.NewRecorder(), req)
			if got := g.NodeK("n1"); got != 0 {
				t.Fatalf("gauge após %s=%d, want 0", tc.name, got)
			}
			if len(g.node) != 0 || len(g.fn) != 0 || len(g.entries) != 0 {
				t.Fatalf("mapas vazaram após %s: node=%d fn=%d entries=%d", tc.name, len(g.node), len(g.fn), len(g.entries))
			}
		})
	}
}

// TestLateSupersedePreservesCPUPctAvg proves that the late supersede preserves
// the load and the cost captured at the dispatch: the average processor usage
// from the resource profile, the requests in flight and the projected load all
// survive.
func TestLateSupersedePreservesCPUPctAvg(t *testing.T) {
	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	mon := monitor.NewMonitor()
	gw := lateTestGateway(h, mon, nil)
	logPath := t.TempDir() + "/inv.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gw.SetInvocationLog(l)

	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	gw.RegisterLateGroup("req-cpu", "f", slo)
	metrics := json.RawMessage(`{"cold_start":false,"resource_profile":{"cpu_pct_avg":37.5,"sample_count":9}}`)
	sc := history.ScenarioContext{LoadBucket: history.BucketUnknown}
	gw.recordAttempt(node, 50*time.Millisecond, nil, slo, "f", "f", metrics, true, "req-cpu", 1, attemptCapture{sc: sc, inflight: 1, projected: 42})

	rep := LateReport{
		RequestID:         "req-cpu",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 3200,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
		CPUUsagePct:       f64ptr(80),
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}

	key := history.SegmentKey("f", "n1", sc)
	fs, ok := h.StatsFull(key, time.Minute, time.Now())
	if !ok || fs.Count != 1 || fs.P95MS != 3200 {
		t.Fatalf("segmento síncrono = (count=%d,p95=%d,ok=%v), want (1,3200,true): late supersede 1x", fs.Count, fs.P95MS, ok)
	}
	recs := readInvRecords(t, logPath)
	last := recs[len(recs)-1].Entry
	if math.Abs(last.CPUPctAvg-37.5) > 1e-9 || last.SampleCount != 9 {
		t.Fatalf("late CPUPctAvg/SampleCount=(%v,%d), want (37.5,9) preservados do sync", last.CPUPctAvg, last.SampleCount)
	}
	if last.Inflight != 1 || math.Abs(last.ProjectedLoadPct-42) > 1e-9 {
		t.Fatalf("late Inflight/Projected=(%d,%v), want (1,42) preservados do sync", last.Inflight, last.ProjectedLoadPct)
	}
}
