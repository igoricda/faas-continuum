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
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// queueFakeSelector returns the queue verbatim and, when a prediction is given,
// implements the latency predictor with a fixed prediction per node. It serves
// the tests of the per-attempt budget. The request predictor, when given,
// implements the prediction of a new request, which is the class-aware
// prediction of the gate, and a nil value makes the gateway fall back to the
// prediction of the budget. The queue ceiling, when given, implements the
// learned ceiling per node, and a nil value makes the gate use only the
// projection of the predicted load against the concurrency.
type queueFakeSelector struct {
	queue      []nodes.Node
	predicted  map[string]int64
	newRequest map[string]int64
	queueCap   map[string]int
}

func (f queueFakeSelector) Select(_ []nodes.Node, _ string, _ *selection.SLO) []nodes.Node {
	return f.queue
}

func (f queueFakeSelector) PredictLatencyMS(_ string, n nodes.Node, _ *selection.SLO) (int64, bool) {
	if f.predicted == nil {
		return 0, false
	}
	ms, ok := f.predicted[n.NodeID]
	return ms, ok
}

func (f queueFakeSelector) PredictLatencyMSNewRequest(_ string, n nodes.Node, _ *selection.SLO) (int64, bool) {
	if f.newRequest == nil {
		return 0, false
	}
	ms, ok := f.newRequest[n.NodeID]
	return ms, ok
}

func (f queueFakeSelector) PredictMaxQueueK(_ string, n nodes.Node, _ *selection.SLO) (int, bool) {
	if f.queueCap == nil {
		return 0, false
	}
	k, ok := f.queueCap[n.NodeID]
	return k, ok
}

func TestAttemptSlice(t *testing.T) {
	cases := []struct {
		name         string
		remaining    time.Duration
		attemptsLeft int
		predicted    time.Duration
		hasPredicted bool
		reserve      time.Duration
		want         time.Duration
	}{
		{name: "sem orcamento", remaining: 0, attemptsLeft: 2, want: 0},
		{name: "ultima tentativa fica com tudo", remaining: 800 * time.Millisecond, attemptsLeft: 1, want: 800 * time.Millisecond},
		{name: "divisao igual sem previsao", remaining: 800 * time.Millisecond, attemptsLeft: 2, want: 400 * time.Millisecond},
		// Without a prediction, up to half of the remaining budget, and not the quota.
		{name: "metade do restante sem previsao", remaining: 3 * time.Second, attemptsLeft: 3, want: 1500 * time.Millisecond},
		{name: "d4: slo de 5s com 5 candidatos sem previsao", remaining: 5 * time.Second, attemptsLeft: 5, want: 2500 * time.Millisecond},
		{name: "metade limitada pelo teto", remaining: 1600 * time.Millisecond, attemptsLeft: 4, want: 800 * time.Millisecond},
		{name: "metade com reserva de nuvem", remaining: 5 * time.Second, attemptsLeft: 5, reserve: 2 * time.Second, want: 2 * time.Second},
		{name: "reserva corta a metade", remaining: 3 * time.Second, attemptsLeft: 4, reserve: time.Second, want: 1250 * time.Millisecond},
		{name: "piso com previsao baixa", remaining: 800 * time.Millisecond, attemptsLeft: 2, predicted: 100 * time.Millisecond, hasPredicted: true, want: minAttemptSlice},
		{name: "duas vezes a previsao", remaining: 800 * time.Millisecond, attemptsLeft: 2, predicted: 150 * time.Millisecond, hasPredicted: true, want: 300 * time.Millisecond},
		{name: "reserva o piso da proxima", remaining: 800 * time.Millisecond, attemptsLeft: 2, predicted: 500 * time.Millisecond, hasPredicted: true, want: 550 * time.Millisecond},
		{name: "orcamento apertado divide igual", remaining: 100 * time.Millisecond, attemptsLeft: 2, want: 50 * time.Millisecond},
		{name: "tres com previsao", remaining: 5 * time.Second, attemptsLeft: 3, predicted: time.Second, hasPredicted: true, want: 2 * time.Second},
		// Reservation of the cloud.
		{name: "ultima com reserva de nuvem", remaining: 5 * time.Second, attemptsLeft: 1, reserve: 2 * time.Second, want: 3 * time.Second},
		{name: "ultima com reserva limitada a metade", remaining: 3 * time.Second, attemptsLeft: 1, reserve: 1500 * time.Millisecond, want: 1500 * time.Millisecond},
		{name: "ultima apertada ignora reserva", remaining: 300 * time.Millisecond, attemptsLeft: 1, reserve: 150 * time.Millisecond, want: 300 * time.Millisecond},
		{name: "reserva limita previsao longa", remaining: 5 * time.Second, attemptsLeft: 2, predicted: 5 * time.Second, hasPredicted: true, reserve: 2 * time.Second, want: 2750 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := attemptSlice(tc.remaining, tc.attemptsLeft, tc.predicted, tc.hasPredicted, tc.reserve)
			if got != tc.want {
				t.Fatalf("attemptSlice(%v, %d, %v, %v, %v) = %v; want %v",
					tc.remaining, tc.attemptsLeft, tc.predicted, tc.hasPredicted, tc.reserve, got, tc.want)
			}
		})
	}
}

func TestCloudReserve(t *testing.T) {
	cases := []struct {
		name      string
		remaining time.Duration
		hasCloud  bool
		want      time.Duration
	}{
		{"sem nuvem", 5 * time.Second, false, 0},
		{"teto de 2s", 5 * time.Second, true, 2 * time.Second},
		{"metade quando apertado", time.Second, true, 500 * time.Millisecond},
		{"sem orcamento", 0, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := cloudReserve(tc.remaining, tc.hasCloud); got != tc.want {
				t.Fatalf("cloudReserve(%v, %v) = %v; want %v", tc.remaining, tc.hasCloud, got, tc.want)
			}
		})
	}
}

// TestAttemptViable pins the gate of viability: the ceiling is the declared
// objective, so a prediction above the slice but within the objective passes,
// while a prediction above the objective is skipped even with a loose slice.
// Without an objective of latency the ceiling is the budget per node and
// function, limited to the remaining part of the invocation, since the slice is
// a window of wait and does not admit a request, and using it would degenerate
// the concurrency to two. Without a prediction and in the last candidate the
// gate fails open.
func TestAttemptViable(t *testing.T) {
	cases := []struct {
		name         string
		teto         time.Duration
		predicted    time.Duration
		hasPredicted bool
		slo          *selection.SLO
		k            int
		isLast       bool
		want         bool
	}{
		{"sem previsao passa", time.Second, 0, false, nil, 1, false, true},
		{"previsao zero com flag passa", time.Second, 0, true, nil, 1, false, true},
		{"previsao abaixo do teto passa", time.Second, 800 * time.Millisecond, true, nil, 1, false, true},
		{"previsao igual ao teto passa", time.Second, time.Second, true, nil, 1, false, true},
		{"sem slo: previsao acima do teto e pulada", 300 * time.Millisecond, 1500 * time.Millisecond, true, nil, 1, false, false},
		{"slo vazio mantem o criterio do teto", 300 * time.Millisecond, 1500 * time.Millisecond, true, &selection.SLO{}, 1, false, false},
		// The ceiling is the objective, which sizes the wait and not the admission.
		{"teto slo: previsto entre a fatia e o slo passa", 2720 * time.Millisecond, 3129 * time.Millisecond, true, &selection.SLO{MaxLatencyMS: 5000}, 1, false, true},
		{"teto slo: previsto igual ao slo passa", 300 * time.Millisecond, 5000 * time.Millisecond, true, &selection.SLO{MaxLatencyMS: 5000}, 1, false, true},
		{"teto slo: previsto acima do slo e pulada com fatia folgada", 10 * time.Second, 5001 * time.Millisecond, true, &selection.SLO{MaxLatencyMS: 5000}, 1, false, false},
		{"ultimo candidato e sempre tentado", 300 * time.Millisecond, 1500 * time.Millisecond, true, nil, 1, true, true},
		{"ultimo sem previsao tambem passa", 300 * time.Millisecond, 0, false, nil, 1, true, true},
		// Admission by load: with at least two requests in flight the queue enters the prediction.
		{"carga k=2: previsto entre a fatia e o slo e pulada", 2720 * time.Millisecond, 3129 * time.Millisecond, true, &selection.SLO{MaxLatencyMS: 5000}, 2, false, false},
		{"carga k=2: ultimo candidato tambem e pulado", 2720 * time.Millisecond, 3129 * time.Millisecond, true, &selection.SLO{MaxLatencyMS: 5000}, 2, true, false},
		{"carga k=2 com folga no slo passa", time.Second, time.Second, true, &selection.SLO{MaxLatencyMS: 5000}, 2, false, true},
		// Without an objective, the ceiling is the budget per node and function, and never the slice.
		{"carga sem slo: projecao acima do orcamento e pulada", 3 * time.Second, 2 * time.Second, true, nil, 2, false, false},
		{"carga sem slo: projecao dentro do orcamento passa", 5 * time.Second, 2 * time.Second, true, nil, 2, false, true},
		{"regressao arb9: borda 45s admite 443ms com k=3", 45 * time.Second, 443 * time.Millisecond, true, nil, 3, false, true},
		{"regressao arb9: nevoa 15s admite fila de 3x443ms", 15 * time.Second, 443 * time.Millisecond, true, nil, 3, false, true},
		{"sem slo: projecao acima do teto do no e pulada", 15 * time.Second, 6 * time.Second, true, nil, 3, false, false},
		{"carga k=0 e tratada como ocioso", time.Second, time.Second, true, &selection.SLO{MaxLatencyMS: 5000}, 0, false, true},
		{"carga sem previsao passa", 300 * time.Millisecond, 0, false, &selection.SLO{MaxLatencyMS: 5000}, 5, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := attemptViable(tc.teto, tc.predicted, tc.hasPredicted, tc.slo, tc.k, tc.isLast); got != tc.want {
				t.Fatalf("attemptViable(%v, %v, %v, %+v, %d, %v) = %v; want %v",
					tc.teto, tc.predicted, tc.hasPredicted, tc.slo, tc.k, tc.isLast, got, tc.want)
			}
		})
	}
}

// TestGateway_AttemptBudgetPortaoDeViabilidade covers the effect of the gate on
// the dispatch: the first candidate has a percentile above the slice it would
// receive and is skipped without being called, while the second runs with a
// larger slice and answers. Without the gate the first one burned the slice and
// became an exceeded budget, which is an artifact of the arbiter, before the
// failover.
func TestGateway_AttemptBudgetPortaoDeViabilidade(t *testing.T) {
	var aCalls, bCalls, cCalls int32
	unviable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&aCalls, 1)
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte(`{"result":"a"}`))
	}))
	defer unviable.Close()
	fastB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&bCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"b"}`))
	}))
	defer fastB.Close()
	neverC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&cCalls, 1)
		_, _ = w.Write([]byte(`{"result":"c"}`))
	}))
	defer neverC.Close()

	ns, _ := nodes.NewNodeStore("")
	nodeA := nodes.Node{NodeID: "a", Layer: nodes.LayerBorda, Host: hostFromURL(t, unviable.URL), Functions: []string{"f"}}
	nodeB := nodes.Node{NodeID: "b", Layer: nodes.LayerNevoa, Host: hostFromURL(t, fastB.URL), Functions: []string{"f"}}
	nodeC := nodes.Node{NodeID: "c", Layer: nodes.LayerNevoa, Host: hostFromURL(t, neverC.URL), Functions: []string{"f"}}
	_, _ = ns.Upsert(nodeA)
	_, _ = ns.Upsert(nodeB)
	_, _ = ns.Upsert(nodeC)

	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:   ns,
		Monitor: monitor.NewMonitor(),
		Selector: queueFakeSelector{
			queue:     []nodes.Node{nodeA, nodeB, nodeC},
			predicted: map[string]int64{"a": 1500, "b": 50, "c": 50},
		},
		DefaultBudget: 800 * time.Millisecond,
	})
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"f","slo":{"max_latency_ms":800},"input":{}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"b"`) {
		t.Fatalf("esperado corpo do nó b, got %q", rec.Body.String())
	}
	if got := atomic.LoadInt32(&aCalls); got != 0 {
		t.Fatalf("nó a chamado %d vez(es): o portão não pulou a fatia inviável", got)
	}
	if got := atomic.LoadInt32(&bCalls); got != 1 {
		t.Fatalf("nó b chamado %d vez(es), want 1", got)
	}
	if got := atomic.LoadInt32(&cCalls); got != 0 {
		t.Fatalf("nó c chamado %d vez(es), want 0", got)
	}
	if got := gw.UnviableSkips(); got != 1 {
		t.Fatalf("UnviableSkips=%d, want 1", got)
	}
}

// TestGateway_PortaoTetoSLO is the regression of the validation with a five
// second objective: the first candidate has a percentile of the segment of about
// three seconds, which is above the slice of two and a half seconds it would
// receive with the cloud reservation and below the objective. The ceiling is the
// objective, so the node runs and answers. Previously the gate skipped it because
// the prediction exceeded the slice and the requests went to the cloud with the
// edge idle. The second case pins the estimator of the gate, which uses the
// class-aware prediction of an idle node and not the mixed percentile.
func TestGateway_PortaoTetoSLO(t *testing.T) {
	cases := []struct {
		name     string
		segmento map[string]int64 // prediction of the budget, which sizes the slice
		classe   map[string]int64 // prediction of the admission, which judges the ceiling
	}{
		{
			name:     "previsto entre a fatia e o slo e tentado",
			segmento: map[string]int64{"a": 3129, "b": 50},
			classe:   map[string]int64{"a": 3129, "b": 50},
		},
		{
			name:     "portao usa a previsao classe-aware",
			segmento: map[string]int64{"a": 5422, "b": 50},
			classe:   map[string]int64{"a": 2288, "b": 50},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var aCalls, bCalls, cloudCalls int32
			a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&aCalls, 1)
				time.Sleep(300 * time.Millisecond)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"result":"a"}`))
			}))
			defer a.Close()
			b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&bCalls, 1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"result":"b"}`))
			}))
			defer b.Close()
			cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&cloudCalls, 1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"result":"cloud"}`))
			}))
			defer cloudSrv.Close()

			ns, _ := nodes.NewNodeStore("")
			nodeA := nodes.Node{NodeID: "a", Layer: nodes.LayerBorda, Host: hostFromURL(t, a.URL), Functions: []string{"f"}}
			nodeB := nodes.Node{NodeID: "b", Layer: nodes.LayerNevoa, Host: hostFromURL(t, b.URL), Functions: []string{"f"}}
			_, _ = ns.Upsert(nodeA)
			_, _ = ns.Upsert(nodeB)

			gw := NewGatewayWithHistory(GatewayConfig{
				Nodes:   ns,
				Monitor: monitor.NewMonitor(),
				Selector: queueFakeSelector{
					queue:      []nodes.Node{nodeA, nodeB},
					predicted:  tc.segmento,
					newRequest: tc.classe,
				},
				// A configured cloud shrinks the slice of the first candidate to
				// about two and a half seconds, below the prediction of three.
				RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
				DefaultBudget:  5 * time.Second,
			})
			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"f","slo":{"max_latency_ms":5000},"input":{}}`))
			rec := httptest.NewRecorder()
			gw.Handle(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d want 200; body=%s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"a"`) {
				t.Fatalf("esperado corpo do nó a, got %q", rec.Body.String())
			}
			if got := atomic.LoadInt32(&aCalls); got != 1 {
				t.Fatalf("nó a chamado %d vez(es), want 1 (o portão não pode pular nó dentro do SLO)", got)
			}
			if got := atomic.LoadInt32(&bCalls); got != 0 {
				t.Fatalf("nó b chamado %d vez(es), want 0", got)
			}
			if got := atomic.LoadInt32(&cloudCalls); got != 0 {
				t.Fatalf("nuvem chamada %d vez(es), want 0", got)
			}
			if got := gw.UnviableSkips(); got != 0 {
				t.Fatalf("UnviableSkips=%d, want 0", got)
			}
		})
	}
}

// TestGateway_PortaoAdmissaoPorCarga covers the effect of the admission by load
// on the dispatch: with the node already in flight, at two requests on the
// second call, the gate skips it even within the declared objective and serves
// through the second node, since previously the burst piled up on the same node
// up to its ceiling. The first call proves that a single request in flight
// preserves the previous behavior, where an idle node is attempted even with a
// slice smaller than the prediction.
func TestGateway_PortaoAdmissaoPorCarga(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var aCalls, bCalls, cloudCalls int32
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&aCalls, 1)
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"a"}`))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&bCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"b"}`))
	}))
	defer b.Close()
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&cloudCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"cloud"}`))
	}))
	defer cloudSrv.Close()

	ns, _ := nodes.NewNodeStore("")
	nodeA := nodes.Node{NodeID: "a", Layer: nodes.LayerBorda, Host: hostFromURL(t, a.URL), Functions: []string{"f"}}
	nodeB := nodes.Node{NodeID: "b", Layer: nodes.LayerNevoa, Host: hostFromURL(t, b.URL), Functions: []string{"f"}}
	_, _ = ns.Upsert(nodeA)
	_, _ = ns.Upsert(nodeB)

	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:   ns,
		Monitor: monitor.NewMonitor(),
		Selector: queueFakeSelector{
			queue:      []nodes.Node{nodeA, nodeB},
			predicted:  map[string]int64{"a": 3129, "b": 50},
			newRequest: map[string]int64{"a": 3129, "b": 50},
		},
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
		DefaultBudget:  5 * time.Second,
	}).WithInflight(NewInflightGauge())
	invoke := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"f","slo":{"max_latency_ms":5000},"input":{}}`))
		rec := httptest.NewRecorder()
		gw.Handle(rec, req)
		return rec
	}

	// First call: the node is idle, with one request in flight, so it is attempted and kept in flight by the handler.
	var rec1 *httptest.ResponseRecorder
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		rec1 = invoke()
	}()
	<-started // the increase of the first node is done, and the decrement happens only after the release

	// Second call with the first node in flight: the projection of twice the
	// prediction exceeds the objective, so it is skipped and the second node serves.
	rec2 := invoke()
	close(release)
	<-done1

	if rec1 == nil || rec1.Code != http.StatusOK || !strings.Contains(rec1.Body.String(), `"a"`) {
		t.Fatalf("1ª chamada: want 200 com corpo de a; rec=%v", rec1)
	}
	if rec2.Code != http.StatusOK || !strings.Contains(rec2.Body.String(), `"b"`) {
		t.Fatalf("2ª chamada: status=%d body=%q want 200 com o corpo de b (a pulado por carga)", rec2.Code, rec2.Body.String())
	}
	if got := atomic.LoadInt32(&aCalls); got != 1 {
		t.Fatalf("nó a chamado %d vez(es), want 1 (a 2ª não pode empilhar)", got)
	}
	if got := atomic.LoadInt32(&bCalls); got != 1 {
		t.Fatalf("nó b chamado %d vez(es), want 1", got)
	}
	if got := atomic.LoadInt32(&cloudCalls); got != 0 {
		t.Fatalf("nuvem chamada %d vez(es), want 0", got)
	}
	if got := gw.UnviableSkips(); got != 1 {
		t.Fatalf("UnviableSkips=%d, want 1 (a pulada por carga)", got)
	}
}

// TestGateway_AttemptBudgetLiberaFallback is the central test of the change: the
// first attempt, on the slow edge, exceeds the slice, with a floor of two hundred
// and fifty milliseconds and a prediction of one hundred, and the overall budget
// stays alive, so the second attempt, on the fast fog, runs and answers. Before
// the change the single context had already expired and the fog died cancelled
// without being attempted.
func TestGateway_AttemptBudgetLiberaFallback(t *testing.T) {
	var slowCalls, fastCalls int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&slowCalls, 1)
		time.Sleep(1 * time.Second)
		_, _ = w.Write([]byte(`{"result":"slow"}`))
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fastCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"fast"}`))
	}))
	defer fast.Close()

	ns, _ := nodes.NewNodeStore("")
	slowNode := nodes.Node{NodeID: "slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slow.URL), Functions: []string{"f"}}
	fastNode := nodes.Node{NodeID: "fast", Layer: nodes.LayerNevoa, Host: hostFromURL(t, fast.URL), Functions: []string{"f"}}
	_, _ = ns.Upsert(slowNode)
	_, _ = ns.Upsert(fastNode)

	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:   ns,
		Monitor: monitor.NewMonitor(),
		Selector: queueFakeSelector{
			queue:     []nodes.Node{slowNode, fastNode},
			predicted: map[string]int64{"slow": 100, "fast": 100},
		},
		DefaultBudget: 800 * time.Millisecond,
	})
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"f","slo":{"max_latency_ms":800},"input":{}}`))
	rec := httptest.NewRecorder()
	start := time.Now()
	gw.Handle(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "fast") {
		t.Fatalf("esperado corpo da névoa, got %q", rec.Body.String())
	}
	if atomic.LoadInt32(&slowCalls) != 1 || atomic.LoadInt32(&fastCalls) != 1 {
		t.Fatalf("chamadas slow=%d fast=%d; want 1/1", slowCalls, fastCalls)
	}
	if elapsed > 700*time.Millisecond {
		t.Fatalf("elapsed %v > 700ms (fatia 250ms + névoa imediata)", elapsed)
	}
	if attempts, _, _, _ := gw.Counters(); attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

// TestGateway_AttemptBudgetDivisaoIgual covers the path without a prediction, in
// the baseline of the hard limits: the slice of four hundred milliseconds out of
// eight hundred, with two candidates, coincides with the equal quota and releases
// the second attempt in the same way.
func TestGateway_AttemptBudgetDivisaoIgual(t *testing.T) {
	var slowCalls, fastCalls int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&slowCalls, 1)
		time.Sleep(1 * time.Second)
		_, _ = w.Write([]byte(`{"result":"slow"}`))
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fastCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"fast"}`))
	}))
	defer fast.Close()

	ns, _ := nodes.NewNodeStore("")
	slowNode := nodes.Node{NodeID: "slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slow.URL), Functions: []string{"f"}}
	fastNode := nodes.Node{NodeID: "fast", Layer: nodes.LayerNevoa, Host: hostFromURL(t, fast.URL), Functions: []string{"f"}}
	_, _ = ns.Upsert(slowNode)
	_, _ = ns.Upsert(fastNode)

	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:         ns,
		Monitor:       monitor.NewMonitor(),
		Selector:      queueFakeSelector{queue: []nodes.Node{slowNode, fastNode}},
		DefaultBudget: 800 * time.Millisecond,
	})
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"f","slo":{"max_latency_ms":800},"input":{}}`))
	rec := httptest.NewRecorder()
	start := time.Now()
	gw.Handle(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "fast") {
		t.Fatalf("esperado corpo da névoa, got %q", rec.Body.String())
	}
	if atomic.LoadInt32(&slowCalls) != 1 || atomic.LoadInt32(&fastCalls) != 1 {
		t.Fatalf("chamadas slow=%d fast=%d; want 1/1", slowCalls, fastCalls)
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("elapsed %v > 800ms (fatia 400ms + névoa imediata)", elapsed)
	}
}

// TestGateway_AttemptBudgetReservaNuvem anchors the reservation of the cloud:
// with a cloud configured, the last local attempt reserves up to two seconds,
// which is the real percentile of a warm fallback, and the slow node does not
// consume the whole budget. Previously the overflow of the last attempt zeroed
// the remaining budget and the client received a timeout without the cloud being
// attempted.
func TestGateway_AttemptBudgetReservaNuvem(t *testing.T) {
	var slowCalls, cloudCalls int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&slowCalls, 1)
		time.Sleep(4 * time.Second)
		_, _ = w.Write([]byte(`{"result":"slow"}`))
	}))
	defer slow.Close()
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&cloudCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"cloud"}`))
	}))
	defer cloudSrv.Close()

	ns, _ := nodes.NewNodeStore("")
	slowNode := nodes.Node{NodeID: "slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slow.URL), Functions: []string{"f"}}
	_, _ = ns.Upsert(slowNode)

	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:          ns,
		Monitor:        monitor.NewMonitor(),
		Selector:       queueFakeSelector{queue: []nodes.Node{slowNode}, predicted: map[string]int64{"slow": 100}},
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
		DefaultBudget:  5 * time.Second,
	})
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"f","slo":{"max_latency_ms":5000},"input":{}}`))
	rec := httptest.NewRecorder()
	start := time.Now()
	gw.Handle(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "cloud") {
		t.Fatalf("esperado corpo da nuvem, got %q", rec.Body.String())
	}
	if atomic.LoadInt32(&slowCalls) != 1 || atomic.LoadInt32(&cloudCalls) != 1 {
		t.Fatalf("chamadas slow=%d cloud=%d; want 1/1", slowCalls, cloudCalls)
	}
	if elapsed > 4200*time.Millisecond {
		t.Fatalf("elapsed %v > 4,2s (fatia local 3s + nuvem imediata)", elapsed)
	}
}

// TestGateway_AttemptBudgetNuvemAlcancavel guarantees that the cloud fallback
// keeps seeing the rest of the overall budget: the edge exceeds the slice, the
// fog answers a fast rejection and the cloud answers, which was impossible
// before the change, when the single context died at the edge and the cloud saw
// a non-positive remaining budget.
func TestGateway_AttemptBudgetNuvemAlcancavel(t *testing.T) {
	var slowCalls, cloudCalls int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&slowCalls, 1)
		time.Sleep(1 * time.Second)
		_, _ = w.Write([]byte(`{"result":"slow"}`))
	}))
	defer slow.Close()
	overloaded := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"POOL_EXHAUSTED"}`))
	}))
	defer overloaded.Close()
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&cloudCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"cloud"}`))
	}))
	defer cloudSrv.Close()

	ns, _ := nodes.NewNodeStore("")
	slowNode := nodes.Node{NodeID: "slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slow.URL), Functions: []string{"f"}}
	fogNode := nodes.Node{NodeID: "fog", Layer: nodes.LayerNevoa, Host: hostFromURL(t, overloaded.URL), Functions: []string{"f"}}
	_, _ = ns.Upsert(slowNode)
	_, _ = ns.Upsert(fogNode)

	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:   ns,
		Monitor: monitor.NewMonitor(),
		Selector: queueFakeSelector{
			queue:     []nodes.Node{slowNode, fogNode},
			predicted: map[string]int64{"slow": 100, "fog": 100},
		},
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
		DefaultBudget:  2 * time.Second,
	})
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"f","slo":{"max_latency_ms":2000},"input":{}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "cloud") {
		t.Fatalf("esperado corpo da nuvem, got %q", rec.Body.String())
	}
	if atomic.LoadInt32(&slowCalls) != 1 {
		t.Fatalf("slow calls=%d want 1", slowCalls)
	}
	if atomic.LoadInt32(&cloudCalls) != 1 {
		t.Fatalf("cloud calls=%d want 1", cloudCalls)
	}
}
