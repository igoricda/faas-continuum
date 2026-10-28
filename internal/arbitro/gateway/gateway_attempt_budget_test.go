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

// queueFakeSelector devolve a fila verbatim e, quando predicted != nil,
// implementa LatencyPredictor com previsão fixa por NodeID. Serve para os
// testes da emenda de orçamento por tentativa (AUD-22/AUD-26, 2026-09-24).
// newRequest, quando não-nil, implementa também NewRequestPredictor (a
// previsão classe-aware do portão); nil devolve ok=false e o gateway cai na
// previsão do orçamento. queueCap, quando não-nil, implementa
// QueueCapPredictor (o teto de fila aprendido por NodeID); nil devolve
// ok=false e o portão usa só a projeção previsto×k.
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
		// D4 (2026-09-30): sem previsão, até METADE do restante (não a cota).
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
		// Emenda pós-simulação: reserva da nuvem.
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

// TestAttemptViable fixa o portão de viabilidade: o TETO é o SLO declarado
// (2026-10-01) — previsão acima da fatia mas dentro do SLO passa; acima do SLO
// é pulada mesmo com fatia folgada. Sem SLO de latência, o teto é o orçamento
// por nó/função (function_timeout_ms, limitado ao restante do invoke): a fatia
// é janela de espera e não admite (usá-la degenerava em k≤2 — regressão da
// rampa arb9 a 0,1 s); sem previsão e no último candidato, fail-open.
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
		// Teto = SLO (campo 2026-10-01): a fatia dimensiona a espera, não admite.
		{"teto slo: previsto entre a fatia e o slo passa", 2720 * time.Millisecond, 3129 * time.Millisecond, true, &selection.SLO{MaxLatencyMS: 5000}, 1, false, true},
		{"teto slo: previsto igual ao slo passa", 300 * time.Millisecond, 5000 * time.Millisecond, true, &selection.SLO{MaxLatencyMS: 5000}, 1, false, true},
		{"teto slo: previsto acima do slo e pulada com fatia folgada", 10 * time.Second, 5001 * time.Millisecond, true, &selection.SLO{MaxLatencyMS: 5000}, 1, false, false},
		{"ultimo candidato e sempre tentado", 300 * time.Millisecond, 1500 * time.Millisecond, true, nil, 1, true, true},
		{"ultimo sem previsao tambem passa", 300 * time.Millisecond, 0, false, nil, 1, true, true},
		// Admissão por carga (2026-10-01/2): k≥2 projeta a fila na previsão.
		{"carga k=2: previsto entre a fatia e o slo e pulada", 2720 * time.Millisecond, 3129 * time.Millisecond, true, &selection.SLO{MaxLatencyMS: 5000}, 2, false, false},
		{"carga k=2: ultimo candidato tambem e pulado", 2720 * time.Millisecond, 3129 * time.Millisecond, true, &selection.SLO{MaxLatencyMS: 5000}, 2, true, false},
		{"carga k=2 com folga no slo passa", time.Second, time.Second, true, &selection.SLO{MaxLatencyMS: 5000}, 2, false, true},
		// Sem SLO: teto = orçamento por nó/função (nunca a fatia).
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

// TestGateway_AttemptBudgetPortaoDeViabilidade é o efeito do portão no
// dispatch: o primeiro candidato tem p95 (1500 ms) acima da fatia que
// receberia (300 ms) e é PULADO sem ser chamado; o segundo (p95 50 ms) roda
// com fatia maior e responde. Antes do portão, o primeiro queimava a fatia e
// virava um `orcamento_excedido` (artefato do árbitro) antes do failover.
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

// TestGateway_PortaoTetoSLO é a regressão da validação da TG3 (2026-10-01,
// SLO 5 s): o primeiro candidato tem p95 do segmento 3.129 ms — acima da
// fatia de 2.750 ms que ele receberia com a reserva de nuvem de 2 s, e ABAIXO
// do SLO de 5.000 ms. O teto é o SLO: o nó roda e responde. Antes, o portão
// pulava por `previsto > fatia` e as 51 requisições iam para a nuvem com a
// borda ociosa (23 acima do SLO). O segundo caso fixa o estimador do portão:
// vale a previsão classe-aware (solo em nó ocioso), não o p95 misto.
func TestGateway_PortaoTetoSLO(t *testing.T) {
	cases := []struct {
		name     string
		segmento map[string]int64 // previsão do orçamento (dimensiona a fatia)
		classe   map[string]int64 // previsão da admissão (julga o teto)
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
				// Nuvem CONFIGURADA: a reserva de 2 s encolhe a fatia do 1º
				// candidato para ~2.750 ms — abaixo da previsão de 3.129 ms.
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

// TestGateway_PortaoAdmissaoPorCarga é o efeito da admissão por carga no
// dispatch (2026-10-01/2): com o nó "a" JÁ em voo (k=2 na segunda chamada), o
// portão pula "a" mesmo dentro do SLO declarado e serve por "b" — antes, a
// rajada empilhava no mesmo nó (34 tentativas perdedoras do xp4 na bateria
// arb8) até o cap do nó. A primeira chamada prova que k≤1 preserva o
// comportamento de 2026-10-01 (nó ocioso é tentado, mesmo com a fatia menor
// que a previsão).
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

	// 1ª chamada: "a" ocioso (k=1) → tentado e mantido em voo pelo handler.
	var rec1 *httptest.ResponseRecorder
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		rec1 = invoke()
	}()
	<-started // Inc de "a" feito; Dec só depois do release.

	// 2ª chamada com "a" em voo (k=2): projeção 3.129×2 > SLO 5.000 → pulado;
	// "b" (k=1, p95 50 ms) atende.
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

// TestGateway_AttemptBudgetLiberaFallback é o teste central da emenda: a
// tentativa 1 (borda, lenta) estoura a FATIA (piso de 250 ms com previsão de
// 100 ms) e o orçamento overall segue vivo — a tentativa 2 (névoa, rápida) é
// executada e responde 200. Antes da emenda, o ctx único já estava expirado e
// a névoa morria em CONTEXT_CANCELED sem ser tentada.
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

// TestGateway_AttemptBudgetDivisaoIgual cobre o caminho SEM previsão
// (QueueSelector baseline): a fatia de 400 ms de 800 ms (com 2 candidatos,
// metade do restante coincide com a cota igualitária) libera a segunda
// tentativa do mesmo jeito.
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

// TestGateway_AttemptBudgetReservaNuvem ancora a emenda pós-simulação: com
// nuvem CONFIGURADA, a última tentativa local reserva até 2 s (p95 real do
// fallback warm = 1,9 s) e o nó lento NÃO consome o orçamento inteiro —
// antes, o estouro da última tentativa zerava o remaining e o cliente
// recebia 504 sem a nuvem ser tentada.
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

// TestGateway_AttemptBudgetNuvemAlcancavel garante que o fallback de nuvem
// continua enxergando o RESTANTE do overall: a borda estoura a fatia, a névoa
// devolve 503 rápido e a nuvem responde — cenário impossível antes da emenda
// (o ctx único morria na borda e a nuvem via remaining<=0).
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
