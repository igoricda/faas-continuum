package gateway

import (
	"encoding/json"
	"fmt"
	"io"
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

// TestPressaoDoCorpo504ChegaAoEntry fecha a FIAÇÃO que os dois testes de
// unidade não cobrem: nó real (httptest) respondendo 504 com o bloco
// `pressure` no corpo, passando pelo dispatch, pelo parser e pelo buildEntry
// ATÉ o invocation log. O produtor é testado no nodeapi (o corpo que ele
// escreve) e o consumidor aqui, mas a emenda entre os dois era confiada — um
// erro no `io.ReadAll` do ramo 504, no nome do campo JSON ou no caminho do
// `captured.admPressure` passaria pelos dois lados.
//
// Além dos três campos no Entry, confere o efeito no History: o 504 de
// admissão é SATURAÇÃO DO NÓ (Total=0, Saturation=1, NodeSaturation=1) — o
// mesmo contrato que o D1/D3 consomem.
func TestPressaoDoCorpo504ChegaAoEntry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		_, _ = io.WriteString(w, `{"code":"ACQUIRE_TIMEOUT","message":"estourou na fila","pressure":{"pool_alive":36,"queue_depth":5,"acquire_wait_ms":14998}}`)
	}))
	t.Cleanup(srv.Close)

	now := time.Now().UTC().Truncate(time.Millisecond)
	h, err := history.NewHistory("", 500)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "nevoa-1", Layer: nodes.LayerNevoa, Host: hostFromURL(t, srv.URL)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	mon := monitor.NewMonitor()
	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:    ns,
		Monitor:  mon,
		Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		History:  h,
		Clock:    fixedClock(now),
	})
	gw.SetInvocationLog(l)

	rec := httptest.NewRecorder()
	gw.Handle(rec, httptest.NewRequest(http.MethodPost, "/invoke",
		strings.NewReader(`{"function":"fn","input":{"a":1}}`)))

	e := leEntryDoLog(t, logPath)
	if e.InstancesAvailable != 36 {
		t.Errorf("InstancesAvailable = %d, esperado 36 (pool_alive do corpo 504)", e.InstancesAvailable)
	}
	if e.QueueDepth != 5 {
		t.Errorf("QueueDepth = %d, esperado 5", e.QueueDepth)
	}
	if e.AcquireWaitMS != 14998 {
		t.Errorf("AcquireWaitMS = %d, esperado 14998", e.AcquireWaitMS)
	}
	if keys := h.Keys(); len(keys) == 0 {
		t.Fatal("history vazio: o 504 de admissão não foi gravado")
	} else {
		full, okFull := h.StatsFull(keys[0], time.Hour, now)
		if !okFull || full.Total != 0 || full.Saturation != 1 || full.NodeSaturation != 1 {
			t.Fatalf("StatsFull=(total=%d sat=%d nodesat=%d ok=%v), esperado (0,1,1,true)",
				full.Total, full.Saturation, full.NodeSaturation, okFull)
		}
	}
}

// TestNodeAdmissionPressureParse cobre o PARSER REAL com os casos que
// importam: bloco completo, corpo sem bloco, corpo invalido/vazio, bloco nulo
// e corpo acima do teto. Antes desta versao o teste vivia no nodeapi como um
// SHIM que reimplementava a decodificacao — pinava a forma do JSON mas nao o
// parser que roda em producao, e podia divergir dele em silencio.
func TestNodeAdmissionPressureParse(t *testing.T) {
	grande := fmt.Sprintf(`{"code":"ACQUIRE_TIMEOUT","pressure":{"pool_alive":1},"x":%q}`,
		strings.Repeat("a", int(maxCapacityErrorBodyBytes)+1))
	casos := []struct {
		nome     string
		body     string
		ok       bool
		vivas    int
		fila     int
		esperaMS int64
	}{
		{"completo", `{"code":"ACQUIRE_TIMEOUT","message":"x","pressure":{"pool_alive":36,"queue_depth":5,"acquire_wait_ms":14998}}`, true, 36, 5, 14998},
		{"no antigo, sem bloco", `{"code":"ACQUIRE_TIMEOUT","message":"x"}`, false, 0, 0, 0},
		{"nao e JSON", `nao e json`, false, 0, 0, 0},
		{"vazio", ``, false, 0, 0, 0},
		{"pressure nulo explicito", `{"code":"X","pressure":null}`, false, 0, 0, 0},
		{"so o codigo", `{"code":"QUEUE_FULL"}`, false, 0, 0, 0},
		{"acima do teto de corpo", grande, false, 0, 0, 0},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			p, ok := nodeAdmissionPressure([]byte(c.body))
			if ok != c.ok {
				t.Fatalf("ok = %v, esperado %v (body=%d bytes)", ok, c.ok, len(c.body))
			}
			if p.PoolAlive != c.vivas || p.QueueDepth != c.fila || p.AcquireWaitMS != c.esperaMS {
				t.Fatalf("retrato = %+v, esperado vivas=%d fila=%d espera=%d", p, c.vivas, c.fila, c.esperaMS)
			}
		})
	}
}

// TestRetratoDeAdmissaoNoEntryDo504 e o outro lado do C6.3(a).
//
// O produtor ja escrevia o retrato no corpo do 504, mas o Arbitro o descartava
// por um motivo estrutural: `buildEntry` so lia ocupacao do piggyback de
// capacidade, que existe SO no 2xx (`if err == nil`,
// gateway_attempt_record.go). Entao um pedido que nunca executou — o unico
// caso em que a pressao de admissao e diretamente observavel — era gravado sem
// `queue_depth`, sem `instances_available` e sem `acquire_wait_ms`.
//
// Este teste fixa o efeito do CONSUMIDOR: o retrato que o no mandou tem de
// aparecer no Entry. Sem ele, a mudanca no produtor seria um no-op do ponto de
// vista de quem decide.
func TestRetratoDeAdmissaoNoEntryDo504(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := nodes.Node{NodeID: "nevoa-1"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	group := selection.ResolveRequestGroup("fn-adm", []nodes.Node{node})

	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	h, _ := history.NewHistory("", 100)
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Monitor: monitor.NewMonitor(), Clock: fixedClock(now)})
	gw.SetInvocationLog(l)

	pr := NodeAdmissionPressure{PoolAlive: 36, QueueDepth: 5, AcquireWaitMS: 14998}
	captured := attemptCapture{concrete: "fn-adm", admPressure: &pr}
	gw.recordAttempt(node, 15000*time.Millisecond, ErrAttemptAdmissionTimeout,
		slo, "fn-adm", group, nil, true, "req-1", 1, captured)

	e := leEntryDoLog(t, logPath)
	if e.InstancesAvailable != 36 {
		t.Errorf("InstancesAvailable = %d, esperado 36 (containers VIVAS, nao livres)", e.InstancesAvailable)
	}
	if e.QueueDepth != 5 {
		t.Errorf("QueueDepth = %d, esperado 5 (profundidade da fila no instante da desistencia)", e.QueueDepth)
	}
	if e.AcquireWaitMS != 14998 {
		t.Errorf("AcquireWaitMS = %d, esperado 14998", e.AcquireWaitMS)
	}
}

// TestRetratoAusenteNaoViraZero: no antigo, ou corpo truncado, nao ha bloco.
// O Entry tem de ficar com os valores que JÁ teria (zero), e o histrico nao
// pode ganhar um numero inventado. Ausencia e zero sao coisas diferentes —
// "nao sei a fila" nao e "a fila estava vazia".
func TestRetratoAusenteNaoViraZero(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := nodes.Node{NodeID: "nevoa-1"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	group := selection.ResolveRequestGroup("fn-adm2", []nodes.Node{node})

	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	h, _ := history.NewHistory("", 100)
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Monitor: monitor.NewMonitor(), Clock: fixedClock(now)})
	gw.SetInvocationLog(l)
	gw.recordAttempt(node, 15000*time.Millisecond, ErrAttemptAdmissionTimeout,
		slo, "fn-adm2", group, nil, true, "req-2", 1, attemptCapture{concrete: "fn-adm2"})

	e := leEntryDoLog(t, logPath)
	if e.QueueDepth != 0 || e.AcquireWaitMS != 0 {
		t.Errorf("sem retrato o Entry nao deveria ter valor: %+v", e)
	}
}

// TestRetratoNaoContaminaOSucesso: o caminho 2xx tem o SEU proprio retrato (o
// piggyback) e ele manda. Se o do 504 invadisse o 2xx, um no saudavel
// receberia numeros de um desfecho que nao foi seu.
func TestRetratoNaoContaminaOSucesso(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := nodes.Node{NodeID: "borda-1"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	group := selection.ResolveRequestGroup("fn-ok", []nodes.Node{node})

	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	h, _ := history.NewHistory("", 100)
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Monitor: monitor.NewMonitor(), Clock: fixedClock(now)})
	gw.SetInvocationLog(l)
	captured := attemptCapture{concrete: "fn-ok"}
	captured.admPressure = &NodeAdmissionPressure{PoolAlive: 99, QueueDepth: 99, AcquireWaitMS: 99}
	// Piggyback do 2xx com a MESMA variante concreta (fn-ok), entao e este
	// retrato que tem de ganhar: queue_depth=3, nao o 99 do 504.
	metrics := json.RawMessage(`{"telemetry":{"capacity":{"fn-ok":{"max_instances":2,"instances_available":1,"queue_depth":3}}}}`)
	gw.recordAttempt(node, 200*time.Millisecond, nil, slo, "fn-ok", group, metrics, true, "req-3", 1, captured)

	e := leEntryDoLog(t, logPath)
	if e.QueueDepth != 3 {
		t.Errorf("QueueDepth = %d, esperado 3 (o do piggyback do 2xx, nao o 99 do 504)", e.QueueDepth)
	}
	if e.AcquireWaitMS == 99 {
		t.Error("AcquireWaitMS = 99: o retrato do 504 invadiu o caminho de sucesso")
	}
	if e.InstancesAvailable == 99 {
		t.Error("InstancesAvailable = 99: o retrato do 504 invadiu o caminho de sucesso")
	}
}

// leEntryDoLog le a ÚLTIMA linha do invocation log e devolve a Entry, que o
// log achata no JSON (invocation_log.go:30). Ler por aqui em vez de por um
// getter do History cobre o caminho INTEIRO — buildEntry, storeEntry e disco —
// e não só a montagem do Entry.
func leEntryDoLog(t *testing.T, path string) history.Entry {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile invLog: %v", err)
	}
	linhas := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(linhas) == 0 || linhas[len(linhas)-1] == "" {
		t.Fatalf("invLog vazio (%d bytes)", len(data))
	}
	var e history.Entry
	if err := json.Unmarshal([]byte(linhas[len(linhas)-1]), &e); err != nil {
		t.Fatalf("unmarshal da linha %q: %v", linhas[len(linhas)-1], err)
	}
	return e
}
