package gateway

// Bateria de efeitos (adendo 2026-09-30, Passo 4.8): T4 (fail-fast cancela
// pendências), T6a (teto de corpo na pendência), T7 (nó antigo = síncrono) e
// T8 (pool/join sob concorrência). T1/T2/T3/T5/T6b vivem em
// gateway_wait_test.go e gateway_headers_test.go (referenciados abaixo).
//
// T1 TestWaitDeliversLateSuccess — borda estoura a fatia e entrega depois.
// T2 TestWaitFirstSuccessWins — névoa vence, borda cancelada.
// T3 TestWaitLateFailureSettles — 503 tardio supersede o orcamento.
// T5 TestWaitClientDisconnect — desconexão aborta sem escrever.
// T6b TestWaitHardExpiryFallsThrough — prazo rígido cai em nuvem/failure.
//
// Gate test-audit: cada caso trava um invariante §6 ou um ramo fail-fast sem
// dono. Regressões críveis: fail-fast que ignora órfãs (vazam até o cap),
// framing tardio tratado como sucesso, nó antigo entrando no async, ou Put
// do pool antes do join (corrupção de corpo). Sem seam novo.
import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// T4 (revisto em 2026-10-01/2): 401/400 síncrono responde imediato e a órfã
// armada NÃO é cancelada — ela conclui depois, é assentada com o desfecho real
// (aprendizado) e o contador de canceladas fica em zero. Antes, o fail-fast
// matava a execução anunciada anterior na fila ("cold perpétuo" da bateria
// arb8).
func TestFailFastKeepsArmedRunning(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		code   string
	}{
		{"401", http.StatusUnauthorized, "UNAUTHORIZED"},
		{"400", http.StatusBadRequest, "NODE_BAD_REQUEST"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			var slowCalls int32
			slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&slowCalls, 1)
				select {
				case <-release:
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"result":"orfao"}`))
				case <-r.Context().Done():
				}
			}))
			defer slow.Close()
			var releaseOnce sync.Once
			releaseArmed := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseArmed()
			strict := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer strict.Close()
			ns, _ := nodes.NewNodeStore("")
			// Nó anunciado antes do antigo: a órfã armada segue viva após o
			// fail-fast (401/400) do nó seguinte.
			_, _ = ns.Upsert(nodes.Node{NodeID: "a-slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slow.URL), Functions: []string{"fn"}, FunctionTimeoutMS: map[string]int{"fn": 15000}})
			_, _ = ns.Upsert(nodes.Node{NodeID: "b-strict", Layer: nodes.LayerBorda, Host: hostFromURL(t, strict.URL), Functions: []string{"fn"}})
			logPath := t.TempDir() + "/inv.jsonl"
			gw := waitTestGateway(t, ns, logPath)

			start := time.Now()
			rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
			elapsed := time.Since(start)
			if rec.Code != tc.status {
				t.Fatalf("status=%d want %d body=%s", rec.Code, tc.status, rec.Body.String())
			}
			if er := decodeErrResp(t, rec); er.Code != tc.code {
				t.Fatalf("code=%q want %s", er.Code, tc.code)
			}
			if elapsed > 3*time.Second {
				t.Fatalf("fail-fast em %v: segurou a resposta até a órfã", elapsed)
			}
			if armed, _, _, cancelled := gw.BestEffortCounters(); armed != 1 || cancelled != 0 {
				t.Fatalf("armed=%d cancelled=%d want 1/0 (fail-fast não cancela mais)", armed, cancelled)
			}
			// Libera a órfã: o drain pós-resposta a assenta com o desfecho real.
			releaseArmed()
			deadline := time.Now().Add(3 * time.Second)
			for {
				raw, _ := os.ReadFile(logPath)
				settled := false
				for _, ln := range strings.Split(string(raw), "\n") {
					if strings.Contains(ln, `"node_id":"a-slow"`) && strings.Contains(ln, `"outcome":"sucesso"`) {
						settled = true
						break
					}
				}
				if settled {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("órfã não assentada no invLog em 3s: %s", string(raw))
				}
				time.Sleep(20 * time.Millisecond)
			}
			if got := atomic.LoadInt32(&slowCalls); got != 1 {
				t.Fatalf("nó a-slow chamado %d vez(es), want 1 (execução não foi morta)", got)
			}
		})
	}
}

// T6a: corpo > 8 MiB na pendência vira o mesmo erro do caminho síncrono
// (502 NODE_ERROR, só invLog — framing não é registrável).
func TestLateOversizeBodyFraming(t *testing.T) {
	large := bytes.Repeat([]byte("a"), 9*1024*1024)
	bigJSON := append(append([]byte(`{"data":"`), large...), []byte(`"}`)...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bigJSON)
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	logPath := t.TempDir() + "/inv.jsonl"
	gw := waitTestGateway(t, ns, logPath)

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502 body=%s", rec.Code, rec.Body.String())
	}
	if er := decodeErrResp(t, rec); er.Code != "NODE_ERROR" {
		t.Fatalf("code=%q want NODE_ERROR (mesmo do síncrono)", er.Code)
	}
	recs := readInvLogLines(t, logPath)
	if len(recs) != 2 {
		t.Fatalf("linhas=%d want 2 (orcamento + falha de framing)", len(recs))
	}
	if recs[1].Outcome != history.OutcomeFalha {
		t.Fatalf("última outcome=%q want falha", recs[1].Outcome)
	}
}

// T7: alvo sem function_timeout_ms segue o caminho síncrono de hoje —
// fatia estourada é 504 budget, sem entrega tardia (defensivo, não modo).
func TestLegacyNodeStaysSynchronous(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"tarde-demais"}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL), Functions: []string{"fn"}})
	gw := waitTestGateway(t, ns, "")

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504 (sem espera best-effort) body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Faas-Late"); got != "" {
		t.Fatalf("X-Faas-Late=%q want ausente (nó antigo)", got)
	}
	if armed, served, expired, cancelled := gw.BestEffortCounters(); armed+served+expired+cancelled != 0 {
		t.Fatalf("contadores=%d/%d/%d/%d want zeros", armed, served, expired, cancelled)
	}
}

// T8: pool/join sob concorrência — N invokes concorrentes com inputs
// distintos, todos armados e entregues tarde; cada corpo devolvido é o do
// próprio invoke (o Put nunca recicla buffer lido por goroutine viva).
func TestPooledBufferJoinUnderRace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		var fwd struct {
			Input json.RawMessage `json:"input"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &fwd); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, _ := json.Marshal(map[string]json.RawMessage{"echo": fwd.Input})
		_, _ = w.Write(resp)
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	mon := monitor.NewMonitor()
	ig := NewInflightGauge()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}}).WithInflight(ig)

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			want := fmt.Sprintf(`{"data":"call-%d"}`, i)
			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","slo":{"max_latency_ms":20},"input":`+want+`}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			gw.Handle(rec, req)
			if rec.Code != http.StatusOK {
				errs[i] = fmt.Errorf("call %d: status=%d body=%s", i, rec.Code, rec.Body.String())
				return
			}
			var resp map[string]json.RawMessage
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				errs[i] = fmt.Errorf("call %d: decode: %v", i, err)
				return
			}
			if strings.TrimSpace(string(resp["echo"])) != want {
				errs[i] = fmt.Errorf("call %d: eco=%q want %q (corrupção de buffer?)", i, resp["echo"], want)
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("invoke %d: %v", i, err)
		}
	}
	if got := ig.NodeK("n1"); got != 0 {
		t.Errorf("inflight=%d want 0 (Dec de todas as pendências)", got)
	}
	if _, served, _, _ := gw.BestEffortCounters(); served != n {
		t.Errorf("served=%d want %d (todas tardias)", served, n)
	}
}
