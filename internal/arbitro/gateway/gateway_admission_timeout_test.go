package gateway

// Timeout de ADMISSÃO vs timeout de SERVIÇO (D1 do plano
// sinal-saturacao-selflearning). O 504 do nó carrega no corpo o ESTÁGIO em que
// o prazo estourou, e só o nó sabe qual foi:
//
//	ACQUIRE_TIMEOUT             esperou fila/provisionamento; a função NUNCA
//	                            executou — saturação de admissão
//	INSTANCE_NOT_READY_TIMEOUT  o container subiu e não ficou pronto; a função
//	                            NUNCA executou — saturação de admissão
//	INVOCATION_TIMEOUT          a função executou e não respondeu — falha de
//	                            serviço, esta SIM pode vetar o nó
//
// Antes desta emenda o ramo 504 do dispatch não lia o corpo, e os três
// colapsavam em ErrAttemptTimeout. Medido na campanha arb4-* (YOLO26n,
// 29-30/09): a névoa emitia INVOCATION_TIMEOUT pelo caminho de aquisição, com
// a latência travada no timeout_ms do próprio nó (15005-15425 ms),
// `function_ms` nulo e o host a 2-10% de CPU com 36 contêineres vivos.
// Contados como falha de serviço, os 264 de 531 pedidos davam
// LaplaceRate(23,264)=0,090 — e vetavam o nó mais rápido do sistema.
//
// O que estes testes fixam é o EFEITO no veto: o que nunca executou não pode
// entrar na taxa de confiabilidade; o que executou e não respondeu continua
// entrando.

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// nodeTimeoutFixture monta um nó que responde 504 com o `code` informado no
// corpo, e devolve o histórico onde a tentativa foi registrada.
func nodeTimeoutFixture(t *testing.T, nodeCode string) *history.History {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		fmt.Fprintf(w, `{"code":%q,"message":"estourou em %s"}`, nodeCode, "15s")
	}))
	t.Cleanup(srv.Close)

	h, err := history.NewHistory("", 500)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	mon := monitor.NewMonitor()
	now := time.Now().UTC().Truncate(time.Millisecond)
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:    ns,
		Monitor:  mon,
		Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		History:  h,
		Clock:    fixedClock(now),
	})
	req := httptest.NewRequest(http.MethodPost, "/invoke",
		strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if len(h.Keys()) == 0 {
		t.Fatalf("nenhuma chave no history: status=%d body=%s", rec.Code, rec.Body.String())
	}
	return h
}

// lerServico devolve (total de serviço, taxa, saturação) do único segmento
// gravado pela tentativa.
func lerServico(t *testing.T, h *history.History) (int, float64, int) {
	t.Helper()
	keys := h.Keys()
	if len(keys) == 0 {
		t.Fatal("nenhuma chave no history: a tentativa nem foi registrada")
	}
	count, _, total, rate, _ := h.Stats(keys[0])
	if count != 0 {
		t.Fatalf("o timeout contaminou o p95/Count (count=%d): deve ser falha sem latência", count)
	}
	full, ok := h.StatsFull(keys[0], time.Hour, time.Now())
	if !ok {
		t.Fatal("StatsFull ok=false: o segmento tem que existir (Total>0 OU Saturation>0)")
	}
	return total, rate, full.Saturation
}

// TestTimeoutDeAdmissaoNaoContaComoFalhaDeServico: os dois estágios em que a
// função NÃO executa ficam fora da taxa de confiabilidade e vão para
// Saturation. É o reparo que impede o veto de banir o nó saturado.
func TestTimeoutDeAdmissaoNaoContaComoFalhaDeServico(t *testing.T) {
	for _, code := range []string{"ACQUIRE_TIMEOUT", "INSTANCE_NOT_READY_TIMEOUT"} {
		t.Run(code, func(t *testing.T) {
			h := nodeTimeoutFixture(t, code)
			total, rate, sat := lerServico(t, h)
			if total != 0 {
				t.Fatalf("%s: total=%d, esperado 0: a função nunca executou, não é falha de serviço", code, total)
			}
			if history.VetoByRate(total, rate) {
				t.Fatalf("%s vetou o nó (rate=%v): admissão-starvation não pode ser culpa de confiabilidade", code, rate)
			}
			if sat != 1 {
				t.Fatalf("%s: Saturation=%d, esperado 1: a espera precisa ficar visível", code, sat)
			}
		})
	}
}

// TestTimeoutDeFuncaoContinuaContandoComoFalhaDeServico é o contrapeso: sem
// isto, o reparo do D1 viraria "nenhum timeout veta mais".
func TestTimeoutDeFuncaoContinuaContandoComoFalhaDeServico(t *testing.T) {
	h := nodeTimeoutFixture(t, "INVOCATION_TIMEOUT")
	total, rate, sat := lerServico(t, h)
	if total != 1 {
		t.Fatalf("total=%d, esperado 1: a função executou e não respondeu, isso É falha de serviço", total)
	}
	if want := history.LaplaceRate(0, 1); rate != want {
		t.Fatalf("rate=%v, esperado %v", rate, want)
	}
	if sat != 0 {
		t.Fatalf("Saturation=%d, esperado 0: timeout de execução não é admissão", sat)
	}
}

// TestTimeoutSemCorpoUtilCaiNoCaminhoDeServico: corpo truncado, vazio,
// não-JSON ou com código desconhecido não pode ABSOLVER o nó — a ambiguidade
// silenciada vira falha de serviço, que é o fail-safe certo (o Árbitro não
// pode punir um nó por um dado que ele não conseguiu ler).
func TestTimeoutSemCorpoUtilCaiNoCaminhoDeServico(t *testing.T) {
	casos := map[string]string{
		"corpo vazio":       ``,
		"nao-JSON":          `lixo`,
		"sem campo code":    `{"message":"esgotou"}`,
		"code desconhecido": `{"code":"TIMEOUT_DO_FUTURO"}`,
	}
	for nome, corpo := range casos {
		t.Run(nome, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusGatewayTimeout)
				_, _ = w.Write([]byte(corpo))
			}))
			defer srv.Close()
			h, err := history.NewHistory("", 500)
			if err != nil {
				t.Fatalf("NewHistory: %v", err)
			}
			ns, _ := nodes.NewNodeStore("")
			if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			mon := monitor.NewMonitor()
			now := time.Now().UTC().Truncate(time.Millisecond)
			gw := NewGatewayWithHistory(GatewayConfig{
				Nodes: ns, Monitor: mon,
				Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
				History:  h, Clock: fixedClock(now),
			})
			rec := httptest.NewRecorder()
			gw.Handle(rec, httptest.NewRequest(http.MethodPost, "/invoke",
				strings.NewReader(`{"function":"fn","input":{"a":1}}`)))
			total, _, _ := lerServico(t, h)
			if total != 1 {
				t.Fatalf("total=%d, esperado 1: corpo ilegível não absolve o nó (fail-safe)", total)
			}
		})
	}
}

// TestClassifyAdmissionTimeoutSoDoisCodigos fixa o contrato do mapeamento: só
// os dois estágios de admissão entram; INVOCATION_TIMEOUT e qualquer outro
// continuam no caminho de serviço.
func TestClassifyAdmissionTimeoutSoDoisCodigos(t *testing.T) {
	for _, code := range []string{"ACQUIRE_TIMEOUT", "INSTANCE_NOT_READY_TIMEOUT"} {
		err, ok := classifyAdmissionTimeout(code, "n1", "msg")
		if !ok || err == nil {
			t.Fatalf("%s deveria mapear para timeout de admissão", code)
		}
		if !errors.Is(err, ErrAttemptAdmissionTimeout) {
			t.Fatalf("%s: erro não carrega ErrAttemptAdmissionTimeout", code)
		}
	}
	for _, code := range []string{"INVOCATION_TIMEOUT", "", "POOL_EXHAUSTED", "qualquer"} {
		if err, ok := classifyAdmissionTimeout(code, "n1", "msg"); ok || err != nil {
			t.Fatalf("%q não deveria mapear para admissão (retornou %v)", code, err)
		}
	}
}
