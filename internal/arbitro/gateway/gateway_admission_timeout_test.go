package gateway

// Admission timeout against service timeout. The timed out answer of the node
// carries in the body the stage in which the deadline expired, and only the
// node knows which one it was:
//
//	ACQUIRE_TIMEOUT             the request waited for the queue or for the
//	                            provisioning and the function never ran, which
//	                            is an admission saturation
//	INSTANCE_NOT_READY_TIMEOUT  the container started and did not become
//	                            ready and the function never ran, which is an
//	                            admission saturation
//	INVOCATION_TIMEOUT          the function ran and did not answer, which is
//	                            a service failure that may veto the node
//
// Without this separation the timed out branch of the dispatch did not read the
// body and the three stages collapsed into a single timeout error. In the
// measured campaign the fog emitted an invocation timeout through the
// acquisition path, with the latency stuck at the timeout of the node itself,
// a null function time and the host at two to ten percent of processor usage
// with thirty-six live containers. Counted as service failures, those requests
// lowered the smoothed success rate and vetoed the fastest node of the system.
//
// These tests pin the effect on the veto: what never ran cannot enter the
// reliability rate, and what ran and did not answer still enters it.

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

// nodeTimeoutFixture builds a node that answers timed out with the given code
// in the body, and returns the history where the attempt was recorded.
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

// serviceReading returns the service total, the rate and the saturation of the
// single segment recorded by the attempt.
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

// TestTimeoutDeAdmissaoNaoContaComoFalhaDeServico covers the two stages in which
// the function never runs, which stay out of the reliability rate and go to the
// saturation counter. It is the repair that stops the veto from banning a
// saturated node.
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

// TestTimeoutDeFuncaoContinuaContandoComoFalhaDeServico is the counterweight:
// without it the repair would become the rule that no timeout ever vetoes.
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

// TestTimeoutSemCorpoUtilCaiNoCaminhoDeServico covers a truncated, empty,
// non-JSON body or an unknown code, which cannot absolve the node: the silenced
// ambiguity becomes a service failure, which is the correct fail-safe, since the
// arbiter cannot punish a node for data it could not read.
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

// TestClassifyAdmissionTimeoutSoDoisCodigos pins the mapping: only the two
// admission stages enter, while the invocation timeout and any other code stay
// in the service path.
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
