package gateway

// Amarração 503→monitor/history (capacity-history-arbitro, todo 7 +
// ADR-0007, AUD-21): TestGateway503Capacity prova status/history mas nunca
// lê o monitor; aqui cada 503 válido marca esgotado (avail=0, ok=true) e o
// genérico (code desconhecido/corpo inválido) vai só ao invLog, sem History
// e sem tocar o monitor — semântica do status, não do corpo. Mais as
// combinações nil de hist/invLog no recordAttempt (ingestão antes do early
// return).

import (
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

func TestGatewayExhaustedMarksMonitor(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantHistory bool
		wantMarked  bool
	}{
		{"pool_exhausted", `{"code":"POOL_EXHAUSTED","message":"pool busy"}`, true, true},
		{"queue_full", `{"code":"QUEUE_FULL","message":"queue full"}`, true, true},
		{"unknown_code", `{"code":"FUTURE_CODE"}`, true, false},
		{"garbage_body", `not-json{{{`, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.body
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()
			ns, _ := nodes.NewNodeStore("")
			if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			h, err := history.NewHistory("", 100)
			if err != nil {
				t.Fatalf("NewHistory: %v", err)
			}
			now := time.Now().UTC().Truncate(time.Millisecond)
			mon := monitor.NewMonitor()
			gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, History: h, Clock: fixedClock(now)})
			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
			rec := httptest.NewRecorder()
			gw.Handle(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
			}

			got, ok := mon.GetCapacity("n1", "fn", now)
			if tc.wantMarked {
				if !ok || got.InstancesAvailable != 0 {
					t.Fatalf("monitor após %s = (%+v,%v), esperado (avail=0,true)", tc.name, got, ok)
				}
			} else {
				if ok {
					t.Fatalf("monitor após %s = (%+v,true), esperado false (genérico não marca)", tc.name, got)
				}
			}
			// O 503 treina o History como SATURAÇÃO sem latência
			// (primária + agregada), inclusive o genérico: p95/Count
			// seguem só com sucesso, e a recusa fica fora de
			// Total/SuccessRate — é o nó recusando o que não dá conta, não
			// falta de confiabilidade (plano-sinal-saturacao-selflearning
			// D1).
			if tc.wantHistory {
				if len(h.Keys()) != 2 {
					t.Fatalf("history tem %d chaves após %s, esperado 2 (primária + agregada)", len(h.Keys()), tc.name)
				}
				count, p95, total, rate, _ := h.Stats(h.Keys()[0])
				if count != 0 || p95 != 0 || total != 0 {
					t.Fatalf("stats após %s = (count=%d p95=%d total=%d), esperado zero nos três", tc.name, count, p95, total)
				}
				if history.VetoByRate(total, rate) {
					t.Fatalf("rejeição %s vetou o nó (rate=%v): o veto só pode ver falha de serviço", tc.name, rate)
				}
				if full, okFull := h.StatsFull(h.Keys()[0], time.Hour, now); !okFull || full.Saturation != 1 {
					t.Fatalf("Saturation após %s = (%d,%v), esperado (1,true)", tc.name, full.Saturation, okFull)
				}
			}
		})
	}
}

func TestRecordAttemptCapacityNilCombinations(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := nodes.Node{NodeID: "borda-1"}
	slo := &selection.SLO{MaxLatencyMS: 500}
	group := selection.ResolveRequestGroup("fn-cap", []nodes.Node{node})

	// hist==nil + invLog presente: rejeição cai no log, sem pânico.
	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	monLog := monitor.NewMonitor()
	gwLog := NewGatewayWithHistory(GatewayConfig{Monitor: monLog, Clock: fixedClock(now)})
	gwLog.SetInvocationLog(l)
	gwLog.recordAttempt(node, 3*time.Millisecond, capacityErr(t, "POOL_EXHAUSTED", node.NodeID), slo, "fn-cap", group, nil, true, "", 0, attemptCapture{})
	if got := invLogLines(t, logPath); got != 1 {
		t.Fatalf("invLog = %d linhas, esperado 1 (só-log ainda registra rejeição)", got)
	}
	if _, ok := monLog.GetCapacity(node.NodeID, "fn-cap", now); !ok {
		t.Fatal("monitor após rejeição só-log = false, esperado true (ingestão antes do early return)")
	}

	// hist presente + invLog==nil: rejeição treina como saturação sem
	// latência (D1), sem log e sem pânico; sem Layer não há chave agregada
	// (só a primária).
	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	monHist := monitor.NewMonitor()
	gwHist := NewGatewayWithHistory(GatewayConfig{History: h, Monitor: monHist, Clock: fixedClock(now)})
	gwHist.recordAttempt(node, 3*time.Millisecond, capacityErr(t, "QUEUE_FULL", node.NodeID), slo, "fn-cap", group, nil, true, "", 0, attemptCapture{})
	if len(h.Keys()) != 1 {
		t.Fatalf("history tem %d chaves, esperado 1 (saturação sem latência; sem Layer não há agregada)", len(h.Keys()))
	}
	count, p95, total, _, ok := h.Stats(h.Keys()[0])
	if ok || count != 0 || p95 != 0 || total != 0 {
		t.Fatalf("stats=(count=%d p95=%d total=%d ok=%v), esperado sem taxa e sem latência", count, p95, total, ok)
	}
	if _, ok := monHist.GetCapacity(node.NodeID, "fn-cap", now); !ok {
		t.Fatal("monitor após rejeição sem-log = false, esperado true")
	}

	// hist==nil + invLog==nil: early return, mas a ingestão já marcou.
	monBare := monitor.NewMonitor()
	gwBare := NewGatewayWithHistory(GatewayConfig{Monitor: monBare, Clock: fixedClock(now)})
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("recordAttempt bare causou panic: %v", r)
			}
		}()
		gwBare.recordAttempt(node, 3*time.Millisecond, capacityErr(t, "POOL_EXHAUSTED", node.NodeID), slo, "fn-cap", group, nil, true, "", 0, attemptCapture{})
	}()
	got, ok := monBare.GetCapacity(node.NodeID, "fn-cap", now)
	if !ok || got.InstancesAvailable != 0 {
		t.Fatalf("monitor bare após rejeição = (%+v,%v), esperado (avail=0,true)", got, ok)
	}
}
