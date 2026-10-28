package gateway

// Testes da liquidação (adendo 2026-09-30, Passo 4.5): T4b — dupla fonte
// (assentamento da espera + late report do nó) grava 1x nas duas ordens, com
// supersede do orcamento_excedido e lateDropped no perdedor.
//
// Gate test-audit: protegem o dedup COMPARTILHADO (contrato novo do Passo
// 4.5) — sem eles, uma regressão no claim/publish duplicaria a Entry
// (sucesso contado 2x no p95/Count). Regressão crível: dedup separado por
// fonte ou supersede perdido. Os testes de late legados cobrem só o lado do
// fio (sem disputa). Sem seam novo (recordAttempt/settlePending/RecordLate
// reais + History em memória).
import (
	"fmt"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

func settleFixture(t *testing.T, reqID string) (*Gateway, nodes.Node, time.Time, string, *pendingAttempt, pendingOutcome) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	h, err := history.NewHistory("", 500)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	ns, _ := nodes.NewNodeStore("")
	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
	if _, err := ns.Upsert(node); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: mon,
		Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		History:  h, Clock: fixedClock(now),
	})
	logPath := t.TempDir() + "/inv.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gw.SetInvocationLog(l)
	gw.RegisterLateGroup(reqID, "fn", &selection.SLO{MaxLatencyMS: 100})

	// Armamento: o orcamento_excedido síncrono (registrável → liveIdx).
	sc := history.ScenarioContext{LoadBucket: history.BucketUnknown}
	gw.recordAttempt(node, 100*time.Millisecond,
		fmt.Errorf("%w contra %q (fatia esgotada)", ErrAttemptBudgetExceeded, node.NodeID),
		nil, "fn", "fn", nil, true, reqID, 1, attemptCapture{sc: sc})

	pa := &pendingAttempt{
		target: node, function: "fn", group: "fn",
		slo:          &selection.SLO{MaxLatencyMS: 100},
		requestID:    reqID,
		attemptN:     1,
		captured:     attemptCapture{sc: sc},
		attemptStart: now,
		cap:          15 * time.Second,
	}
	out := pendingOutcome{
		hasResponse: true,
		res:         attemptResult{status: 200, body: []byte(`{"result":1}`)},
		latency:     300 * time.Millisecond,
	}
	return gw, node, now, logPath, pa, out
}

func settleRep(reqID string, now time.Time) LateReport {
	return LateReport{
		RequestID: reqID, AttemptN: 1, Function: "fn", NodeID: "n1",
		Outcome: history.OutcomeSucesso, ObservedLatencyMS: 300,
		StartedAt: now.Format(time.RFC3339Nano),
	}
}

// primarySuccessStats conta sucessos/saturação na chave primária do attempt.
func primarySuccessStats(t *testing.T, gw *Gateway, now time.Time) (count int, saturation int) {
	t.Helper()
	key := history.SegmentKey("fn", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	fs, ok := gw.hist.StatsFull(key, time.Hour, now)
	if !ok {
		t.Fatalf("segmento primário ausente")
	}
	return fs.Count, fs.Saturation
}

// Ordem A→B: assenta primeiro, late report depois — late cai no dedup.
func TestSettleThenLateDedups(t *testing.T) {
	gw, _, now, logPath, pa, out := settleFixture(t, "req-a")
	kind, _, _ := gw.settlePending(pa, out, true)
	if kind != attemptSuccess {
		t.Fatalf("kind=%d want success", kind)
	}
	applied, err := gw.RecordLate(settleRep("req-a", now))
	if err != nil {
		t.Fatalf("RecordLate: %v", err)
	}
	if applied {
		t.Fatalf("late aplicado após o assentamento: dedup compartilhado quebrou")
	}
	if count, sat := primarySuccessStats(t, gw, now); count != 1 || sat != 0 {
		t.Fatalf("count=%d sat=%d want 1/0 (dupla contagem ou supersede perdido)", count, sat)
	}
	_, _, dropped, _ := gw.Counters()
	if dropped != 1 {
		t.Fatalf("lateDropped=%d want 1", dropped)
	}
	recs := readInvLogLines(t, logPath)
	if len(recs) != 2 {
		t.Fatalf("linhas=%d want 2 (orcamento + sucesso; late sem linha)", len(recs))
	}
	last := recs[1]
	if last.Outcome != history.OutcomeSucesso || last.SloMet {
		t.Fatalf("última outcome=%q slo_met=%v want sucesso/false (latência 300 > SLO 100)", last.Outcome, last.SloMet)
	}
	if !last.LateDelivered {
		t.Fatalf("late_delivered=false want true")
	}
}

// Ordem B→A: late report primeiro, assentamento depois — o settle perde o
// dedup (sem regravar) mas o sucesso ainda vence.
func TestLateThenSettleDedups(t *testing.T) {
	gw, _, now, logPath, pa, out := settleFixture(t, "req-b")
	applied, err := gw.RecordLate(settleRep("req-b", now))
	if err != nil {
		t.Fatalf("RecordLate: %v", err)
	}
	if !applied {
		t.Fatalf("late deveria aplicar primeiro")
	}
	kind, _, _ := gw.settlePending(pa, out, true)
	if kind != attemptSuccess {
		t.Fatalf("kind=%d want success (dedup não tira a vitória)", kind)
	}
	if count, sat := primarySuccessStats(t, gw, now); count != 1 || sat != 0 {
		t.Fatalf("count=%d sat=%d want 1/0 (assentamento regravou)", count, sat)
	}
	_, _, dropped, _ := gw.Counters()
	if dropped != 1 {
		t.Fatalf("lateDropped=%d want 1", dropped)
	}
	recs := readInvLogLines(t, logPath)
	if len(recs) != 2 {
		t.Fatalf("linhas=%d want 2 (orcamento + late; settle sem linha)", len(recs))
	}
}
