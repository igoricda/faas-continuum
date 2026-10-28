package gateway

// Política de recordability por classe (ADR-0007, AUD-01/AUD-21): só
// sucesso e timeout-legítimo entram no History (p95/Count = custo de
// sucesso); capacidade, 401, 404, framing e falha genérica vão só para o
// invLog, incondicional.

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func capacityBurstGateway(t *testing.T, h *history.History, now time.Time) (*Gateway, string) {
	t.Helper()
	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Monitor: monitor.NewMonitor(), Clock: fixedClock(now)})
	gw.SetInvocationLog(l)
	return gw, logPath
}

func capacityErr(t *testing.T, code, nodeID string) error {
	t.Helper()
	err := capacityAttemptErr([]byte(`{"code":"`+code+`"}`), nodeID, "busy")
	if err == nil {
		t.Fatalf("capacityAttemptErr(%s) = nil, want capacity error", code)
	}
	if !IsCapacityRejection(err) {
		t.Fatalf("IsCapacityRejection(%s) = false", code)
	}
	return err
}

func invLogLines(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile invLog: %v", err)
	}
	n := strings.Count(string(data), "\n")
	if n == 0 && len(data) != 0 {
		t.Fatalf("invLog sem trailing newline (%d bytes)", len(data))
	}
	return n
}

func TestHistoryCapacityWriteSkip(t *testing.T) {
	h, err := history.NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	gw, logPath := capacityBurstGateway(t, h, now)

	node := nodes.Node{NodeID: "borda-1"}
	slo := &selection.SLO{MaxLatencyMS: 500}
	group := selection.ResolveRequestGroup("fn-cap", []nodes.Node{node})
	sc := history.ScenarioFrom(telemetry.Package{}, false)
	key := history.SegmentKey(group, node.NodeID, sc)

	rejections := []error{
		capacityErr(t, "POOL_EXHAUSTED", node.NodeID),
		capacityErr(t, "QUEUE_FULL", node.NodeID),
		capacityErr(t, "POOL_EXHAUSTED", node.NodeID),
		capacityErr(t, "QUEUE_FULL", node.NodeID),
	}
	for _, rerr := range rejections {
		gw.recordAttempt(node, 3*time.Millisecond, rerr, slo, "fn-cap", group, nil, true, "", 0, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})
	}

	if count, _, _, _, _, ok := h.StatsAndRecent(key, time.Minute, now); ok || count != 0 {
		t.Fatalf("StatsAndRecent após só-rejeições = (%d,%v), esperado (0,false)", count, ok)
	}
	if got := invLogLines(t, logPath); got != len(rejections) {
		t.Fatalf("invLog = %d linhas, esperado %d (rejeições pulam History, não o log)", got, len(rejections))
	}

	gw.recordAttempt(node, 4*time.Millisecond, errors.New("boom"), slo, "fn-cap", group, nil, true, "", 0, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})
	gw.recordAttempt(node, 6*time.Millisecond, context.DeadlineExceeded, slo, "fn-cap", group, nil, true, "", 0, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})

	// Cenário AUD-01: falha genérica vai só ao invLog; timeout-legítimo é
	// gravado no segmento mas fora do p95/Count (p95-sobre-sucessos) —
	// segmento sem sucesso segue !ready para MinSamples.
	if len(h.Keys()) != 1 {
		t.Fatalf("Keys após falha+timeout = %d, esperado 1 (só o timeout é gravado)", len(h.Keys()))
	}
	if count, _, _, _, ok := h.Stats(key); ok || count != 0 {
		t.Fatalf("Stats após falha+timeout = (%d,%v), esperado (0,false): sem sucesso, sem p95", count, ok)
	}
	if got := invLogLines(t, logPath); got != len(rejections)+2 {
		t.Fatalf("invLog = %d linhas, esperado %d", got, len(rejections)+2)
	}

	// Um sucesso ancora o segmento: Count/p95 passam a refletir só ele.
	gw.recordAttempt(node, 8*time.Millisecond, nil, slo, "fn-cap", group, nil, true, "", 0, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})
	if count, p95, _, _, ok := h.Stats(key); !ok || count != 1 || p95 != 8 {
		t.Fatalf("Stats após 1 sucesso = (%d,%d,%v), esperado (1,8,true)", count, p95, ok)
	}
	if got := invLogLines(t, logPath); got != len(rejections)+3 {
		t.Fatalf("invLog = %d linhas, esperado %d", got, len(rejections)+3)
	}
}

func TestRecordAttemptCapacitySkipsHistory(t *testing.T) {
	h, err := history.NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	gw, logPath := capacityBurstGateway(t, h, now)

	node := nodes.Node{NodeID: "borda-1"}
	slo := &selection.SLO{MaxLatencyMS: 500}
	group := selection.ResolveRequestGroup("fn-cap", []nodes.Node{node})
	sc := history.ScenarioFrom(telemetry.Package{}, false)
	key := history.SegmentKey(group, node.NodeID, sc)

	latencies := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	}
	for i, lat := range latencies {
		gw.recordAttempt(node, lat, nil, slo, "fn-cap", group, nil, true, "", 0, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})
		code := "POOL_EXHAUSTED"
		if i%2 == 1 {
			code = "QUEUE_FULL"
		}
		gw.recordAttempt(node, 2*time.Millisecond, capacityErr(t, code, node.NodeID), slo, "fn-cap", group, nil, true, "", 0, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})
	}

	count, p95, _, _, _, ok := h.StatsAndRecent(key, time.Minute, now)
	if !ok || count != len(latencies) {
		t.Fatalf("StatsAndRecent = (%d,%v), esperado (%d,true)", count, ok, len(latencies))
	}
	if p95 != 50 {
		t.Fatalf("p95 = %d, esperado 50 (máx dos 5 sucessos; n=5 => ceil(0.95*5)-1=4)", p95)
	}
	if got := invLogLines(t, logPath); got != 2*len(latencies) {
		t.Fatalf("invLog = %d linhas, esperado %d (5 sucesso + 5 rejeição)", got, 2*len(latencies))
	}

	edge := edgeHistory(t, now)
	edgeNode := nodes.Node{NodeID: "borda-9", Layer: nodes.LayerBorda}
	edgeGroup := selection.ResolveRequestGroup("fn-edge", []nodes.Node{edgeNode})
	edgeSLO := &selection.SLO{MaxLatencyMS: 300}
	for i := 0; i < 5; i++ {
		edge.gw.recordAttempt(edgeNode, 400*time.Millisecond, nil, edgeSLO, "fn-edge", edgeGroup, nil, true, "", 0, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})
	}
	cands := []nodes.Node{edgeNode}
	mon := monitor.NewMonitor()
	cfg := selection.DefaultRiskConfig()
	// Evidência aprendida no learner (n_eff=5 >= nmin=3): B(~400+) >
	// SLO 300 => candidato excluído pelo crivo de previsão e a fila chega
	// vazia (paridade PREDICTED_OVER_SLO do v1; o Handle entrega
	// 503/nuvem). O v1 decidia isso pelo gate ready do SegmentStats; o
	// RiskSelector decide pelo limite superior calibrado B.
	learner := learn.NewLearner(cfg.LearnConfig())
	for i := 0; i < 5; i++ {
		learner.Observe(edgeGroup, edgeNode.NodeID, learn.Obs{At: now, Outcome: learn.OutcomeSuccess, LatencyMS: 400, UseRegr: true, M: 1})
	}
	sel := &selection.RiskSelector{
		Base:             selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		Hist:             edge.h,
		ThroughputWindow: time.Minute,
		Criteria:         selection.DefaultCriteria,
		Clock:            fixedClock(now),
		Learner:          learner,
		Cfg:              cfg,
	}
	if got := sel.Select(cands, "fn-edge", edgeSLO); len(got) != 0 {
		t.Fatalf("Select com B> SLO = %d nós, esperado 0 (crivo de previsão)", len(got))
	}
	// Sem evidência (probation) o nó segue na fila por bootstrap — o RiskSelector
	// nunca bane nó frio: a exploração vive na probation ao fim da fila.
	sel.Learner = learn.NewLearner(cfg.LearnConfig())
	if got := sel.Select(cands, "fn-edge", edgeSLO); len(got) != 1 {
		t.Fatalf("Select em probation = %d nós, esperado 1 (bootstrap)", len(got))
	}
}

type edgeFixture struct {
	h  *history.History
	gw *Gateway
}

func edgeHistory(t *testing.T, now time.Time) edgeFixture {
	t.Helper()
	h, err := history.NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Monitor: monitor.NewMonitor(), Clock: fixedClock(now)})
	return edgeFixture{h: h, gw: gw}
}
