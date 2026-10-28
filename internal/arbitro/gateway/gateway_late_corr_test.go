package gateway

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func f64ptr(v float64) *float64 { return &v }

func lateTestGateway(hist *history.History, mon *monitor.Monitor, clock func() time.Time) *Gateway {
	ns, err := nodes.NewNodeStore("")
	if err != nil {
		panic(err)
	}
	return NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, History: hist, Clock: clock})
}

// TestLateSupersedesLiveSameAttempt cobre o AUD-02: o attempt estoura o
// budget (live grava timeout com latência truncada) e o nó conclui depois
// (late grava sucesso com a latência real). Sem supersede, 1 execução conta
// 2x em Count/p95; com supersede, conta 1x com os dados reais.
func TestLateSupersedesLiveSameAttempt(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	mon := monitor.NewMonitor()
	gw := lateTestGateway(hist, mon, nil)
	logPath := filepath.Join(t.TempDir(), "inv.jsonl")
	invLog, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer invLog.Close()
	gw.SetInvocationLog(invLog)

	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	gw.RegisterLateGroup("req-live", "f", slo)
	// Live: timeout do budget, latência truncada 200ms.
	gw.recordAttempt(node, 200*time.Millisecond, context.DeadlineExceeded, slo, "f", "f", nil, true, "req-live", 1, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})

	key := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	// Interação AUD-01×AUD-02: o live timeout é gravado no segmento (dado
	// de treino, supersedível pelo late), mas Count/p95 exigem sucesso
	// (p95-sobre-sucessos) — segmento segue !ready até o late chegar.
	// Fan-out por camada: nó borda grava primária + agregada => 2 chaves.
	if len(hist.Keys()) != 2 {
		t.Fatalf("live: keys=%d, want 2 (primária + agregada por layer)", len(hist.Keys()))
	}
	if count, _, _, _, _, ok := hist.StatsAndRecent(key, 60*time.Second, time.Now()); ok || count != 0 {
		t.Fatalf("live: count=%d ok=%v, want 0 false (timeout fora do Count)", count, ok)
	}

	// Late: sucesso com a latência real 3200ms.
	rep := LateReport{
		RequestID:         "req-live",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 3200,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}

	count, p95, recent, _, _, ok := hist.StatsAndRecent(key, 60*time.Second, time.Now())
	if !ok {
		t.Fatalf("sem entrada após late")
	}
	if count != 1 {
		t.Errorf("AUD-02: count=%d após live+late, want 1 (dupla contagem)", count)
	}
	if p95 != 3200 {
		t.Errorf("p95=%d, want 3200 (latência real do late)", p95)
	}
	if recent != 1 {
		t.Errorf("recent=%d, want 1", recent)
	}

	// SloMet derivado (AUD-03): sucesso 3200ms dentro do SLO 5000ms => true.
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("invLog tem %d linhas, want 2 (live + late)", len(lines))
	}
	var late InvocationRecord
	if err := json.Unmarshal([]byte(lines[1]), &late); err != nil {
		t.Fatal(err)
	}
	if !late.SloMet {
		t.Errorf("AUD-03: late SloMet=false, want true (sucesso 3200ms <= SLO 5000ms)")
	}
	if late.SLO == nil || late.SLO.MaxLatencyMS != 5000 {
		t.Errorf("invLog do late deveria carregar o SLO do invoke")
	}
}

// TestLateDoubleSuccessCountedOnce é a regressão forte do AUD-02: se o
// live e o late do mesmo attempt gravassem 2 Entries de sucesso, Count,
// p95 e RecentRPS dobrariam (MinSamples 2x mais rápido). Com supersede,
// 1 execução conta 1x com os dados reais do late. Falha sem o fix
// (contaria 2).
func TestLateDoubleSuccessCountedOnce(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	mon := monitor.NewMonitor()
	gw := lateTestGateway(hist, mon, nil)

	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	gw.RegisterLateGroup("req-dup", "f", slo)
	// Live: sucesso com latência truncada 50ms (budget estourou na borda
	// do SLO, mas a Entry síncrona existe para este attempt).
	gw.recordAttempt(node, 50*time.Millisecond, nil, slo, "f", "f", nil, true, "req-dup", 1, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})
	// Late: o mesmo attempt concluiu com a latência real 3200ms.
	rep := LateReport{
		RequestID:         "req-dup",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 3200,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}

	key := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	count, p95, recent, _, _, ok := hist.StatsAndRecent(key, 60*time.Second, time.Now())
	if !ok {
		t.Fatalf("sem entrada após live+late")
	}
	if count != 1 {
		t.Errorf("AUD-02: count=%d, want 1 (1 execução, 1 Entry)", count)
	}
	if p95 != 3200 {
		t.Errorf("p95=%d, want 3200 (latência real do late)", p95)
	}
	if recent != 1 {
		t.Errorf("recent=%d, want 1 (RecentRPS dobrado sem supersede)", recent)
	}
}

// TestLateSloMetFalseWhenOverSLO cobre a outra perna do AUD-03: sucesso
// tardio acima do SLO deriva SloMet=false (antes era false fixo para tudo,
// inclusive sucesso dentro do SLO).
func TestLateSloMetFalseWhenOverSLO(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	mon := monitor.NewMonitor()
	gw := lateTestGateway(hist, mon, nil)
	logPath := filepath.Join(t.TempDir(), "inv.jsonl")
	invLog, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer invLog.Close()
	gw.SetInvocationLog(invLog)

	gw.RegisterLateGroup("req-slow", "f", &selection.SLO{MaxLatencyMS: 1000})
	rep := LateReport{
		RequestID:         "req-slow",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 3200,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	var rec InvocationRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(raw))), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.SloMet {
		t.Errorf("AUD-03: late SloMet=true, want false (3200ms > SLO 1000ms)")
	}
}

// TestConcurrentDuplicateLate cobre o AUD-25: retry do nó (resposta do 1º
// POST perdida, late_sender reenvia) com N RecordLate concorrentes do mesmo
// par só pode gravar 1x. Exige -race no pacote.
func TestConcurrentDuplicateLate(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	mon := monitor.NewMonitor()
	gw := lateTestGateway(hist, mon, nil)

	rep := LateReport{
		RequestID:         "req-race",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 3200,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
	}
	const n = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := gw.RecordLate(rep); err != nil {
				t.Errorf("RecordLate concorrente: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	key := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	count, _, _, _, _, ok := hist.StatsAndRecent(key, 60*time.Second, time.Now())
	if !ok || count != 1 {
		t.Errorf("AUD-25: count=%d ok=%v após %d lates concorrentes, want 1 true", count, ok, n)
	}
	_, received, dropped, _ := gw.Counters()
	if received != 1 {
		t.Errorf("lateReceived=%d, want 1", received)
	}
	if dropped != n-1 {
		t.Errorf("lateDropped=%d, want %d", dropped, n-1)
	}
}

// TestLateTrainsRealSegment cobre o AUD-03: o late treina o segmento com os
// buckets reais do nó (legível pelo Select), não unknown/unknown; pct no
// relatório tem precedência sobre a telemetria.
func TestLateTrainsRealSegment(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	mon := monitor.NewMonitor()
	mon.Update(telemetry.Package{NodeID: "n1", Timestamp: time.Now(), CPUUsagePct: 5, MemoryUsagePct: 30})
	gw := lateTestGateway(hist, mon, nil)

	rep := LateReport{
		RequestID:         "req-buckets",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 100,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}

	realKey := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.Bucket25_50})
	if count, _, _, _, _, ok := hist.StatsAndRecent(realKey, 60*time.Second, time.Now()); !ok || count != 1 {
		t.Errorf("AUD-03: segmento real count=%d ok=%v, want 1 true", count, ok)
	}
	unknownKey := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	if _, _, _, _, _, ok := hist.StatsAndRecent(unknownKey, 60*time.Second, time.Now()); ok {
		t.Errorf("AUD-03: late caiu em unknown/unknown, want só segmento real")
	}

	// Relatório com pct próprio vence a telemetria do monitor.
	rep2 := rep
	rep2.RequestID = "req-override"
	rep2.CPUUsagePct = f64ptr(80)
	if _, err := gw.RecordLate(rep2); err != nil {
		t.Fatalf("RecordLate override: %v", err)
	}
	overrideKey := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.Bucket75Plus})
	if count, _, _, _, _, ok := hist.StatsAndRecent(overrideKey, 60*time.Second, time.Now()); !ok || count != 1 {
		t.Errorf("pct do relatório deveria vencer: count=%d ok=%v, want 1 true", count, ok)
	}

	// Legível pelo Select: o histórico real (alimentado pelo late) deixa o
	// nó viável/probation no RiskSelector — o nó segue selecionável.
	sel := &selection.RiskSelector{
		Base:             selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		Hist:             hist,
		Criteria:         selection.DefaultCriteria,
		ThroughputWindow: 60 * time.Second,
		Clock:            time.Now,
		Learner:          learn.NewLearner(selection.DefaultRiskConfig().LearnConfig()),
		Cfg:              selection.DefaultRiskConfig(),
	}
	queue := sel.Select([]nodes.Node{{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}}, "f", &selection.SLO{MaxLatencyMS: 8000})
	found := false
	for _, n := range queue {
		if n.NodeID == "n1" {
			found = true
		}
	}
	if !found {
		t.Errorf("nó n1 fora da fila do Select após late no segmento real")
	}
}

// TestLateClockInjectable cobre a parte late do AUD-29: started_at é
// julgado contra g.clock(), nunca wall. started_at no futuro do clock
// fake (mas passado no wall) rejeita; no passado do clock fake (mas
// futuro no wall) aceita.
func TestLateClockInjectable(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	mon := monitor.NewMonitor()
	wall := time.Now()
	gw := lateTestGateway(hist, mon, func() time.Time { return wall.Add(-time.Hour) })

	futureForClock := LateReport{
		RequestID:         "req-clock-future",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 100,
		StartedAt:         wall.Add(-time.Hour).Add(time.Second).UTC().Format(time.RFC3339Nano),
	}
	if _, err := gw.RecordLate(futureForClock); err == nil {
		t.Errorf("AUD-29: started_at no futuro do clock deveria rejeitar")
	}

	hist2, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	gw2 := lateTestGateway(hist2, mon, func() time.Time { return wall.Add(2 * time.Hour) })
	pastForClock := LateReport{
		RequestID:         "req-clock-past",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 100,
		StartedAt:         wall.Add(time.Hour).UTC().Format(time.RFC3339Nano),
	}
	if _, err := gw2.RecordLate(pastForClock); err != nil {
		t.Errorf("AUD-29: started_at no passado do clock deveria aceitar: %v", err)
	}
}
