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

// TestLateSupersedesLiveSameAttempt covers an attempt that exceeds the budget,
// where the live path records a timeout with a truncated latency, and the node
// concludes later, where the late report records a success with the real latency.
// Without the supersede a single execution counts twice in the count and in the
// percentile, and with it the execution counts once with the real data.
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
	// Live: the budget timed out, with a truncated latency of two hundred milliseconds.
	gw.recordAttempt(node, 200*time.Millisecond, context.DeadlineExceeded, slo, "f", "f", nil, true, "req-live", 1, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})

	key := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	// The live timeout is recorded in the segment as training data, which the late
	// report may supersede, but the count and the percentile require a success, so
	// the segment stays unready until the late report arrives. The fan-out by layer
	// records the primary key and the aggregated key of the edge, which gives two
	// keys.
	if len(hist.Keys()) != 2 {
		t.Fatalf("live: keys=%d, want 2 (primária + agregada por layer)", len(hist.Keys()))
	}
	if count, _, _, _, _, ok := hist.StatsAndRecent(key, 60*time.Second, time.Now()); ok || count != 0 {
		t.Fatalf("live: count=%d ok=%v, want 0 false (timeout fora do Count)", count, ok)
	}

	// Late: a success with the real latency of thirty-two hundred milliseconds.
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

	// The objective flag is derived from the latency: a success within the objective is met.
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

// TestLateDoubleSuccessCountedOnce is the strong regression of the supersede: if
// the live path and the late report of the same attempt recorded two successful
// entries, the count, the percentile and the recent rate would double and the
// segment would become ready twice as fast. With the supersede a single execution
// counts once with the real data of the late report.
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
	// Live: a success with a truncated latency of fifty milliseconds, since the budget expired at the edge of the objective.
	gw.recordAttempt(node, 50*time.Millisecond, nil, slo, "f", "f", nil, true, "req-dup", 1, attemptCapture{sc: history.ScenarioFrom(telemetry.Package{}, false)})
	// Late: the same attempt concluded with the real latency of thirty-two hundred milliseconds.
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

// TestLateSloMetFalseWhenOverSLO covers the other half: a late success above
// the objective derives an unmet objective, while before the flag was false for
// everything, including a success within the objective.
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

// TestConcurrentDuplicateLate covers a retry of the node, where the answer of the
// first post is lost and the sender repeats it, with several concurrent reports
// of the same pair, which have to record only once. It requires the race detector.
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

// TestLateTrainsRealSegment covers the training of the real segment: the late
// report trains the buckets of the node, which the selection can read, and never
// the unknown ones, and the percentile of the report wins over the telemetry.
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

	// A report with its own percentile wins over the telemetry of the monitor.
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

	// Readable by the selector: the real history, fed by the late report, leaves
	// the node viable in the probation of the risk selector, so it stays selectable.
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

// TestLateClockInjectable covers the late half of the injectable clock: the start
// instant is judged against the clock of the gateway and never against the wall
// clock, so a start in the future of the fake clock, but in the past of the wall
// clock, is rejected, while the opposite is accepted.
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
