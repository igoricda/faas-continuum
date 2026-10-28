package gateway

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// TestLateColdStartApplied cobre o cold_start do late (selflearning-cold-
// contention, todo 4): o relatório cold marcar ColdStart=true na Entry, o
// que a exclui do p95/Count e a mantém em Recent/Total (AUD-01).
func TestLateColdStartApplied(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	gw := lateTestGateway(hist, monitor.NewMonitor(), nil)

	rep := LateReport{
		RequestID:         "req-cold",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 30000,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
		ColdStart:         true,
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}

	key := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	fs, ok := hist.StatsFull(key, 60*time.Second, time.Now())
	if !ok {
		t.Fatalf("sem entrada após late cold")
	}
	if fs.ColdN != 1 {
		t.Errorf("ColdN=%d, want 1 (cold_start não aplicado)", fs.ColdN)
	}
	if fs.Count != 0 {
		t.Errorf("Count=%d, want 0 (cold fora do p95/Count)", fs.Count)
	}
	if fs.Recent != 1 {
		t.Errorf("Recent=%d, want 1 (cold conta em Recent)", fs.Recent)
	}
	if fs.Total != 1 {
		t.Errorf("Total=%d, want 1", fs.Total)
	}
}

// TestLateColdAbsentBackCompat é a regressão de compatibilidade do fio: um
// payload antigo SEM a chave cold_start decodifica false e a Entry sai warm
// (comportamento pré-campo preservado).
func TestLateColdAbsentBackCompat(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	gw := lateTestGateway(hist, monitor.NewMonitor(), nil)

	wire := []byte(`{"request_id":"req-warm","attempt_n":1,"function":"f","node_id":"n1","outcome":"sucesso","observed_latency_ms":3200,` +
		`"started_at":"` + time.Now().Add(-2*time.Second).UTC().Format(time.RFC3339Nano) + `"}`)
	var rep LateReport
	if err := json.Unmarshal(wire, &rep); err != nil {
		t.Fatalf("decode fio antigo: %v", err)
	}
	if rep.ColdStart {
		t.Fatalf("ColdStart=true com chave ausente, want false (back-compat)")
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}

	key := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	fs, ok := hist.StatsFull(key, 60*time.Second, time.Now())
	if !ok {
		t.Fatalf("sem entrada após late warm")
	}
	if fs.ColdN != 0 {
		t.Errorf("ColdN=%d, want 0 (chave ausente não é cold)", fs.ColdN)
	}
	if fs.Count != 1 {
		t.Errorf("Count=%d, want 1", fs.Count)
	}
}

// TestLateSegmentInheritance cobre a herança de segmento (todo 4): o late
// do MESMO attempt do sync supersede a Entry síncrona no segmento que o sync
// usou (herdado via liveRef), mesmo quando o relatório, sozinho, cairia
// noutro bucket. Sem herança o late iria para 75+ e deixaria a Entry
// síncrona intacta (dupla contagem e segmento errado).
func TestLateSegmentInheritance(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	mon := monitor.NewMonitor()
	mon.Update(telemetry.Package{NodeID: "n1", Timestamp: time.Now(), CPUUsagePct: 5, MemoryUsagePct: 30})
	gw := lateTestGateway(hist, mon, nil)

	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	gw.RegisterLateGroup("req-seg", "f", slo)
	gw.recordAttempt(node, 50*time.Millisecond, nil, slo, "f", "f", nil, true, "req-seg", 1, attemptCapture{sc: history.ScenarioContext{LoadBucket: history.Bucket25_50}})

	syncKey := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.Bucket25_50})
	if _, _, _, _, _, ok := hist.StatsAndRecent(syncKey, 60*time.Second, time.Now()); !ok {
		t.Fatalf("sync não gravou no segmento 25-50")
	}

	rep := LateReport{
		RequestID:         "req-seg",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 3200,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
		CPUUsagePct:       f64ptr(80),
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}

	fs, ok := hist.StatsFull(syncKey, 60*time.Second, time.Now())
	if !ok {
		t.Fatalf("late não caiu no segmento síncrono (herança quebrada)")
	}
	if fs.Count != 1 {
		t.Errorf("Count=%d, want 1 (dupla contagem síncrona+late)", fs.Count)
	}
	if fs.P95MS != 3200 {
		t.Errorf("p95=%d, want 3200 (latência real herdada)", fs.P95MS)
	}

	wrongKey := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.Bucket75Plus})
	if _, _, _, _, _, ok := hist.StatsAndRecent(wrongKey, 60*time.Second, time.Now()); ok {
		t.Errorf("late caiu no bucket do lateScenario (75+) em vez de herdar o sync")
	}
}

// TestRecordLateOrphanScenario garante que um late ÓRFÃO (sem liveRef, sem
// attempt síncrono correlacionado) continua derivando o cenário real via
// lateScenario e gravando no bucket correspondente.
func TestRecordLateOrphanScenario(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	gw := lateTestGateway(hist, monitor.NewMonitor(), nil)

	rep := LateReport{
		RequestID:         "req-orphan",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 100,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
		CPUUsagePct:       f64ptr(80),
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}

	realKey := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.Bucket75Plus})
	fs, ok := hist.StatsFull(realKey, 60*time.Second, time.Now())
	if !ok || fs.Count != 1 {
		t.Errorf("late órfão count=%d ok=%v, want 1 true no bucket do relatório", fs.Count, ok)
	}
	unknownKey := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	if _, _, _, _, _, ok := hist.StatsAndRecent(unknownKey, 60*time.Second, time.Now()); ok {
		t.Errorf("late órfão caiu em unknown, want bucket real")
	}
}

func TestRecordLate(t *testing.T) {
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatal(err)
	}
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, History: hist})

	// Late sucesso at 3200ms with SLO 200ms: SloMet=false, ObservedLatencyMS=3200, Timestamp=started_at.
	startedAt := time.Now().Add(-2 * time.Second).UTC()
	rep := LateReport{
		RequestID:         "req-1",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 3200,
		StartedAt:         startedAt.Format(time.RFC3339Nano),
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}

	key := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	count, p95, recent, _, _, ok := hist.StatsAndRecent(key, 60*time.Second, time.Now())
	if !ok {
		t.Fatalf("no entry for key %q", key)
	}
	if count != 1 {
		t.Errorf("count=%d want 1", count)
	}
	if p95 != 3200 {
		t.Errorf("p95=%d want 3200", p95)
	}
	if recent != 1 {
		t.Errorf("recent=%d want 1", recent)
	}

	// Duplicate key: no double append.
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate duplicate: %v", err)
	}
	count, _, _, _, _, ok = hist.StatsAndRecent(key, 60*time.Second, time.Now())
	if !ok {
		t.Fatalf("no entry after duplicate")
	}
	if count != 1 {
		t.Errorf("count after duplicate=%d want 1", count)
	}

	// Invalid: negative latency.
	bad := rep
	bad.ObservedLatencyMS = -1
	if _, err := gw.RecordLate(bad); err == nil {
		t.Fatal("expected error for negative latency")
	}

	// Invalid: future started_at.
	future := rep
	future.StartedAt = time.Now().Add(time.Hour).Format(time.RFC3339Nano)
	if _, err := gw.RecordLate(future); err == nil {
		t.Fatal("expected error for future started_at")
	}
}

// TestLatePSI cobre o fio tardio de PSI (selflearning-telemetry-seeded-
// history, todo 8): LateReport aditivo omitempty, herança correlacionada via
// take único (liveRef vence o corpo, consistente com D12), derivação órfã
// (corpo > mon.Latest > unknown, sem panic) e escrita 1x sob retry
// concorrente. InstanceID do late segue "" e dedup (request_id,attempt_n)
// + TTL 5m + bound 10k ficam intocados.
func TestLatePSI(t *testing.T) {
	newCorrelatedGW := func(t *testing.T, mon *monitor.Monitor) (*Gateway, *history.History, string) {
		t.Helper()
		hist, err := history.NewHistory("", 100)
		if err != nil {
			t.Fatal(err)
		}
		ns, err := nodes.NewNodeStore("")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}); err != nil {
			t.Fatal(err)
		}
		gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, History: hist})
		logPath := t.TempDir() + "/inv.jsonl"
		invLog, err := NewInvocationLog(logPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { invLog.Close() })
		gw.SetInvocationLog(invLog)
		return gw, hist, logPath
	}
	syncWithPSI := func(t *testing.T, gw *Gateway) history.ScenarioContext {
		t.Helper()
		node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
		slo := &selection.SLO{MaxLatencyMS: 5000}
		sc := history.ScenarioFrom(telemetry.Package{}, false)
		psiBlob := loadMetricsFixture(t, "execution_metrics_with_psi.json")
		gw.RegisterLateGroup("req-psi", "f", slo)
		gw.recordAttempt(node, 120*time.Millisecond, nil, slo, "f", "f", psiBlob, true, "req-psi", 1,
			attemptCapture{sc: sc, concrete: "f"})
		return sc
	}
	lateStartedAt := func() string {
		return time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano)
	}

	t.Run("correlated inherits PSI via single take", func(t *testing.T) {
		mon := monitor.NewMonitor()
		gw, hist, logPath := newCorrelatedGW(t, mon)
		sc := syncWithPSI(t, gw)

		rep := LateReport{
			RequestID:         "req-psi",
			AttemptN:          1,
			Function:          "f",
			NodeID:            "n1",
			Outcome:           history.OutcomeSucesso,
			ObservedLatencyMS: 3200,
			StartedAt:         lateStartedAt(),
		}
		applied, err := gw.RecordLate(rep)
		if err != nil {
			t.Fatalf("RecordLate: %v", err)
		}
		if !applied {
			t.Fatalf("applied=false, want true (primeiro late do par)")
		}
		// Mesmo segmento, sem dupla contagem, nas duas chaves do fan-out.
		now := time.Now()
		for _, key := range []string{
			history.SegmentKey("f", "n1", sc),
			history.AggLayerKey("f", nodes.LayerBorda, sc),
		} {
			count, _, _, _, _, ok := hist.StatsAndRecent(key, time.Hour, now)
			if !ok {
				t.Fatalf("sem entrada em %q após late", key)
			}
			if count != 1 {
				t.Errorf("count=%d em %q, want 1 (dual-key supersede quebrado)", count, key)
			}
		}
		// PSI herdado do sync (12.5/8.25 do piggyback with_psi) via liveRef.
		recs := readInvRecords(t, logPath)
		if len(recs) != 2 {
			t.Fatalf("invLog records=%d, want 2 (sync+late)", len(recs))
		}
		late := recs[1].Entry
		if late.PSICpuSomeAvg10 != 12.5 || late.PSIMemFullAvg10 != 8.25 {
			t.Errorf("late PSI=(%v,%v), want (12.5,8.25) herdado via liveRef", late.PSICpuSomeAvg10, late.PSIMemFullAvg10)
		}
		if late.InstanceID != "" {
			t.Errorf("late InstanceID=%q, want \"\"", late.InstanceID)
		}
		// Take único: segundo consumo miss; retry é dedup (applied=false).
		if _, ok := gw.liveIdx.take("req-psi", 1); ok {
			t.Errorf("segundo take devolveu liveRef (take único quebrado)")
		}
		if applied, err := gw.RecordLate(rep); err != nil || applied {
			t.Errorf("retry RecordLate=(%v,%v), want (false,nil) dedup", applied, err)
		}
	})

	t.Run("liveRef wins over body PSI", func(t *testing.T) {
		mon := monitor.NewMonitor()
		gw, _, logPath := newCorrelatedGW(t, mon)
		syncWithPSI(t, gw)

		rep := LateReport{
			RequestID:         "req-psi",
			AttemptN:          1,
			Function:          "f",
			NodeID:            "n1",
			Outcome:           history.OutcomeSucesso,
			ObservedLatencyMS: 3200,
			StartedAt:         lateStartedAt(),
			PSICpuSomeAvg10:   99,
			PSIMemFullAvg10:   99,
		}
		if _, err := gw.RecordLate(rep); err != nil {
			t.Fatalf("RecordLate: %v", err)
		}
		recs := readInvRecords(t, logPath)
		late := recs[1].Entry
		if late.PSICpuSomeAvg10 != 12.5 || late.PSIMemFullAvg10 != 8.25 {
			t.Errorf("late PSI=(%v,%v), want (12.5,8.25): liveRef vence o corpo (D12)", late.PSICpuSomeAvg10, late.PSIMemFullAvg10)
		}
	})

	t.Run("orphan prefers body then monitor then unknown", func(t *testing.T) {
		// Corpo presente: vence mesmo com monitor populado.
		mon := monitor.NewMonitor()
		mon.Update(telemetry.Package{NodeID: "n1", Timestamp: time.Now(),
			CPUPressure:        &telemetry.PressureFields{Avg10: 9.5},
			MemoryPressureFull: &telemetry.PressureFields{Avg10: 4.5}})
		gw, _, logPath := newCorrelatedGW(t, mon)
		rep := LateReport{
			RequestID: "req-orphan-body", AttemptN: 1, Function: "f", NodeID: "n1",
			Outcome: history.OutcomeSucesso, ObservedLatencyMS: 100, StartedAt: lateStartedAt(),
			PSICpuSomeAvg10: 7.5, PSIMemFullAvg10: 3.25,
		}
		if _, err := gw.RecordLate(rep); err != nil {
			t.Fatalf("RecordLate órfão com corpo: %v", err)
		}
		if got := readInvRecords(t, logPath)[0].Entry; got.PSICpuSomeAvg10 != 7.5 || got.PSIMemFullAvg10 != 3.25 {
			t.Errorf("órfão com corpo PSI=(%v,%v), want (7.5,3.25)", got.PSICpuSomeAvg10, got.PSIMemFullAvg10)
		}

		// Sem corpo: deriva de mon.Latest.
		gw2, _, logPath2 := newCorrelatedGW(t, mon)
		rep2 := LateReport{
			RequestID: "req-orphan-mon", AttemptN: 1, Function: "f", NodeID: "n1",
			Outcome: history.OutcomeSucesso, ObservedLatencyMS: 100, StartedAt: lateStartedAt(),
		}
		if _, err := gw2.RecordLate(rep2); err != nil {
			t.Fatalf("RecordLate órfão via monitor: %v", err)
		}
		if got := readInvRecords(t, logPath2)[0].Entry; got.PSICpuSomeAvg10 != 9.5 || got.PSIMemFullAvg10 != 4.5 {
			t.Errorf("órfão via monitor PSI=(%v,%v), want (9.5,4.5)", got.PSICpuSomeAvg10, got.PSIMemFullAvg10)
		}

		// Sem corpo nem telemetria: unknown (zeros), sem panic.
		gw3, hist3, logPath3 := newCorrelatedGW(t, monitor.NewMonitor())
		rep3 := LateReport{
			RequestID: "req-orphan-unknown", AttemptN: 1, Function: "f", NodeID: "n1",
			Outcome: history.OutcomeSucesso, ObservedLatencyMS: 100, StartedAt: lateStartedAt(),
		}
		if _, err := gw3.RecordLate(rep3); err != nil {
			t.Fatalf("RecordLate órfão unknown: %v", err)
		}
		got3 := readInvRecords(t, logPath3)[0].Entry
		if got3.PSICpuSomeAvg10 != 0 || got3.PSIMemFullAvg10 != 0 {
			t.Errorf("órfão unknown PSI=(%v,%v), want (0,0)", got3.PSICpuSomeAvg10, got3.PSIMemFullAvg10)
		}
		if got3.InstanceID != "" {
			t.Errorf("órfão InstanceID=%q, want \"\"", got3.InstanceID)
		}
		unknownKey := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
		if count, _, _, _, _, ok := hist3.StatsAndRecent(unknownKey, time.Hour, time.Now()); !ok || count != 1 {
			t.Errorf("órfão unknown caiu fora do unknown (ok=%v count=%d)", ok, count)
		}
	})

	t.Run("old wire without PSI keys stays valid", func(t *testing.T) {
		var rep LateReport
		if err := json.Unmarshal([]byte(`{"request_id":"req-old","attempt_n":1,"function":"f","node_id":"n1","outcome":"sucesso","observed_latency_ms":100,"started_at":"2026-09-22T22:00:00Z"}`), &rep); err != nil {
			t.Fatalf("unmarshal fio antigo: %v", err)
		}
		if rep.PSICpuSomeAvg10 != 0 || rep.PSIMemFullAvg10 != 0 {
			t.Errorf("fio antigo PSI=(%v,%v), want (0,0)", rep.PSICpuSomeAvg10, rep.PSIMemFullAvg10)
		}
		rep.StartedAt = lateStartedAt()
		gw, hist, _ := newCorrelatedGW(t, monitor.NewMonitor())
		if _, err := gw.RecordLate(rep); err != nil {
			t.Fatalf("RecordLate fio antigo: %v", err)
		}
		unknownKey := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
		if count, _, _, _, _, ok := hist.StatsAndRecent(unknownKey, time.Hour, time.Now()); !ok || count != 1 {
			t.Errorf("fio antigo caiu fora do unknown (ok=%v count=%d)", ok, count)
		}
	})

	t.Run("concurrent retry writes once", func(t *testing.T) {
		hist, err := history.NewHistory("", 100)
		if err != nil {
			t.Fatal(err)
		}
		gw := lateTestGateway(hist, monitor.NewMonitor(), nil)
		rep := LateReport{
			RequestID: "req-race", AttemptN: 1, Function: "f", NodeID: "n1",
			Outcome: history.OutcomeSucesso, ObservedLatencyMS: 100, StartedAt: lateStartedAt(),
		}
		const n = 16
		var wg sync.WaitGroup
		applied := make([]bool, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ok, err := gw.RecordLate(rep)
				if err != nil {
					t.Errorf("RecordLate concorrente: %v", err)
					return
				}
				applied[i] = ok
			}(i)
		}
		wg.Wait()
		wins := 0
		for _, ok := range applied {
			if ok {
				wins++
			}
		}
		if wins != 1 {
			t.Errorf("applied=%d, want exatamente 1 (dedup sob -race)", wins)
		}
		unknownKey := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
		if count, _, _, _, _, ok := hist.StatsAndRecent(unknownKey, time.Hour, time.Now()); !ok || count != 1 {
			t.Errorf("concorrente caiu fora do unknown/1 (ok=%v count=%d)", ok, count)
		}
	})
}
