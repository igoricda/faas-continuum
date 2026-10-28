// Contract tests for observed node cpu/mem ingestion
// (selflearning-telemetry-seeded-history, usage-curve fallback): the
// dispatch-time pkg.CPUUsagePct/MemoryUsagePct ride attemptCapture into
// history.Entry.ObservedCPUPct/ObservedMemPct + liveRef, and the late path
// inherits via single take or falls back (corpo > mon.Latest > 0).
// Mirrors psi_contract_test.go + TestLatePSI; 0 = unknown on every axis.
package gateway

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func observedTestGateway(t *testing.T, mon *monitor.Monitor) (*Gateway, *history.History, string) {
	t.Helper()
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	gw := lateTestGateway(hist, mon, nil)
	logPath := filepath.Join(t.TempDir(), "inv.jsonl")
	invLog, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { invLog.Close() })
	gw.SetInvocationLog(invLog)
	return gw, hist, logPath
}

// Piggyback values stamped: capture rides VERBATIM onto the Entry and the
// liveRef (metrics==nil tolerated — observed comes from the dispatch
// snapshot, never re-decoded).
func TestObservedContract_SyncStampsCapture(t *testing.T) {
	mon := monitor.NewMonitor()
	gw, _, logPath := observedTestGateway(t, mon)
	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	sc := history.ScenarioFrom(telemetry.Package{}, false)

	gw.recordAttempt(node, 50*time.Millisecond, nil, slo, "f", "f", nil, true, "req-obs", 1,
		attemptCapture{sc: sc, inflight: 1, projected: 42, observedCPU: 42.5, observedMem: 61.25, concrete: "f"})

	recs := readInvRecords(t, logPath)
	if len(recs) != 1 {
		t.Fatalf("invLog records=%d, want 1", len(recs))
	}
	if recs[0].Entry.ObservedCPUPct != 42.5 || recs[0].Entry.ObservedMemPct != 61.25 {
		t.Errorf("Entry observed=(%v,%v), want (42.5,61.25)",
			recs[0].Entry.ObservedCPUPct, recs[0].Entry.ObservedMemPct)
	}
	ref, ok := gw.liveIdx.take("req-obs", 1)
	if !ok {
		t.Fatalf("liveRef miss, want hit")
	}
	if ref.observedCPU != 42.5 || ref.observedMem != 61.25 {
		t.Errorf("liveRef observed=(%v,%v), want (42.5,61.25)", ref.observedCPU, ref.observedMem)
	}
}

// Legacy zero-value capture + nil metrics => zeros, never panic.
func TestObservedContract_NilMetricsIsUnknown(t *testing.T) {
	mon := monitor.NewMonitor()
	gw, _, logPath := observedTestGateway(t, mon)
	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	sc := history.ScenarioFrom(telemetry.Package{}, false)

	gw.recordAttempt(node, 50*time.Millisecond, nil, slo, "f", "f", nil, true, "req-zero", 1,
		attemptCapture{sc: sc, concrete: "f"})

	recs := readInvRecords(t, logPath)
	if len(recs) != 1 {
		t.Fatalf("invLog records=%d, want 1", len(recs))
	}
	if recs[0].Entry.ObservedCPUPct != 0 || recs[0].Entry.ObservedMemPct != 0 {
		t.Errorf("Entry observed=(%v,%v), want (0,0)",
			recs[0].Entry.ObservedCPUPct, recs[0].Entry.ObservedMemPct)
	}
}

// Correlated late inherits observed via single take; liveRef wins over the
// body; second take misses (double-take still impossible).
func TestObservedContract_LateInheritsViaSingleTake(t *testing.T) {
	mon := monitor.NewMonitor()
	gw, _, logPath := observedTestGateway(t, mon)
	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	sc := history.ScenarioFrom(telemetry.Package{}, false)

	gw.RegisterLateGroup("req-obs", "f", slo)
	gw.recordAttempt(node, 120*time.Millisecond, nil, slo, "f", "f", nil, true, "req-obs", 1,
		attemptCapture{sc: sc, inflight: 1, projected: 30, observedCPU: 33.5, observedMem: 44.25, concrete: "f"})

	rep := LateReport{
		RequestID:         "req-obs",
		AttemptN:          1,
		Function:          "f",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 3200,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
		CPUUsagePct:       f64ptr(99),
		MemoryUsagePct:    f64ptr(99),
	}
	applied, err := gw.RecordLate(rep)
	if err != nil {
		t.Fatalf("RecordLate: %v", err)
	}
	if !applied {
		t.Fatalf("applied=false, want true")
	}
	recs := readInvRecords(t, logPath)
	if len(recs) != 2 {
		t.Fatalf("invLog records=%d, want 2 (sync+late)", len(recs))
	}
	late := recs[1].Entry
	if late.ObservedCPUPct != 33.5 || late.ObservedMemPct != 44.25 {
		t.Errorf("late observed=(%v,%v), want (33.5,44.25): liveRef vence o corpo",
			late.ObservedCPUPct, late.ObservedMemPct)
	}
	if _, ok := gw.liveIdx.take("req-obs", 1); ok {
		t.Errorf("segundo take devolveu liveRef (take único quebrado)")
	}
}

// Orphan fallback chain: corpo > mon.Latest > 0, per-axis.
func TestObservedContract_OrphanFallbackChain(t *testing.T) {
	lateStartedAt := func() string {
		return time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano)
	}

	t.Run("body wins", func(t *testing.T) {
		gw, _, logPath := observedTestGateway(t, monitor.NewMonitor())
		rep := LateReport{
			RequestID: "req-body", AttemptN: 1, Function: "f", NodeID: "n1",
			Outcome: history.OutcomeSucesso, ObservedLatencyMS: 100,
			StartedAt:      lateStartedAt(),
			CPUUsagePct:    f64ptr(55.5),
			MemoryUsagePct: f64ptr(66.5),
		}
		if _, err := gw.RecordLate(rep); err != nil {
			t.Fatalf("RecordLate: %v", err)
		}
		late := readInvRecords(t, logPath)[0].Entry
		if late.ObservedCPUPct != 55.5 || late.ObservedMemPct != 66.5 {
			t.Errorf("orphan observed=(%v,%v), want (55.5,66.5) do corpo",
				late.ObservedCPUPct, late.ObservedMemPct)
		}
	})

	t.Run("monitor fills absent axis", func(t *testing.T) {
		mon := monitor.NewMonitor()
		mon.Update(telemetry.Package{NodeID: "n1", CPUUsagePct: 21.5, MemoryUsagePct: 32.5})
		gw, _, logPath := observedTestGateway(t, mon)
		rep := LateReport{
			RequestID: "req-mon", AttemptN: 1, Function: "f", NodeID: "n1",
			Outcome: history.OutcomeSucesso, ObservedLatencyMS: 100,
			StartedAt:      lateStartedAt(),
			MemoryUsagePct: f64ptr(77),
		}
		if _, err := gw.RecordLate(rep); err != nil {
			t.Fatalf("RecordLate: %v", err)
		}
		late := readInvRecords(t, logPath)[0].Entry
		if late.ObservedCPUPct != 21.5 || late.ObservedMemPct != 77 {
			t.Errorf("orphan observed=(%v,%v), want (21.5,77): cpu do mon, mem do corpo",
				late.ObservedCPUPct, late.ObservedMemPct)
		}
	})

	t.Run("no data is unknown", func(t *testing.T) {
		gw, _, logPath := observedTestGateway(t, monitor.NewMonitor())
		rep := LateReport{
			RequestID: "req-unknown", AttemptN: 1, Function: "f", NodeID: "n1",
			Outcome: history.OutcomeSucesso, ObservedLatencyMS: 100,
			StartedAt: lateStartedAt(),
		}
		if _, err := gw.RecordLate(rep); err != nil {
			t.Fatalf("RecordLate: %v", err)
		}
		late := readInvRecords(t, logPath)[0].Entry
		if late.ObservedCPUPct != 0 || late.ObservedMemPct != 0 {
			t.Errorf("orphan observed=(%v,%v), want (0,0)",
				late.ObservedCPUPct, late.ObservedMemPct)
		}
	})
}
