package selection

// Tests of the veto by full pressure: the veto is preserved and the behavior is
// fail-open. The tests were written before the implementation existed, and they
// only pass after it.
//
// The conventions of the neighboring tests are reused, so nothing is
// reimplemented: the veto by rate and the revival are exercised through the
// existing paths, which are the veto by rate of the history, still consumed by
// the learned queue ceiling, and the fail-open of the pressure veto in the risk
// selector.

import (
	"math"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// TestPSIVetoFull_MemFullAboveThresholdVetoes is the first case: a full memory
// pressure of twenty exceeds the default threshold, so the node is vetoed even
// with a low processor pressure.
func TestPSIVetoFull_MemFullAboveThresholdVetoes(t *testing.T) {
	th := DefaultPSIVetoThresholds()
	if !PSIVetoFull(5.0, 20.0, true, th) {
		t.Fatalf("mem_full=20 deveria vetar (T_full=%.2f)", th.MemFullAvg10)
	}
}

// TestPSIVetoFull_CPUSomeAboveThresholdVetoes covers the other arm of the
// disjunction: a high processor pressure vetoes even with a low memory
// pressure, and the comparison is strict, as in the overloaded filter.
func TestPSIVetoFull_CPUSomeAboveThresholdVetoes(t *testing.T) {
	th := DefaultPSIVetoThresholds()
	if !PSIVetoFull(35.0, 2.0, true, th) {
		t.Fatalf("cpu_some=35 deveria vetar (T_cpu=%.2f)", th.CPUSomeAvg10)
	}
	if PSIVetoFull(th.CPUSomeAvg10, th.MemFullAvg10, true, th) {
		t.Fatalf("valores EXATAMENTE no limiar não vetam (> estrito)")
	}
	if PSIVetoFull(5.0, 2.0, true, th) {
		t.Fatalf("PSI limpo não deveria vetar")
	}
}

// TestPSIVetoFull_FailOpenOnUnknown is the second case: a nil, stale or
// unknown pressure never vetoes alone, which is the fail-open convention where
// zero means unknown.
func TestPSIVetoFull_FailOpenOnUnknown(t *testing.T) {
	th := DefaultPSIVetoThresholds()
	cases := map[string]struct {
		cpu, mem float64
		hasPSI   bool
	}{
		"hasPSI=false (nil/stale)":    {cpu: 99, mem: 99, hasPSI: false},
		"zeros (0=unknown)":           {cpu: 0, mem: 0, hasPSI: true},
		"cpu unknown, mem baixo":      {cpu: 0, mem: 2.0, hasPSI: true},
		"NaN (leitura corrompida)":    {cpu: math.NaN(), mem: math.NaN(), hasPSI: true},
		"cpu NaN, mem baixo":          {cpu: math.NaN(), mem: 2.0, hasPSI: true},
		"hasPSI=false com zeros":      {cpu: 0, mem: 0, hasPSI: false},
		"negativos (impossível, PSI)": {cpu: -1, mem: -1, hasPSI: true},
	}
	for name, tc := range cases {
		if PSIVetoFull(tc.cpu, tc.mem, tc.hasPSI, th) {
			t.Errorf("%s: PSI desconhecido vetou (fail-open violado)", name)
		}
	}
}

// TestPSIVetoFull_FromPackage exercises the adapter over the real telemetry: a
// nil reading or an unavailable one is fail-open, while a full memory pressure
// of twenty vetoes.
func TestPSIVetoFull_FromPackage(t *testing.T) {
	th := DefaultPSIVetoThresholds()
	clean := telemetry.Package{NodeID: "n1", CPUUsagePct: 10, MemoryUsagePct: 10}
	if PSIVetoFullFromPackage(clean, true, th) {
		t.Fatalf("pacote sem campos PSI (modo compatibilidade) não deveria vetar")
	}
	if PSIVetoFullFromPackage(clean, false, th) {
		t.Fatalf("ok=false não deveria vetar")
	}
	loaded := telemetry.Package{
		NodeID:             "n1",
		CPUUsagePct:        10,
		MemoryUsagePct:     10,
		CPUPressure:        &telemetry.PressureFields{Avg10: 5.0},
		MemoryPressureFull: &telemetry.PressureFields{Avg10: 20.0},
	}
	if !PSIVetoFullFromPackage(loaded, true, th) {
		t.Fatalf("mem_full=20 no pacote deveria vetar")
	}
	onlyCPU := telemetry.Package{
		NodeID:      "n1",
		CPUPressure: &telemetry.PressureFields{Avg10: 5.0},
	}
	if PSIVetoFullFromPackage(onlyCPU, true, th) {
		t.Fatalf("só cpu_some=5 (sem mem_full) não deveria vetar")
	}
}

// TestPSIVetoThresholds_DefaultsInRange pins the chosen defaults, with the
// processor threshold between twenty-five and thirty and the memory threshold
// between ten and fifteen, against an accidental drift, since the real
// calibration belongs to the flags.
func TestPSIVetoThresholds_DefaultsInRange(t *testing.T) {
	th := DefaultPSIVetoThresholds()
	if th.CPUSomeAvg10 < 25 || th.CPUSomeAvg10 > 30 {
		t.Errorf("T_cpu=%.2f fora da faixa 25-30", th.CPUSomeAvg10)
	}
	if th.MemFullAvg10 < 10 || th.MemFullAvg10 > 15 {
		t.Errorf("T_full=%.2f fora da faixa 10-15", th.MemFullAvg10)
	}
	if err := ValidatePSIVetoThresholds(th); err != nil {
		t.Errorf("defaults deveriam validar: %v", err)
	}
}

// TestPSIVetoThresholds_ValidateRejectsNonPositive is the failure case: a zero
// or negative threshold is a fail-fast error and never a silent default.
func TestPSIVetoThresholds_ValidateRejectsNonPositive(t *testing.T) {
	bad := []PSIVetoThresholds{
		{CPUSomeAvg10: 0, MemFullAvg10: 12},
		{CPUSomeAvg10: 28, MemFullAvg10: 0},
		{CPUSomeAvg10: -1, MemFullAvg10: 12},
		{CPUSomeAvg10: 28, MemFullAvg10: -5},
		{CPUSomeAvg10: 0, MemFullAvg10: 0},
	}
	for i, th := range bad {
		if err := ValidatePSIVetoThresholds(th); err == nil {
			t.Errorf("caso %d (%+v): esperava erro fail-fast, obteve nil", i, th)
		}
	}
}

// seedFailuresOnly records failures without successes in the given bucket,
// which is the exact scenario of the third case, with five failures and no
// success.
func seedFailuresOnly(t *testing.T, h *history.History, fn, nodeID string, sc history.ScenarioContext, now time.Time, nFail int) {
	t.Helper()
	for i := 0; i < nFail; i++ {
		if err := h.AppendWithGroup(history.Entry{
			FunctionName:      fn,
			NodeID:            nodeID,
			Timestamp:         now,
			ScenarioContext:   sc,
			ObservedLatencyMS: 0,
			Outcome:           history.OutcomeFalha,
		}, fn); err != nil {
			t.Fatalf("AppendWithGroup (falha): %v", err)
		}
	}
}

// TestPSIVeto_VetoByRatePreserved is the third case: the predicate of the
// history, which is shared and still consumed by the learned queue ceiling,
// vetoes with five failures and no success, while the floor below five never
// vetoes. The exact veto per segment belonged to the previous selector and was
// retired with it, so the predicate of the history stays pinned here.
func TestPSIVeto_VetoByRatePreserved(t *testing.T) {
	rate := history.LaplaceRate(0, 5)
	if !history.VetoByRate(5, rate) {
		t.Fatalf("VetoByRate(5, LaplaceRate(0,5)=%.4f) deveria vetar", rate)
	}
	// The floor is intact: a total below five never vetoes, even with a hundred percent of failures.
	if history.VetoByRate(4, history.LaplaceRate(0, 4)) {
		t.Fatalf("VetoByRate com total=4 não deveria vetar (piso>=5)")
	}

	// The evidence that would feed the exact veto stays recordable, since five
	// failures in the bucket produce a vetoable rate in the history.
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	h, _ := history.NewHistory("", 500)
	seedFailuresOnly(t, h, "myfn", "n1", scBucket(history.Bucket50_75), now, 5)
	_, _, _, total, got, _ := h.StatsAndRecent(history.SegmentKey("myfn", "n1", scBucket(history.Bucket50_75)), 60*time.Second, now)
	if total != 5 || !history.VetoByRate(total, got) {
		t.Fatalf("histórico com 5 falhas: total=%d taxa=%.4f (deveria vetar)", total, got)
	}
}
