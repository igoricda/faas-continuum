package selection

// TDD (todo 3, teste-antes-do-código): veto full PSI preservado +
// fail-open. Estes testes foram escritos ANTES de psi_veto.go existir
// (a primeira execução falha em compilação: undefined:
// PSIVetoFull/PSIVetoThresholds — ver evidence log) e só passam depois
// da implementação.
//
// Convenções reusadas dos testes vizinhos (selection_test.go:
// psiPtr/scBucket/defaultLimits): nada reimplementado — VetoByRate e o
// revival são exercitados pelos caminhos EXISTENTES (history.VetoByRate,
// que segue consumido pelo teto de fila aprendido em queuecap.go, e o
// fail-open do veto PSI no RiskSelector, em psi_veto_wiring_test.go).

import (
	"math"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// TestPSIVetoFull_MemFullAboveThresholdVetoes é o caso (a) do enunciado:
// mem_full_avg10=20 estoura o default T_full (10-15) => veta, mesmo com
// cpu_some baixo.
func TestPSIVetoFull_MemFullAboveThresholdVetoes(t *testing.T) {
	th := DefaultPSIVetoThresholds()
	if !PSIVetoFull(5.0, 20.0, true, th) {
		t.Fatalf("mem_full=20 deveria vetar (T_full=%.2f)", th.MemFullAvg10)
	}
}

// TestPSIVetoFull_CPUSomeAboveThresholdVetoes cobre o outro braço do OR:
// cpu_some alto veta mesmo com mem_full baixo; e o limite é estrito
// (> — igual a Overloaded/filterProjectedOverloaded).
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

// TestPSIVetoFull_FailOpenOnUnknown é o caso (b): PSI nil/stale/unknown
// NUNCA veta sozinho — fail-open (convenção `0`=unknown do todo 1).
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

// TestPSIVetoFull_FromPackage exercita o adaptador de telemetria real:
// ponteiros nil => sem PSI => fail-open; ok=false => fail-open;
// mem_full=20 => veta.
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

// TestPSIVetoThresholds_DefaultsInRange fixa os defaults escolhidos
// (T_cpu em 25-30, T_full em 10-15): pin contra deriva acidental — a
// calibração real é do todo 16 (flags).
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

// TestPSIVetoThresholds_ValidateRejectsNonPositive é o failure case do
// enunciado: threshold zero/negativo => erro fail-fast, nunca defaults
// silenciosos.
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

// seedFailuresOnly grava nFail falhas (sem sucessos) no bucket indicado —
// o cenário exato do caso (c): 5 falhas + 0 sucessos.
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

// TestPSIVeto_VetoByRatePreserved é o caso (c): o predicado VetoByRate do
// histórico (compartilhado — o teto de fila aprendido maxQueueKForNode
// ainda o consome, queuecap.go) veta com 5 falhas + 0 sucessos e o piso
// <5 nunca veta. O veto exato por segmento era exclusivo do v1
// (SelfLearningSelector) e foi aposentado com ele no ADR-0012; o
// predicado do histórico segue pinado aqui.
func TestPSIVeto_VetoByRatePreserved(t *testing.T) {
	rate := history.LaplaceRate(0, 5)
	if !history.VetoByRate(5, rate) {
		t.Fatalf("VetoByRate(5, LaplaceRate(0,5)=%.4f) deveria vetar", rate)
	}
	// Piso intacto: total<5 nunca veta, mesmo com 100% de falha.
	if history.VetoByRate(4, history.LaplaceRate(0, 4)) {
		t.Fatalf("VetoByRate com total=4 não deveria vetar (piso>=5)")
	}

	// A evidência que alimentaria o veto exato continua gravável: 5
	// falhas no bucket produzem uma taxa vetável no histórico.
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	h, _ := history.NewHistory("", 500)
	seedFailuresOnly(t, h, "myfn", "n1", scBucket(history.Bucket50_75), now, 5)
	_, _, _, total, got, _ := h.StatsAndRecent(history.SegmentKey("myfn", "n1", scBucket(history.Bucket50_75)), 60*time.Second, now)
	if total != 5 || !history.VetoByRate(total, got) {
		t.Fatalf("histórico com 5 falhas: total=%d taxa=%.4f (deveria vetar)", total, got)
	}
}
