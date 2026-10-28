// TDD contract tests for PSI ingestion (selflearning-telemetry-seeded-history,
// todo 4). TESTS ONLY — no production code is implemented here.
//
// CONVENTION: TestPSIContract* = FUTURE contract, MUST FAIL until todos 5-8
// land. TestPSIOldWire* = behavior pins of the CURRENT wire, MUST PASS now
// and stay green (absent/invalid PSI => 0 = unknown, never error/panic).
//
// FUTURE DECODER CONTRACT (todo 7 implements, same mold as the decoders in
// gateway_classify.go:370-448):
//
//		func psiOfMetrics(metrics json.RawMessage) (cpuSomeAvg10, memFullAvg10 float64)
//
//	  - Input: the execution_metrics blob VERBATIM from the node (the object
//	    itself, so keys are top-level — same convention as coldInfoOfMetrics).
//	  - Path: execution_metrics.telemetry.cpu_pressure.avg10 and
//	    execution_metrics.telemetry.memory_pressure_full.avg10
//	    (telemetry.Package: CPUPressure/MemoryPressureFull *PressureFields).
//	  - Gate: avg10 > 0 passes; avg10 <= 0 => 0 (0 = unknown, fail-open).
//	  - Tolerant: nil blob, invalid JSON, missing block, wrong-typed block or
//	    wrong-typed avg10 => (0, 0), NEVER error/panic (mirrors
//	    resourceProfileOfMetrics: sample_count<=0 => zeros).
//	  - recordAttempt calls it ONCE next to capacityOfMetrics/coldInfoOfMetrics
//	    and stamps history.Entry.PSICpuSomeAvg10/PSIMemFullAvg10.
//
// ENTRY FIELDS (todo 5, additive omitempty — old files keep decoding):
//
//	PSICpuSomeAvg10 float64 `json:"psi_cpu_some_avg10,omitempty"`
//	PSIMemFullAvg10 float64 `json:"psi_mem_full_avg10,omitempty"`
//
// LIVEREF (todo 8): liveRef gains psiCpuSomeAvg10/psiMemFullAvg10 float64;
// recordAttempt populates them in the SAME g.liveIdx.note call (single-take
// discipline preserved: recordLateAttempt keeps its SINGLE take in
// gateway_late.go:319-322); the hasRef block (gateway_late.go:340-353)
// copies them onto the late Entry.
//
// LATEREPORT (todo 8): LateReport gains the same two fields omitempty so a
// node that measured PSI per-invocation can report it; absent => 0.
//
// SUPERSEDE (already dual-key, gateway_late.go:360-369): the late path
// supersedes BOTH the primary and the AggLayerKey segment; the PSI contract
// only adds that the surviving late Entry carries the inherited PSI values.
//
// MIGRATOR (todo 6): dry-run over the pinned r3 fixture
// (internal/arbitro/history/testdata/history-r3.json: 12 keys / 560
// entries / 0 PSI keys) reports would=560, migrated=0. The gateway test
// loads the JSON DIRECTLY (os.ReadFile) because the replay helpers live in
// history _test files, which are NOT importable; the migrator itself must
// live where it can reuse the history package internals.
package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// ---------------------------------------------------------------------------
// Fixture loader.
// ---------------------------------------------------------------------------

func loadMetricsFixture(t *testing.T, name string) json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	return json.RawMessage(raw)
}

// ---------------------------------------------------------------------------
// CONTRACT (a): full-PSI piggyback decodes to the stamped values.
// FAILS now: stub returns zeros + history.Entry has no PSI fields.
// ---------------------------------------------------------------------------

func TestPSIContract_DecodeFullPSI(t *testing.T) {
	raw := loadMetricsFixture(t, "execution_metrics_with_psi.json")

	// The fixture REALLY carries the piggyback (guard against a bad fixture
	// masking as a contract failure).
	var probe struct {
		Telemetry *struct {
			CPU *struct {
				Avg10 float64 `json:"avg10"`
			} `json:"cpu_pressure"`
			MemF *struct {
				Avg10 float64 `json:"avg10"`
			} `json:"memory_pressure_full"`
		} `json:"telemetry"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		t.Fatalf("fixture inválida: %v", err)
	}
	if probe.Telemetry == nil || probe.Telemetry.CPU == nil || probe.Telemetry.CPU.Avg10 != 12.5 {
		t.Fatalf("fixture sem cpu_pressure.avg10=12.5 (got %+v)", probe.Telemetry)
	}
	if probe.Telemetry.MemF == nil || probe.Telemetry.MemF.Avg10 != 8.25 {
		t.Fatalf("fixture sem memory_pressure_full.avg10=8.25 (got %+v)", probe.Telemetry)
	}

	// Wired to psiOfMetrics (todo 7).
	cpu, mem := psiOfMetrics(raw)
	if cpu != 12.5 {
		t.Errorf("psi cpu_some_avg10 = %v, want 12.5 (gate avg10>0)", cpu)
	}
	if mem != 8.25 {
		t.Errorf("psi mem_full_avg10 = %v, want 8.25 (gate avg10>0)", mem)
	}

}

// ---------------------------------------------------------------------------
// CONTRACT (a2): decode gate avg10>0 — zero/negative collapse to unknown.
// FAILS now for the same reason (stub + missing Entry fields); the zero and
// negative rows already assert the fail-open half of the gate.
// ---------------------------------------------------------------------------

func TestPSIContract_DecodeGate(t *testing.T) {
	cases := []struct {
		name string
		blob string
		want [2]float64
	}{
		{"positive passes", `{"telemetry":{"cpu_pressure":{"avg10":12.5},"memory_pressure_full":{"avg10":8.25}}}`, [2]float64{12.5, 8.25}},
		{"zero is unknown", `{"telemetry":{"cpu_pressure":{"avg10":0},"memory_pressure_full":{"avg10":0}}}`, [2]float64{0, 0}},
		{"negative is unknown", `{"telemetry":{"cpu_pressure":{"avg10":-3},"memory_pressure_full":{"avg10":-1}}}`, [2]float64{0, 0}},
		{"one-sided zero", `{"telemetry":{"cpu_pressure":{"avg10":12.5},"memory_pressure_full":{"avg10":0}}}`, [2]float64{12.5, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Wired to psiOfMetrics (todo 7).
			cpu, mem := psiOfMetrics(json.RawMessage(tc.blob))
			if cpu != tc.want[0] || mem != tc.want[1] {
				t.Errorf("psi decode = (%v,%v), want (%v,%v)", cpu, mem, tc.want[0], tc.want[1])
			}
		})
	}

}

// ---------------------------------------------------------------------------
// CONTRACT (b/c): absent/invalid telemetry => zeros, never error/panic.
// FAILS now (missing Entry fields); the decoder-half rows pass today and
// are ALSO pinned as old-wire behavior in TestPSIOldWire_* below.
// ---------------------------------------------------------------------------

func TestPSIContract_DecodeAbsentInvalidIsUnknown(t *testing.T) {
	for _, name := range []string{
		"execution_metrics_no_telemetry.json",
		"execution_metrics_bad_telemetry.json",
		"execution_metrics_invalid.json",
	} {
		raw := loadMetricsFixture(t, name)
		// Wired to psiOfMetrics (todo 7). Must not panic
		// on any of these blobs, even the truncated-JSON one.
		cpu, mem := psiOfMetrics(raw)
		_ = cpu
		_ = mem
		if cpu != 0 || mem != 0 {
			t.Errorf("%s: psi decode = (%v,%v), want (0,0)", name, cpu, mem)
		}
	}

}

// ---------------------------------------------------------------------------
// CONTRACT: Entry carries the new omitempty fields.
// FAILS now (fields do not exist).
// ---------------------------------------------------------------------------

// (TestPSIContract_EntryFields removido: a segunda metade — zero Entry sem
// as chaves PSI no JSON — é literalmente a de
// history/entry_r4_test.go:79 ("zero-Value Entry marshaliza SEM as chaves
// PSI (r3 byte-clean)"), inclusive a mensagem de erro. A primeira metade
// era reflection das tags de history.Entry, cujo owner é o próprio package
// history: entry_r4_test.go:21-70 faz round-trip de Save+NewHistory com
// 12.5/8.25, o que prova existência, tipo e serialização.)

// ---------------------------------------------------------------------------
// CONTRACT: liveRef + LateReport carry PSI; late inherits via single take.
// FAILS now (fields do not exist). The dual-key supersede half already
// works today and is exercised below so todos 5-8 cannot regress it.
// ---------------------------------------------------------------------------

// (O antigo TestPSIContract_LiveRefAndLateReportFields — só reflection de
// nomes/tipos dos 4 campos PSI — e o TestPSIContract_LateInheritsPSIViaSingleTake
// foram removidos: ambos repetiam, mais fraco, o que TestLatePSI já prova
// POR VALOR no boundary real (herança liveRef 12.5/8.25, órfão body 7.5/3.25,
// via monitor 9.5/4.5, unknown 0/0, e fio antigo sem as chaves). A forma
// dos campos e o omitempty são consequência do round-trip; o comportamento
// é o contrato.)

// (TestPSIContract_LateInheritsPSIViaSingleTake removido: mesmo cenário e
// mesmo fixture de TestLatePSI/"correlated inherits PSI via single take",
// que assere o sinal que aqui faltava — late.PSICpuSomeAvg10 == 12.5 —
// enquanto este terminava em duas chamadas de reflection e passaria mesmo
// se recordLate deixasse de herdar o PSI do liveRef.)

// ---------------------------------------------------------------------------
// CONTRACT (e): migrator r3->r4 dry-run — would=560, migrated=0.
// FAILS now: stub returns (0,0). The fixture facts (560 entries, 0 PSI
// keys) are asserted for real — they are the dry-run INPUT todo 6 consumes.
// ---------------------------------------------------------------------------

// (TestPSIContract_MigratorDryRun removido: reachava ../history/testdata
// para contar 12 segmentos / 560 entries / 0 chaves PSI — metadados da
// fixture, não comportamento — e chamava history.DryRunMigrateR3R4, que o
// próprio package history prova contra a MESMA fixture, em
// migrate_r3_r4_test.go, com MigrateR3R4File real + byte-compare de que o
// dry-run não reescreve + ausência de .r3.bak. A migração é coberta no
// owner.)

// ===========================================================================
// OLD-WIRE BEHAVIOR PINS — MUST PASS NOW and stay green through todos 5-8.
// Absent/invalid PSI => 0 = unknown; old wire intact; no error/panic.
// ===========================================================================

// Absent PSI block: every CURRENT decoder returns its zero value and the
// piggyback still decodes everywhere else (cold, profile, phases, capacity).
func TestPSIOldWire_AbsentIsUnknown(t *testing.T) {
	plain := loadMetricsFixture(t, "execution_metrics_no_telemetry.json")
	full := loadMetricsFixture(t, "execution_metrics_with_psi.json")

	if caps := capacityOfMetrics(plain); caps != nil {
		t.Errorf("sem telemetry: capacity=%v, want nil", caps)
	}
	if cold, inst := coldInfoOfMetrics(plain); cold || inst != "inst-plain-1" {
		t.Errorf("coldInfo = (%v,%q), want (false, inst-plain-1)", cold, inst)
	}
	if avg, peak, mem, n := resourceProfileOfMetrics(plain); avg != 31.5 || peak != 58.0 || mem != 134217728 || n != 6 {
		t.Errorf("profile plain = (%v,%v,%v,%v), want (31.5,58,134217728,6)", avg, peak, mem, n)
	}
	if a, r, f := phasesOfMetrics(plain); a != 3 || r != 5 || f != 42 {
		t.Errorf("phases plain = (%v,%v,%v), want (3,5,42)", a, r, f)
	}

	// The PSI-bearing blob MUST still decode on the old wire (PSI keys
	// ignored, everything else intact) — this is the "não quebrar fio
	// antigo" guard todos 5-8 must keep green.
	if caps := capacityOfMetrics(full); len(caps) != 1 {
		t.Errorf("PSI blob: capacity len=%d, want 1", len(caps))
	}
	if avg, _, _, _ := resourceProfileOfMetrics(full); avg != 31.5 {
		t.Errorf("PSI blob: cpu_avg=%v, want 31.5", avg)
	}
}

// Wrong-typed telemetry block and truncated JSON: zeros, never error/panic.
// (The decoders return values only, so "never error" = correct zeros + no
// panic; exercised under -race in CI.)
func TestPSIOldWire_InvalidNeverFails(t *testing.T) {
	bad := loadMetricsFixture(t, "execution_metrics_bad_telemetry.json")
	if caps := capacityOfMetrics(bad); caps != nil {
		t.Errorf("bad telemetry: capacity=%v, want nil", caps)
	}
	if avg, peak, mem, n := resourceProfileOfMetrics(bad); avg != 0 || peak != 0 || mem != 0 || n != 0 {
		t.Errorf("bad telemetry: profile=(%v,%v,%v,%v), want zeros", avg, peak, mem, n)
	}

	invalid := loadMetricsFixture(t, "execution_metrics_invalid.json")
	if !json.Valid(invalid) {
		t.Logf("fixture invalid.json realmente inválida (esperado)")
	}
	if caps := capacityOfMetrics(invalid); caps != nil {
		t.Errorf("invalid JSON: capacity=%v, want nil", caps)
	}
	if cold, inst := coldInfoOfMetrics(invalid); cold || inst != "" {
		t.Errorf("invalid JSON: coldInfo=(%v,%q), want (false,\"\")", cold, inst)
	}
	if avg, peak, mem, n := resourceProfileOfMetrics(invalid); avg != 0 || peak != 0 || mem != 0 || n != 0 {
		t.Errorf("invalid JSON: profile=(%v,%v,%v,%v), want zeros", avg, peak, mem, n)
	}
	if a, r, f := phasesOfMetrics(invalid); a != 0 || r != 0 || f != 0 {
		t.Errorf("invalid JSON: phases=(%v,%v,%v), want zeros", a, r, f)
	}
	var nilBlob json.RawMessage
	if avg, peak, mem, n := resourceProfileOfMetrics(nilBlob); avg != 0 || peak != 0 || mem != 0 || n != 0 {
		t.Errorf("nil blob: profile=(%v,%v,%v,%v), want zeros", avg, peak, mem, n)
	}
}

// Late orphan (no liveRef, empty monitor): falls back to unknown via
// mon.Latest miss, applies cleanly, never panics. Pins the fallback half
// of contract (d); the PSI half is TestPSIContract_LateInheritsPSIViaSingleTake.
func TestPSIOldWire_LateOrphanFallback(t *testing.T) {
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
		ObservedLatencyMS: 1500,
		StartedAt:         time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano),
	}
	applied, err := gw.RecordLate(rep)
	if err != nil {
		t.Fatalf("RecordLate órfão: %v", err)
	}
	if !applied {
		t.Fatalf("applied=false, want true (órfão aplica via fallback)")
	}
	key := history.SegmentKey("f", "n1", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	count, _, _, _, _, ok := hist.StatsAndRecent(key, time.Hour, time.Now())
	if !ok || count != 1 {
		t.Fatalf("órfão caiu fora do unknown (ok=%v count=%d), want unknown/1", ok, count)
	}
}

// Single-take discipline + 5m TTL / 10k bound shape: take consome a
// correlação e recordAttempt sem requestID/attemptN não indexa nada. Pins
// a disciplina de liveRef que o todo 8 precisa preservar.
func TestPSIOldWire_SingleTakeDiscipline(t *testing.T) {
	// A disciplina take-consome em si é provada no boundary real por
	// TestLatePSI/"correlated inherits PSI via single take"
	// (gateway_late_test.go:347) e TestObservedContract_LateInheritsViaSingleTake.
	// Aqui resta o caso que nenhum deles alcança: recordAttempt sem
	// correlação não pode indexar liveRef.
	//
	// recordAttempt sem correlação (requestID vazio / attemptN<=0) não
	// indexa: o late seguinte é órfão (fallback), nunca supersede fantasma.
	hist, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	gw := lateTestGateway(hist, monitor.NewMonitor(), nil)
	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	sc := history.ScenarioFrom(telemetry.Package{}, false)
	gw.recordAttempt(node, 50*time.Millisecond, nil, slo, "f", "f", nil, true, "", 0,
		attemptCapture{sc: sc, concrete: "f"})
	if _, ok := gw.liveIdx.take("", 0); ok {
		t.Fatalf("recordAttempt sem correlação indexou liveRef")
	}
}
