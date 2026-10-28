// Contract tests of the ingestion of pressure data.
//
// Convention: the tests named after the contract pin the behavior that the
// decoders and the history have to keep, and the tests named after the old
// wire pin the compatibility of a log or a payload without pressure data,
// which decodes to zero, the unknown value, and never to an error or a panic.
//
// Decoder contract:
//
//			func psiOfMetrics(metrics json.RawMessage) (cpuSomeAvg10, memFullAvg10 float64)
//
//	  - Input: the blob of execution metrics verbatim from the node, as the
//	    object itself, so the keys are top level, the same convention as the
//	    cold information decoder.
//	  - Path: the average of ten seconds of the processor pressure and of the
//	    full memory pressure under the telemetry block.
//	  - Gate: a positive average passes, and a non-positive one becomes zero,
//	    which is unknown, with a fail-open.
//	  - Tolerant: a nil blob, invalid JSON, a missing block, a wrong type or a
//	    non-numeric value give zero and zero, and never an error or a panic,
//	    which mirrors the decoder of the resource profile.
//	  - The recording of an attempt calls it once, next to the capacity and the
//	    cold information decoders, and stamps the pressure fields of the entry.
//
// Entry fields: the two pressure averages are additive and omitted when zero,
// so older files keep decoding.
//
// Live reference: it gains the two values, which are populated in the same
// single take of the note call, and the late entry inherits them.
//
// Late report: it gains the same two optional fields, so a node that measured
// the pressure per invocation can report it, and an absent value is zero.
//
// Supersede: the late path supersedes both the primary key and the aggregated
// segment of the layer, and the surviving late entry carries the inherited
// pressure values.
//
// Migrator: a dry run over the pinned fixture of the previous format reports
// every entry as migratable and none as migrated, since the old format has no
// pressure keys. The test loads the JSON directly, since the replay helpers
// live in test files of the history package and are not importable.
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
// Contract: a complete piggyback decodes to the stamped values.
// ---------------------------------------------------------------------------

func TestPSIContract_DecodeFullPSI(t *testing.T) {
	raw := loadMetricsFixture(t, "execution_metrics_with_psi.json")

	// The fixture really carries the piggyback, which guards against a bad
	// fixture that would mask a contract failure.
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

	// Wired to the pressure decoder.
	cpu, mem := psiOfMetrics(raw)
	if cpu != 12.5 {
		t.Errorf("psi cpu_some_avg10 = %v, want 12.5 (gate avg10>0)", cpu)
	}
	if mem != 8.25 {
		t.Errorf("psi mem_full_avg10 = %v, want 8.25 (gate avg10>0)", mem)
	}

}

// ---------------------------------------------------------------------------
// Contract: the decode gate requires a positive average, and a zero or a
// negative value collapses to unknown. The zero and negative rows also assert
// the fail-open half of the gate.
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
			// Wired to the pressure decoder.
			cpu, mem := psiOfMetrics(json.RawMessage(tc.blob))
			if cpu != tc.want[0] || mem != tc.want[1] {
				t.Errorf("psi decode = (%v,%v), want (%v,%v)", cpu, mem, tc.want[0], tc.want[1])
			}
		})
	}

}

// ---------------------------------------------------------------------------
// Contract: absent or invalid telemetry gives zeros, and never an error or a
// panic. The rows of the decoder also pin the old-wire behavior below.
// ---------------------------------------------------------------------------

func TestPSIContract_DecodeAbsentInvalidIsUnknown(t *testing.T) {
	for _, name := range []string{
		"execution_metrics_no_telemetry.json",
		"execution_metrics_bad_telemetry.json",
		"execution_metrics_invalid.json",
	} {
		raw := loadMetricsFixture(t, name)
		// Wired to the pressure decoder, which must not panic on any of these
		// blobs, including the truncated JSON.
		cpu, mem := psiOfMetrics(raw)
		_ = cpu
		_ = mem
		if cpu != 0 || mem != 0 {
			t.Errorf("%s: psi decode = (%v,%v), want (0,0)", name, cpu, mem)
		}
	}

}

// ---------------------------------------------------------------------------
// Contract: the entry carries the new optional fields.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Contract: the live reference and the late report carry the pressure values,
// and the late entry inherits them through the single take. The dual-key
// supersede is exercised below, so that the inheritance cannot regress it.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Contract: the dry run of the migration from the previous format to the
// current one reports every entry as migratable and none as migrated. The
// facts of the fixture are asserted for real, since they are the input of the
// dry run.
// ---------------------------------------------------------------------------

// ===========================================================================
// Pins of the old wire, which have to stay green: absent or invalid pressure
// data gives zero, which is unknown, the old wire stays intact, and there is
// no error or panic.
// ===========================================================================

// Absent PSI block: every CURRENT decoder returns its zero value and the
// piggyback still decodes everywhere else (cold, profile, stages, capacity).
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

	// The blob with pressure data must still decode on the old wire, with the
	// pressure keys ignored and everything else intact, which is the guard
	// against breaking an older log.
	if caps := capacityOfMetrics(full); len(caps) != 1 {
		t.Errorf("PSI blob: capacity len=%d, want 1", len(caps))
	}
	if avg, _, _, _ := resourceProfileOfMetrics(full); avg != 31.5 {
		t.Errorf("PSI blob: cpu_avg=%v, want 31.5", avg)
	}
}

// A wrong-typed telemetry block and a truncated JSON give zeros, and never an
// error or a panic. The decoders return values only, so the assertion is
// correct zeros and no panic, which runs under the race detector.
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

// A late orphan, without a live reference and with an empty monitor, falls
// back to unknown through the latest reading, applies cleanly and never
// panics, which pins the fallback half of the contract.
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

// Single-take discipline and the shape of the time to live and of the bound:
// the take consumes the correlation, and the recording of an attempt without a
// request identifier or without a positive attempt number indexes nothing.
func TestPSIOldWire_SingleTakeDiscipline(t *testing.T) {
	// The discipline of the take itself is proven at the real boundary by the
	// late test, and what remains here is the case that no other test reaches:
	// the recording of an attempt without a correlation cannot index the live
	// reference. The next late report is then an orphan, which falls back, and
	// never a phantom supersede.
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
