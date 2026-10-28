package gateway

// Capture of the complete measurement set per invocation: the peak of the
// processor and of the memory usage, the stages of the acquisition, of the
// readiness and of the function, and the occupancy of the pool of the resolved
// concrete variant become fields of the history entry, inherited by the late
// report through the live reference. It is capture only, with no consumer in
// any decision, so the guards, the ordering, the percentile and the expected
// cost stay untouched.

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// fullMeasurementMetrics is the blob of execution metrics verbatim from a node
// with a resource profile, the stages and the capacity of the concrete variant.
const fullMeasurementMetrics = `{
	"cold_start": false,
	"instance_id": "cat-v1-1",
	"phases": {"acquire_wait_ms": 11, "readiness_ms": 22, "function_ms": 33},
	"resource_profile": {"cpu_pct_avg": 42.5, "cpu_pct_peak": 77.5, "mem_peak_bytes": 1048576, "sample_count": 13},
	"telemetry": {"capacity": {"cat-v1": {"max_instances": 2, "instances_available": 1, "queue_depth": 3}}}
}`

func TestPhasesOfMetricsTolerant(t *testing.T) {
	tests := []struct {
		name         string
		metrics      json.RawMessage
		wantAcquire  int64
		wantReady    int64
		wantFunction int64
	}{
		{
			name:         "full",
			metrics:      json.RawMessage(`{"phases":{"acquire_wait_ms":11,"readiness_ms":22,"function_ms":33}}`),
			wantAcquire:  11,
			wantReady:    22,
			wantFunction: 33,
		},
		{
			name:    "absent-block",
			metrics: json.RawMessage(`{"response_time_ms":4}`),
		},
		{
			name:    "empty-object",
			metrics: json.RawMessage(`{}`),
		},
		{
			name:    "nil-metrics",
			metrics: nil,
		},
		{
			name:    "invalid-json-garbage",
			metrics: json.RawMessage(`not-json{{{`),
		},
		{
			name:    "wrong-types",
			metrics: json.RawMessage(`{"phases":{"acquire_wait_ms":"x","readiness_ms":true,"function_ms":[]}}`),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("phasesOfMetrics panicou com metrics %q: %v", tc.metrics, r)
				}
			}()
			gotAcquire, gotReady, gotFunction := phasesOfMetrics(tc.metrics)
			if gotAcquire != tc.wantAcquire || gotReady != tc.wantReady || gotFunction != tc.wantFunction {
				t.Errorf("phasesOfMetrics = (%d,%d,%d), esperado (%d,%d,%d)",
					gotAcquire, gotReady, gotFunction, tc.wantAcquire, tc.wantReady, tc.wantFunction)
			}
		})
	}
}

func TestResourceProfilePeakMemTolerant(t *testing.T) {
	tests := []struct {
		name        string
		metrics     json.RawMessage
		wantAvg     float64
		wantPeak    float64
		wantMem     uint64
		wantSamples int
	}{
		{
			name:        "full",
			metrics:     json.RawMessage(`{"resource_profile":{"cpu_pct_avg":42.5,"cpu_pct_peak":77.5,"mem_peak_bytes":1048576,"sample_count":13}}`),
			wantAvg:     42.5,
			wantPeak:    77.5,
			wantMem:     1048576,
			wantSamples: 13,
		},
		{
			name:        "negative-avg-and-peak-clamped",
			metrics:     json.RawMessage(`{"resource_profile":{"cpu_pct_avg":-5.0,"cpu_pct_peak":-9.5,"mem_peak_bytes":10,"sample_count":3}}`),
			wantMem:     10,
			wantSamples: 3,
		},
		{
			name:    "absent-block",
			metrics: json.RawMessage(`{"phases":{"function_ms":1}}`),
		},
		{
			name:    "nil-metrics",
			metrics: nil,
		},
		{
			name:    "invalid-json-garbage",
			metrics: json.RawMessage(`not-json{{{`),
		},
		{
			name:    "zero-samples-whole-profile-zero",
			metrics: json.RawMessage(`{"resource_profile":{"cpu_pct_avg":42,"cpu_pct_peak":77,"mem_peak_bytes":100,"sample_count":0}}`),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("resourceProfileOfMetrics panicou com metrics %q: %v", tc.metrics, r)
				}
			}()
			gotAvg, gotPeak, gotMem, gotSamples := resourceProfileOfMetrics(tc.metrics)
			if math.Abs(gotAvg-tc.wantAvg) > 1e-9 || math.Abs(gotPeak-tc.wantPeak) > 1e-9 ||
				gotMem != tc.wantMem || gotSamples != tc.wantSamples {
				t.Errorf("resourceProfileOfMetrics = (%v,%v,%d,%d), esperado (%v,%v,%d,%d)",
					gotAvg, gotPeak, gotMem, gotSamples, tc.wantAvg, tc.wantPeak, tc.wantMem, tc.wantSamples)
			}
		})
	}
}

// TestRecordAttemptCapturesFullMeasurementSet proves that a synchronous success
// with a profile, the stages and the capacity fills all seven new fields of the
// entry.
func TestRecordAttemptCapturesFullMeasurementSet(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	gw, logPath := capacityBurstGateway(t, h, now)
	node := nodes.Node{NodeID: "borda-cap"}
	group := selection.ResolveRequestGroup("cat", []nodes.Node{node})
	slo := &selection.SLO{MaxLatencyMS: 1000}

	gw.recordAttempt(node, 5*time.Millisecond, nil, slo, "cat", group,
		[]byte(fullMeasurementMetrics), true, "", 0,
		attemptCapture{sc: history.ScenarioContext{LoadBucket: history.BucketUnknown}, concrete: "cat-v1"})

	recs := readInvRecords(t, logPath)
	if len(recs) != 1 {
		t.Fatalf("invLog = %d linhas, esperado 1", len(recs))
	}
	got := recs[0].Entry
	if math.Abs(got.CPUPctAvg-42.5) > 1e-9 || got.SampleCount != 13 {
		t.Errorf("campos pré-existentes = (%v,%d), esperado (42.5,13)", got.CPUPctAvg, got.SampleCount)
	}
	if math.Abs(got.CPUPctPeak-77.5) > 1e-9 {
		t.Errorf("CPUPctPeak = %v, esperado 77.5", got.CPUPctPeak)
	}
	if got.MemPeakBytes != 1048576 {
		t.Errorf("MemPeakBytes = %d, esperado 1048576", got.MemPeakBytes)
	}
	if got.AcquireWaitMS != 11 || got.ReadinessMS != 22 || got.FunctionMS != 33 {
		t.Errorf("phases = (%d,%d,%d), esperado (11,22,33)", got.AcquireWaitMS, got.ReadinessMS, got.FunctionMS)
	}
	if got.QueueDepth != 3 || got.InstancesAvailable != 1 {
		t.Errorf("capacidade = (queue=%d,avail=%d), esperado (3,1)", got.QueueDepth, got.InstancesAvailable)
	}
}

// TestRecordAttemptCapacityFromConcreteKey proves that the captured occupancy
// comes from the concrete key, and never from the requested name.
func TestRecordAttemptCapacityFromConcreteKey(t *testing.T) {
	tests := []struct {
		name      string
		capacity  string
		wantQueue int
		wantAvail int
	}{
		{
			name:      "only-concrete-present",
			capacity:  `{"cat-v1":{"instances_available":2,"queue_depth":7}}`,
			wantQueue: 7,
			wantAvail: 2,
		},
		{
			name:     "only-requested-present",
			capacity: `{"cat":{"instances_available":9,"queue_depth":9}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Millisecond)
			h, err := history.NewHistory("", 100)
			if err != nil {
				t.Fatalf("NewHistory: %v", err)
			}
			gw, logPath := capacityBurstGateway(t, h, now)
			node := nodes.Node{NodeID: "borda-key"}
			group := selection.ResolveRequestGroup("cat", []nodes.Node{node})
			slo := &selection.SLO{MaxLatencyMS: 1000}
			metrics := json.RawMessage(`{"telemetry":{"capacity":` + tc.capacity + `}}`)

			gw.recordAttempt(node, 5*time.Millisecond, nil, slo, "cat", group, metrics, true, "", 0,
				attemptCapture{sc: history.ScenarioContext{LoadBucket: history.BucketUnknown}, concrete: "cat-v1"})

			recs := readInvRecords(t, logPath)
			if len(recs) != 1 {
				t.Fatalf("invLog = %d linhas, esperado 1", len(recs))
			}
			got := recs[0].Entry
			if got.QueueDepth != tc.wantQueue || got.InstancesAvailable != tc.wantAvail {
				t.Errorf("capacidade = (queue=%d,avail=%d), esperado (%d,%d) (chave concreta cat-v1)",
					got.QueueDepth, got.InstancesAvailable, tc.wantQueue, tc.wantAvail)
			}
		})
	}
}

// TestRecordAttemptMeasurementsAbsentOrErrZeros covers the tolerant decoding
// and the guard of the capture of the capacity on an error.
func TestRecordAttemptMeasurementsAbsentOrErrZeros(t *testing.T) {
	tests := []struct {
		name         string
		metrics      []byte
		err          error
		wantAllZero  bool
		wantFunction int64
		wantQueue    int
		wantAvail    int
	}{
		{
			name:        "absent-blocks-2xx",
			metrics:     []byte(`{}`),
			wantAllZero: true,
		},
		{
			name:        "nil-metrics-2xx",
			metrics:     nil,
			wantAllZero: true,
		},
		{
			name:        "nil-metrics-with-error",
			metrics:     nil,
			err:         errors.New("boom"),
			wantAllZero: true,
		},
		{
			// On an error the stages and the peak still decode, since the decoding
			// is not gated by the error, while the occupancy of the pool is not
			// captured, since the capacity reading is gated by a nil error.
			name:         "capacity-present-with-error-not-captured",
			metrics:      []byte(fullMeasurementMetrics),
			err:          errors.New("boom"),
			wantFunction: 33,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Millisecond)
			h, err := history.NewHistory("", 100)
			if err != nil {
				t.Fatalf("NewHistory: %v", err)
			}
			gw, logPath := capacityBurstGateway(t, h, now)
			node := nodes.Node{NodeID: "borda-zero"}
			group := selection.ResolveRequestGroup("cat", []nodes.Node{node})
			slo := &selection.SLO{MaxLatencyMS: 1000}

			gw.recordAttempt(node, 5*time.Millisecond, tc.err, slo, "cat", group, tc.metrics, true, "", 0,
				attemptCapture{sc: history.ScenarioContext{LoadBucket: history.BucketUnknown}, concrete: "cat-v1"})

			recs := readInvRecords(t, logPath)
			if len(recs) != 1 {
				t.Fatalf("invLog = %d linhas, esperado 1", len(recs))
			}
			got := recs[0].Entry
			if tc.wantAllZero && (got.CPUPctPeak != 0 || got.MemPeakBytes != 0 || got.AcquireWaitMS != 0 || got.ReadinessMS != 0) {
				t.Errorf("peak/mem/phases não zerados: %+v", got)
			}
			if got.FunctionMS != tc.wantFunction {
				t.Errorf("FunctionMS = %d, esperado %d", got.FunctionMS, tc.wantFunction)
			}
			if got.QueueDepth != tc.wantQueue || got.InstancesAvailable != tc.wantAvail {
				t.Errorf("capacidade = (queue=%d,avail=%d), esperado (%d,%d)",
					got.QueueDepth, got.InstancesAvailable, tc.wantQueue, tc.wantAvail)
			}
		})
	}
}

// TestLateSupersedeInheritsCapturedMeasurements proves that the late report of
// the same attempt inherits the seven fields captured at the dispatch through
// the live reference, since the wire of the late report carries none of them.
func TestLateSupersedeInheritsCapturedMeasurements(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	gw, logPath := capacityBurstGateway(t, h, now)
	node := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda}
	group := selection.ResolveRequestGroup("cat", []nodes.Node{node})
	slo := &selection.SLO{MaxLatencyMS: 5000}
	gw.RegisterLateGroup("req-cap", "cat", slo)

	gw.recordAttempt(node, 50*time.Millisecond, nil, slo, "cat", group,
		[]byte(fullMeasurementMetrics), true, "req-cap", 1,
		attemptCapture{sc: history.ScenarioContext{LoadBucket: history.BucketUnknown}, concrete: "cat-v1"})

	rep := LateReport{
		RequestID:         "req-cap",
		AttemptN:          1,
		Function:          "cat",
		NodeID:            "n1",
		Outcome:           history.OutcomeSucesso,
		ObservedLatencyMS: 3200,
		StartedAt:         now.Add(-time.Second).UTC().Format(time.RFC3339Nano),
	}
	if _, err := gw.RecordLate(rep); err != nil {
		t.Fatalf("RecordLate: %v", err)
	}

	recs := readInvRecords(t, logPath)
	if len(recs) != 2 {
		t.Fatalf("invLog = %d linhas, esperado 2 (live + late)", len(recs))
	}
	late := recs[1].Entry
	if math.Abs(late.CPUPctPeak-77.5) > 1e-9 {
		t.Errorf("late CPUPctPeak = %v, esperado 77.5 (herdado)", late.CPUPctPeak)
	}
	if late.MemPeakBytes != 1048576 {
		t.Errorf("late MemPeakBytes = %d, esperado 1048576 (herdado)", late.MemPeakBytes)
	}
	if late.AcquireWaitMS != 11 || late.ReadinessMS != 22 || late.FunctionMS != 33 {
		t.Errorf("late phases = (%d,%d,%d), esperado (11,22,33) (herdado)",
			late.AcquireWaitMS, late.ReadinessMS, late.FunctionMS)
	}
	if late.QueueDepth != 3 || late.InstancesAvailable != 1 {
		t.Errorf("late capacidade = (queue=%d,avail=%d), esperado (3,1) (herdado)",
			late.QueueDepth, late.InstancesAvailable)
	}
}
