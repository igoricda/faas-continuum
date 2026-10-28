package history

// Capture of the complete measurement set per invocation: the seven additive
// fields of the persisted entry, which are omitted when zero, survive the JSON
// round trip and disappear when zero, with no migration, so an old file decodes
// as zero.

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func TestEntryCapturedMeasurementFieldsRoundTrip(t *testing.T) {
	e := Entry{
		FunctionName:       "fn",
		NodeID:             "n1",
		Timestamp:          time.Now().UTC().Truncate(time.Millisecond),
		ScenarioContext:    ScenarioContext{LoadBucket: Bucket0_10},
		ObservedLatencyMS:  100,
		Outcome:            OutcomeSucesso,
		CPUPctAvg:          42.5,
		CPUPctPeak:         77.5,
		MemPeakBytes:       1048576,
		SampleCount:        13,
		AcquireWaitMS:      11,
		ReadinessMS:        22,
		FunctionMS:         33,
		QueueDepth:         3,
		InstancesAvailable: 2,
	}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, want := range []string{
		`"cpu_pct_peak"`, `"mem_peak_bytes"`, `"acquire_wait_ms"`,
		`"readiness_ms"`, `"function_ms"`, `"queue_depth"`, `"instances_available"`,
	} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("json não contém %s: %s", want, string(data))
		}
	}
	var got Entry
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if math.Abs(got.CPUPctPeak-77.5) > 1e-9 || got.MemPeakBytes != 1048576 ||
		got.AcquireWaitMS != 11 || got.ReadinessMS != 22 || got.FunctionMS != 33 ||
		got.QueueDepth != 3 || got.InstancesAvailable != 2 {
		t.Fatalf("round-trip mismatch: got %+v", got)
	}

	t.Run("omitempty zera campos ausentes", func(t *testing.T) {
		zero := Entry{
			FunctionName:    "fn",
			NodeID:          "n1",
			Timestamp:       e.Timestamp,
			ScenarioContext: e.ScenarioContext,
			Outcome:         OutcomeSucesso,
		}
		data, err := json.Marshal(zero)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		for _, unwanted := range []string{
			`"cpu_pct_peak"`, `"mem_peak_bytes"`, `"acquire_wait_ms"`,
			`"readiness_ms"`, `"function_ms"`, `"queue_depth"`, `"instances_available"`,
		} {
			if strings.Contains(string(data), unwanted) {
				t.Fatalf("%s deveria ser omitido quando zero: %s", unwanted, string(data))
			}
		}
	})
}
