package gateway

import (
	"encoding/json"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// executionMetricsOf extracts the execution metrics block verbatim from
// the answer of the node, for the record in the invocation log. An answer
// without the block, or with an invalid body that was already filtered
// before, gives nothing in this field, which is then omitted from the
// log.
func executionMetricsOf(body []byte) json.RawMessage {
	var v struct {
		ExecutionMetrics json.RawMessage `json:"execution_metrics"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return nil
	}
	return v.ExecutionMetrics
}

// decodeMetrics is the fail-open skeleton shared by the decoders below:
// an empty blob or invalid JSON gives false and never an error, and the
// caller returns the zero value of its own shape. The envelope carries the
// shape of each decoder, and on a decoding error the partial content is
// discarded by the caller, which returns the explicit zero. There is one
// decoding per call, as before.
func decodeMetrics[T any](metrics json.RawMessage, v *T) bool {
	if len(metrics) == 0 {
		return false
	}
	if err := json.Unmarshal(metrics, v); err != nil {
		return false
	}
	return true
}

// capacityOfMetrics extracts the capacity map from inside the execution
// metrics block, along the path of the telemetry block. It is tolerant: an
// absent block, invalid JSON or a missing key gives nothing and never an
// error, so a metric without capacity, from an old node or from an omitted
// collection, simply does not feed the monitor.
func capacityOfMetrics(metrics json.RawMessage) map[string]telemetry.FunctionCapacity {
	var v struct {
		Telemetry *struct {
			Capacity map[string]telemetry.FunctionCapacity `json:"capacity"`
		} `json:"telemetry"`
	}
	if !decodeMetrics(metrics, &v) {
		return nil
	}
	if v.Telemetry == nil {
		return nil
	}
	return v.Telemetry.Capacity
}

// coldInfoOfMetrics extracts the cold start flag and the instance
// identifier from the execution metrics block. The blob already is the
// execution metrics object, so the keys are at the top level. The decoding
// is tolerant, in the same pattern as the capacity extraction: an absent
// blob, invalid JSON, a wrong type or a missing key gives false and an
// empty identifier and never an error. Every error path of the gateway
// passes no metrics, so on those paths the behavior before the enrichment
// remains, which is warm and without an instance.
func coldInfoOfMetrics(metrics json.RawMessage) (coldStart bool, instanceID string) {
	var v struct {
		ColdStart  bool   `json:"cold_start"`
		InstanceID string `json:"instance_id"`
	}
	if !decodeMetrics(metrics, &v) {
		return false, ""
	}
	return v.ColdStart, v.InstanceID
}

// resourceProfileOfMetrics extracts the average and the peak of the
// processor usage, the peak of the memory and the number of samples from
// the resource profile of the execution metrics. The decoding is tolerant:
// an absent blob, invalid JSON, a wrong type, an absent block or key, or a
// non-positive number of samples gives zeros and never an error, where a
// zero processor usage means unknown and never feeds the estimator of the
// expected cost. A negative processor usage, in the average or in the peak,
// is clamped to zero, since the normalization of the node is never negative
// and a negative value only comes from a corrupted payload. The peak and
// the memory usage are capture only and are inherited by the late report
// through the live reference.
func resourceProfileOfMetrics(metrics json.RawMessage) (cpuPctAvg, cpuPctPeak float64, memPeakBytes uint64, sampleCount int) {
	var v struct {
		ResourceProfile *struct {
			CPUPctAvg    float64 `json:"cpu_pct_avg"`
			CPUPctPeak   float64 `json:"cpu_pct_peak"`
			MemPeakBytes uint64  `json:"mem_peak_bytes"`
			SampleCount  int     `json:"sample_count"`
		} `json:"resource_profile"`
	}
	if !decodeMetrics(metrics, &v) {
		return 0, 0, 0, 0
	}
	if v.ResourceProfile == nil || v.ResourceProfile.SampleCount <= 0 {
		return 0, 0, 0, 0
	}
	cpuAvg := v.ResourceProfile.CPUPctAvg
	if cpuAvg < 0 {
		cpuAvg = 0
	}
	cpuPeak := v.ResourceProfile.CPUPctPeak
	if cpuPeak < 0 {
		cpuPeak = 0
	}
	return cpuAvg, cpuPeak, v.ResourceProfile.MemPeakBytes, v.ResourceProfile.SampleCount
}

// phasesOfMetrics extracts the phases of the invocation, which are the wait
// of the acquisition, the readiness and the function time. The decoding is
// tolerant, in the same pattern as the resource profile: an absent blob,
// invalid JSON, a wrong type or an absent block or key gives zeros and
// never an error. It is capture only, since the late report does not carry
// the phases on the wire and inherits them through the live reference.
func phasesOfMetrics(metrics json.RawMessage) (acquireWaitMS, readinessMS, functionMS int64) {
	var v struct {
		Phases *struct {
			AcquireWaitMS int64 `json:"acquire_wait_ms"`
			ReadinessMS   int64 `json:"readiness_ms"`
			FunctionMS    int64 `json:"function_ms"`
		} `json:"phases"`
	}
	if !decodeMetrics(metrics, &v) {
		return 0, 0, 0
	}
	if v.Phases == nil {
		return 0, 0, 0
	}
	return v.Phases.AcquireWaitMS, v.Phases.ReadinessMS, v.Phases.FunctionMS
}

// psiOfMetrics extracts the average pressure of the processor and the
// average of the full memory pressure from the telemetry block of the
// execution metrics. The blob already is the execution metrics object, so
// the keys are at the top level, the same convention as the cold start
// extraction. The decoding is tolerant, in the same pattern as the resource
// profile: an absent blob, invalid JSON, an absent block or one with the
// wrong type, and an average with the wrong type all give zeros and never
// an error. The gate is per axis: a positive average passes, and a
// non-positive one becomes zero on that axis, where zero means unknown and
// fails open. It is capture only.
func psiOfMetrics(metrics json.RawMessage) (cpuSomeAvg10, memFullAvg10 float64) {
	var v struct {
		Telemetry *struct {
			CPU *struct {
				Avg10 float64 `json:"avg10"`
			} `json:"cpu_pressure"`
			MemFull *struct {
				Avg10 float64 `json:"avg10"`
			} `json:"memory_pressure_full"`
		} `json:"telemetry"`
	}
	if !decodeMetrics(metrics, &v) {
		return 0, 0
	}
	if v.Telemetry == nil {
		return 0, 0
	}
	if v.Telemetry.CPU != nil && v.Telemetry.CPU.Avg10 > 0 {
		cpuSomeAvg10 = v.Telemetry.CPU.Avg10
	}
	if v.Telemetry.MemFull != nil && v.Telemetry.MemFull.Avg10 > 0 {
		memFullAvg10 = v.Telemetry.MemFull.Avg10
	}
	return cpuSomeAvg10, memFullAvg10
}
