// The gateway receives and classifies late reports. The recording of the
// late attempt is separate from the recording of the attempt, since the
// rules of timestamp, scenario and objective are different.
package gateway

import (
	"errors"
	"fmt"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
)

// LateReport is the schema of the late report of the node. The processor
// and memory usage are optional, and the pointer tells an absent value from
// a zero: when the node informs them, the late report trains the real
// buckets of the report, and when they are absent the gateway uses the
// current telemetry of the node and, without telemetry, the unknown value.
// The new fields are omitted when empty, so an older wire that does not
// send them stays valid.
type LateReport struct {
	RequestID         string   `json:"request_id"`
	AttemptN          int      `json:"attempt_n"`
	Function          string   `json:"function"`
	NodeID            string   `json:"node_id"`
	Outcome           string   `json:"outcome"`
	ObservedLatencyMS int64    `json:"observed_latency_ms"`
	StartedAt         string   `json:"started_at"`
	CPUUsagePct       *float64 `json:"cpu_usage_pct,omitempty"`
	MemoryUsagePct    *float64 `json:"memory_usage_pct,omitempty"`
	// The cold start flag carries the signal of the pool of the node,
	// which says whether it provisioned during this call. It is additive
	// and tolerant: an older wire that does not emit the key decodes it
	// as false, which preserves the previous behavior where the late
	// report was always warm. The node omits the key in the warm case, so
	// its absence means false.
	ColdStart bool `json:"cold_start,omitempty"`
	// The pressure values carry the signals measured per invocation, which
	// are the average of the processor pressure and the average of the
	// full memory pressure of the telemetry block of the execution
	// metrics. They are additive and omitted when empty: an older wire
	// without the keys decodes zero, which means unknown and falls open,
	// and behaves exactly as today. The positive gate is applied in the
	// derivation, and a zero or negative value collapses to unknown
	// without an error or a panic.
	PSICpuSomeAvg10 float64 `json:"psi_cpu_some_avg10,omitempty"`
	PSIMemFullAvg10 float64 `json:"psi_mem_full_avg10,omitempty"`
}

// ErrLateReportInvalid is returned when the payload violates the schema.
var ErrLateReportInvalid = errors.New("late-report: payload inválido")

// LateSink is the interface used by the endpoint to deliver the reports to
// the history. It is implemented by the gateway. It reports that the report
// was not applied, without an error, when it is a duplicate, and the caller
// answers 208, while an applied report gives 202.
type LateSink interface {
	RecordLate(report LateReport) (bool, error)
}

// ValidateLateReport validates the schema of the late report.
func ValidateLateReport(rep *LateReport) error {
	if rep == nil {
		return fmt.Errorf("%w: payload nil", ErrLateReportInvalid)
	}
	if rep.RequestID == "" {
		return fmt.Errorf("%w: request_id vazio", ErrLateReportInvalid)
	}
	if rep.AttemptN <= 0 {
		return fmt.Errorf("%w: attempt_n deve ser >= 1", ErrLateReportInvalid)
	}
	if rep.Function == "" {
		return fmt.Errorf("%w: function vazio", ErrLateReportInvalid)
	}
	if rep.NodeID == "" {
		return fmt.Errorf("%w: node_id vazio", ErrLateReportInvalid)
	}
	if rep.ObservedLatencyMS < 0 {
		return fmt.Errorf("%w: observed_latency_ms negativo: %d", ErrLateReportInvalid, rep.ObservedLatencyMS)
	}
	if _, err := time.Parse(time.RFC3339Nano, rep.StartedAt); err != nil {
		return fmt.Errorf("%w: started_at inválido: %w", ErrLateReportInvalid, err)
	}
	if rep.Outcome != history.OutcomeSucesso && rep.Outcome != history.OutcomeFalha && rep.Outcome != history.OutcomeTimeout {
		return fmt.Errorf("%w: outcome inválido: %q", ErrLateReportInvalid, rep.Outcome)
	}
	return nil
}
