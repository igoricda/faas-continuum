package gateway

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// Sentinel node identifiers for the lines of the invocation log that do
// not correspond to an attempt in a real node. The segmented history does
// not receive these lines, since the self-learning only operates on the
// edge and the fog.
const (
	// cloudFallbackNodeID identifies the outcomes of the cloud fallback.
	cloudFallbackNodeID = "cloud-fallback"
	// noNodeAvailableID identifies the invocations rejected with no node
	// available, before any attempt.
	noNodeAvailableID = "no-node-available"
)

// InvocationRecord is the line of the invocation log: the fields of the
// history, with the entry embedded and flattened in the JSON, plus the
// requested objective and the execution metrics returned. It is a type
// separate from the history on purpose: the in-memory history stays lean,
// with the capacity and the snapshot, and only the cold log carries the
// rich block of metrics.
//
// The request identifier and the attempt number group the attempts by
// invocation, the armed flag marks that the attempt became an armed pending
// report, and the delivered flag marks the attempt whose answer was
// returned to the client, which is exactly one per invocation with an
// answer. All of them are omitted when empty, so older lines decode as
// zero, with the absent value meaning false, the same convention as the
// capture-only fields of the entry.
type InvocationRecord struct {
	history.Entry
	SLO              *selection.SLO  `json:"slo,omitempty"`
	ExecutionMetrics json.RawMessage `json:"execution_metrics,omitempty"`
	RequestID        string          `json:"request_id,omitempty"`
	AttemptN         int             `json:"attempt_n,omitempty"`
	Armed            bool            `json:"armed,omitempty"`
	Delivered        bool            `json:"delivered,omitempty"`
}

// maxInvLogMetricsBytes is the ceiling per line of the execution metrics
// block in the JSONL: the block comes from the node bounded only by the
// read ceiling of eight megabytes of the gateway, and without its own
// ceiling a compromised node would write about eight megabytes per
// invocation to the flash disk of the edge. Above the ceiling the field is
// replaced by a stub that marks the truncation with a valid JSON object,
// since truncating bytes in the middle would break the whole line.
const maxInvLogMetricsBytes = 64 * 1024

// InvocationLog is an optional append-only logger that persistently records
// the results of the invocations of the arbiter. Every append writes an
// invocation record as a JSON line, without segmentation and without a
// limit of invocations.
//
// An empty path disables the logger: the constructor returns a nil logger
// and a nil error, and an append to a nil logger does nothing, so the hot
// path of the gateway needs no nil check. A write failure never brings the
// invocation down, since the caller only logs it.
type InvocationLog struct {
	mu        sync.Mutex
	f         *os.File
	closeOnce sync.Once
	closeErr  error
}

// NewInvocationLog opens, or creates with mode 0600, the JSONL file in
// append mode. An empty path gives a nil logger, which is disabled.
func NewInvocationLog(path string) (*InvocationLog, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("gateway: falha ao abrir invocation log %q: %w", path, err)
	}
	return &InvocationLog{f: f}, nil
}

// Append serializes the record as a single JSON line and appends it to the
// file. The write is direct and unbuffered, so every call results in a
// write syscall and there is no loss in a user buffer on a crash, with only
// the buffer of the operating system left. It is safe for concurrent use
// through a mutex, and a nil logger does nothing. The line ceiling replaces
// execution metrics above the limit by a truncated stub, so the JSONL never
// carries the eight megabytes that a compromised node could send.
func (l *InvocationLog) Append(r InvocationRecord) error {
	if l == nil {
		return nil
	}
	r.ExecutionMetrics = truncateMetrics(r.ExecutionMetrics)
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("gateway: falha ao serializar invocation log: %w", err)
	}
	data = append(data, '\n')

	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.f.Write(data); err != nil {
		return fmt.Errorf("gateway: falha ao anexar invocation log: %w", err)
	}
	return nil
}

// truncateMetrics enforces the ceiling per line of the invocation log:
// within the ceiling the value is returned verbatim, and above it a valid
// JSON stub with the original size is returned, so a later analysis knows
// that there was a truncation without carrying the bytes.
func truncateMetrics(m json.RawMessage) json.RawMessage {
	if len(m) <= maxInvLogMetricsBytes {
		return m
	}
	stub, _ := json.Marshal(map[string]any{"truncated": true, "original_bytes": len(m), "cap_bytes": maxInvLogMetricsBytes})
	return stub
}

// Close closes the file. It is idempotent through a single-use
// synchronization, and a nil logger does nothing.
func (l *InvocationLog) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.closeErr = l.f.Close()
	})
	return l.closeErr
}
