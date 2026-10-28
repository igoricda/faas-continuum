package gateway

import (
	"encoding/json"
	"errors"

	"github.com/igoricda/faas-continuum/internal/pool"
)

const maxCapacityErrorBodyBytes = 64 * 1024

type capacityError struct {
	sentinel error
	nodeID   string
	code     string
	msg      string
}

// CapacityError is the typed identity of a capacity rejection. It is
// exported so that the consumers can inspect it through the error chain.
type CapacityError = capacityError

func (e *capacityError) Error() string { return e.msg }
func (e *capacityError) Unwrap() error { return e.sentinel }

// nodeErrorCode extracts the code field of the error body of the node. It
// reports no value when the body is not usable: truncated above 64
// kibibytes, empty, not JSON or without a code. It is shared by the path of
// the overload, which identifies the pool, and by the path of the timeout,
// which identifies the stage of the timeout, since the node knows in which
// stage the request expired and the timeout discarded that information
// before. It never panics.
func nodeErrorCode(body []byte) (string, bool) {
	if int64(len(body)) > maxCapacityErrorBodyBytes {
		return "", false
	}
	if !json.Valid(body) {
		return "", false
	}
	var v struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(body, &v) != nil || v.Code == "" {
		return "", false
	}
	return v.Code, true
}

// nodeErrorCodeOrEmpty is the extraction of the code with an empty string
// in place of the absent value, for a direct use in a switch, which treats
// the empty string as unknown.
func nodeErrorCodeOrEmpty(body []byte) string {
	code, _ := nodeErrorCode(body)
	return code
}

// NodeAdmissionPressure is the admission snapshot that the node writes in
// the body of the timeout when the function did not run.
//
// The number of live instances is the total of the live instances, and the
// name is the same as the available instances of the piggyback on purpose,
// since it is the same quantity. It is not the number of available
// instances: the pool has no count of free slots, and a reader who expects
// zero there will conclude wrongly. In a measured campaign the fog node
// reported 36, 37 and 39 live instances without ever reaching zero, and
// there were 36 live instances and yet no container, which is the
// signature of an admission starvation. That is what this field exists to
// make visible.
type NodeAdmissionPressure struct {
	PoolAlive     int   `json:"pool_alive"`
	QueueDepth    int   `json:"queue_depth"`
	AcquireWaitMS int64 `json:"acquire_wait_ms"`
}

// nodeAdmissionPressure extracts the snapshot from the body of the timeout
// that was already read. It reports no value, and the record keeps the
// values it already had, when the body is not valid JSON, exceeds the
// maximum size of the body or does not carry the pressure block. It is the
// same guard as the extraction of the code, reused: if the body was not
// reliable enough to decide the code, it is not reliable enough to bring a
// number, and inventing a zero would be worse than having none.
//
// The absence is a legitimate state and must not become a zero: an old node
// without the block and a node with an empty pool are different things, and
// the second carries information, since giving up with an empty queue is a
// starvation of resources and not of admission.
func nodeAdmissionPressure(body []byte) (NodeAdmissionPressure, bool) {
	var p NodeAdmissionPressure
	if int64(len(body)) > maxCapacityErrorBodyBytes || !json.Valid(body) {
		return p, false
	}
	var v struct {
		Pressure *NodeAdmissionPressure `json:"pressure"`
	}
	if json.Unmarshal(body, &v) != nil || v.Pressure == nil {
		return p, false
	}
	return *v.Pressure, true
}

// capacityAttemptErr extracts the identity of the capacity from the body of
// the overload that was already read. It returns the wrapped error of the
// exhausted pool or the full queue, and nothing when the body has to follow
// the generic path, which is the case of a truncated, empty or non-JSON body
// and of an unknown code. It never panics.
func capacityAttemptErr(body []byte, nodeID, fallbackMsg string) error {
	code, ok := nodeErrorCode(body)
	if !ok {
		return nil
	}
	return classifyCapacityError(code, nodeID, fallbackMsg)
}

// classifyAdmissionTimeout maps the code of the body of the timeout to the
// sentinel of the admission timeout. It reports no value for any other
// code, including the timeout of the invocation, which is the only timeout
// of the service, where the function ran and did not answer, and has to
// keep counting as a failure of the node.
//
// The distinction is the stage in which the deadline expired, and only the
// node knows which one it was:
//
//	ACQUIRE_TIMEOUT             it waited for the queue or the
//	                            provisioning, so the function never ran
//	INSTANCE_NOT_READY_TIMEOUT  the container came up and never became
//	                            ready, so the function never ran
//	INVOCATION_TIMEOUT          the function ran and did not answer
//
// The distinction was measured in a campaign: the fog node emitted the
// timeout of the invocation through the path of the acquisition, with the
// latency stuck at the timeout of the node, no function time and the host
// at two to ten percent of the processor with 36 live containers. The
// arbiter counted 264 of the 531 requests as a service failure and vetoed
// the fastest node of the system.
func classifyAdmissionTimeout(code string, nodeID, fallbackMsg string) (error, bool) {
	switch code {
	case "ACQUIRE_TIMEOUT", "INSTANCE_NOT_READY_TIMEOUT":
		return &capacityError{sentinel: ErrAttemptAdmissionTimeout, nodeID: nodeID, code: code, msg: fallbackMsg}, true
	default:
		return nil, false
	}
}

// IsCapacityRejection reports whether the error carries the identity of a
// capacity rejection, which is the exhausted pool or the full queue. It is
// the detection path that uses the sentinels of the pool package.
func IsCapacityRejection(err error) bool {
	return errors.Is(err, pool.ErrPoolExhausted) || errors.Is(err, pool.ErrQueueFull)
}

// classifyCapacityError maps the code of the body of the overload to an
// error that wraps the corresponding sentinel. It returns nothing for an
// unknown or empty code, so that the caller keeps the generic path. It
// never panics with an empty input.
func classifyCapacityError(code, nodeID, fallbackMsg string) error {
	switch code {
	case "POOL_EXHAUSTED":
		return &capacityError{sentinel: pool.ErrPoolExhausted, nodeID: nodeID, code: code, msg: fallbackMsg}
	case "QUEUE_FULL":
		return &capacityError{sentinel: pool.ErrQueueFull, nodeID: nodeID, code: code, msg: fallbackMsg}
	default:
		return nil
	}
}
