// Package history implements the history segmented by the scenario of load.
// The store lives in another file of the package, and this file only defines
// types and pure helpers, with no mutable state and no input or output.
package history

import (
	"math"
	"strings"
	"sync"
	"time"
)

var segKeyPool = sync.Pool{New: func() any { return new(strings.Builder) }}

// ScenarioContext segments the history by the range of load at the moment
// of the invocation. The collapse uses a maximum and has no backward
// compatibility: the two dimensions of the processor and the memory
// collapse into a single bucket, which is the maximum of their ranks, so
// there are six segments per pair of group and node instead of 36. The
// reason is the density of samples, which reaches the ready state faster,
// since the two-dimensional granularity fragmented the curve into sparse
// keys without any gain in the decision, and the veto of overload remains
// two-dimensional in the selection package.
type ScenarioContext struct {
	LoadBucket string `json:"load_bucket"`
}

// Ranges of load used to group the historical records. A percentage below
// ten is consistent with the telemetry, and a negative percentage or a
// non-numeric value is unknown.
const (
	BucketUnknown = "unknown"
	Bucket0_10    = "0-10%"
	Bucket10_25   = "10-25%"
	Bucket25_50   = "25-50%"
	Bucket50_75   = "50-75%"
	Bucket75Plus  = "75%+"
)

// LoadBucket converts a percentage into a discrete bucket.
//   - a negative or non-numeric value gives the unknown bucket
//   - below ten gives the first range
//   - below 25 gives the second range
//   - below 50 gives the third range
//   - below 75 gives the fourth range
//   - 75 or more gives the last range, and a value above 100 is clamped
//     into it
func LoadBucket(pct float64) string {
	if math.IsNaN(pct) || pct < 0 {
		return BucketUnknown
	}
	switch {
	case pct < 10:
		return Bucket0_10
	case pct < 25:
		return Bucket10_25
	case pct < 50:
		return Bucket25_50
	case pct < 75:
		return Bucket50_75
	default:
		return Bucket75Plus
	}
}

// BucketRank orders the buckets by severity, and the unknown value, which
// is negative, loses to any known bucket in the maximum. It is exported
// because the learned queue ceiling of the selection package iterates the
// gaps of the concurrency in order of severity and the late flow collapses
// the percentage into a single load bucket.
func BucketRank(b string) int {
	switch b {
	case Bucket0_10:
		return 0
	case Bucket10_25:
		return 1
	case Bucket25_50:
		return 2
	case Bucket50_75:
		return 3
	case Bucket75Plus:
		return 4
	default:
		return -1
	}
}

// MaxBucket returns the most severe of two buckets, which is the collapse
// of the old key with four dimensions into the key of the group, the node
// and the maximum.
func MaxBucket(a, b string) string {
	if BucketRank(b) > BucketRank(a) {
		return b
	}
	return a
}

// SegmentKey produces the primary key of the history, which is the group,
// the node and the maximum bucket, with null separators against collisions.
// This is an authorized break of the format: files persisted without the
// version marker are rejected by the constructor with a fail-closed error
// that points to the migration tool, since the previous note that the
// operator would simply remove the file was revoked. A file of the previous
// version decodes a zero pressure at the level of the entry, with a
// tolerant decoding, but the file without the marker does not load.
func SegmentKey(group, nodeID string, sc ScenarioContext) string {
	b := segKeyPool.Get().(*strings.Builder)
	b.Grow(len(group) + len(nodeID) + len(sc.LoadBucket) + 2)
	b.WriteString(group)
	b.WriteByte(0)
	b.WriteString(nodeID)
	b.WriteByte(0)
	b.WriteString(sc.LoadBucket)
	s := b.String()
	b.Reset()
	segKeyPool.Put(b)
	return s
}

// aggLayerPrefix marks the keys aggregated by layer. The prefix starts with
// a null byte, so it never collides with a primary key, since the group
// never contains a null byte, and the filtering by this prefix is how the
// keys of a node are told apart.
const aggLayerPrefix = "\x00L\x00"

// AggLayerKey produces the secondary key of the fan-out, which is the
// group, the layer and the maximum bucket. Every append with a layer writes
// the same entry here in addition to the primary key, which gives a dense
// read of constant cost per layer while the segment by node has not reached
// the minimum number of samples, since the selection picks the segment with
// the highest count.
func AggLayerKey(group, layer string, sc ScenarioContext) string {
	return aggLayerPrefix + group + "\x00" + layer + "\x00" + sc.LoadBucket
}

// IsAggLayerKey tells an aggregated key from a primary one.
func IsAggLayerKey(key string) bool {
	return strings.HasPrefix(key, aggLayerPrefix)
}

// Entry is the record of a historical execution. The function name keeps
// the requested name, the key uses the group resolved by the selection
// package, and the layer routes the fan-out to the aggregated key, with an
// empty layer disabling the fan-out.
//
// Enrichment per invocation:
//   - Inflight is the number of requests dispatched by the arbiter that
//     are concurrent in the node, including this one, so it is at least
//     one. Zero or absent means unknown, which is the case of data from
//     before the field and of an orphan late report without the side
//     channel, and such a record stays out of the idle and contended
//     classes.
//   - CPUPctAvg is the processor usage observed per invocation, with the
//     same normalization as the node, and zero or absent means unknown,
//     which never pulls the estimator of the expected cost.
//   - SampleCount is the confidence of the processor sample, that is, the
//     number of sampled windows.
//   - ProjectedLoadPct is the projected processor load of the node at the
//     dispatch, for audit only, since it takes part neither in the
//     percentile, nor in the classes, nor in the expected cost.
//
// Complete capture per invocation, which is capture only and takes part in
// no guard, ordering, percentile or expected cost:
//   - CPUPctPeak is the peak of the processor usage per window, with the
//     same normalization as the average, and a negative value is clamped
//     to zero.
//   - MemPeakBytes is the peak of the memory in bytes.
//   - AcquireWaitMS, ReadinessMS and FunctionMS are the stages of the
//     invocation, which the late report does not carry on the wire and
//     inherits through the live reference.
//   - QueueDepth and InstancesAvailable are the occupancy of the pool of
//     the concrete variant at the instant of the answer, used to calibrate
//     the tier of the queue depth, and zero or absent means unknown.
//
// Unknown fields decode as zero, with a tolerant decoding and without any
// migration of the file, which fails open, since an absent value never
// diverges the percentile or the classes.
//
// The pressure signals per invocation:
//   - PSICpuSomeAvg10 is the average processor pressure of the telemetry
//     piggyback at the moment of the dispatch, and PSIMemFullAvg10 is the
//     average of the full memory pressure, with the same convention of a
//     positive value passing and zero meaning unknown.
//   - Both are omitted when empty, so a zero entry never serializes the
//     keys and the older files stay clean, and a file of the previous
//     version without the keys decodes a zero pressure, which is the same
//     convention of the average usage.
//   - They are capture only, since none of them takes part in a guard,
//     ordering, percentile or expected cost, and the estimator reads them
//     through an accessor.
//   - The file gains the version marker, which is a sentinel key, and the
//     constructor rejects a file without the marker with a fail-closed
//     error that points to the migration tool. There is no permanent dual
//     write.
//
// Continuous use without the pressure signals: a device without the
// pressure support always carries a zero pressure, so the curve of usage
// trains on the continuous processor and memory usage instead:
//   - ObservedCPUPct is the observed processor usage of the node at the
//     dispatch, with the same normalization as the average, and zero or
//     absent means unknown.
//   - ObservedMemPct is the observed memory usage of the node at the
//     dispatch, with the same normalization as the telemetry package, and
//     zero or absent means unknown.
//   - Both are omitted when empty, so a zero entry never serializes the
//     keys and the older files stay clean, and a file without the keys
//     decodes zero, which is the same convention of the pressure signals.
//   - They are capture only, and the estimator reads them through an
//     accessor.
//
// The fields are ordered from the largest to the smallest for the alignment
// of eight bytes.
type Entry struct {
	Timestamp          time.Time       `json:"timestamp"`
	FunctionName       string          `json:"function_name"`
	NodeID             string          `json:"node_id"`
	Outcome            string          `json:"outcome"`
	InstanceID         string          `json:"instance_id,omitempty"`
	Layer              string          `json:"layer,omitempty"`
	ScenarioContext    ScenarioContext `json:"scenario_context"`
	ObservedLatencyMS  int64           `json:"observed_latency_ms"`
	AcquireWaitMS      int64           `json:"acquire_wait_ms,omitempty"`
	ReadinessMS        int64           `json:"readiness_ms,omitempty"`
	FunctionMS         int64           `json:"function_ms,omitempty"`
	ProjectedLoadPct   float64         `json:"projected_load_pct,omitempty"`
	CPUPctAvg          float64         `json:"cpu_pct_avg,omitempty"`
	CPUPctPeak         float64         `json:"cpu_pct_peak,omitempty"`
	PSICpuSomeAvg10    float64         `json:"psi_cpu_some_avg10,omitempty"`
	PSIMemFullAvg10    float64         `json:"psi_mem_full_avg10,omitempty"`
	ObservedCPUPct     float64         `json:"observed_cpu_pct,omitempty"`
	ObservedMemPct     float64         `json:"observed_mem_pct,omitempty"`
	MemPeakBytes       uint64          `json:"mem_peak_bytes,omitempty"`
	Inflight           int             `json:"inflight,omitempty"`
	SampleCount        int             `json:"sample_count,omitempty"`
	QueueDepth         int             `json:"queue_depth,omitempty"`
	InstancesAvailable int             `json:"instances_available,omitempty"`
	SloMet             bool            `json:"slo_met"`
	ColdStart          bool            `json:"cold_start"`
	// LateDelivered marks the best-effort delivery, which is the success of
	// an armed pending report delivered after the slice. Only the
	// settlement of the waiting stage sets it, and the synchronous path
	// and the late report never do. It is omitted when empty, so older
	// lines decode as false, the same convention as the capture-only
	// fields.
	LateDelivered bool `json:"late_delivered,omitempty"`
}

// Possible outcomes of an invocation.
//
// The exceeded budget means that the budget of the arbiter ran out in the
// middle of the attempt and is not a measurement of the node, since the
// latency would be the remaining budget, so the record only reaches the
// invocation log and never the percentile, the count or the recent rate.
// It is distinct from a timeout, where the function timed out, either from
// a real timeout of the node or from the deadline of the call with budget
// to spare.
//
// The capacity rejection is the overload of the node with an exhausted pool
// or a full queue, where the node is correctly refusing work that it cannot
// do. It is a saturation and not a lack of reliability, which is why it has
// its own outcome and stays out of the totals and of the success rate that
// measure the reliability of the service. Before this change it fell into
// the generic failure, and the veto by rate punished the node for being
// saturated, which was measured in a campaign where the fastest node of the
// system ended up vetoed in every bucket with 97 samples and no success.
const (
	OutcomeSucesso           = "sucesso"
	OutcomeFalha             = "falha"
	OutcomeTimeout           = "timeout"
	OutcomeOrcamentoExcedido = "orcamento_excedido"
	// The capacity rejection is the capacity overload of the node. It is
	// distinct from the generic failure, which is an error of the function
	// or of the transport, and from the timeout, where the function did
	// not answer in time.
	OutcomeRejeicaoCapacidade = "rejeicao_capacidade"
	// The admission timeout is the timeout in which the deadline expired
	// in the admission stage of the node, where the request did not get an
	// instance, in the queue and the provisioning or in an instance that
	// never became ready, and the function never ran. It goes to the same
	// bucket of saturation as the capacity rejection, with its own outcome
	// so that the arbiter can tell a fast refusal from a wait until the
	// deadline, which require different demotions when the pressure enters
	// the proportional demotion.
	//
	// It was measured in a campaign where the fog node emitted the same
	// code as the timeout of the function through this path, with no
	// function time, the latency stuck at the timeout of the node and the
	// host idle. The veto could not see that the function never ran.
	OutcomeTimeoutAdmissao = "timeout_admissao"
)
