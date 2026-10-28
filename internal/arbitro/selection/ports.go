// Reading ports of the selection algorithm: the package declares the
// interfaces it consumes, and the producers, which are the monitor, the
// history, the learner, the node store and the meter of the gateway, satisfy
// them structurally, without a reverse import.
//
// Replacing the canonical selector means implementing the selector interface
// over these ports, without touching the gateway or the server. The cost in
// the hot path is zero, since the call goes through the interface over a
// concrete receiver, without reflection and without an extra allocation per
// decision.
//
// Nil convention: pass a nil interface and never a typed nil pointer, since a
// typed pointer inside an interface is not nil and breaks the fail-open guards
// of the helpers.
package selection

import (
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// TelemetrySource is the reading of telemetry and occupancy that the
// selection consumes, with the snapshot, the instant of reception and the
// capacity per function. The producer is the monitor.
type TelemetrySource interface {
	Latest(nodeID string) (telemetry.Package, bool)
	LatestAt(nodeID string) (pkg telemetry.Package, recvAt time.Time, ok bool)
	GetCapacity(nodeID, funcName string, now time.Time) (telemetry.FunctionCapacity, bool)
	GetCapacityEx(nodeID, funcName string, now time.Time) (cap telemetry.FunctionCapacity, exhaustedMark bool, ok bool)
}

// HistorySource is the reading of the history that the selection consumes,
// with the expected cost, the queue curve, the replay and the observation
// hook. The producer is the history.
type HistorySource interface {
	ExpectedCost(group, nodeID string, window time.Duration, now time.Time) (cpuPct float64, n int, ok bool, seeded bool)
	QueueCurve(group, nodeID string, window time.Duration, now time.Time) (history.QueueCurve, bool)
	ForEachEntry(fn func(key string, e history.Entry))
	SetObserver(fn history.EntryObserver)
}

// LearnerPort is the statistical predictor that the selector consults per
// decision and feeds per observation. The producer is the learner.
type LearnerPort interface {
	Predict(now time.Time, group, node string, s learn.LiveState) learn.Prediction
	Observe(group, node string, o learn.Obs)
}

// InflightReader is the reading of the meter of requests in flight mutated by
// the gateway: the count per node, the count per pair of node and function and
// the projected cost since an instant. It is declared here, without importing
// the gateway, so that no cycle from the selection to the gateway exists, and
// the binding is by methods, as the previous function fields were. The
// producer is the meter of the gateway, with a structural assertion at the
// call site and without an import.
type InflightReader interface {
	NodeK(nodeID string) int
	FnK(nodeID, function string) int
	NodeCostSince(nodeID string, since time.Time) float64
}

// NodeLayerReader resolves the layer of a node, which is the fallback of the
// priors per layer. The producer is the node store.
type NodeLayerReader interface {
	Get(nodeID string) (nodes.Node, bool)
}

// StatsSnapshooter is the observational instrumentation that the server
// exposes in the node query, with atomic counters that no decision ever reads.
// The producer is the statistics of the selector. A future selector with
// another format exposes another port, without changing this one.
type StatsSnapshooter interface {
	Snapshot() RiskStatsSnapshot
}

var (
	_ TelemetrySource  = (*monitor.Monitor)(nil)
	_ HistorySource    = (*history.History)(nil)
	_ LearnerPort      = (*learn.Learner)(nil)
	_ NodeLayerReader  = (*nodes.NodeStore)(nil)
	_ StatsSnapshooter = (*RiskStats)(nil)
)
