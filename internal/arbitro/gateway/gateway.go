// Package gateway implements the request and forwarding module of the
// arbiter: an invocation proxy with node selection, a queue and the
// cloud fallback.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/cattokens"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

var nullBytes = []byte("null")

var gatewayBufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// buildNodeURL builds the invocation URL of the node with a simple
// concatenation, without a pool. The benchmark shows one allocation of 48
// bytes, the same as the pooled version and about 5 ns faster, since the
// pool paid for the getter, the assertion and the growth for nothing. The
// buffer that is worth pooling is the one of 8 MiB, gatewayBufPool, which
// is reused across attempts.
func buildNodeURL(host, fn string) string {
	return "http://" + host + "/function/" + fn + "/invoke"
}

const maxInvokeBodyBytes = 8 * 1024 * 1024 // 8 MiB — espelha nodeapi.maxFunctionOutputBytes (B3)

const defaultGatewayTimeout = 30 * time.Second

const defaultStaleAfter = 90 * time.Second

// contentTypeHeader and applicationJSON duplicate the definitions of the
// server on purpose: the server imports the gateway, so importing them
// back would create a cycle.
const contentTypeHeader = "Content-Type"
const applicationJSON = "application/json"

// InvocationRequest represents an invocation request.
type InvocationRequest struct {
	Function string          `json:"function"`
	SLO      *selection.SLO  `json:"slo,omitempty"`
	Input    json.RawMessage `json:"input"`
}

// ErrorResponse represents an error returned by the API.
type ErrorResponse struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	Retryable       bool   `json:"retryable"`
	FallbackApplied bool   `json:"fallback_applied"`
}

// RemoteExecutor runs a function on the remote provider, which is the
// cloud. The port is declared in the consumer and the gateway depends only
// on it, never on the cloud package nor on its configuration. The producer
// satisfies it structurally, without a reverse import.
type RemoteExecutor interface {
	// Invoke runs the function with the raw input and returns the raw body.
	Invoke(ctx context.Context, fn string, input json.RawMessage) ([]byte, error)
	// Configured reports whether a fallback is in force. It tells apart the
	// absence of a configuration, in which case the caller answers with
	// the last failure of the queue, from a configured fallback that
	// failed, which gives the cloud fallback error.
	Configured() bool
}

// ColdEstimator is the optional capability of estimating the cold start in
// the cloud. It is optional on purpose: the base port stays minimal and
// the estimator is detected by an assertion, so an implementation that
// lacks it reports no cold start as before. The estimate is read
// immediately before the invocation of the same goroutine.
type ColdEstimator interface {
	// EstimateCold reports whether the next call of the function would
	// probably provision a container, according to the idle time or the
	// occupancy.
	EstimateCold(fn string) bool
}

// LatencyPredictor is the optional capability of the selector of
// estimating the latency of an execution in a node, used to size the slice
// of the attempt. It is optional and detected by an assertion, like the
// remote cold estimator: the gateway uses the prediction to size the slice
// of the attempt and the baseline queue selector, which does not implement
// it, keeps the equal division of the budget. The group is the canonical
// group already resolved by the handler and the objective may be absent.
type LatencyPredictor interface {
	PredictLatencyMS(group string, n nodes.Node, slo *selection.SLO) (ms int64, ok bool)
}

// NewRequestPredictor is the optional capability of the selector of
// estimating the latency of a new request, which is not yet in flight, in a
// node. It is the same estimator as the admission criterion, which only
// applies when the node is idle, with a single request and enough samples
// of the idle case. The viability gate prefers it to decide whether to try
// or to skip a candidate, since the decision has to see the node the way
// the criterion saw it. Without it the gate falls back to the prediction of
// the budget, which is conservative on purpose.
type NewRequestPredictor interface {
	PredictLatencyMSNewRequest(group string, n nodes.Node, slo *selection.SLO) (ms int64, ok bool)
}

// QueueCapPredictor is the optional capability of the selector of
// calibrating the queue ceiling of a node from the history: it returns the
// highest concurrency that is admissible for a new request in that node,
// learned from the delivery rate by queue depth. It reports no learned
// limit when there is none, which fails open: the gate keeps only the
// projection of the prediction times the concurrency. The ceiling is a
// capacity of the node and not a service level objective, so it applies
// even without a declared objective.
type QueueCapPredictor interface {
	PredictMaxQueueK(group string, n nodes.Node, slo *selection.SLO) (k int, ok bool)
}

// Gateway implements the invocation proxy of the arbiter.
type Gateway struct {
	nodes    *nodes.NodeStore
	mon      *monitor.Monitor
	selector selection.Selector
	// predictor is the selector seen as a latency predictor when it
	// implements the capability. A nil value means an equal division
	// of the budget.
	predictor LatencyPredictor
	// newReqPredictor is the selector seen as a predictor of new requests
	// when it implements the capability. A nil value means that the
	// gate uses the prediction of the budget.
	newReqPredictor NewRequestPredictor
	// queueCapPredictor is the selector seen as a predictor of the queue
	// ceiling when it implements the capability. A nil value means no
	// learned queue ceiling, so the gate uses only the projection of
	// the prediction times the concurrency.
	queueCapPredictor QueueCapPredictor
	client            *http.Client
	staleAfter        time.Duration
	defaultBudget     time.Duration
	remote            RemoteExecutor
	hist              *history.History
	invLog            *InvocationLog
	// catTokens translates a category token into the token of each node
	// per attempt. A nil value preserves the legacy verbatim
	// forwarding.
	catTokens  *cattokens.Store
	minSamples int
	criteria   []selection.SLOCriterion
	clock      func() time.Time
	lateGroups *lateGroupMap
	lateDedup  *lateSeen
	// liveIdx correlates the request identifier and the attempt number
	// with the synchronous entry, so that a late report supersedes the
	// entry of the budget instead of counting twice. It is bounded like
	// the late deduplication, since an external input needs a ceiling.
	// The mutex protects only the lazy initialization, since the
	// constructor already delivers everything ready.
	liveIdx       *liveAttemptIndex
	lateMu        sync.Mutex
	maxRaceFanout int
	// inflight is the shared concurrent meter: the gateway increments and
	// decrements it per attempt and the self-learning selector reads the
	// same data through function fields. It is safe with a nil value,
	// since an absent gauge means an unknown number of requests in
	// flight. The throughput window is the window of the history used
	// by the expected cost for the cost of the request itself.
	inflight         *InflightGauge
	throughputWindow time.Duration
	attemptsTotal    int64
	lateReceived     int64
	lateDropped      int64
	raceCancelled    int64
	// fanoutCuts counts the invocations whose queue was truncated by the
	// limit of the fan-out. Each truncation adds one here and one log
	// line. It has its own accessor, outside the counters, to preserve
	// the signature used by the late report tests.
	fanoutCuts int64
	// unviableSkips counts the candidates skipped by the viability gate,
	// whose predicted percentile exceeds the available slice while there
	// is a queue ahead, so the attempt never starts. Each skip adds one
	// here and one log line. It has its own accessor, for the same
	// reason as the truncation counter.
	unviableSkips int64
	// queueCapSkips counts the candidates skipped by the learned queue
	// ceiling: a concurrency above the last concurrency that the
	// delivery rate of the node did not contradict. Each skip adds one
	// here and one log line, with its own accessor.
	queueCapSkips int64
	// Counters of the best-effort delivery: the armed pending reports,
	// the late pending reports served, the pending reports that expired
	// within the hard deadline and the pending reports that were
	// cancelled, by a winner, a disconnection or the deadline. They have
	// their own accessor, following the same pattern as the truncation
	// counter.
	bestEffortArmed     int64
	bestEffortServed    int64
	bestEffortExpired   int64
	bestEffortCancelled int64
	// observeArmed enables the correction that observes the armed pending
	// reports during the slice of the current attempt and delivers the
	// first success without waiting for the queue to drain. It is
	// disabled by default. The counter of the wins of this path has its
	// own accessor.
	observeArmed     bool
	observeArmedWins int64
}

// GatewayConfig groups the construction parameters of the gateway in a
// typed object instead of ten positional arguments, so that the reader of
// the call site sees the name of every dependency. The zero value is
// valid: the constructor applies the same defaults as before for the
// staleness, the clock and the criteria.
type GatewayConfig struct {
	Nodes          *nodes.NodeStore
	Monitor        *monitor.Monitor
	Selector       selection.Selector
	RemoteExecutor RemoteExecutor
	History        *history.History
	// CatTokens enables the translation of the token per category in the
	// dispatch. A nil value, which is the default, preserves the legacy
	// verbatim forwarding.
	CatTokens     *cattokens.Store
	Criteria      []selection.SLOCriterion
	Clock         func() time.Time
	DefaultBudget time.Duration
	StaleAfter    time.Duration
	MinSamples    int
	MaxRaceFanout int
	// ThroughputWindow is the window of the history, given to the gateway
	// for the calculation of the cost of the request itself through the
	// expected cost of the history in the dispatch. The meter itself is
	// shared through the inflight binding, created once in the arbiter
	// and read by the selector.
	ThroughputWindow time.Duration
	// ObserveArmed enables the correction that observes the armed pending
	// reports during the loop. It is disabled by default until it is
	// validated.
	ObserveArmed bool
}

func newGatewayHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			Proxy:             nil,
		},
		Timeout: 0,
	}
}

// NewGatewayWithHistory creates a gateway with an injectable history,
// criteria and clock. A nil clock means the wall clock and a nil set of
// criteria means the default criteria of the selection package. The
// staleness is set explicitly by the caller and a zero value selects the
// default.
func NewGatewayWithHistory(cfg GatewayConfig) *Gateway {
	staleAfter := cfg.StaleAfter
	if staleAfter == 0 {
		staleAfter = defaultStaleAfter
	}
	client := newGatewayHTTPClient()
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	criteria := cfg.Criteria
	if criteria == nil {
		criteria = selection.DefaultCriteria
	}
	var predictor LatencyPredictor
	if p, ok := cfg.Selector.(LatencyPredictor); ok {
		predictor = p
	}
	var newReqPredictor NewRequestPredictor
	if p, ok := cfg.Selector.(NewRequestPredictor); ok {
		newReqPredictor = p
	}
	var queueCapPredictor QueueCapPredictor
	if p, ok := cfg.Selector.(QueueCapPredictor); ok {
		queueCapPredictor = p
	}
	return &Gateway{
		nodes:             cfg.Nodes,
		mon:               cfg.Monitor,
		selector:          cfg.Selector,
		predictor:         predictor,
		newReqPredictor:   newReqPredictor,
		queueCapPredictor: queueCapPredictor,
		client:            client,
		staleAfter:        staleAfter,
		defaultBudget:     cfg.DefaultBudget,
		remote:            cfg.RemoteExecutor,
		hist:              cfg.History,
		catTokens:         cfg.CatTokens,
		minSamples:        cfg.MinSamples,
		criteria:          criteria,
		clock:             clock,
		maxRaceFanout:     cfg.MaxRaceFanout,
		// The lazy initialization under a concurrent handler would be a
		// data race, so the constructor is the only production path.
		lateGroups: newLateGroupMap(10000, 5*time.Minute, clock),
		lateDedup:  newLateSeen(10000),
		liveIdx:    newLiveAttemptIndex(10000),

		throughputWindow: cfg.ThroughputWindow,
		observeArmed:     cfg.ObserveArmed,
	}
}

// WithInflight binds the shared meter of the requests in flight. A nil
// value keeps the meter disabled, so the number of requests in flight is
// unknown. It returns the gateway itself, so the calls can be chained.
func (g *Gateway) WithInflight(ig *InflightGauge) *Gateway {
	g.inflight = ig
	return g
}

// SetInvocationLog binds the optional append-only logger to the gateway.
// A nil value keeps the logger disabled. It returns the gateway itself, so
// the calls can be chained.
func (g *Gateway) SetInvocationLog(l *InvocationLog) *Gateway {
	g.invLog = l
	return g
}

// Counters exposes the observability counters of the plan: the attempts,
// the late reports received, the late reports discarded and the races
// cancelled. They are used by the tests.
func (g *Gateway) Counters() (attempts, lateReceived, lateDropped, raceCancelled int64) {
	return atomic.LoadInt64(&g.attemptsTotal),
		atomic.LoadInt64(&g.lateReceived),
		atomic.LoadInt64(&g.lateDropped),
		atomic.LoadInt64(&g.raceCancelled)
}

// FanoutCuts returns the truncations of the queue by the limit of the fan-out.
func (g *Gateway) FanoutCuts() int64 {
	return atomic.LoadInt64(&g.fanoutCuts)
}

// UnviableSkips returns the candidates skipped by the viability gate,
// whose prediction exceeds the slice while there is a queue ahead.
func (g *Gateway) UnviableSkips() int64 {
	return atomic.LoadInt64(&g.unviableSkips)
}

// QueueCapSkips returns the candidates skipped by the learned queue
// ceiling, whose concurrency exceeds the last concurrency that the delivery
// rate of the node did not contradict.
func (g *Gateway) QueueCapSkips() int64 {
	return atomic.LoadInt64(&g.queueCapSkips)
}

// BestEffortCounters returns the counters of the best-effort delivery:
// the armed pending reports, the served ones, the expired ones and the
// cancelled ones.
func (g *Gateway) BestEffortCounters() (armed, served, expired, cancelled int64) {
	return atomic.LoadInt64(&g.bestEffortArmed),
		atomic.LoadInt64(&g.bestEffortServed),
		atomic.LoadInt64(&g.bestEffortExpired),
		atomic.LoadInt64(&g.bestEffortCancelled)
}

// ObserveArmedWins returns the deliveries of the path that observes the
// armed pending reports, where an armed report won during the slice of the
// following attempt.
func (g *Gateway) ObserveArmedWins() int64 {
	return atomic.LoadInt64(&g.observeArmedWins)
}

// nodeFunctionTimeout returns the hard deadline of the function in the
// node of origin, which is the effective ceiling announced in the
// registration. It is the single source, with no fallback. It reports no
// value when the node does not announce one, which is the case of an old
// node, and then the attempt follows the current synchronous path. The
// function is pure.
func nodeFunctionTimeout(target nodes.Node, function string) (time.Duration, bool) {
	if len(target.FunctionTimeoutMS) == 0 {
		return 0, false
	}
	ms, ok := target.FunctionTimeoutMS[function]
	if !ok || ms <= 0 {
		return 0, false
	}
	return time.Duration(ms) * time.Millisecond, true
}

type nodeInvokePayload struct {
	Input json.RawMessage `json:"input"`
}

type gatewayCloudPhases struct {
	AcquireWaitMS int `json:"acquire_wait_ms"`
	ReadinessMS   int `json:"readiness_ms"`
	FunctionMS    int `json:"function_ms"`
	OverheadMS    int `json:"overhead_ms"`
}

type gatewayCloudMetrics struct {
	ResponseTimeMS int                `json:"response_time_ms"`
	StartedAt      string             `json:"started_at"`
	EndedAt        string             `json:"ended_at"`
	InstanceID     string             `json:"instance_id"`
	ColdStart      bool               `json:"cold_start"`
	Phases         gatewayCloudPhases `json:"phases"`
	CloudFallback  bool               `json:"cloud_fallback"`
}

type gatewayCloudSuccess struct {
	Result           json.RawMessage     `json:"result"`
	ExecutionMetrics gatewayCloudMetrics `json:"execution_metrics"`
}
