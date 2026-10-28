package gateway

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// inheritLiveRef copies the inheritable fields of the segment and of the
// cost of the synchronous attempt into the entry of the late report: the
// scenario, the number of requests in flight, the projected load, the
// average and the peak of the processor usage, the peak of the memory, the
// wait of the acquisition, the readiness, the function time, the depth of
// the queue, the number of available instances, the observed usage and the
// pressure signals. It is only called with a live reference.
func inheritLiveRef(entry *history.Entry, ref liveRef) {
	entry.ScenarioContext = ref.sc
	entry.Inflight = ref.inflight
	entry.ProjectedLoadPct = ref.projected
	entry.CPUPctAvg = ref.cpuPctAvg
	entry.SampleCount = ref.sampleCount
	entry.CPUPctPeak = ref.cpuPctPeak
	entry.MemPeakBytes = ref.memPeakBytes
	entry.AcquireWaitMS = ref.acquireWaitMS
	entry.ReadinessMS = ref.readinessMS
	entry.FunctionMS = ref.functionMS
	entry.QueueDepth = ref.queueDepth
	entry.InstancesAvailable = ref.instancesAvailable
	entry.ObservedCPUPct = ref.observedCPU
	entry.ObservedMemPct = ref.observedMem
	entry.PSICpuSomeAvg10 = ref.psiCpuSomeAvg10
	entry.PSIMemFullAvg10 = ref.psiMemFullAvg10
}

// supersedeLiveAttempt removes the synchronous entry of the same attempt
// from the two keys of the fan-out, the primary and the aggregated one, so
// that neither counts twice. There is no correlation outside this helper,
// and without a live reference the caller does not even call it.
func (g *Gateway) supersedeLiveAttempt(ref liveRef) {
	segKey, aggKey := ref.supersedeKeys()
	ts, node := ref.timestamp, ref.nodeID
	sameAttempt := func(e history.Entry) bool {
		return e.NodeID == node && e.Timestamp.Equal(ts)
	}
	g.hist.Supersede(segKey, sameAttempt)
	if aggKey == "" {
		return
	}
	g.hist.Supersede(aggKey, sameAttempt)
}

// recordLateAttempt records a late report in the history and in the
// invocation log. The timestamp is the start of the report and the report
// is rejected when it is after the injectable clock, never the wall clock.
// The deduplication is by the pair of the request identifier and the
// attempt number, with an atomic reservation under one lock and the appends
// outside of it. The synchronous entry of the same attempt is superseded
// through the live index, so the statistics do not count it twice. The
// scenario comes from the real buckets, taking the report, the telemetry of
// the node and the unknown value in this order, under the group of the
// invocation, so that the late report trains the segment that the selection
// actually queries. The objective is derived by the same criteria as the
// classification of the attempt, over the real latency.
//
// It reports whether the report was applied: true means it was recorded and
// the endpoint answers 202, and false without an error means a duplicate
// and the endpoint answers 208, while false with an error means a failure
// and the endpoint answers 500. A disabled channel, without a history and
// an invocation log, is an accepted no-op that preserves the legacy
// answer.
func (g *Gateway) recordLateAttempt(rep LateReport) (bool, error) {
	if g.hist == nil && g.invLog == nil {
		return true, nil
	}
	if err := ValidateLateReport(&rep); err != nil {
		return false, err
	}
	startedAt, err := time.Parse(time.RFC3339Nano, rep.StartedAt)
	if err != nil {
		return false, fmt.Errorf("%w: started_at inválido: %w", ErrLateReportInvalid, err)
	}
	if startedAt.After(g.now()) {
		return false, fmt.Errorf("%w: started_at no futuro: %v", ErrLateReportInvalid, startedAt)
	}
	// The atomic reservation and the single take go through the claim of
	// the live attempt: the deduplication and the live index are shared
	// with the settlement of the best-effort delivery, so the first one
	// to arrive records and the other counts a dropped late report.
	claimed, ref, hasRef := g.claimLiveAttempt(rep.RequestID, rep.AttemptN)
	if !claimed {
		return false, nil
	}

	group, slo := g.lateGroupAndSLO(rep.RequestID)
	if group == "" {
		group = rep.Function
	}
	sc := g.lateScenario(rep)
	layer := ""
	if g.nodes != nil {
		if n, ok := g.nodes.Get(rep.NodeID); ok {
			layer = n.Layer
		}
	}

	// The live reference was already consumed once by the claim above,
	// since a second take would return nothing and the synchronous entry
	// would never be superseded, which would count twice. There is a
	// live reference only when the history recorded the synchronous
	// entry.

	entry := history.Entry{
		Timestamp:         startedAt,
		FunctionName:      rep.Function,
		NodeID:            rep.NodeID,
		Layer:             layer,
		Outcome:           rep.Outcome,
		ScenarioContext:   sc,
		ObservedLatencyMS: rep.ObservedLatencyMS,
		SloMet:            g.classifyLateSloMet(rep, slo),
		ColdStart:         rep.ColdStart,
		InstanceID:        "",
	}
	// Inheritance of the segment and of the cost of the synchronous
	// attempt: the late report supersedes the same entry, so it has to
	// land in the same segment that the synchronous record used and
	// preserve the load and the cost captured in the dispatch. Only an
	// orphan late report, without a live reference, derives the scenario
	// from the current telemetry.
	//
	// The precedence of the pressure signals is consistent with the
	// segment and the cost: with a live reference the inherited signals
	// win, since the wire of the late report does not carry a reliable
	// signal per invocation and the synchronous record already stamped
	// the values of the piggyback, so the body of the report is ignored
	// on this path. Without a live reference, in an orphan, the signal of
	// the body wins when it is positive, otherwise the current telemetry
	// of the node is used, and without any data the value stays unknown.
	// It never panics.
	if !hasRef {
		entry.ObservedCPUPct, entry.ObservedMemPct = g.lateObserved(rep)
		entry.PSICpuSomeAvg10, entry.PSIMemFullAvg10 = g.latePSI(rep)
	} else {
		inheritLiveRef(&entry, ref)
	}

	if err := g.publishSettlement(rep.RequestID, rep.AttemptN, ref, hasRef, entry, group, slo, nil, true, false, false); err != nil {
		return false, err
	}
	atomic.AddInt64(&g.lateReceived, 1)
	log.Printf("arbitro: LATE_RECEIVED function=%s node=%s request_id=%s attempt_n=%d outcome=%s latency_ms=%d", rep.Function, rep.NodeID, rep.RequestID, rep.AttemptN, rep.Outcome, rep.ObservedLatencyMS)
	return true, nil
}

// now returns the injectable clock of the gateway.
func (g *Gateway) now() time.Time {
	if g.clock != nil {
		return g.clock()
	}
	return time.Now()
}

// ensureLateState covers a construction outside the constructor, which only
// happens in the legacy tests, since the production path already arrives
// with everything ready.
func (g *Gateway) ensureLateState() {
	g.lateMu.Lock()
	defer g.lateMu.Unlock()
	if g.lateDedup == nil {
		g.lateDedup = newLateSeen(10000)
	}
	if g.lateGroups == nil {
		g.lateGroups = newLateGroupMap(10000, 5*time.Minute, g.clock)
	}
	if g.liveIdx == nil {
		g.liveIdx = newLiveAttemptIndex(10000)
	}
}

// lateGroupAndSLO resolves the group and the objective of the invocation by the request identifier.
func (g *Gateway) lateGroupAndSLO(requestID string) (string, *selection.SLO) {
	g.ensureLateState()
	e, ok := g.lateGroups.entry(requestID)
	if !ok {
		return "", nil
	}
	return e.group, e.slo
}

// RegisterLateGroup records the group and the objective of a request
// identifier in the bounded map. It is called by the handler when an
// attempt is made, so that the late report finds the correct group even
// after the original map expired and derives the objective with the one of
// the invocation.
func (g *Gateway) RegisterLateGroup(requestID, group string, slo *selection.SLO) {
	g.ensureLateState()
	g.lateGroups.set(requestID, group, slo)
}

// RecordLate implements the sink of the late reports.
func (g *Gateway) RecordLate(report LateReport) (bool, error) {
	return g.recordLateAttempt(report)
}
