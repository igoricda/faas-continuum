package gateway

import (
	"encoding/json"
	"log"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// attemptCapture carries the load projection captured once in the dispatch
// of an attempt: the projected scenario, the post-increment count of
// requests in flight in the node and the projected processor usage.
// recordAttempt consumes these values verbatim and never rereads the
// monitor nor recomputes the scenario, since the memory bucket and the
// validity flag could otherwise change between the selection of the
// selector and the recording, breaking the agreement between the training
// and the query that the self-learning requires.
type attemptCapture struct {
	sc        history.ScenarioContext
	inflight  int
	projected float64
	// The observed processor and memory usage carry the continuous values
	// seen in the dispatch, with zero meaning unknown when the monitor
	// was unavailable. They are the mirror of the wire values of the
	// pressure signals: the record consumes them verbatim in the stamp
	// of the entry and in the live reference, without rereading the
	// monitor. The curve of usage trains with the projected load when it
	// is available and with the observed processor usage otherwise.
	observedCPU float64
	observedMem float64
	// concrete is the physical variant resolved in the dispatch, which is
	// the best variant with a declared quality and otherwise the resolved
	// concrete one, with the requested name as the fallback. It keys the
	// ingestion of capacity with the same key as the producer, since the
	// node API reports the occupancy by the concrete name and never by
	// the requested name, which may be a category. It is empty in the
	// legacy calls, where the ingestion falls back to the requested
	// name.
	concrete string
	// admPressure is the admission snapshot that the node wrote in the
	// body of the gateway timeout. It is the only moment in which the node
	// has a direct view of the admission pressure, and it is exactly the
	// path in which nothing was reported before.
	//
	// It is absent when the body does not carry the block, which is the
	// case of an old node, of a truncated body or of an error that is not
	// an admission error. The absence is different from zero: not
	// knowing is not an empty queue, and the entry is only filled when
	// the pointer exists.
	admPressure *NodeAdmissionPressure
}

// recordAttempt records an attempt against an edge or fog node.
//
// The naming is dual: the requested name is the one asked by the client
// and goes to the entry of the function, while the group is the one
// resolved over the candidate set and keys the statistics of the history,
// so that the attempts in a wildcard node are recorded under the same
// resolved group. The cloud never reaches this function.
//
// The quality flag reflects the static match of the quality, which is true
// on the verbatim path and on the attempts with an error, and false only
// on a serve with a downgrade. It is combined with the classified
// objective and does not touch the history beyond the entry.
//
// The captured values carry the scenario and the projected load of the
// dispatch, and the legacy calls and the tests pass the zero value.
//
// It orchestrates the construction of the entry and the storage of the
// entry, with no logic of its own beyond the ingestion, the single
// decoding and the early return.
func (g *Gateway) recordAttempt(target nodes.Node, latency time.Duration, err error, slo *selection.SLO, requested, group string, metrics json.RawMessage, qualityMet bool, requestID string, attemptN int, captured attemptCapture) {
	g.recordAttemptFull(target, latency, err, slo, requested, group, metrics, qualityMet, requestID, attemptN, captured, false, false)
}

// recordAttemptFull is the path with the flags of the armed and delivered
// states. The armed flag is set only in the event of arming, when an
// exceeded budget became a pending report, and the delivered flag only in
// the attempt whose answer was returned to the client. The legacy calls
// and the tests use the simpler path, with both flags false.
func (g *Gateway) recordAttemptFull(target nodes.Node, latency time.Duration, err error, slo *selection.SLO, requested, group string, metrics json.RawMessage, qualityMet bool, requestID string, attemptN int, captured attemptCapture, armed, delivered bool) {
	caps := capacityOfMetrics(metrics)
	g.ingestCapacity(target, requested, captured.concrete, caps, err)
	if g.hist == nil && g.invLog == nil {
		return
	}
	entry := g.buildEntry(target, latency, err, slo, requested, metrics, qualityMet, captured, caps)
	g.storeEntry(entry, group, err, slo, metrics, requestID, attemptN, armed, delivered)
}

// buildEntry builds the entry of an attempt without side effects: it does
// not touch the history, the invocation log or the live index. It only
// reads the clock of the gateway for the stamp and the criteria for the
// classification of the objective. The capacity map was already decoded
// once by the caller and is never decoded again.
func (g *Gateway) buildEntry(target nodes.Node, latency time.Duration, err error, slo *selection.SLO, requested string, metrics json.RawMessage, qualityMet bool, captured attemptCapture, caps map[string]telemetry.FunctionCapacity) history.Entry {
	outcome, sloMet := classifyAttempt(err, latency, slo, g.criteria)
	sloMet = sloMet && qualityMet
	// The enrichment by the invocation carries the decoder of every
	// measured quantity, all of them tolerant and zero when the metrics
	// are absent.
	coldStart, instanceID := coldInfoOfMetrics(metrics)
	cpuPctAvg, cpuPctPeak, memPeakBytes, sampleCount := resourceProfileOfMetrics(metrics)
	acquireWaitMS, readinessMS, functionMS := phasesOfMetrics(metrics)
	psiCpuSomeAvg10, psiMemFullAvg10 := psiOfMetrics(metrics)
	var queueDepth, instancesAvailable int
	if err == nil {
		if cap, ok := caps[captured.concrete]; ok {
			queueDepth, instancesAvailable = cap.QueueDepth, cap.InstancesAvailable
		}
	} else if captured.admPressure != nil {
		// The capacity piggyback only exists in a successful answer, so
		// an error used to mean the absence of occupancy, which was the
		// same gap as the producer seen from the other side. The node
		// writes the snapshot in the body of the timeout and it enters
		// here with the same destination in the entry.
		queueDepth = captured.admPressure.QueueDepth
		instancesAvailable = captured.admPressure.PoolAlive
		// The waiting time as well: in a timeout there are no metrics,
		// so without this it would stay zero exactly in the record that
		// needed it most.
		//
		// This has to stay inside the branch of the error. Outside of it
		// the snapshot of the timeout contaminated the success path,
		// which a test caught. In production the captured snapshot is
		// only filled in the branch of the timeout of the dispatch, so
		// the leak did not reach the real world; but the invariant
		// belongs to whoever guarantees it and not to whoever trusts
		// that the other point of the flow will not fill the field by
		// mistake.
		if captured.admPressure.AcquireWaitMS > 0 {
			acquireWaitMS = captured.admPressure.AcquireWaitMS
		}
	}
	return history.Entry{
		FunctionName:       requested,
		NodeID:             target.NodeID,
		Layer:              target.Layer,
		Timestamp:          g.clock(),
		ScenarioContext:    captured.sc,
		ObservedLatencyMS:  latency.Milliseconds(),
		SloMet:             sloMet,
		Outcome:            outcome,
		CPUPctAvg:          cpuPctAvg,
		CPUPctPeak:         cpuPctPeak,
		MemPeakBytes:       memPeakBytes,
		SampleCount:        sampleCount,
		AcquireWaitMS:      acquireWaitMS,
		ReadinessMS:        readinessMS,
		FunctionMS:         functionMS,
		QueueDepth:         queueDepth,
		InstancesAvailable: instancesAvailable,
		ColdStart:          coldStart,
		InstanceID:         instanceID,
		Inflight:           captured.inflight,
		ProjectedLoadPct:   captured.projected,
		ObservedCPUPct:     captured.observedCPU,
		ObservedMemPct:     captured.observedMem,
		PSICpuSomeAvg10:    psiCpuSomeAvg10,
		PSIMemFullAvg10:    psiMemFullAvg10,
	}
}

// storeEntry performs the dual write of an already built entry: the
// history, only when the outcome is recordable, and the invocation log,
// unconditionally. In the branch of the history the entry is indexed in
// both keys of the fan-out, the primary and the one aggregated by layer,
// through the live index, which only holds memory under its own lock. The
// cloud never reaches this function.
//
// The armed and delivered flags only go to the invocation log and leave
// the history and the entry untouched. The armed flag is set in the event
// of arming and the delivered flag in the attempt answered to the
// client.
func (g *Gateway) storeEntry(entry history.Entry, group string, err error, slo *selection.SLO, metrics json.RawMessage, requestID string, attemptN int, armed, delivered bool) {
	if g.hist != nil && isRecordable(err) {
		// The recordability policy admits the success, the legitimate
		// timeout, the exhausted budget and the capacity rejection into
		// the history, the last three as a failure without latency. The
		// unauthorized, the not found, the framing errors and the generic
		// failure only go to the invocation log below, unconditionally.
		if err := g.hist.AppendWithGroup(entry, group); err != nil {
			log.Printf("arbitro: history append: %v", err)
		}
		// The live correlation indexes the recorded entry by the request
		// identifier and the attempt number, so that the late report of
		// the same attempt supersedes it instead of counting twice, in
		// both keys of the fan-out, so that the aggregated segment does
		// not count the synchronous attempt and the late one. It only
		// holds memory under its own lock, without input or output, and
		// an empty request identifier or a non-positive attempt number
		// does not index anything.
		aggKey := ""
		if entry.Layer != "" {
			aggKey = history.AggLayerKey(group, entry.Layer, entry.ScenarioContext)
		}
		g.ensureLateState()
		// Besides the scenario, the live reference carries the number of
		// requests in flight, the projected load, the average
		// processor usage and the number of samples, together with the
		// complete captured set, so that the late report supersedes the
		// attempt and preserves everything that the dispatch captured,
		// since the wire of the late report carries none of it.
		g.liveIdx.note(requestID, attemptN, liveRef{
			segKey:             history.SegmentKey(group, entry.NodeID, entry.ScenarioContext),
			aggKey:             aggKey,
			nodeID:             entry.NodeID,
			timestamp:          entry.Timestamp,
			sc:                 entry.ScenarioContext,
			inflight:           entry.Inflight,
			projected:          entry.ProjectedLoadPct,
			cpuPctAvg:          entry.CPUPctAvg,
			cpuPctPeak:         entry.CPUPctPeak,
			memPeakBytes:       entry.MemPeakBytes,
			sampleCount:        entry.SampleCount,
			acquireWaitMS:      entry.AcquireWaitMS,
			readinessMS:        entry.ReadinessMS,
			functionMS:         entry.FunctionMS,
			queueDepth:         entry.QueueDepth,
			instancesAvailable: entry.InstancesAvailable,
			observedCPU:        entry.ObservedCPUPct,
			observedMem:        entry.ObservedMemPct,
			psiCpuSomeAvg10:    entry.PSICpuSomeAvg10,
			psiMemFullAvg10:    entry.PSIMemFullAvg10,
		})
	}
	// The append to a nil logger does nothing, since the logger is optional and disabled by default.
	if err := g.invLog.Append(InvocationRecord{Entry: entry, SLO: slo, ExecutionMetrics: metrics, RequestID: requestID, AttemptN: attemptN, Armed: armed, Delivered: delivered}); err != nil {
		log.Printf("arbitro: invocation log append: %v", err)
	}
}

// recordCloudAttempt records the outcome of the cloud fallback in the
// invocation log, with the requested objective and the metrics built by
// the gateway. The segmented history does not receive this line, since the
// self-learning only operates on the edge and the fog. With no telemetry
// of a node, the scenario is unknown. A nil logger does nothing.
func (g *Gateway) recordCloudAttempt(latency time.Duration, err error, slo *selection.SLO, functionName string, metrics json.RawMessage) {
	if g.invLog == nil {
		return
	}
	outcome, sloMet := classifyAttempt(err, latency, slo, g.criteria)
	entry := history.Entry{
		FunctionName:      functionName,
		NodeID:            cloudFallbackNodeID,
		Timestamp:         g.clock(),
		ScenarioContext:   history.ScenarioContext{LoadBucket: history.BucketUnknown},
		ObservedLatencyMS: latency.Milliseconds(),
		SloMet:            sloMet,
		Outcome:           outcome,
		ColdStart:         false,
		InstanceID:        cloudFallbackNodeID,
	}
	if err := g.invLog.Append(InvocationRecord{Entry: entry, SLO: slo, ExecutionMetrics: metrics}); err != nil {
		log.Printf("arbitro: invocation log append: %v", err)
	}
}

// recordNoNode records in the invocation log the invocations rejected with
// no node available, before any attempt, with the objective that could not
// be met. The objective is always unmet, since outside the domain of a
// real attempt a latency of zero cannot count as a met objective. A nil
// logger does nothing.
func (g *Gateway) recordNoNode(functionName string, slo *selection.SLO) {
	if g.invLog == nil {
		return
	}
	entry := history.Entry{
		FunctionName:      functionName,
		NodeID:            noNodeAvailableID,
		Timestamp:         g.clock(),
		ScenarioContext:   history.ScenarioContext{LoadBucket: history.BucketUnknown},
		ObservedLatencyMS: 0,
		SloMet:            false,
		Outcome:           history.OutcomeFalha,
		ColdStart:         false,
		InstanceID:        "",
	}
	if err := g.invLog.Append(InvocationRecord{Entry: entry, SLO: slo}); err != nil {
		log.Printf("arbitro: invocation log append: %v", err)
	}
}
