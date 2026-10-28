package gateway

import (
	"context"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// minAttemptSlice is the floor of the budget of one attempt, and the
// minimum reserve per pending attempt. Without it, a tight budget would be
// divided into useless slices, such as 50 ms, and no node would have a
// real chance to answer. 250 ms covers the acquisition, the readiness and
// the warm function of the edge or the fog in the priming data, whose
// median is between 50 and 500 ms. The number is a policy constant and not
// a service level threshold.
const minAttemptSlice = 250 * time.Millisecond

// cloudReserveCap is the ceiling of the budget reserve for the cloud
// fallback when a connector is configured. It is calibrated from the real
// percentile of the warm fallback with some margin. Without a connector
// there is no reserve and the whole budget stays with the local nodes, as
// before.
const cloudReserveCap = 2 * time.Second

// cloudReserve returns how much of the remaining budget is kept for the
// cloud: at most the reserve cap and never more than half of the remaining
// budget, since the current local attempt also needs a chance. It returns
// zero when nothing remains or when there is no cloud.
func cloudReserve(remaining time.Duration, hasCloud bool) time.Duration {
	if !hasCloud || remaining <= 0 {
		return 0
	}
	r := remaining / 2
	if r > cloudReserveCap {
		r = cloudReserveCap
	}
	return r
}

// attemptSlice computes the slice of the global budget of the current
// attempt. It is a pure function, testable with an injected clock.
//
// The rules are:
//   - a remaining budget of zero or less gives zero, so the caller does
//     not try;
//   - the last attempt gets the remaining budget minus the cloud reserve,
//     falling back to the remaining budget when the reserve would put the
//     attempt below the floor;
//   - any other attempt gets at most the remaining budget minus the
//     reserve minus the floor times the number of pending attempts, which
//     guarantees the floor of every pending attempt and the cloud
//     reserve, so that the last attempt does not consume everything and
//     the cloud fallback stays reachable;
//   - with a prediction, the slice is twice the prediction, clamped to the
//     floor and to the ceiling above, and the margin of a factor of two
//     absorbs the variance of the percentile without leaving the next
//     attempt or the cloud without time;
//   - without a prediction, at most half of the remaining budget, limited
//     by the ceiling, and not the equal share. The equal share cut below
//     the real cost of the node when the budget was not the bottleneck:
//     with an objective of five seconds and five candidates each attempt
//     got a thousand milliseconds and a node that needs two seconds was
//     never reached, and the cut became an exceeded budget, which is a
//     decision of the arbiter and not a cost of the node. The absence of
//     data gets the benefit of the doubt, mirroring the fail-open
//     behaviour of the selector, while the next attempts and the cloud
//     keep the floor and the reserve guaranteed by the ceiling. A budget
//     so tight that the share falls below the floor gives the share
//     anyway, since there is nothing to reserve.
//
// The result never exceeds the remaining budget and is never negative.
func attemptSlice(remaining time.Duration, attemptsLeft int, predicted time.Duration, hasPredicted bool, reserve time.Duration) time.Duration {
	if remaining <= 0 || attemptsLeft <= 0 {
		return 0
	}
	if attemptsLeft == 1 {
		if reserve > 0 && remaining > reserve+minAttemptSlice {
			return remaining - reserve
		}
		return remaining
	}
	share := remaining / time.Duration(attemptsLeft)
	if share < minAttemptSlice {
		// The budget is too tight to reserve the floor for the next
		// attempts, so it is divided equally and the last attempt gets
		// the rest, if any.
		return share
	}
	maxCurrent := remaining - reserve - minAttemptSlice*time.Duration(attemptsLeft-1)
	if maxCurrent < minAttemptSlice {
		maxCurrent = share
	}
	if !hasPredicted || predicted <= 0 {
		// The absence of a prediction gets the benefit of the doubt: at
		// most half of the remaining budget, with the ceiling
		// guaranteeing the floor of the next attempts and the cloud
		// reserve.
		target := remaining / 2
		if target > maxCurrent {
			target = maxCurrent
		}
		return target
	}
	target := 2 * predicted
	if target < minAttemptSlice {
		target = minAttemptSlice
	}
	if target > maxCurrent {
		target = maxCurrent
	}
	return target
}

// attemptViable is the viability gate. With a prediction, a candidate is
// skipped when the projected prediction exceeds the declared ceiling of
// the invocation, which is the same criterion as the admission criterion of
// the selector. The projection is the prediction times the concurrency
// rank of the node, which is the number of requests in flight plus one:
// the percentile of the estimator describes a single execution and does
// not see the queue that the burst itself creates. The slice is a waiting
// window and not an admission criterion: in the best-effort mode the
// attempt that exceeds the slice arms a pending report and stays alive,
// so a slice smaller than the prediction is no longer a guaranteed waste
// and is in fact the mechanism.
//
// Without a declared objective, the ceiling is the budget of the node and
// the function, which is the effective ceiling announced by the agent in
// the registration, limited by the remaining budget of the invocation. The
// slice is not admissible as a ceiling, since it is a waiting window of
// twice the prediction, and using it degenerated into a rule that skipped
// every node with a concurrency of two or more.
//
// The gate fails open in three cases:
//   - without a prediction, the absence of data cannot be treated as
//     knowledge that it does not fit, mirroring the selector, which only
//     removes a prediction above the objective;
//   - the last candidate with a concurrency of one or less, where there is
//     no local alternative and no queue of our own, so trying is better
//     than answering a service unavailable without trying. The cloud is a
//     separate path with its own reserve. The admission by load does not
//     inherit this fail-open: with the node already queued, the losing
//     attempt only saturates the layer, so the last candidate is skipped
//     as well and the cloud is the reserved path;
//   - any doubt about the margin: the prediction is a percentile and not a
//     median, and the policy is conservative on purpose.
//
// The skips are measured by the counter of the unviable candidates and by
// the log line that carries the ceiling, the prediction and the
// concurrency.
func attemptViable(teto, predicted time.Duration, hasPredicted bool, slo *selection.SLO, k int, isLast bool) bool {
	if !hasPredicted || predicted <= 0 {
		return true
	}
	if k < 1 {
		k = 1
	}
	if k == 1 && isLast {
		return true
	}
	// A concurrency of one gives the prediction of a single execution. A
	// concurrency of two or more gives the projection of the queue, which
	// applies even to the last candidate. The domain of the prediction is
	// milliseconds of telemetry or history and the concurrency is at most
	// the number of nodes, so there is no risk of overflow.
	projected := predicted * time.Duration(k)
	if slo != nil && slo.MaxLatencyMS > 0 {
		return projected <= time.Duration(slo.MaxLatencyMS)*time.Millisecond
	}
	return projected <= teto
}

// newRequestLatencyMS queries the optional predictor of new requests,
// which predicts the latency of a request that is not yet in flight and
// takes the class of the node into account, and falls back to the
// prediction of the budget when the selector does not implement the
// capability or has no sample. It is the number that the viability gate
// compares with the ceiling: the admission of the selector and the gate
// of the dispatch can never disagree about the same node, otherwise the
// best local candidate is discarded after it was admitted.
func (g *Gateway) newRequestLatencyMS(group string, target nodes.Node, slo *selection.SLO) (time.Duration, bool) {
	if g.newReqPredictor != nil {
		if ms, ok := g.newReqPredictor.PredictLatencyMSNewRequest(group, target, slo); ok && ms > 0 {
			return time.Duration(ms) * time.Millisecond, true
		}
	}
	return g.predictedLatencyMS(group, target, slo)
}

// remainingOverall returns the time left in the overall budget of the
// invocation. With a deadline in the context, which is the production
// path since the handler always creates the context with a timeout, it
// uses the same formula as the cloud fallback, so that the slice and the
// fallback see the same clock. Without a deadline, in a test context that
// does not go through the handler, it falls back to the overall budget
// minus the real elapsed time. It does not use the injectable clock on
// purpose: an extra call would consume a tick of the progressive fake
// clocks and shift the measured latency.
func remainingOverall(ctx context.Context, overall time.Duration, start time.Time) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline)
	}
	return overall - time.Since(start)
}

// predictedLatencyMS queries the optional predictor of the selector. It
// reports no value when the selector does not implement the capability,
// when there is no sample for the segment or when the value is not
// positive. It never invents a prediction: the absence of data falls back
// to the slice of half of the remaining budget and never reaches the
// viability gate.
func (g *Gateway) predictedLatencyMS(group string, target nodes.Node, slo *selection.SLO) (time.Duration, bool) {
	if g.predictor == nil {
		return 0, false
	}
	ms, ok := g.predictor.PredictLatencyMS(group, target, slo)
	if !ok || ms <= 0 {
		return 0, false
	}
	return time.Duration(ms) * time.Millisecond, true
}

// maxQueueK queries the learned queue ceiling. It reports no value when
// the selector does not implement the capability, when there is no learned
// limit or when the value is below one. It fails open on purpose: without
// a ceiling the gate keeps only the projection of the prediction times the
// concurrency, so the self-learning never invents a limit without
// evidence.
func (g *Gateway) maxQueueK(group string, target nodes.Node, slo *selection.SLO) (int, bool) {
	if g.queueCapPredictor == nil {
		return 0, false
	}
	k, ok := g.queueCapPredictor.PredictMaxQueueK(group, target, slo)
	if !ok || k < 1 {
		return 0, false
	}
	return k, true
}
