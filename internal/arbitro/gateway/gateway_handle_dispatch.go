package gateway

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// gatewayDispatchShared carries the shared state of the invocation, with
// explicit parameters and no globals. The context is the overall budget of
// the invocation, which the cloud consumes in full, and every attempt
// derives a child with its own slice. The overall budget and the start are
// the fallback of the computation of the remaining time when the context
// has no deadline. The authorization header is the one of the client,
// captured once in the handler and never logged. The category token tells
// that it is a token of a category, in which case every attempt translates
// it into the token that the destination node expects for the resolved
// concrete variant, and without a mapping it falls back to the legacy
// verbatim forwarding.
type gatewayDispatchShared struct {
	ctx             context.Context
	clientCtx       context.Context
	overall         time.Duration
	start           time.Time
	requestID       string
	group           string
	bodyBytes       []byte
	authHeader      string
	catToken        bool
	qualityReqs     map[string]float64
	qualityDeclared bool
	queue           []nodes.Node
	// pend is the set of armed pending reports of the invocation. It is
	// created by the handler and never absent in production, while its
	// absence means no best-effort mode, which is the case of the legacy
	// test calls that take the synchronous path.
	pend *pendencySet
}

// dispatchAttempts walks the queue with a budget per attempt derived from
// the overall budget: every node receives a slice, and the attempt that
// exceeds its slice is recorded as an exceeded budget without consuming
// the budget of the next ones, so that the cloud keeps seeing the rest of
// the overall budget. It returns the last failure, whether an attempt ran
// and whether the answer was already written, which is the case of the
// success and of the fail-fast answers.
func (g *Gateway) dispatchAttempts(w http.ResponseWriter, req InvocationRequest, sh gatewayDispatchShared) (failure gatewayFailure, tried bool, responded bool) {
	queue := sh.queue
	failure = gatewayFailure{httpStatus: http.StatusBadGateway, code: "NODE_ERROR", msg: "internal error", retryable: false}
	// tried tells the exhausted queue apart from the budget exhausted
	// before the first attempt: without an attempt, the initial failure
	// is replaced after the loop by a retryable timeout.
	tried = false
	// A configured cloud reserves budget in every attempt, so that the
	// last local attempt does not consume the rest of the budget and the
	// spill over the cloud stays reachable when the node exceeds its
	// slice, which is exactly the scenario that the real battery
	// exposed.
	hasCloud := g.remote != nil && g.remote.Configured()

	for i, target := range queue {
		if sh.ctx.Err() != nil {
			atomic.AddInt64(&g.raceCancelled, int64(len(queue)-i))
			log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=CONTEXT_CANCELED", i+1, len(queue), target.NodeID, req.Function)
			break
		}
		// The viability gate skips a candidate when the projected
		// prediction exceeds the ceiling, which is the declared objective
		// or, without one, the budget of the node and the function
		// announced in the registration and limited by the remaining
		// budget of the invocation. The slice sizes the wait of this
		// turn and does not admit.
		//
		// The projection multiplies the prediction by the current
		// concurrency of the node, which is the number of requests in
		// flight plus one, the same value used by the meter and by the
		// selector, and the prediction of the gate uses the same
		// estimator as the admission criterion, which applies to the idle
		// class. The slice keeps being sized by the percentile of the
		// segment, which is conservative on purpose.
		{
			remaining := remainingOverall(sh.ctx, sh.overall, sh.start)
			viable, hasViable := g.newRequestLatencyMS(sh.group, target, req.SLO)
			// The concurrency rank of the node is read before the
			// increment of this attempt, the same value used by the meter
			// and by the selector, where one means an idle node.
			rank := g.inflight.NodeK(target.NodeID) + 1
			// The ceiling without an objective is the budget of the node
			// and the function, announced in the registration and limited
			// by the remaining budget of the invocation. A node that does
			// not announce falls back to the remaining budget. The client
			// may address a category, in which case the concrete variant
			// hosted by the node is resolved before the lookup, with the
			// same resolution as the legacy dispatch.
			teto := remaining
			if hard, ok := nodeFunctionTimeout(target, req.Function); ok && hard < teto {
				teto = hard
			} else if concrete, _, _, ok := selection.ResolveConcrete(target, req.Function); ok {
				if hard, ok := nodeFunctionTimeout(target, concrete); ok && hard < teto {
					teto = hard
				}
			}
			if !attemptViable(teto, viable, hasViable, req.SLO, rank, i == len(queue)-1) {
				atomic.AddInt64(&g.unviableSkips, 1)
				log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=UNVIABLE_SLICE teto_ms=%d previsto_ms=%d k=%d", i+1, len(queue), target.NodeID, req.Function, teto.Milliseconds(), viable.Milliseconds(), rank)
				continue
			}
			// The learned queue ceiling: a node that already showed a
			// collapse of delivery at a given concurrency does not receive
			// new attempts above the last concurrency that was not
			// contradicted, independently of the budget, since it is a
			// ceiling of capacity and applies even without an objective.
			// It replaces the ceiling of the armed reports, since the
			// curve of delivery by concurrency is the signal of the
			// losing attempts, which become a timeout or a failure in the
			// bad range and stay out of the rate. It fails open: without
			// a learned limit, only the projection decides.
			if kMax, ok := g.maxQueueK(sh.group, target, req.SLO); ok && rank > kMax {
				atomic.AddInt64(&g.queueCapSkips, 1)
				log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=UNVIABLE_QUEUE k=%d kmax=%d", i+1, len(queue), target.NodeID, req.Function, rank, kMax)
				continue
			}
		}
		tried = true
		attemptStart := g.now()
		atomic.AddInt64(&g.attemptsTotal, 1)

		// The unified projection of load is captured once before the
		// dispatch and reused verbatim in the record and in the segment
		// of the history: the projected load is the observed processor
		// usage plus the cost in flight since the reception plus the cost
		// of the request itself. The cost in flight is read before the
		// increment of this request, so it does not include itself and
		// the cost of the request enters exactly once. The instant of
		// reception is the one of the telemetry at the arbiter and never
		// the timestamp of the node. The readers are safe with a nil
		// value, so without a monitor, a meter or a history the
		// projection collapses to the observed value and the estimator
		// starts neutral.
		var pkg telemetry.Package
		var recvAt time.Time
		monOK := false
		if g.mon != nil {
			pkg, recvAt, monOK = g.mon.LatestAt(target.NodeID)
		}
		observed := 0.0
		observedMem := 0.0
		if monOK {
			observed = pkg.CPUUsagePct
			observedMem = pkg.MemoryUsagePct
		}
		costSum := g.inflight.NodeCostSince(target.NodeID, recvAt)
		ownCost, _, _, ownSeeded := g.hist.ExpectedCost(sh.group, target.NodeID, g.throughputWindow, attemptStart)
		projected := observed + costSum + ownCost
		if projected < 0 {
			projected = 0
		}
		sc := history.ScenarioFromProjected(pkg, monOK, projected)
		// The increment uses the same instant as the dispatch, which is
		// the start of the attempt, for the cost of the request and for
		// the stamp of the entry. An extra call to the clock here would
		// consume a tick of the progressive fake clocks and shift the
		// measured latency.
		k := g.inflight.Inc(target.NodeID, req.Function, ownCost, attemptStart)
		// The observable surface of the seed of the cost: when the cost
		// of the request comes from the prior of the layer, the dispatch
		// logs that it was seeded with the value it used. The projection
		// consumes the seed exactly like a measured value, without an
		// alternative branch, and the line is what distinguishes a seed
		// from a measurement. It only happens with a seed, and the measured
		// path stays byte for byte the same.
		if ownSeeded {
			log.Printf("arbitro: gateway dispatch nó %s function=%s cost_seeded=true ownCost=%.2f", target.NodeID, req.Function, ownCost)
		}
		captured := attemptCapture{sc: sc, inflight: k, projected: projected, observedCPU: observed, observedMem: observedMem}

		// The attempt runs in a closure with a deferred decrement, which
		// covers every exit path, the success, the transport, the
		// overload, the budget and the unavailable node, and also a panic
		// in the body, running exactly once per attempt. A defer in the
		// loop of the handler would only run at the final return and
		// would leak the meter.
		done := func() bool {
			// Every node receives the concrete variant it hosts, while a
			// wildcard node receives the request verbatim. With a
			// declared quality the best variant chooses the qualifying
			// one, or the best one with a downgrade; without an objective
			// the path is the verbatim resolution, byte for byte the
			// same as before.
			concrete := req.Function
			if sh.qualityDeclared {
				concrete, _ = selection.BestVariant(target, req.Function, sh.qualityReqs)
			} else if c, ok := selection.AvailableConcrete(g.mon, target, req.Function, attemptStart); ok {
				// It prefers a variant of the category with a fresh and
				// available reading, such as the accelerated one when the
				// plain one is at its ceiling. Without a fresh reading it
				// falls back to the legacy resolution, and the same value
				// goes to the captured concrete name, with parity with the
				// producer and the marking of the exhausted variant.
				concrete = c
			} else if c, _, _, ok := selection.ResolveConcrete(target, req.Function); ok {
				concrete = c
			}
			// The concrete key of the ingestion of capacity is the same
			// variant resolved above, carried in the capture that the
			// record of the attempt consumes, so that the marking of the
			// exhausted variant uses the key of the producer.
			captured.concrete = concrete
			// The translation of the token per category: with a valid
			// category token, every node receives the token it expects
			// for the concrete variant and never the token of the
			// client. Without a mapping for the pair of the node and
			// the variant, it falls back to the legacy verbatim
			// forwarding, so a partial migration keeps working and the
			// unauthorized answer of the node stays visible for
			// debugging. The values are never logged, only the presence
			// of the mapping.
			outAuth := sh.authHeader
			translated := false
			if sh.catToken {
				if tok, ok := g.catTokens.DownstreamFor(sh.group, target.NodeID, concrete); ok {
					outAuth = tok
					translated = true
				}
			}
			// The budget per attempt is derived from the overall budget
			// and the attempt runs in a child with its own deadline.
			// With a prediction from the selector the slice is twice
			// the prediction, with a floor and a reserve for the pending
			// attempts, and without a prediction it is at most half of
			// the remaining budget. The attempt that exceeds its slice
			// does not prevent the next node, and the cloud keeps
			// seeing the rest of the overall budget and not the child.
			remaining := remainingOverall(sh.ctx, sh.overall, sh.start)
			predicted, hasPredicted := g.predictedLatencyMS(sh.group, target, req.SLO)
			reserve := cloudReserve(remaining, hasCloud)
			slice := attemptSlice(remaining, len(queue)-i, predicted, hasPredicted, reserve)
			// The best-effort delivery runs the attempt asynchronously when
			// the node announces a timeout, and the expiration of the
			// slice arms a pending report instead of killing the
			// attempt; without an announcement the legacy synchronous
			// path is used. The announcement is keyed by the name of the
			// function in the registration of the node, and the client
			// may address a category, so the lookup uses the concrete
			// variant resolved above. With the requested name the
			// category does not find the announcement and the invocation
			// falls back to the synchronous path. In the asynchronous
			// branch the decrement of the meter is deferred to the
			// settlement.
			if cap, ok := nodeFunctionTimeout(target, concrete); ok && sh.pend != nil {
				f, responded := g.dispatchAsyncAttempt(w, req, sh, target, i, attemptStart, concrete, translated, outAuth, slice, cap, &captured)
				failure = f
				return responded
			}
			defer g.inflight.Dec(target.NodeID, req.Function, attemptStart)
			attemptCtx := sh.ctx
			if slice > 0 && slice < remaining {
				var cancel context.CancelFunc
				attemptCtx, cancel = context.WithTimeout(sh.ctx, slice)
				defer cancel()
			}
			url := buildNodeURL(target.Host, concrete)
			httpReq, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, url, bytes.NewReader(sh.bodyBytes))
			if err != nil {
				g.recordAttempt(target, g.now().Sub(attemptStart), err, req.SLO, req.Function, sh.group, nil, true, sh.requestID, i+1, captured)
				failure = gatewayFailure{httpStatus: http.StatusBadGateway, code: "NODE_ERROR", msg: err.Error(), retryable: true}
				log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=NODE_ERROR err=%v", i+1, len(queue), target.NodeID, req.Function, err)
				return false
			}
			httpReq.Header.Set(contentTypeHeader, applicationJSON)
			// The credential is forwarded to the node, translated when it
			// is a category token with a mapping and verbatim
			// otherwise. The arbiter neither interprets nor records the
			// values, and the enforcement is the responsibility of the
			// node with a constant-time comparison. The value is never
			// logged.
			if outAuth != "" {
				httpReq.Header.Set("Authorization", outAuth)
			}
			httpReq.Header.Set("X-Faas-Attempt-ID", sh.requestID)
			httpReq.Header.Set("X-Faas-Attempt-N", strconv.Itoa(i+1))

			resp, err := g.client.Do(httpReq)
			if err != nil {
				// The budget exhausted in the middle of the attempt, when the
				// slice of this attempt or the overall budget expired, is a
				// timeout of the arbiter and not a measurement of the node.
				// It has its own outcome and a retryable timeout, and
				// without an expiration the path of the transport applies.
				// The return of false continues the queue, since with a
				// budget per attempt the next node still has the rest of
				// the budget, while with a single context the next one
				// failed with a cancelled context without even being
				// tried.
				if attemptCtx.Err() != nil {
					budgetErr := fmt.Errorf("%w contra %q: %v", ErrAttemptBudgetExceeded, target.NodeID, err)
					g.recordAttempt(target, g.now().Sub(attemptStart), budgetErr, req.SLO, req.Function, sh.group, nil, true, sh.requestID, i+1, captured)
					failure = gatewayFailure{httpStatus: http.StatusGatewayTimeout, code: "NODE_INVOCATION_TIMEOUT", msg: fmt.Sprintf("orçamento do árbitro esgotado na tentativa contra %q", target.NodeID), retryable: true}
					log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=BUDGET_EXCEEDED fatia_ms=%d err=%v", i+1, len(queue), target.NodeID, req.Function, slice.Milliseconds(), err)
					return false
				}
				g.recordAttempt(target, g.now().Sub(attemptStart), err, req.SLO, req.Function, sh.group, nil, true, sh.requestID, i+1, captured)
				failure = gatewayFailure{httpStatus: http.StatusBadGateway, code: "NODE_UNAVAILABLE", msg: fmt.Sprintf("falha ao contatar nó %q: %v", target.NodeID, err), retryable: true}
				log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=NODE_UNAVAILABLE err=%v", i+1, len(queue), target.NodeID, req.Function, err)
				return false
			}

			res := readAttemptResult(resp)
			latency := g.now().Sub(attemptStart)
			attemptErr, f, kind, reason, pressure := classifyAttemptResult(res, target, req.Function, sh.group, sh.qualityReqs, sh.qualityDeclared, translated)
			failure = f
			if pressure != nil {
				captured.admPressure = pressure
			}
			// The synchronous path never arms, and the delivered flag is
			// only set on a terminal answer, since the success, the
			// downgrade and the fail-fast write to the response.
			delivered := kind == attemptSuccess || kind == attemptDowngrade || kind == attemptFailFast
			g.recordAttemptFull(target, latency, attemptErr, req.SLO, req.Function, sh.group, res.metrics, kind != attemptDowngrade, sh.requestID, i+1, captured, false, delivered)
			// Being alive is not a success: any HTTP answer received,
			// including the error ones, proves that the node is alive,
			// and the touch here prevents an overloaded node from aging
			// into the unavailable state and causing a flapping of the
			// membership. Only a transport error, without an answer,
			// does not touch.
			_ = g.nodes.Touch(target.NodeID)
			switch kind {
			case attemptSuccess:
				w.Header().Set(contentTypeHeader, applicationJSON)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(res.body)
				log.Printf("arbitro: gateway function=%s->%s [%s] node=%s latency_ms=%d result=OK tentativa=%d/%d", req.Function, concrete, sh.group, target.NodeID, g.now().Sub(sh.start).Milliseconds(), i+1, len(queue))
				log.Printf("arbitro: RACE_WINNER function=%s node=%s tentativa=%d/%d downgrade=false", req.Function, target.NodeID, i+1, len(queue))
				// The armed orphans run until the ceiling and are not
				// cancelled, so that the drain settles them and keeps
				// the learning and the container warm.
				return true
			case attemptDowngrade:
				w.Header().Set(contentTypeHeader, applicationJSON)
				w.Header().Set("X-Faas-Quality-Downgrade", "true")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(res.body)
				log.Printf("arbitro: gateway function=%s->%s [%s] node=%s result=DOWNGRADE required=%s expected=%s", req.Function, concrete, sh.group, target.NodeID, downgradeRequired(req.SLO), downgradeExpected(target, concrete))
				log.Printf("arbitro: RACE_WINNER function=%s node=%s tentativa=%d/%d downgrade=true", req.Function, target.NodeID, i+1, len(queue))
				return true
			case attemptFailFast:
				// The pass-through of the authentication, with the
				// credential of the client, and the deterministic bad
				// request fail fast, with no failover to the next node.
				log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=%s", i+1, len(queue), target.NodeID, req.Function, reason)
				writeGatewayError(w, failure.httpStatus, failure.code, failure.msg, failure.retryable, false)
				// The orphan runs until the ceiling and the drain settles it.
				return true
			default:
				// The not found of an obsolete map, the overload, the
				// timeout, the framing error of a successful answer and
				// the unauthorized answer with a translated token are
				// recorded and continue with the next node.
				if reason == "NODE_ERROR" {
					log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=NODE_ERROR status=%d", i+1, len(queue), target.NodeID, req.Function, res.status)
				} else if reason == "response_too_large" || reason == "invalid_json" {
					log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=NODE_ERROR reason=%s", i+1, len(queue), target.NodeID, req.Function, reason)
				} else {
					log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=%s", i+1, len(queue), target.NodeID, req.Function, reason)
				}
				return false
			}
		}()
		if done {
			return failure, tried, true
		}
	}
	return failure, tried, false
}
