package gateway

// Best-effort delivery separates the two clocks of an invocation.
//
//   - The objective governs the turn, with the slices, the cloud reserve
//     and the refusal of new attempts. It is unchanged.
//   - The hard deadline, which is the timeout announced by the node,
//     governs the wait for the delivery of attempts that already started.
//
// When the target announces a function timeout, the attempt runs
// asynchronously: the loop waits for the result until the slice and, if
// the slice expires, the attempt becomes an armed pending report instead
// of being cancelled, and the loop goes on. Once the queue is exhausted,
// the waiting stage delivers the first success within the hard deadline.
//
// The invariants are:
//   - only the loop and the waiting stage write to the response writer,
//     and the goroutine of a pending report never writes;
//   - the handler never returns before the cancellation and the join of
//     the pending reports, with the drain registered after the return of
//     the buffer to the pool;
//   - an armed orphan that lost the race is not cancelled: it runs until
//     the ceiling of the node, is settled with the real outcome, which
//     supersedes the exceeded budget and keeps the learning, and keeps
//     the container warm in the node. The drain runs after the answer, in
//     a goroutine, and every attempt detaches the body with its own copy
//     so that the buffer of the handler can be recycled;
//   - without an announcement the synchronous path of today is used.
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// pendingOutcome is the outcome of a single asynchronous attempt, produced
// by the goroutine and consumed by the loop inside the slice or by the
// waiting phase. It is written once before the channel is closed and read
// afterwards, which establishes the ordering.
type pendingOutcome struct {
	// The response holds when there is an HTTP answer, and the transport
	// error holds when the request failed, either because the ceiling
	// expired or because it was cancelled.
	res          attemptResult
	hasResponse  bool
	transportErr error
	// ctxErr is the error of the context of the attempt at the instant of
	// the transport error: the deadline exceeded means the ceiling
	// expired, which is a real timeout of the node, and the cancellation
	// means our own cancellation, by a winner, the deadline or a
	// disconnection, in which case nothing is recorded and the
	// synchronous entry remains.
	ctxErr error
	// latency measures the start of the attempt to the end of the read or
	// of the error, so that the settlement does not read the clock
	// again.
	latency time.Duration
}

// pendingAttempt is a single attempt in flight that can become an armed
// pending report. It is created before the dispatch, armed when the slice
// expires, settled by the waiting stage and discarded by the drain.
type pendingAttempt struct {
	target          nodes.Node
	function        string // pedida (chave do gauge, como no Inc)
	concrete        string
	group           string
	slo             *selection.SLO
	qualityReqs     map[string]float64
	qualityDeclared bool
	translated      bool // token de categoria traduzido (401 traduzido continua)
	requestID       string
	attemptN        int
	captured        attemptCapture
	attemptStart    time.Time
	cap             time.Duration
	cancel          context.CancelFunc
	stopClient      func() bool
	done            chan struct{}
	settled         chan<- *pendingAttempt
	outcome         pendingOutcome
	armed           atomic.Bool
	// concluded marks that the waiting stage decided the pending report,
	// settling it, discarding it or delivering it. The drain only counts
	// as cancelled an armed report without a decision. Only the waiting
	// stage writes it and the drain reads it after its return.
	concluded atomic.Bool
	decOnce   sync.Once
	dec       func()
}

// settleDec executes the decrement of the meter of the pending report
// exactly once. The decrement of the captured count is deferred from the
// end of the closure to the settlement, whether it is a success, a
// failure, a cancellation or a drain.
func (pa *pendingAttempt) settleDec() {
	if pa == nil {
		return
	}
	pa.decOnce.Do(func() {
		if pa.dec != nil {
			pa.dec()
		}
	})
}

// stopClientWatch releases the callback of the disconnection of the
// client. It is idempotent and, without it, the registration would live
// until the end of the context of the client.
func (pa *pendingAttempt) stopClientWatch() {
	if pa == nil || pa.stopClient == nil {
		return
	}
	pa.stopClient()
}

// cancelAttempt releases the context of the pending attempt. It is
// idempotent and safe with a nil value: the outcome already arrived and
// the timer of the ceiling does not need to live until the deadline.
func (pa *pendingAttempt) cancelAttempt() {
	if pa == nil || pa.cancel == nil {
		return
	}
	pa.cancel()
}

// pendencySet is the set of armed pending reports of one invocation. The
// settled channel receives every attempt exactly once when it concludes,
// with a buffer of the size of the queue, which bounds the attempts, so
// the send never blocks. The methods of a nil set do nothing, which keeps
// the legacy callers without pending report working.
type pendencySet struct {
	mu      sync.Mutex
	armed   []*pendingAttempt
	settled chan *pendingAttempt
	max     int // teto de pendências = maxRaceFanout (D7)
}

func newPendencySet(queueLen, maxFanout int) *pendencySet {
	if queueLen < 1 {
		queueLen = 1
	}
	return &pendencySet{
		settled: make(chan *pendingAttempt, queueLen),
		max:     maxFanout,
	}
}

// arm registers the pending report. It returns false when it did not
// arm, in which case the caller waits for the outcome synchronously
// instead of arming.
func (s *pendencySet) arm(pa *pendingAttempt) bool {
	if s == nil || pa == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.max > 0 && len(s.armed) >= s.max {
		return false
	}
	pa.armed.Store(true)
	s.armed = append(s.armed, pa)
	return true
}

// armedList returns the snapshot of the armed pending reports, for
// the computation of the hard deadline. It is safe with a nil set.
func (s *pendencySet) armedList() []*pendingAttempt {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pendingAttempt, len(s.armed))
	copy(out, s.armed)
	return out
}

// cancelAll cancels the armed pending reports. Their outcome arrives
// with the cancellation error and the settlement ignores it. It is
// idempotent and safe with a nil set.
func (s *pendencySet) cancelAll() {
	if s == nil {
		return
	}
	s.mu.Lock()
	armed := make([]*pendingAttempt, len(s.armed))
	copy(armed, s.armed)
	s.mu.Unlock()
	for _, pa := range armed {
		pa.cancel()
	}
}

// drainPendencies is the join of the handler: it waits for every
// goroutine, without a leak under a disconnection or a fail-fast, and
// settles what the waiting stage did not consume. It does not cancel: an
// armed attempt that lost the race runs until the ceiling of the node, is
// not aborted, has its real outcome recorded, which supersedes the
// exceeded budget and keeps the learning, and keeps the container warm in
// the node. It runs in a goroutine after the answer, so that the handler
// does not hold the connection of the client until the ceiling, which is
// why the attempts detach the body with their own copy in the dispatch.
// The deferred decrement runs once per pending report. An external
// cancellation, such as the disconnection of the client, is still counted
// in the cancellation counter. It is safe with a nil set.
func (g *Gateway) drainPendencies(s *pendencySet) {
	if s == nil {
		return
	}
	var cancelled int64
	for _, pa := range s.armedList() {
		<-pa.done
		pa.stopClientWatch()
		pa.cancelAttempt()
		if !pa.concluded.Load() {
			pa.concluded.Store(true)
			out := pa.outcome
			if !out.hasResponse && out.ctxErr != nil && out.ctxErr != context.DeadlineExceeded {
				// The cancellation of the disconnection of the client records
				// nothing, so the synchronous entry remains.
				cancelled++
				log.Printf("arbitro: BEST_EFFORT_CANCELLED function=%s node=%s tentativa=%d", pa.function, pa.target.NodeID, pa.attemptN)
			} else {
				// A real outcome outside the waiting stage is recorded for
				// the learning but not delivered, since the answer of the
				// client already left.
				_, _, _ = g.settlePending(pa, out, false)
				if out.hasResponse {
					_ = g.nodes.Touch(pa.target.NodeID)
				}
			}
		}
		pa.settleDec()
	}
	if cancelled > 0 {
		atomic.AddInt64(&g.bestEffortCancelled, cancelled)
	}
}

// startAttempt fires a single asynchronous attempt against the target: the
// request and the read of the body run in a goroutine under the context of
// the attempt, bounded by the ceiling of the node and with no fallback. The
// loop consumes the outcome until the slice and, when the slice expires,
// arms the pending report and goes on. The settled channel receives the
// attempt exactly once when it concludes, in a buffered send that never
// blocks.
func (g *Gateway) startAttempt(pendCtx context.Context, set *pendencySet, target nodes.Node, function, concrete, outAuth, requestID string, attemptN int, captured attemptCapture, attemptStart time.Time, cap time.Duration, slo *selection.SLO, group string, qualityReqs map[string]float64, qualityDeclared, translated bool, bodyBytes []byte, cancel context.CancelFunc, stopClient func() bool) *pendingAttempt {
	pa := &pendingAttempt{
		target:          target,
		function:        function,
		concrete:        concrete,
		group:           group,
		slo:             slo,
		qualityReqs:     qualityReqs,
		qualityDeclared: qualityDeclared,
		translated:      translated,
		requestID:       requestID,
		attemptN:        attemptN,
		captured:        captured,
		attemptStart:    attemptStart,
		cap:             cap,
		cancel:          cancel,
		stopClient:      stopClient,
		done:            make(chan struct{}),
		settled:         set.settled,
		dec:             func() { g.inflight.Dec(target.NodeID, function, attemptStart) },
	}
	go func() {
		var out pendingOutcome
		url := buildNodeURL(target.Host, concrete)
		httpReq, err := http.NewRequestWithContext(pendCtx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			out = pendingOutcome{transportErr: err, ctxErr: pendCtx.Err(), latency: g.now().Sub(attemptStart)}
		} else {
			httpReq.Header.Set(contentTypeHeader, applicationJSON)
			if outAuth != "" {
				httpReq.Header.Set("Authorization", outAuth)
			}
			httpReq.Header.Set("X-Faas-Attempt-ID", requestID)
			httpReq.Header.Set("X-Faas-Attempt-N", strconv.Itoa(attemptN))
			if resp, derr := g.client.Do(httpReq); derr != nil {
				out = pendingOutcome{transportErr: derr, ctxErr: pendCtx.Err(), latency: g.now().Sub(attemptStart)}
			} else {
				res := readAttemptResult(resp)
				out = pendingOutcome{res: res, hasResponse: true, latency: g.now().Sub(attemptStart)}
			}
		}
		pa.outcome = out
		close(pa.done)
		set.settled <- pa
	}()
	return pa
}

// dispatchAsyncAttempt runs a single attempt with an announcement: it
// fires the goroutine under the ceiling of the node and waits for the
// outcome until the slice. An outcome inside the slice follows the
// current synchronous path, while the expiration of the slice arms the
// pending report and the loop goes on. It returns the failure and whether
// the client was answered, as the synchronous loop does.
func (g *Gateway) dispatchAsyncAttempt(w http.ResponseWriter, req InvocationRequest, sh gatewayDispatchShared, target nodes.Node, i int, attemptStart time.Time, concrete string, translated bool, outAuth string, slice, cap time.Duration, captured *attemptCapture) (failure gatewayFailure, responded bool) {
	// The context of the attempt is detached from the overall context, so
	// the attempt lives until the ceiling of the node and not until the
	// slice, and the disconnection of the client propagates through the
	// callback.
	pendCtx, cancel := context.WithTimeout(context.WithoutCancel(sh.ctx), cap)
	stopClient := context.AfterFunc(sh.clientCtx, cancel)
	// The body has its own copy, since the goroutine of the pending report
	// can live beyond the return of the handler and the buffer of the
	// pool is recycled in the defer.
	pa := g.startAttempt(pendCtx, sh.pend, target, req.Function, concrete, outAuth, sh.requestID, i+1, *captured, attemptStart, cap, req.SLO, sh.group, sh.qualityReqs, sh.qualityDeclared, translated, append([]byte(nil), sh.bodyBytes...), cancel, stopClient)
	if g.observeArmed && sh.pend != nil {
		return g.dispatchAsyncObserved(w, req, sh, target, i, concrete, translated, pa, slice)
	}
	wait := slice
	if wait < 0 {
		wait = 0
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-pa.done:
		return g.handleAsyncOutcome(w, req, sh, target, i, concrete, translated, pa, pa.outcome)
	case <-timer.C:
		f, ok := g.armPending(req, sh, target, i, pa, slice)
		if !ok {
			// The ceiling is defensive, since the queue is cut before the
			// dispatch and its length never exceeds the limit, so the
			// arming never refuses today. Without arming, the attempt
			// waits for the outcome, but never beyond the overall budget
			// nor the disconnection of the client. Without the cut, an
			// attempt that the waiting stage would not deliver would
			// hold the loop until the ceiling of the node.
			select {
			case <-pa.done:
			case <-sh.ctx.Done():
				pa.cancelAttempt()
				<-pa.done // net/http honra o ctx: retorna prompto
			case <-sh.clientCtx.Done():
				// The callback already cancelled it, so the cancelled
				// outcome is confirmed.
				<-pa.done
			}
			return g.handleAsyncOutcome(w, req, sh, target, i, concrete, translated, pa, pa.outcome)
		}
		return f, false
	}
}

// dispatchAsyncObserved is the correction that observes the armed pending
// reports: like the legacy path, but the selection also observes the
// settled channel of the pending reports. An armed report that concludes
// during the slice of the current attempt is delivered immediately, since
// the first successful answer wins, instead of waiting for the queue to
// drain in the waiting phase.
func (g *Gateway) dispatchAsyncObserved(w http.ResponseWriter, req InvocationRequest, sh gatewayDispatchShared, target nodes.Node, i int, concrete string, translated bool, pa *pendingAttempt, slice time.Duration) (failure gatewayFailure, responded bool) {
	wait := slice
	if wait < 0 {
		wait = 0
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-pa.done:
			return g.handleAsyncOutcome(w, req, sh, target, i, concrete, translated, pa, pa.outcome)
		case other := <-sh.pend.settled:
			if other == nil {
				continue
			}
			if other == pa {
				// The current attempt also sends to the settled channel when
				// it concludes: it is treated like the branch of its own
				// outcome, without settling twice, since the single
				// message was already consumed here.
				return g.handleAsyncOutcome(w, req, sh, target, i, concrete, translated, pa, pa.outcome)
			}
			if !other.armed.Load() || other.concluded.Load() {
				// The messages of the attempts that are not armed keep
				// being ignored, and the conclusion is defensive.
				continue
			}
			other.concluded.Store(true)
			out := other.outcome
			other.stopClientWatch()
			other.cancelAttempt()
			if !out.hasResponse && out.ctxErr != nil && out.ctxErr != context.DeadlineExceeded {
				// Our own cancellation records nothing, as in the waiting
				// phase, and the current attempt keeps waiting.
				other.settleDec()
				continue
			}
			kind, f, sloMet := g.settlePending(other, out, true)
			if out.hasResponse {
				_ = g.nodes.Touch(other.target.NodeID)
			}
			other.settleDec()
			if kind != attemptSuccess && kind != attemptDowngrade {
				// A non-success settles the attempt and the wait goes on with
				// the remaining time of the timer.
				_ = f
				continue
			}
			// The armed attempt won: the late delivery happens immediately,
			// with the same wire as the waiting phase, and the current
			// attempt becomes an adopted orphan.
			w.Header().Set(contentTypeHeader, applicationJSON)
			w.Header().Set("X-Faas-Late", "true")
			w.Header().Set("X-Faas-Slo-Met", boolHeader(sloMet))
			if kind == attemptDowngrade {
				w.Header().Set("X-Faas-Quality-Downgrade", "true")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(out.res.body)
			atomic.AddInt64(&g.bestEffortServed, 1)
			atomic.AddInt64(&g.observeArmedWins, 1)
			if kind == attemptDowngrade {
				log.Printf("arbitro: gateway function=%s->%s [%s] node=%s result=DOWNGRADE required=%s expected=%s", req.Function, other.concrete, sh.group, other.target.NodeID, downgradeRequired(req.SLO), downgradeExpected(other.target, other.concrete))
			} else {
				log.Printf("arbitro: gateway function=%s->%s [%s] node=%s latency_ms=%d result=OK tentativa=%d", req.Function, other.concrete, sh.group, other.target.NodeID, g.now().Sub(sh.start).Milliseconds(), other.attemptN)
			}
			log.Printf("arbitro: RACE_WINNER function=%s node=%s tentativa=%d downgrade=%v", req.Function, other.target.NodeID, other.attemptN, kind == attemptDowngrade)
			log.Printf("arbitro: BEST_EFFORT_SERVED function=%s node=%s tentativa=%d slo_met=%v", req.Function, other.target.NodeID, other.attemptN, sloMet)
			g.adoptOrphan(sh.pend, pa)
			return gatewayFailure{}, true
		case <-timer.C:
			f, ok := g.armPending(req, sh, target, i, pa, slice)
			if !ok {
				// The defensive ceiling of the arming.
				select {
				case <-pa.done:
				case <-sh.ctx.Done():
					pa.cancelAttempt()
					<-pa.done // net/http honra o ctx: retorna prompto
				case <-sh.clientCtx.Done():
					<-pa.done
				}
				return g.handleAsyncOutcome(w, req, sh, target, i, concrete, translated, pa, pa.outcome)
			}
			return f, false
		}
	}
}

// adoptOrphan registers the current attempt as a pending report without
// recording an exceeded budget. Without the adoption the goroutine leaks,
// the decrement of the meter never runs and the learning is lost. The
// drain settles it with the real outcome, since the settlement accepts a
// pending report without a live reference and only appends to the history.
// It respects the limit of the fan-out and, in the defensive refusal, it
// cancels so that nothing leaks.
func (g *Gateway) adoptOrphan(set *pendencySet, pa *pendingAttempt) {
	if set == nil || pa == nil {
		return
	}
	if !set.arm(pa) {
		log.Printf("arbitro: ORPHAN_ADOPT_REFUSED function=%s node=%s tentativa=%d (teto D7)", pa.function, pa.target.NodeID, pa.attemptN)
		pa.cancelAttempt()
		pa.stopClientWatch()
		go func() {
			<-pa.done
			pa.settleDec()
		}()
	}
}

// handleAsyncOutcome processes the outcome of an asynchronous attempt
// resolved inside the slice: it is identical to the synchronous path, since
// it classifies, records, touches, writes or continues. A terminal answer
// cancels the other pending reports and ends. Without an answer, from the
// ceiling, the cancellation or the transport, it records the real class and
// continues. It returns the failure and whether the client was answered.
func (g *Gateway) handleAsyncOutcome(w http.ResponseWriter, req InvocationRequest, sh gatewayDispatchShared, target nodes.Node, i int, concrete string, translated bool, pa *pendingAttempt, out pendingOutcome) (failure gatewayFailure, responded bool) {
	pa.stopClientWatch()
	defer pa.settleDec()
	// The outcome arrived, so the timer of the ceiling can die now. The
	// cancellation is idempotent and the context is no longer used by
	// the goroutine.
	defer pa.cancelAttempt()
	if !out.hasResponse {
		if out.ctxErr == context.DeadlineExceeded {
			// The ceiling expired: the node did not answer within its own
			// timeout, which is a real timeout and a service failure and not
			// a cut of the arbiter.
			msg := fmt.Sprintf("nó %q timeout na invocação", target.NodeID)
			g.recordAttempt(target, out.latency, fmt.Errorf("%w: %s", ErrAttemptTimeout, msg), req.SLO, req.Function, sh.group, nil, true, sh.requestID, i+1, pa.captured)
			log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=NODE_INVOCATION_TIMEOUT cap_ms=%d", i+1, len(sh.queue), target.NodeID, req.Function, pa.cap.Milliseconds())
			return gatewayFailure{httpStatus: http.StatusGatewayTimeout, code: "NODE_INVOCATION_TIMEOUT", msg: msg, retryable: true}, false
		}
		// The cancellation, when the client disappeared, and the error of
		// the transport mirror the synchronous path, which distinguishes the
		// budget from an unavailable node.
		if out.ctxErr != nil {
			budgetErr := fmt.Errorf("%w contra %q: %v", ErrAttemptBudgetExceeded, target.NodeID, out.transportErr)
			g.recordAttempt(target, out.latency, budgetErr, req.SLO, req.Function, sh.group, nil, true, sh.requestID, i+1, pa.captured)
			log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=BUDGET_EXCEEDED err=%v", i+1, len(sh.queue), target.NodeID, req.Function, out.transportErr)
			return gatewayFailure{httpStatus: http.StatusGatewayTimeout, code: "NODE_INVOCATION_TIMEOUT", msg: fmt.Sprintf("orçamento do árbitro esgotado na tentativa contra %q", target.NodeID), retryable: true}, false
		}
		g.recordAttempt(target, out.latency, out.transportErr, req.SLO, req.Function, sh.group, nil, true, sh.requestID, i+1, pa.captured)
		log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=NODE_UNAVAILABLE err=%v", i+1, len(sh.queue), target.NodeID, req.Function, out.transportErr)
		return gatewayFailure{httpStatus: http.StatusBadGateway, code: "NODE_UNAVAILABLE", msg: fmt.Sprintf("falha ao contatar nó %q: %v", target.NodeID, out.transportErr), retryable: true}, false
	}
	res := out.res
	attemptErr, f, kind, reason, pressure := classifyAttemptResult(res, target, req.Function, sh.group, sh.qualityReqs, sh.qualityDeclared, translated)
	captured := pa.captured
	if pressure != nil {
		captured.admPressure = pressure
	}
	// Resolved inside the slice, the attempt never armed, and the
	// delivered flag is only set on a terminal answer, since the success,
	// the downgrade and the fail-fast write to the response.
	delivered := kind == attemptSuccess || kind == attemptDowngrade || kind == attemptFailFast
	g.recordAttemptFull(target, out.latency, attemptErr, req.SLO, req.Function, sh.group, res.metrics, kind != attemptDowngrade, sh.requestID, i+1, captured, false, delivered)
	_ = g.nodes.Touch(target.NodeID)
	switch kind {
	case attemptSuccess:
		w.Header().Set(contentTypeHeader, applicationJSON)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(res.body)
		log.Printf("arbitro: gateway function=%s->%s [%s] node=%s latency_ms=%d result=OK tentativa=%d/%d", req.Function, concrete, sh.group, target.NodeID, g.now().Sub(sh.start).Milliseconds(), i+1, len(sh.queue))
		log.Printf("arbitro: RACE_WINNER function=%s node=%s tentativa=%d/%d downgrade=false", req.Function, target.NodeID, i+1, len(sh.queue))
		// The armed orphans run until the ceiling and are not cancelled,
		// so that the drain settles them afterwards and keeps the
		// learning and the container warm.
		return f, true
	case attemptDowngrade:
		w.Header().Set(contentTypeHeader, applicationJSON)
		w.Header().Set("X-Faas-Quality-Downgrade", "true")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(res.body)
		log.Printf("arbitro: gateway function=%s->%s [%s] node=%s result=DOWNGRADE required=%s expected=%s", req.Function, concrete, sh.group, target.NodeID, downgradeRequired(req.SLO), downgradeExpected(target, concrete))
		log.Printf("arbitro: RACE_WINNER function=%s node=%s tentativa=%d/%d downgrade=true", req.Function, target.NodeID, i+1, len(sh.queue))
		return f, true
	case attemptFailFast:
		log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=%s", i+1, len(sh.queue), target.NodeID, req.Function, reason)
		writeGatewayError(w, f.httpStatus, f.code, f.msg, f.retryable, false)
		return f, true
	default:
		if reason == "NODE_ERROR" {
			log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=NODE_ERROR status=%d", i+1, len(sh.queue), target.NodeID, req.Function, res.status)
		} else if reason == "response_too_large" || reason == "invalid_json" {
			log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=NODE_ERROR reason=%s", i+1, len(sh.queue), target.NodeID, req.Function, reason)
		} else {
			log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=%s", i+1, len(sh.queue), target.NodeID, req.Function, reason)
		}
		return f, false
	}
}

// armPending records the synchronous exceeded budget at the moment of
// arming, as today, and arms the pending report, so that the future
// settlement supersedes it. Without arming, which is the case of the
// ceiling, the caller waits for the outcome within the overall budget and
// the disconnection of the client, never until the ceiling of the node.
func (g *Gateway) armPending(req InvocationRequest, sh gatewayDispatchShared, target nodes.Node, i int, pa *pendingAttempt, slice time.Duration) (failure gatewayFailure, armedOK bool) {
	if !sh.pend.arm(pa) {
		return gatewayFailure{}, false
	}
	latency := g.now().Sub(pa.attemptStart)
	// The event of arming sets the armed flag and leaves the delivered
	// flag false, since a delivery, if any, comes with the settlement.
	g.recordAttemptFull(target, latency, fmt.Errorf("%w contra %q (fatia esgotada, tentativa segue em pendência)", ErrAttemptBudgetExceeded, target.NodeID), req.SLO, req.Function, sh.group, nil, true, sh.requestID, i+1, pa.captured, true, false)
	atomic.AddInt64(&g.bestEffortArmed, 1)
	log.Printf("arbitro: gateway tentativa %d/%d no nó %s function=%s result=BUDGET_EXCEEDED fatia_ms=%d", i+1, len(sh.queue), target.NodeID, req.Function, slice.Milliseconds())
	log.Printf("arbitro: BEST_EFFORT_ARM function=%s node=%s tentativa=%d/%d fatia_ms=%d cap_ms=%d", req.Function, target.NodeID, i+1, len(sh.queue), slice.Milliseconds(), pa.cap.Milliseconds())
	return gatewayFailure{httpStatus: http.StatusGatewayTimeout, code: "NODE_INVOCATION_TIMEOUT", msg: fmt.Sprintf("orçamento do árbitro esgotado na tentativa contra %q", target.NodeID), retryable: true}, true
}

// claimLiveAttempt reserves the pair of the request identifier and the
// attempt number in the shared deduplication and consumes the live
// reference in a single call: the late delivery and the late report of
// the node compete for the same deduplication and the same supersede, so
// the first one to arrive records and the other counts as a dropped late
// report. A refused claim means a duplicate and does not touch the
// history.
func (g *Gateway) claimLiveAttempt(requestID string, attemptN int) (claimed bool, ref liveRef, hasRef bool) {
	g.ensureLateState()
	if g.lateDedup.checkAndMark(requestID, attemptN) {
		atomic.AddInt64(&g.lateDropped, 1)
		return false, liveRef{}, false
	}
	if g.hist != nil {
		ref, hasRef = g.liveIdx.take(requestID, attemptN)
	}
	return true, ref, hasRef
}

// publishSettlement records the real entry of a settlement: it supersedes
// the synchronous entry, so that the attempt is not counted twice, appends
// to the history and writes a line to the invocation log. When the outcome
// is not recordable the append is skipped, mirroring the synchronous path
// that only reached the invocation log, but the supersede is kept. A
// failure of the append undoes the reservation of the deduplication so
// that the other side can try. It only returns the error of the append to
// the history, since the invocation log is best-effort.
//
// The armed and delivered flags only go to the invocation log. The
// best-effort settlement uses the armed flag, while a late report uses
// no armed flag, since it never armed through the gateway. The delivered
// flag is only set in the attempt answered to the client.
func (g *Gateway) publishSettlement(requestID string, attemptN int, ref liveRef, hasRef bool, entry history.Entry, group string, slo *selection.SLO, metrics json.RawMessage, recordHist bool, armed, delivered bool) error {
	if hasRef {
		g.supersedeLiveAttempt(ref)
	}
	if recordHist && g.hist != nil {
		if err := g.hist.AppendWithGroup(entry, group); err != nil {
			g.lateDedup.unmark(requestID, attemptN)
			return fmt.Errorf("gateway: assentamento no histórico: %w", err)
		}
	}
	if err := g.invLog.Append(InvocationRecord{Entry: entry, SLO: slo, ExecutionMetrics: metrics, RequestID: requestID, AttemptN: attemptN, Armed: armed, Delivered: delivered}); err != nil {
		log.Printf("arbitro: invocation log append: %v", err)
	}
	return nil
}

// settlePending settles a single armed pending report: it builds the real
// result, with the same inputs as a late report, and publishes it through
// the shared path of the deduplication, the supersede and the append, with
// the objective recalculated. The synchronous exceeded budget leaves and
// the real result enters, which is the learning with the failure. It
// returns the kind of the outcome, where the success and the downgrade
// mean a winner, the corresponding failure, which the waiting stage uses
// when nothing wins, and the computed objective, for the header of the
// delivery. Our own cancellation never reaches this function, since the
// caller discards it without settling.
func (g *Gateway) settlePending(pa *pendingAttempt, out pendingOutcome, delivered bool) (kind attemptKind, failure gatewayFailure, sloMet bool) {
	// The classification comes first and is pure: the kind holds even
	// when the deduplication loses, since the other side recorded the
	// same outcome of the same attempt and the success still wins, it
	// simply is not recorded again.
	var attemptErr error
	var metrics json.RawMessage
	qualityMet := true
	kind = attemptNext
	captured := pa.captured
	if out.hasResponse {
		var pressure *NodeAdmissionPressure
		attemptErr, failure, kind, _, pressure = classifyAttemptResult(out.res, pa.target, pa.function, pa.group, pa.qualityReqs, pa.qualityDeclared, pa.translated)
		metrics = out.res.metrics
		if pressure != nil {
			captured.admPressure = pressure
		}
		if kind == attemptDowngrade {
			qualityMet = false
		}
	} else if out.ctxErr == context.DeadlineExceeded {
		// Only the expiration of the ceiling reaches this point, where
		// the deadline exceeded means the node did not answer within its
		// own timeout, which is a real timeout like the service timeout.
		// Our own cancellation is discarded by the caller without a
		// settlement.
		msg := fmt.Sprintf("nó %q timeout na invocação", pa.target.NodeID)
		attemptErr = fmt.Errorf("%w: %s", ErrAttemptTimeout, msg)
		failure = gatewayFailure{httpStatus: http.StatusGatewayTimeout, code: "NODE_INVOCATION_TIMEOUT", msg: msg, retryable: true}
	} else {
		// A transport error without an answer, in the assembly or the
		// dial, is a generic failure, like in the synchronous path.
		attemptErr = out.transportErr
		failure = gatewayFailure{httpStatus: http.StatusBadGateway, code: "NODE_UNAVAILABLE", msg: fmt.Sprintf("falha ao contatar nó %q: %v", pa.target.NodeID, out.transportErr), retryable: true}
	}
	claimed, ref, hasRef := g.claimLiveAttempt(pa.requestID, pa.attemptN)
	if !claimed {
		_, sloMet := classifyAttempt(attemptErr, out.latency, pa.slo, g.criteria)
		return kind, failure, sloMet && qualityMet
	}
	won := kind == attemptSuccess || kind == attemptDowngrade
	// The delivered flag is false when the outcome is settled outside
	// the waiting phase, in the drain that runs after the answer: the
	// attempt won in the node but did not deliver.
	lateDelivered := won && delivered
	caps := capacityOfMetrics(metrics)
	g.ingestCapacity(pa.target, pa.function, captured.concrete, caps, attemptErr)
	entry := g.buildEntry(pa.target, out.latency, attemptErr, pa.slo, pa.function, metrics, qualityMet, captured, caps)
	entry.Timestamp = pa.attemptStart
	if lateDelivered {
		entry.LateDelivered = true
	}
	if err := g.publishSettlement(pa.requestID, pa.attemptN, ref, hasRef, entry, pa.group, pa.slo, metrics, isRecordable(attemptErr), true, lateDelivered); err != nil {
		// The delivery happens even without a record, like in the
		// synchronous path, which answers despite the error of the append,
		// since the other side, the late report, can still record.
		log.Printf("arbitro: assentamento pendência function=%s node=%s tentativa=%d: %v", pa.function, pa.target.NodeID, pa.attemptN, err)
		return kind, failure, entry.SloMet
	}
	log.Printf("arbitro: BEST_EFFORT_SETTLED function=%s node=%s tentativa=%d outcome=%s late=%v", pa.function, pa.target.NodeID, pa.attemptN, entry.Outcome, lateDelivered)
	return kind, failure, entry.SloMet
}

// boolHeader formats a boolean for an HTTP header.
func boolHeader(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// launchCloudRace starts the cloud in a race with the waiting phase. It
// leaves at the same instant the queue is exhausted, with a deadline at
// the hard deadline, and the context of the handler is not extended. It
// returns no values without a configured fallback and the caller goes on
// without that branch. The outcome reaches the invocation log even when it
// loses, since it is data for the replay and for the warm path. The
// channel closes when the goroutine returns, which gives a deterministic
// join of the orphan victory.
func (g *Gateway) launchCloudRace(hardAbs time.Time, sh gatewayDispatchShared, req InvocationRequest) (<-chan cloudOutcome, <-chan struct{}, context.CancelFunc) {
	if g.remote == nil || !g.remote.Configured() {
		return nil, nil, nil
	}
	cloudCtx, cancel := context.WithDeadline(context.WithoutCancel(sh.ctx), hardAbs)
	stop := context.AfterFunc(sh.clientCtx, cancel)
	ch := make(chan cloudOutcome, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ch <- g.runCloudAttempt(cloudCtx, sh.start, req)
	}()
	return ch, done, func() { stop(); cancel() }
}

// cloudRaceFailure builds the failure of the cloud that lost the race,
// with the code of a failed cloud fallback and the retryable flag inherited
// from the last failure, as in the sequential path, since a cloud that is
// down does not change the root cause.
func cloudRaceFailure(err error, retryable bool) gatewayFailure {
	return gatewayFailure{httpStatus: http.StatusBadGateway, code: "CLOUD_FALLBACK_FAILED", msg: fmt.Sprintf("fallback de nuvem falhou: %v", err), retryable: retryable}
}

// hardDeadline computes the hard deadline of the waiting phase: the
// maximum between the overall budget and the natural end of every armed
// pending report, which is the start of the attempt plus the ceiling. It
// is never smaller than the budget of the objective and never smaller
// than the work already started.
func hardDeadline(overall time.Duration, start time.Time, armed []*pendingAttempt) time.Time {
	hard := start.Add(overall)
	for _, pa := range armed {
		if end := pa.attemptStart.Add(pa.cap); end.After(hard) {
			hard = end
		}
	}
	return hard
}

// waitPendencies is the waiting phase. Once the queue is exhausted
// without an answer, it waits for the armed pending reports until the hard
// deadline. The first success wins, including a downgrade, and answers 200
// with the body, while a non-success settles the real entry and the wait
// goes on. When all of them are settled without a success or the deadline
// expires, it returns the last failure for the cloud or the final answer,
// replacing the provisional failure of the loop with the last settled
// one, which is the real signal. A client that disappeared aborts without
// writing. Only this method and the loop write to the response writer.
func (g *Gateway) waitPendencies(w http.ResponseWriter, req InvocationRequest, sh gatewayDispatchShared, pend *pendencySet, failure gatewayFailure) (gatewayFailure, bool) {
	armed := pend.armedList()
	if len(armed) == 0 {
		return failure, false
	}
	hardAbs := hardDeadline(sh.overall, sh.start, armed)
	delay := hardAbs.Sub(g.now())
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	// The cloud runs in a race with a single policy: it leaves together
	// with the wait and its deadline is the hard deadline. Without a
	// fallback the channels are nil and the selection ignores them.
	cloudCh, cloudDone, cloudCancel := g.launchCloudRace(hardAbs, sh, req)
	if cloudCancel != nil {
		defer cloudCancel()
	}
	cloudPending := cloudCh != nil
	// Only the armed reports that did not conclude are counted: the ones
	// consumed in the loop do not send another message, and counting
	// them would make the wait block until the hard deadline for
	// nothing. Without the observation all of them have not concluded
	// here, as before.
	remaining := 0
	for _, pa := range armed {
		if pa != nil && pa.armed.Load() && !pa.concluded.Load() {
			remaining++
		}
	}
	for remaining > 0 || cloudPending {
		select {
		case pa := <-pend.settled:
			if pa == nil || !pa.armed.Load() {
				continue // desfecho já consumido no laço (nunca armado)
			}
			pa.concluded.Store(true)
			out := pa.outcome
			// The attempt concluded, so the callback of the disconnection
			// and the clock of the ceiling are released, which is
			// idempotent, in every branch below.
			pa.stopClientWatch()
			pa.cancelAttempt()
			if !out.hasResponse && out.ctxErr != nil && out.ctxErr != context.DeadlineExceeded {
				// Our own cancellation, by a winner, the deadline or a
				// disconnection, records nothing, so the synchronous entry
				// remains.
				pa.settleDec()
				remaining--
				continue
			}
			kind, f, sloMet := g.settlePending(pa, out, true)
			if out.hasResponse {
				_ = g.nodes.Touch(pa.target.NodeID)
			}
			pa.settleDec()
			if kind != attemptSuccess && kind != attemptDowngrade {
				if f.code != "" {
					failure = f
				}
				remaining--
				continue
			}
			// The late delivery is a success with the objective unmet, marked
			// on the wire and not an error. The normal path, which is not
			// late, gets no new header.
			w.Header().Set(contentTypeHeader, applicationJSON)
			w.Header().Set("X-Faas-Late", "true")
			w.Header().Set("X-Faas-Slo-Met", boolHeader(sloMet))
			if kind == attemptDowngrade {
				w.Header().Set("X-Faas-Quality-Downgrade", "true")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(out.res.body)
			atomic.AddInt64(&g.bestEffortServed, 1)
			if kind == attemptDowngrade {
				log.Printf("arbitro: gateway function=%s->%s [%s] node=%s result=DOWNGRADE required=%s expected=%s", req.Function, pa.concrete, sh.group, pa.target.NodeID, downgradeRequired(req.SLO), downgradeExpected(pa.target, pa.concrete))
			} else {
				log.Printf("arbitro: gateway function=%s->%s [%s] node=%s latency_ms=%d result=OK tentativa=%d", req.Function, pa.concrete, sh.group, pa.target.NodeID, g.now().Sub(sh.start).Milliseconds(), pa.attemptN)
			}
			log.Printf("arbitro: RACE_WINNER function=%s node=%s tentativa=%d downgrade=%v", req.Function, pa.target.NodeID, pa.attemptN, kind == attemptDowngrade)
			log.Printf("arbitro: BEST_EFFORT_SERVED function=%s node=%s tentativa=%d slo_met=%v", req.Function, pa.target.NodeID, pa.attemptN, sloMet)
			// The local orphans run until the ceiling and are not
			// cancelled, so that the drain settles them and keeps the
			// learning and the container warm.
			if cloudDone != nil {
				// The join of the cloud that lost is deterministic: the
				// cancellation below interrupts the invocation and the
				// record in the invocation log already happened when the
				// channel closed, before the return of the handler.
				cloudCancel()
				<-cloudDone
			}
			return failure, true
		case cout := <-cloudCh:
			cloudPending = false
			if cout.err == nil {
				writeCloudResponse(w, cout.data)
				log.Printf("arbitro: gateway function=%s result=CLOUD_FALLBACK_OK latency_ms=%d", req.Function, cout.responseTimeMS)
				// The orphan runs until the ceiling and the drain settles it.
				return failure, true
			}
			// The cloud failed: it is already recorded in the invocation
			// log and the wait goes on, since the live orphans can
			// still deliver and the first success wins.
			failure = cloudRaceFailure(cout.err, failure.retryable)
			log.Printf("arbitro: gateway function=%s result=CLOUD_FALLBACK_FAILED err=%v", req.Function, cout.err)
		case <-sh.clientCtx.Done():
			// The client disappeared, so the wait aborts without writing.
			// The drain joins the orphans and the cloud is cancelled by
			// the callback and records on its own.
			log.Printf("arbitro: gateway function=%s result=CLIENT_DISCONNECTED pendencias=%d", req.Function, remaining)
			pend.cancelAll()
			return failure, true
		case <-timer.C:
			// The hard deadline expires and the final failure is returned.
			// The cloud, if it is alive, records on its own until its
			// own deadline, which is the hard deadline, and the local
			// orphans run until the ceiling and are settled by the
			// drain.
			atomic.AddInt64(&g.bestEffortExpired, 1)
			log.Printf("arbitro: BEST_EFFORT_EXPIRED function=%s pendencias=%d", req.Function, remaining)
			remaining = 0
			cloudPending = false
		}
	}
	// The queue and the cloud are exhausted without a success: the last
	// failure is returned, and the fallback flag is set when the cloud
	// ran, since it was tried, as in the sequential path.
	writeGatewayError(w, failure.httpStatus, failure.code, failure.msg, failure.retryable, cloudCh != nil)
	return failure, false
}
