package gateway

import (
	"context"
	"net/http"

	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

func (g *Gateway) Handle(w http.ResponseWriter, r *http.Request) {
	// The injectable clock: the start, the current time, the start of the
	// attempt and every derived latency come from it, never from the wall
	// clock directly, so that the objective is deterministic under a fake
	// clock. In production the clock is the wall clock, whose monotonic
	// reading is preserved by the subtraction.
	start := g.now()

	req, ok := decodeInvokeRequest(w, r)
	if !ok {
		return
	}

	qualityReqs, qualityDeclared, ok := parseInvokeQuality(w, req)
	if !ok {
		return
	}

	now := g.now()
	active := g.listActiveNodes(now)

	// The budget and the context are built before the cut of an empty
	// queue, so that the cloud is reachable even when no candidate
	// remains, whether there is no active node or host, whether all of
	// them are overloaded or projected, ineligible or filtered by a
	// mark. The block used to come after the cut and the empty queue
	// answered with a hard error without consulting the cloud.
	overall := resolveOverallBudget(g.defaultBudget, req)
	ctx, cancel := context.WithTimeout(r.Context(), overall)
	defer cancel()

	queue := g.selector.Select(active, req.Function, req.SLO)
	if g.handleEmptyQueue(w, r, ctx, overall, start, req, queue) {
		return
	}
	queue = g.applyFanoutCut(queue, req.Function)

	// The group is resolved once per invocation and keys the statistics
	// of the history through the record of the attempt.
	group := selection.ResolveRequestGroup(req.Function, active)

	requestID, ok := g.newRequestCorrelation(w, group, req.SLO)
	if !ok {
		return
	}

	// The credential of the client and the category token: the
	// authorization header is captured once here and passed by value to
	// the dispatch, and it is never logged. When it is the token of the
	// category, which is the resolved group, each attempt translates it
	// into the token of the destination node, and otherwise the legacy
	// verbatim forwarding is used. The invariant of a single token per
	// node still holds on the verbatim path.
	authHeader := r.Header.Get("Authorization")

	bodyBytes, buf, ok := encodeNodePayload(w, req.Input)
	if !ok {
		return
	}
	defer gatewayBufPool.Put(buf)

	sh := gatewayDispatchShared{
		ctx:             ctx,
		clientCtx:       r.Context(),
		overall:         overall,
		start:           start,
		requestID:       requestID,
		group:           group,
		bodyBytes:       bodyBytes,
		authHeader:      authHeader,
		catToken:        g.catTokens.Verify(group, authHeader),
		qualityReqs:     qualityReqs,
		qualityDeclared: qualityDeclared,
		queue:           queue,
		pend:            newPendencySet(len(queue), g.maxRaceFanout),
	}
	// The drain runs in a goroutine: the answer already left and the
	// armed orphan is not cancelled, so it runs until the ceiling of the
	// node, is settled with the real outcome, which supersedes the
	// exceeded budget and keeps the learning, and keeps the container
	// warm. Here only the observer of the disconnection is detached, since
	// the context of the request is cancelled by the normal return of the
	// handler and not by a drop of the client, and the join is released.
	// Every attempt detached the body in the dispatch with its own copy,
	// so returning the buffer to the pool is safe.
	defer func() {
		for _, pa := range sh.pend.armedList() {
			pa.stopClientWatch()
		}
		go g.drainPendencies(sh.pend)
	}()
	failure, tried, responded := g.dispatchAttempts(w, req, sh)
	if responded {
		return
	}

	// The waiting stage runs when the queue is exhausted without an
	// answer: the first success of the pending reports within the hard
	// deadline wins, and without pending reports it returns the failure
	// of the loop immediately. With armed pending reports the cloud runs
	// in a race with the wait and the wait always answers, with a
	// victory, a disconnection or the final failure, so it never reaches
	// the path of the final failure, since the cloud already ran.
	if len(sh.pend.armedList()) > 0 {
		g.waitPendencies(w, req, sh, sh.pend, failure)
		return
	}

	g.finishWithFailure(w, r, ctx, overall, start, failure, req, tried)
}
