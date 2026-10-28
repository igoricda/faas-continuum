package gateway

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"
)

// gatewayFailure holds the last error state of the attempts of the
// gateway.
type gatewayFailure struct {
	httpStatus int
	code, msg  string
	retryable  bool
}

// finishWithFailure closes the invocation when the dispatch did not
// answer: it maps the case of zero attempts to a timeout, tries the cloud
// fallback and writes the last failure. A false value of tried means the
// budget was exhausted before the first attempt.
func (g *Gateway) finishWithFailure(w http.ResponseWriter, r *http.Request, ctx context.Context, overall time.Duration, start time.Time, failure gatewayFailure, req InvocationRequest, tried bool) {
	// With zero attempts the budget was exhausted before the first one.
	// The initial failure, a stale internal error, never represented a
	// real classification, so it maps to a retryable timeout instead of
	// leaking the initial value.
	if !tried {
		failure = gatewayFailure{httpStatus: http.StatusGatewayTimeout, code: "NO_NODE_AVAILABLE", msg: fmt.Sprintf("orçamento de %s esgotado antes da 1ª tentativa para função %q", overall, req.Function), retryable: true}
		log.Printf("arbitro: gateway function=%s result=ZERO_ATTEMPT_BUDGET_EXCEEDED overall=%s", req.Function, overall)
	}

	if g.serveCloudFallback(w, r, ctx, overall, start, failure, req) {
		return
	}

	writeGatewayError(w, failure.httpStatus, failure.code, failure.msg, failure.retryable, false)
}
