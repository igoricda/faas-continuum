package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// cloudOutcome is the outcome of a single cloud attempt: the body ready
// to be written, which is absent on an error, and the classification. It is
// produced by the runner of the cloud attempt, which records it in the
// invocation log in both cases, since the loser of the race also counts,
// for the replay and the warm path.
type cloudOutcome struct {
	data           []byte
	err            error
	responseTimeMS int
}

// runCloudAttempt runs a single cloud attempt under the given context and
// records the outcome in the invocation log, whether it wins or loses, since
// the losing execution warms the instance and the replay accounts for the
// waste. It is a pure extraction of the cloud fallback, without changing
// the contract.
func (g *Gateway) runCloudAttempt(ctx context.Context, start time.Time, req InvocationRequest) cloudOutcome {
	forwardStart := g.now()
	// The estimate of the cold start is read immediately before the
	// invocation of the same goroutine, and an implementation that lacks
	// the estimator reports no cold start as before.
	cold := false
	if ec, ok := g.remote.(ColdEstimator); ok {
		cold = ec.EstimateCold(req.Function)
	}
	// The remote port: the gateway does not know the endpoint, the
	// configuration nor the HTTP of the cloud, only the function and the
	// raw input. The connector holds its own shared HTTP client.
	body, err := g.remote.Invoke(ctx, req.Function, req.Input)
	if err != nil {
		g.recordCloudAttempt(g.now().Sub(start), err, req.SLO, req.Function, nil)
		return cloudOutcome{err: err}
	}
	ended := g.now().UTC()
	responseTimeMS := int(ended.Sub(start).Milliseconds())
	functionMS := int(ended.Sub(forwardStart).Milliseconds())
	if functionMS > responseTimeMS {
		functionMS = responseTimeMS
	}
	overheadMS := responseTimeMS - functionMS
	if overheadMS < 0 {
		overheadMS = 0
	}
	resultRaw := json.RawMessage(body)
	if !json.Valid(body) {
		buf := gatewayBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		enc := json.NewEncoder(buf)
		enc.Encode(string(body))
		bodyBytes := bytes.TrimRight(buf.Bytes(), "\n")
		copied := append([]byte(nil), bodyBytes...)
		gatewayBufPool.Put(buf)
		resultRaw = json.RawMessage(copied)
	}
	metrics := gatewayCloudMetrics{
		ResponseTimeMS: responseTimeMS,
		StartedAt:      start.UTC().Format(time.RFC3339Nano),
		EndedAt:        ended.Format(time.RFC3339Nano),
		InstanceID:     cloudFallbackNodeID,
		ColdStart:      cold,
		Phases: gatewayCloudPhases{
			AcquireWaitMS: 0,
			ReadinessMS:   0,
			FunctionMS:    functionMS,
			OverheadMS:    overheadMS,
		},
		CloudFallback: true,
	}
	respObj := gatewayCloudSuccess{
		Result:           resultRaw,
		ExecutionMetrics: metrics,
	}
	data, _ := json.Marshal(respObj)
	cloudMetricsRaw, _ := json.Marshal(metrics)
	g.recordCloudAttempt(ended.Sub(start), nil, req.SLO, req.Function, cloudMetricsRaw)
	return cloudOutcome{data: data, responseTimeMS: responseTimeMS}
}

// writeCloudResponse writes the successful answer of the cloud, in the sequential path and in the race.
func writeCloudResponse(w http.ResponseWriter, data []byte) {
	w.Header().Set(contentTypeHeader, applicationJSON)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// serveCloudFallback tries the cloud fallback when the queue of nodes is
// exhausted. It returns true when it answered, with a success or with the
// error of the fallback, and false when there is no configured fallback,
// in which case the caller answers with the last failure of the queue. All
// the parameters are passed by value, so nothing is aliased with the
// gateway.
//
// The contract of the cloud endpoint sends the raw input of the client,
// without the envelope of the node protocol, with the configured content
// type, and expects a successful answer with a raw body, which is not
// interpreted as JSON, so a body that is not JSON is wrapped as a JSON
// string. An endpoint that is only compatible with the node protocol and
// requires the envelope answers a bad request here, so the endpoint has to
// be documented as a cloud and not as a node. A failure of the fallback
// answers with the code of a failed cloud fallback and the retryable flag
// inherited from the root cause of the queue, since a cloud that is down
// does not turn a retryable cause into a definitive one.
func (g *Gateway) serveCloudFallback(w http.ResponseWriter, r *http.Request, ctx context.Context, overall time.Duration, start time.Time, failure gatewayFailure, req InvocationRequest) bool {
	if g.remote != nil && g.remote.Configured() {
		var remaining time.Duration
		if deadline, ok := ctx.Deadline(); ok {
			remaining = time.Until(deadline)
		} else {
			remaining = overall - g.now().Sub(start)
		}
		if remaining <= 0 {
			writeGatewayError(w, failure.httpStatus, failure.code, failure.msg, failure.retryable, false)
			return true
		}
		ctxC, cancelC := context.WithTimeout(ctx, remaining)
		defer cancelC()
		cout := g.runCloudAttempt(ctxC, start, req)
		if cout.err == nil {
			writeCloudResponse(w, cout.data)
			log.Printf("arbitro: gateway function=%s result=CLOUD_FALLBACK_OK latency_ms=%d", req.Function, cout.responseTimeMS)
			return true
		}
		log.Printf("arbitro: gateway function=%s result=CLOUD_FALLBACK_FAILED err=%v", req.Function, cout.err)
		writeGatewayError(w, http.StatusBadGateway, "CLOUD_FALLBACK_FAILED", fmt.Sprintf("fallback de nuvem falhou: %v", cout.err), failure.retryable, true)
		return true
	}
	return false
}

// downgradeRequired summarizes the requested thresholds for the downgrade
// line: the domain metrics verbatim, without the surrounding spaces, and an
// empty string when the objective is absent.
func downgradeRequired(slo *selection.SLO) string {
	if slo == nil || len(bytes.TrimSpace(slo.DomainMetrics)) == 0 {
		return "{}"
	}
	return string(bytes.TrimSpace(slo.DomainMetrics))
}

// downgradeExpected summarizes the expectations of the served concrete
// variant for the downgrade line, and an empty string when nothing was
// announced.
func downgradeExpected(target nodes.Node, concrete string) string {
	if exp, ok := target.FunctionExpected[concrete]; ok && len(exp) > 0 {
		if data, err := json.Marshal(exp); err == nil {
			return string(data)
		}
	}
	return "{}"
}
