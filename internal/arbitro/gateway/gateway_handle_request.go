package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// decodeInvokeRequest reads the body of the invocation under a limit of
// eight megabytes and validates that the function and the input are
// present. It writes the error, with a payload too large or a bad
// request, and reports no success when the request is invalid.
func decodeInvokeRequest(w http.ResponseWriter, r *http.Request) (req InvocationRequest, ok bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxInvokeBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeGatewayError(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "corpo da requisição excede o limite de 8 MiB", false, false)
		} else {
			writeGatewayError(w, http.StatusBadRequest, "INVALID_REQUEST", "corpo da requisição inválido: esperado JSON com function e input", false, false)
		}
		return req, false
	}
	if req.Function == "" || req.Input == nil || len(req.Input) == 0 || bytes.Equal(bytes.TrimSpace(req.Input), nullBytes) {
		writeGatewayError(w, http.StatusBadRequest, "INVALID_REQUEST", "corpo da requisição inválido: campos function e input são obrigatórios", false, false)
		return req, false
	}
	return req, true
}

// parseInvokeQuality interprets the quality requirements once per
// invocation. The map is passed by target to the variant selection and to
// the downgrade, without being parsed again. An error gives a bad request
// and reports no success.
func parseInvokeQuality(w http.ResponseWriter, req InvocationRequest) (qualityReqs map[string]float64, qualityDeclared bool, ok bool) {
	if req.SLO != nil {
		reqs, declared, err := selection.ParseQualityRequirements(req.SLO)
		if err != nil {
			writeGatewayError(w, http.StatusBadRequest, "INVALID_REQUEST", "corpo da requisição inválido: domain_metrics inválido", false, false)
			return nil, false, false
		}
		qualityReqs, qualityDeclared = reqs, declared
	}
	return qualityReqs, qualityDeclared, true
}

// listActiveNodes filters the nodes whose derived status is active at the
// given instant, which the caller passes explicitly and derives from the
// clock of the gateway.
func (g *Gateway) listActiveNodes(now time.Time) []nodes.Node {
	all := g.nodes.List()
	active := all[:0]
	for _, n := range all {
		if nodes.DerivedStatus(n, g.staleAfter, now) == nodes.StatusAtivo {
			active = append(active, n)
		}
	}
	return active
}

// resolveOverallBudget computes the budget of the invocation: the default
// budget, or the default timeout of the gateway when it is zero, overridden
// by the objective when it is positive. There is no minimum floor on
// purpose, so that a tiny objective exhausts the budget before the first
// attempt and falls into the mapping of zero attempts, instead of masking
// the requested budget. It is coordinated with the ceiling of the node,
// which is the minimum of the timeout of the node and the configured
// ceiling, with a default of 90 seconds, and a value that is not positive
// is fatal.
func resolveOverallBudget(defaultBudget time.Duration, req InvocationRequest) time.Duration {
	overall := defaultBudget
	if overall == 0 {
		overall = defaultGatewayTimeout
	}
	if req.SLO != nil && req.SLO.MaxLatencyMS > 0 {
		overall = time.Duration(req.SLO.MaxLatencyMS) * time.Millisecond
	}
	return overall
}

// handleEmptyQueue handles the empty queue: the same outcome of no node
// available becomes the failure of the fallback when the cloud is
// configured. Without a cloud the fallback reports that it did not answer
// and the legacy path answers byte for byte, with the record, the log and
// the write. The record of a rejected invocation never runs when the cloud
// served the answer. It returns true when it answered, so the caller has
// to return.
func (g *Gateway) handleEmptyQueue(w http.ResponseWriter, r *http.Request, ctx context.Context, overall time.Duration, start time.Time, req InvocationRequest, queue []nodes.Node) bool {
	if len(queue) != 0 {
		return false
	}
	failure := gatewayFailure{httpStatus: http.StatusServiceUnavailable, code: "NO_NODE_AVAILABLE", msg: fmt.Sprintf("nenhum nó disponível para função %q", req.Function), retryable: true}
	if g.serveCloudFallback(w, r, ctx, overall, start, failure, req) {
		return true
	}
	g.recordNoNode(req.Function, req.SLO)
	log.Printf("arbitro: gateway function=%s result=NO_NODE_AVAILABLE latency_ms=%d", req.Function, g.now().Sub(start).Milliseconds())
	writeGatewayError(w, failure.httpStatus, failure.code, failure.msg, failure.retryable, false)
	return true
}

// applyFanoutCut applies the ceiling of the fan-out: at most that many
// attempts per invocation, and a value of zero or less keeps the whole
// queue, as before. The cut is visible: it adds a log line and increments
// the counter of the cuts, since a healthy candidate beyond the ceiling is
// never tried even with budget left.
func (g *Gateway) applyFanoutCut(queue []nodes.Node, function string) []nodes.Node {
	if g.maxRaceFanout > 0 && len(queue) > g.maxRaceFanout {
		atomic.AddInt64(&g.fanoutCuts, 1)
		log.Printf("arbitro: gateway function=%s result=FANOUT_CUT candidatos=%d tentativas=%d", function, len(queue), g.maxRaceFanout)
		queue = queue[:g.maxRaceFanout]
	}
	return queue
}

// newRequestCorrelation generates the request identifier of the invocation
// and registers the group for the late report: one identifier per
// invocation of the client and one attempt number per attempt. The node
// that opted in returns both in the late report, and the sink resolves
// the group through the registration. The identifiers are random and never
// a token, so there is no secret here. A failure of the random source
// gives an internal error and reports no success.
func (g *Gateway) newRequestCorrelation(w http.ResponseWriter, group string, slo *selection.SLO) (requestID string, ok bool) {
	var rid [16]byte
	if _, err := rand.Read(rid[:]); err != nil {
		writeGatewayError(w, http.StatusInternalServerError, "NODE_ERROR", "falha ao gerar id de correlação", true, false)
		return "", false
	}
	requestID = hex.EncodeToString(rid[:])
	g.RegisterLateGroup(requestID, group, slo)
	return requestID, true
}

// encodeNodePayload serializes the invocation envelope in the buffer of
// the pool and returns the body, without the trailing newline of the
// encoder, together with the buffer. The caller keeps the buffer alive
// while the body is used in the loop of the attempts, which avoids the
// copy of up to eight megabytes per invocation that existed before. The
// pool stays safe because no path returns before the deferred return of
// the buffer, and nothing outside the handler references the body after
// the return. A failure of the encoding gives an internal error and
// reports no success, and the buffer goes back to the pool here.
func encodeNodePayload(w http.ResponseWriter, input json.RawMessage) (bodyBytes []byte, buf *bytes.Buffer, ok bool) {
	payload := nodeInvokePayload{Input: input}
	buf = gatewayBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	enc := json.NewEncoder(buf)
	if err := enc.Encode(payload); err != nil {
		gatewayBufPool.Put(buf)
		writeGatewayError(w, http.StatusInternalServerError, "NODE_ERROR", "falha ao serializar corpo para o nó", true, false)
		return nil, nil, false
	}
	bodyBytes = bytes.TrimRight(buf.Bytes(), "\n")
	return bodyBytes, buf, true
}
