package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// attemptKind is the outcome of a single HTTP answer of the node, decided
// by the classification of the result: the synchronous loop and the waiting
// stage consumes the same mapping from the status to the class.
type attemptKind int

const (
	// attemptNext continues with the next node, in the case of a not
	// found, an overload, a timeout, an unexpected status, a framing
	// error in a successful answer or an unauthorized answer with a
	// translated token. It records the attempt and tries the next node.
	attemptNext attemptKind = iota
	// attemptSuccess is a valid successful answer without a downgrade: it
	// answers 200 and ends.
	attemptSuccess
	// attemptDowngrade is a valid successful answer with a downgrade of
	// quality: it answers 200 with the downgrade header and ends.
	attemptDowngrade
	// attemptFailFast answers the error immediately and ends, in the case
	// of an unauthorized answer with the credential of the client or of
	// a deterministic bad request, with no failover.
	attemptFailFast
)

// attemptResult is a single HTTP answer read from the node: the status,
// the body, the block of metrics and the framing error, if any. It is
// produced by the reader of the result and consumed by the classifier,
// neither of which has side effects.
type attemptResult struct {
	status int
	body   []byte
	// metrics is the block of execution metrics verbatim when the
	// successful answer is valid, and absent in every other case,
	// including a framing error.
	metrics json.RawMessage
	// framingErr marks a successful answer outside the framing, with a
	// body above eight megabytes or invalid JSON. It wraps the framing
	// error with the exact message of the legacy synchronous path.
	framingErr error
}

// readAttemptResult reads a single HTTP answer of the node with the same
// framing as the legacy synchronous path: a successful answer is read with
// a limit of eight megabytes and validated for size and JSON, and an error
// is read with a limit of 64 kibibytes for the parsing of the code and the
// pressure. It always closes the body. It is a pure function.
func readAttemptResult(resp *http.Response) attemptResult {
	res := attemptResult{status: resp.StatusCode}
	limit := int64(maxCapacityErrorBodyBytes) + 1
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		limit = int64(maxInvokeBodyBytes) + 1
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, limit))
	resp.Body.Close()
	res.body = body
	if res.status < 200 || res.status >= 300 {
		return res
	}
	if int64(len(body)) > maxInvokeBodyBytes {
		res.framingErr = fmt.Errorf("%w: resposta do nó excede o limite de 8 MiB", ErrAttemptFraming)
		return res
	}
	if !json.Valid(body) {
		res.framingErr = fmt.Errorf("%w: resposta do nó não é JSON válido", ErrAttemptFraming)
		return res
	}
	res.metrics = executionMetricsOf(body)
	return res
}

// classifyAttemptResult reproduces exactly the switch of the legacy
// synchronous path: the unauthorized, the bad request, the not found, the
// overload, the timeout, the unexpected status and the downgrade in a
// successful answer. It reuses the helpers of the capacity error, of the
// admission timeout and of the admission pressure, and the sentinel errors
// of the attempt, with no new logic.
//
// The requested name is the one asked by the client and is used in the
// messages, while the group is the resolved one and is used in the message
// of the translated unauthorized answer. The quality requirements and the
// declared quality come from the single parsing of the handler. The
// translated flag tells whether the category token was translated: a
// translated unauthorized answer continues with the next node, while the
// verbatim one fails fast.
//
// It returns the error of the attempt, the failure, the kind that tells
// the caller what to do and the reason that is the suffix of the legacy log
// line. The pressure is only present in a timeout with the pressure block,
// for the captured snapshot. It is a pure function, without input or
// output.
func classifyAttemptResult(res attemptResult, target nodes.Node, requested, group string, qualityReqs map[string]float64, qualityDeclared, translated bool) (attemptErr error, failure gatewayFailure, kind attemptKind, reason string, pressure *NodeAdmissionPressure) {
	if res.status >= 200 && res.status < 300 {
		if res.framingErr != nil {
			cause := "resposta do nó não é JSON válido"
			reason = "invalid_json"
			if int64(len(res.body)) > maxInvokeBodyBytes {
				cause = "resposta do nó excede o limite de 8 MiB"
				reason = "response_too_large"
			}
			return res.framingErr,
				gatewayFailure{httpStatus: http.StatusBadGateway, code: "NODE_ERROR", msg: cause, retryable: true},
				attemptNext, reason, nil
		}
		if qualityDeclared && selection.Downgraded(target, requested, qualityReqs) {
			return nil, gatewayFailure{}, attemptDowngrade, "DOWNGRADE", nil
		}
		return nil, gatewayFailure{}, attemptSuccess, "OK", nil
	}
	switch res.status {
	case http.StatusUnauthorized:
		if translated {
			msg := fmt.Sprintf("nó %q rejeitou o token traduzido da categoria %q (mapeamento obsoleto?)", target.NodeID, group)
			return fmt.Errorf("%w: token traduzido rejeitado por %q", ErrAttemptUnauthorized, target.NodeID),
				gatewayFailure{httpStatus: http.StatusUnauthorized, code: "UNAUTHORIZED", msg: msg, retryable: true},
				attemptNext, "UNAUTHORIZED_TRANSLATED", nil
		}
		return fmt.Errorf("%w: 401 Unauthorized", ErrAttemptUnauthorized),
			gatewayFailure{httpStatus: http.StatusUnauthorized, code: "UNAUTHORIZED", msg: "credencial de invocação inválida ou ausente", retryable: false},
			attemptFailFast, "UNAUTHORIZED", nil
	case http.StatusBadRequest:
		msg := fmt.Sprintf("nó %q rejeitou o input (400 determinístico, sem failover)", target.NodeID)
		return fmt.Errorf("%w de %q", ErrAttemptBadRequest, target.NodeID),
			gatewayFailure{httpStatus: http.StatusBadRequest, code: "NODE_BAD_REQUEST", msg: msg, retryable: false},
			attemptFailFast, "BAD_REQUEST_FAIL_FAST", nil
	case http.StatusNotFound:
		msg := fmt.Sprintf("função %q não encontrada no nó %q", requested, target.NodeID)
		return fmt.Errorf("%w: %s", ErrAttemptNotFound, msg),
			gatewayFailure{httpStatus: http.StatusNotFound, code: "FUNCTION_NOT_FOUND", msg: msg, retryable: false},
			attemptNext, "FUNCTION_NOT_FOUND", nil
	case http.StatusServiceUnavailable:
		msg := fmt.Sprintf("nó %q sobrecarregado", target.NodeID)
		attemptErr = fmt.Errorf("%w: %s", ErrAttemptOverloaded, msg)
		if capErr := capacityAttemptErr(res.body, target.NodeID, msg); capErr != nil {
			attemptErr = capErr
		}
		return attemptErr,
			gatewayFailure{httpStatus: http.StatusServiceUnavailable, code: "NODE_OVERLOADED", msg: msg, retryable: true},
			attemptNext, "NODE_OVERLOADED", nil
	case http.StatusGatewayTimeout:
		msg := fmt.Sprintf("nó %q timeout na invocação", target.NodeID)
		attemptErr = fmt.Errorf("%w: %s", ErrAttemptTimeout, msg)
		if admErr, ok := classifyAdmissionTimeout(nodeErrorCodeOrEmpty(res.body), target.NodeID, msg); ok {
			attemptErr = admErr
		}
		if pr, ok := nodeAdmissionPressure(res.body); ok {
			prCopy := pr
			pressure = &prCopy
		}
		return attemptErr,
			gatewayFailure{httpStatus: http.StatusGatewayTimeout, code: "NODE_INVOCATION_TIMEOUT", msg: msg, retryable: true},
			attemptNext, "NODE_INVOCATION_TIMEOUT", pressure
	default:
		msg := fmt.Sprintf("nó %q respondeu com status %d", target.NodeID, res.status)
		return errors.New(msg),
			gatewayFailure{httpStatus: http.StatusBadGateway, code: "NODE_ERROR", msg: msg, retryable: true},
			attemptNext, "NODE_ERROR", nil
	}
}
