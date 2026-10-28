package gateway

import (
	"encoding/json"
	"net/http"
)

// Matrix of the codes of the gateway: an identical routing can carry
// distinct codes, so the alerts have to match the status and the code and
// never only one of them.
//
//	503 NO_NODE_AVAILABLE   the queue is empty, with no active node or
//	                        host, and it is retryable
//	504 NO_NODE_AVAILABLE   the budget was exhausted before the first
//	                        attempt, with zero attempts, and it is retryable
//	503 NODE_OVERLOADED     the queue was exhausted by a capacity
//	                        rejection, the exhausted pool or the full
//	                        queue, or by a generic overload
//	504 NODE_INVOCATION_TIMEOUT  a real timeout of the node, where the
//	                        function timed out, or the budget exhausted in
//	                        the middle of the attempt. The invocation log
//	                        tells them apart by the outcome
//	502 NODE_ERROR          a transport or network error without an
//	                        answer, or a framing error in a successful
//	                        answer with a body above eight megabytes or
//	                        invalid JSON. A bad request of the node becomes
//	                        a bad request that fails fast and never a bad
//	                        gateway answer
//	502 CLOUD_FALLBACK_FAILED  the cloud was tried and failed, and the
//	                        retryable flag is inherited from the root
//	                        cause of the queue and is not fixed
//	401 UNAUTHORIZED, 404 FUNCTION_NOT_FOUND and 400 NODE_BAD_REQUEST
//	                        fail fast, with no failover to the next node
//	202 or 208 for the late report route, meaning applied or duplicate, and
//	                        503 without a configured gateway
func writeGatewayError(w http.ResponseWriter, status int, code, message string, retryable, fallback bool) {
	w.Header().Set("Content-Type", "application/json")
	resp := ErrorResponse{
		Code:            code,
		Message:         message,
		Retryable:       retryable,
		FallbackApplied: fallback,
	}
	data, err := json.Marshal(resp)
	if err != nil {
		// It should never fail, but it is a defensive fallback.
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"ENCODE_FAILED","message":"falha ao serializar resposta","retryable":false,"fallback_applied":false}`))
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
