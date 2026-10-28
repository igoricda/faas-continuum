package gateway

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// Sentinel errors of the attempt classes that are not recorded in the
// history: every branch of the handler that produces an outcome without a
// real performance measurement wraps its own sentinel with the wrapping
// operator, and the recordability policy detects it with the error chain
// instead of matching the text of the message.
var (
	// ErrAttemptUnauthorized marks the unauthorized answer of the node, with a wrong or expired credential.
	ErrAttemptUnauthorized = errors.New("gateway: credencial de invocação rejeitada pelo nó")
	// ErrAttemptNotFound marks the not found answer of a resolved concrete variant, with an obsolete map.
	ErrAttemptNotFound = errors.New("gateway: função ausente no nó (mapa obsoleto)")
	// ErrAttemptFraming marks a successful answer outside the framing,
	// with a body above eight megabytes or invalid JSON.
	ErrAttemptFraming = errors.New("gateway: resposta do nó fora do enquadramento")
	// ErrAttemptTimeout marks the real timeout of the node, which is
	// anchored in the status of the handler and never in the text of a
	// message.
	ErrAttemptTimeout = errors.New("gateway: timeout na invocação do nó")
	// ErrAttemptAdmissionTimeout marks the timeout in which the deadline
	// expired in the admission stage of the node, while waiting for the
	// container or the provisioning, or for an instance that never became
	// ready, so the function did not run. It is different from the real
	// timeout of the node, which marks the case in which the function ran
	// and did not answer: that one is a service failure and can veto the
	// node, while this one is a saturation of the admission and cannot.
	//
	// The distinction was measured in a campaign with a small model, where
	// the 264 timeouts of the fog node all came from this path, with the
	// latency stuck at the timeout of the node, no function time and the
	// host at two to ten percent of the processor with 36 live containers.
	// Counted as a service failure, they vetoed the fastest node of the
	// system, and the ambiguity was in the code that the node emitted and
	// not in the measured data.
	ErrAttemptAdmissionTimeout = errors.New("gateway: timeout na admissão do nó (pedido não conseguiu instância)")
	// ErrAttemptBudgetExceeded marks the timeout of the arbiter, when the
	// overall budget ran out in the middle of the attempt and the
	// request failed with an expired context. It is not a measurement of
	// the latency of the node: it classifies as an exceeded budget and
	// enters the history as a failure without latency, so it affects the
	// totals, the success rate and the veto by rate and never the
	// percentile. It has to be checked before the deadline error, since
	// it wraps the error of the transport, which can match the deadline
	// error through the chain.
	ErrAttemptBudgetExceeded = errors.New("gateway: orçamento do árbitro esgotado durante a tentativa")
	// ErrAttemptBadRequest marks the deterministic bad request of the
	// node: a rejected input is not cured by a failover and fails fast,
	// like the unauthorized answer. It is a generic error on purpose, so
	// that the classification gives a failure that only reaches the
	// invocation log and does not touch the recordability policy.
	ErrAttemptBadRequest = errors.New("gateway: nó rejeitou o input (400 determinístico)")
	// ErrAttemptOverloaded marks the generic overload of the node, with a
	// truncated, empty or non-JSON body or an unknown code. It has no
	// identity of a pool on purpose, so it does not count as a capacity
	// rejection and does not mark the monitor, but it keeps the semantics
	// of the status for the recordability. It is still recorded, since the
	// saturation has to stay visible, but the classification maps it to the
	// rejection of a capacity and not to a generic failure: counting it
	// as a service failure made the veto ban the saturated node.
	ErrAttemptOverloaded = errors.New("gateway: nó sobrecarregado (503 genérico)")
)

// isRecordable is the recordability policy by class.
//
// The history trains with the success and with three classes of negative
// signal, each with a different weight:
//
//   - A SERVICE FAILURE, which is a legitimate timeout and an error of
//     the function or of the transport. They feed the totals and the
//     success rate, which are the signal of reliability and the only one
//     that the veto consumes. They never enter the percentile, which only
//     follows the successes without a cold start: the original reason
//     kept holding, the amnesia of the failure left the percentile
//     seeing only the successes and the arbiter kept insisting on the
//     saturated edge.
//   - A SATURATION, which is the budget exhausted during the attempt, the
//     rejection of a capacity and the timeout of the admission, where the
//     body tells the stage in which the deadline expired and the function
//     never ran. They keep being recorded but leave the totals and the
//     success rate and go to the saturation counter. They are the node
//     saying that it cannot cope, and treating them as a service failure
//     made the veto ban the saturated node, which is how the fastest node
//     of the campaign ended up vetoed in every bucket with 97 samples and
//     no success. The timeout of the admission is the largest slice, with
//     264 of the 531 requests of the fog node, whose deadline expired
//     before the function ran.
//   - OUTSIDE, which is the unauthorized, the not found and the framing
//     errors, which concern the credential, the map and the framing and
//     not the performance, and the fast failure, such as a transport
//     error. A fast failure would lower the percentile and make a sick
//     node look fast.
//
// Neither group enters the percentile. The policy fails closed: a new
// class falls into the failure and stays out until an explicit decision.
func isRecordable(err error) bool {
	if err == nil {
		return true
	}
	if IsCapacityRejection(err) || errors.Is(err, ErrAttemptOverloaded) {
		return true
	}
	// The timeout of the admission is recorded as a saturation: the
	// waiting time of the request is a fact of the node, and it is the
	// only record that exists of those cases, since the request dies
	// before the function answers and nothing else reaches the arbiter.
	// Without this branch the whole attempt would go to the invocation
	// log and the node would have no sample at all, invisible instead
	// of saturated.
	if errors.Is(err, ErrAttemptAdmissionTimeout) {
		return true
	}
	if errors.Is(err, ErrAttemptUnauthorized) || errors.Is(err, ErrAttemptNotFound) || errors.Is(err, ErrAttemptFraming) {
		return false
	}
	if errors.Is(err, ErrAttemptBudgetExceeded) {
		return true
	}
	return classifyOutcome(err) == history.OutcomeTimeout
}

func classifyOutcome(err error) string {
	if err == nil {
		return history.OutcomeSucesso
	}
	// The timeout of the arbiter comes before the timeout of the
	// function: the exceeded budget wraps the error of the transport,
	// which would otherwise match the deadline error below. The order
	// anchors the root cause and not the shell.
	if errors.Is(err, ErrAttemptBudgetExceeded) {
		return history.OutcomeOrcamentoExcedido
	}
	// The rejection of a capacity comes before the branches of the
	// timeout: the overload with a pool exhausted or a full queue is the
	// node deliberately refusing what it cannot handle, and the shell of
	// the transport can contain the word timeout, so without this order
	// the fallback below would classify the refusal as a timeout, which
	// is a service failure. It has its own outcome because counting it
	// as a failure punished the saturated node in the veto by rate.
	if IsCapacityRejection(err) || errors.Is(err, ErrAttemptOverloaded) {
		return history.OutcomeRejeicaoCapacidade
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return history.OutcomeTimeout
	}
	// The timeout is anchored in the real status: the branch of the
	// handler wraps the timeout error, with no matching by text, since a
	// port number in a transport message used to produce a spurious
	// timeout.
	if errors.Is(err, ErrAttemptTimeout) {
		return history.OutcomeTimeout
	}
	// The timeout of the admission comes before the timeout of the
	// function: the request did not get an instance and the function
	// never ran. It is a saturation and not a reliability problem, with
	// the same bucket as the rejection of a capacity and its own outcome
	// so that the signal is not lost. It has to come before the
	// fallback that matches the text of the timeout below, which would
	// read the message of the sentinel as a service failure.
	if errors.Is(err, ErrAttemptAdmissionTimeout) {
		return history.OutcomeTimeoutAdmissao
	}
	type timeoutError interface {
		Timeout() bool
	}
	var te timeoutError
	if errors.As(err, &te) && te.Timeout() {
		return history.OutcomeTimeout
	}
	// The debt of the fallback that matches the text: any message
	// containing the word timeout becomes a timeout without a sentinel.
	// The trigger to narrow it is typed errors with a sentinel from end
	// to end, with the transport and the HTTP layer wrapping the timeout
	// and the exceeded budget in every path of the handler, which would
	// allow removing this fallback and the timeout error above without
	// breaking the classification matrix.
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "deadline exceeded") || strings.Contains(lower, "context canceled") || strings.Contains(lower, "timeout") {
		return history.OutcomeTimeout
	}
	return history.OutcomeFalha
}

func classifyAttempt(err error, latency time.Duration, slo *selection.SLO, criteria []selection.SLOCriterion) (string, bool) {
	outcome := classifyOutcome(err)
	if outcome != history.OutcomeSucesso {
		return outcome, false
	}
	sloMet := true
	for _, c := range criteria {
		if c == nil {
			continue
		}
		if c.Declared(slo) && !c.Attempt(slo, latency, outcome) {
			sloMet = false
			break
		}
	}
	return outcome, sloMet
}
