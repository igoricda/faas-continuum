package nodeapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

type invokeRequest struct {
	Input json.RawMessage `json:"input"`
}

type ResourceProfile struct {
	CPUPctAvg    float64 `json:"cpu_pct_avg"`
	CPUPctPeak   float64 `json:"cpu_pct_peak"`
	MemPeakBytes uint64  `json:"mem_peak_bytes"`
	SampleCount  int     `json:"sample_count"`
}

type ExecutionPhases struct {
	AcquireWaitMS int64 `json:"acquire_wait_ms"`
	ReadinessMS   int64 `json:"readiness_ms"`
	FunctionMS    int64 `json:"function_ms"`
	OverheadMS    int64 `json:"overhead_ms"`
}

type invokeResponse struct {
	Result           json.RawMessage `json:"result"`
	ExecutionMetrics struct {
		ResponseTimeMS  int64              `json:"response_time_ms"`
		StartedAt       time.Time          `json:"started_at"`
		EndedAt         time.Time          `json:"ended_at"`
		InstanceID      string             `json:"instance_id"`
		ColdStart       bool               `json:"cold_start"`
		Phases          ExecutionPhases    `json:"phases"`
		ResourceProfile *ResourceProfile   `json:"resource_profile,omitempty"`
		Telemetry       *telemetry.Package `json:"telemetry,omitempty"`
	} `json:"execution_metrics"`
}

func (s *Server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	// The cheap authentication is mandatory and comes before any work: the
	// format of the token is rejected before the lookup, to avoid
	// enumeration, the binding is rejected before the decoding, and the
	// decoding uses the size limit of the invocation and not the one of
	// the management operations.
	if !checkInvokeAuthFormat(r) {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "credencial de invocação inválida ou ausente")
		return
	}
	name := r.PathValue("name")

	// The authentication comes before the lookup, to avoid enumeration:
	// without the function there is no way to verify the binding of the
	// token, so a nonexistent function is denied with an unauthorized
	// answer and never with a not-found one.
	fn, ok := s.registry.Get(name)
	if !ok {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "credencial de invocação inválida ou ausente")
		return
	}
	if !checkInvokeAuth(r, fn) {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "credencial de invocação inválida ou ausente")
		return
	}

	var req invokeRequest
	if !decodeJSONBody(w, r, &req, int64(s.maxInvokeBodyBytes),
		"corpo da requisição inválido: esperado JSON com input") {
		return
	}

	timeout := s.resolveTimeout(fn)
	var ctx context.Context
	var cancel context.CancelFunc
	var attemptID string
	attemptN := 1
	detached := false
	if fn.LateReportable {
		attemptID = r.Header.Get("X-Faas-Attempt-ID")
		if raw := r.Header.Get("X-Faas-Attempt-N"); raw != "" {
			if n, err := strconv.Atoi(raw); err == nil && n >= 1 {
				attemptN = n
			}
		}
		// Admission of the orphans: the request detaches with an active
		// sender and a room in the late queue, and a full queue is the
		// only shedding point. The shedding based on the depth of the
		// queue of the pool was removed, because it discarded the
		// learning exactly under load, which is when the learning
		// matters most.
		if s.detachAllowed() {
			detached = true
			if s.lateCap > 0 && timeout > s.lateCap {
				timeout = s.lateCap
			}
			ctx, cancel = context.WithTimeout(context.WithoutCancel(r.Context()), timeout)
		} else if s.lateQueue != nil {
			log.Printf("nodeapi: detached shed fn=%s (opt-in sem admissão: fila late cheia)", fn.Name)
		}
	}
	if !detached {
		ctx, cancel = context.WithTimeout(r.Context(), timeout)
	}
	defer cancel()

	startedAt := time.Now().UTC()
	// The wait of the acquisition measures the pure queue plus the
	// blocking and coalesced provisioning, so it is not a pure queue
	// measure. The name of the field says exactly that.
	acquireStart := time.Now()
	instanceID, isCold, err := s.pool.AcquireWaitCold(ctx, fn.Name, fn.ImageRef, fn.MaxInstances)
	acquireDuration := time.Since(acquireStart)
	if err != nil {
		if errors.Is(err, pool.ErrPoolExhausted) {
			writeError(w, http.StatusServiceUnavailable, "POOL_EXHAUSTED", err.Error())
			return
		}
		if errors.Is(err, pool.ErrQueueFull) {
			writeError(w, http.StatusServiceUnavailable, "QUEUE_FULL", err.Error())
			return
		}
		// A timeout during the provisioning is also a gateway timeout: the
		// error of the provisioner does not always wrap the deadline
		// error, so the cause is classified by the own clock of the
		// node. When the deadline of the node expired it was a timeout
		// and not an internal failure. Only a deadline error becomes a
		// gateway timeout, while a cancellation, which means that the
		// client gave up, stays an internal failure.
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			// The error is an acquisition timeout and not an invocation
			// timeout: the request expired while waiting for the
			// admission, through the queue and the coalesced
			// provisioning, and the function never ran. It is not the
			// same as a function that ran and did not answer, which is
			// the invocation timeout of the call below. The arbiter has
			// to tell the two apart, because only the second one is a
			// service failure.
			//
			// This distinction was visible in the measurements: the fog
			// node emitted an invocation timeout through this path, with
			// the latency stuck at the timeout of the node itself and
			// the host at a low load with many live containers. The
			// arbiter saw a function timeout and vetoed the fastest node
			// of the system, with a large share of the requests counted
			// as service failures, which triggered the veto. The
			// ambiguity was in the code and not in the data: the node
			// knew the stage and emitted the same code for the three.
			//
			// The admission snapshot goes in the body because this is
			// the only point of the node where the admission pressure is
			// directly observable, and it was exactly here that it was
			// discarded: the successful path fills the wait of the
			// acquisition and the depth of the queue only after the
			// answer, so a request that never ran reported nothing.
			writeErrorWithPressure(w, http.StatusGatewayTimeout, "ACQUIRE_TIMEOUT", err.Error(),
				s.admissionPressure(fn.Name, acquireDuration))
			return
		}
		writeError(w, http.StatusInternalServerError, "ACQUIRE_FAILED", err.Error())
		return
	}
	queueDepth := s.pool.QueueLen(fn.Name)
	// The coalesced provisioning is global to the pool, over every
	// function, and not a property of this one: the function tag gives
	// only the context of the invocation and not the origin of the
	// coalescing, so a coalescing of another function must not be
	// attributed to this one when reading the log.
	coalesced := s.pool.CoalescedCount()
	log.Printf("nodeapi: handleInvoke fn=%s instanceID=%s queue_depth=%d provision_coalesced=%d", fn.Name, instanceID, queueDepth, coalesced)
	defer s.pool.Release(fn.Name, instanceID)

	prof := s.startProfiler(r, instanceID)
	if prof != nil {
		defer prof.cancel()
	}

	readinessStart := time.Now()
	readinessAttempts, rerr := s.waitReady(ctx, instanceID)
	readinessDuration := time.Since(readinessStart)
	log.Printf("nodeapi: handleInvoke fn=%s instanceID=%s readiness_attempts=%d readiness_duration_ms=%d", fn.Name, instanceID, readinessAttempts, readinessDuration.Milliseconds())
	if rerr != nil {
		// It ends the sampler, which is idempotent, before answering
		// the error.
		if prof != nil {
			prof.stop()
		}
		if errors.Is(rerr, context.DeadlineExceeded) {
			// The instance was not ready in time: the function did not run
			// here either, since the container came up and never became
			// ready. The arbiter treats it like an acquisition timeout,
			// as a saturation of the admission and not a service failure,
			// because what timed out was the preparation of the
			// instance and not the execution. The admission snapshot at
			// this instant is what separates a slow admission from an
			// impossible one, which are two defects with a different
			// fix.
			writeErrorWithPressure(w, http.StatusGatewayTimeout, "INSTANCE_NOT_READY_TIMEOUT",
				fmt.Sprintf("instância não respondeu em %s", timeout),
				s.admissionPressure(fn.Name, acquireDuration))
			return
		}
		writeError(w, http.StatusBadGateway, "WAIT_READY_FAILED", rerr.Error())
		return
	}

	funcStart := time.Now()
	result, err := s.callInstance(ctx, instanceID, req.Input)
	// The final sample and the closing of the tracker, which is
	// idempotent.
	if prof != nil {
		prof.stop()
	}

	functionTime := time.Since(funcStart)

	reportLate := func(outcome string, latency time.Duration) {
		if !detached || attemptID == "" {
			return
		}
		s.enqueueLateReport(lateReportMsg{
			RequestID:         attemptID,
			AttemptN:          attemptN,
			Function:          fn.Name,
			NodeID:            s.nodeID,
			Outcome:           outcome,
			ObservedLatencyMS: latency.Milliseconds(),
			StartedAt:         startedAt.Format(time.RFC3339Nano),
			ColdStart:         isCold,
		})
	}

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			// The invocation timeout is the only timeout of the service: the
			// function was running and did not answer in time. It is the
			// only one of the three stages in which the arbiter can blame
			// the reliability of the node, since in the other two the
			// function never ran.
			reportLate("timeout", time.Since(startedAt))
			writeError(w, http.StatusGatewayTimeout, "INVOCATION_TIMEOUT", fmt.Sprintf("instância não respondeu em %s", timeout))
			return
		}
		if errors.Is(err, errInvalidFunctionOutput) {
			reportLate("falha", time.Since(startedAt))
			writeError(w, http.StatusBadGateway, "INVALID_FUNCTION_OUTPUT", err.Error())
			return
		}
		reportLate("falha", time.Since(startedAt))
		writeError(w, http.StatusBadGateway, "INVOCATION_FAILED", err.Error())
		return
	}

	endedAt := time.Now().UTC()
	responseTime := endedAt.Sub(startedAt)
	acquireWaitMs := acquireDuration.Milliseconds()
	readinessMs := readinessDuration.Milliseconds()
	functionMs := functionTime.Milliseconds()
	overheadMs := responseTime.Milliseconds() - acquireWaitMs - readinessMs - functionMs
	if overheadMs < 0 {
		overheadMs = 0
	}
	// The cold start is the signal of the pool, which provisioned during
	// this call, and not a heuristic of the duration: a warm instance with
	// a readiness of a few milliseconds is not cold.
	coldStart := isCold

	resp := invokeResponse{Result: result}
	resp.ExecutionMetrics.ResponseTimeMS = responseTime.Milliseconds()
	resp.ExecutionMetrics.StartedAt = startedAt
	resp.ExecutionMetrics.EndedAt = endedAt
	resp.ExecutionMetrics.InstanceID = instanceID
	resp.ExecutionMetrics.ColdStart = coldStart
	resp.ExecutionMetrics.Phases = ExecutionPhases{
		AcquireWaitMS: acquireWaitMs,
		ReadinessMS:   readinessMs,
		FunctionMS:    functionMs,
		OverheadMS:    overheadMs,
	}
	log.Printf("nodeapi: handleInvoke fn=%s instanceID=%s cold_start=%v acquire_wait_ms=%d readiness_ms=%d function_ms=%d overhead_ms=%d response_time_ms=%d", fn.Name, instanceID, coldStart, acquireWaitMs, readinessMs, functionMs, overheadMs, responseTime.Milliseconds())
	if prof != nil && prof.tracker != nil {
		if profile, ok := prof.tracker.Profile(); ok {
			resp.ExecutionMetrics.ResourceProfile = &ResourceProfile{
				CPUPctAvg:    profile.CPUPctAvg,
				CPUPctPeak:   profile.CPUPctPeak,
				MemPeakBytes: profile.MemPeakBytes,
				SampleCount:  profile.SampleCount,
			}
		}
	}
	if s.telemetryActive() {
		if pkg, terr := s.collector.Collect(telemetry.SourcePiggyback); terr != nil {
			log.Printf("nodeapi: telemetria piggyback indisponível nesta resposta: %v", terr)
		} else {
			pkg.Source = ""
			// The piggyback carries only the invoked function, which keeps
			// the coupling minimal: it is filled here, after the
			// collection, with no link between the collector and the
			// pool. The count and the depth of the queue never fail nor
			// block the invocation, so the fields are filled even under
			// pressure. A failure of the collection above omits the whole
			// block, as before.
			if s.pool != nil {
				pkg.Capacity = map[string]telemetry.FunctionCapacity{
					fn.Name: {
						MaxInstances: fn.MaxInstances,
						// The total number of live instances, not the
						// free ones, with the same semantics as the
						// periodic snapshot of the server.
						InstancesAvailable: s.pool.Count(fn.Name),
						QueueDepth:         s.pool.QueueLen(fn.Name),
					},
				}
			}
			resp.ExecutionMetrics.Telemetry = &pkg
		}
	}
	reportLate("sucesso", responseTime)
	writeJSON(w, http.StatusOK, resp)
}
