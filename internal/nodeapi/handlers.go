package nodeapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/registry"
)

type registerRequest struct {
	ImageRef     string `json:"image_ref"`
	MinInstances int    `json:"min_instances"`
	MaxInstances int    `json:"max_instances"`
	// QueueMax limits the bounded queue when there is no cap on the
	// number of instances, with a value from 1 to 256, while zero or
	// omitted selects the default of the pool. With a positive cap on the
	// number of instances it must be omitted.
	QueueMax    int    `json:"queue_max,omitempty"`
	TimeoutMS   int    `json:"timeout_ms"`
	IdleTimeout string `json:"idle_timeout,omitempty"`
	// Rotate asks for the regeneration of the invocation token in the
	// update. It is ignored in the creation, which always generates one.
	Rotate bool `json:"rotate,omitempty"`
	// ExpectedMetrics is the declared quality of the function. An update
	// replaces it wholesale, so omitting it clears it.
	ExpectedMetrics map[string]registry.ExpectedMetric `json:"expected_metrics,omitempty"`
	// LateReportable enables the late report: a detached execution
	// continues after the client gives up and emits a late report. It is
	// false by default and must be opted in.
	LateReportable bool `json:"late_reportable,omitempty"`
	// GPU requests the graphics device in the containers of the function.
	// It is false by default, so the function runs without a device.
	GPU bool `json:"gpu,omitempty"`
}

// registerResponse is the envelope that shows the invocation token once:
// the plaintext appears only here, in the response of a creation and in
// the response of an update that asked for the rotation. It never appears
// in a read, in the listing, in the logs or in the persistence, where only
// the hash is stored.
//
// The serialization is written explicitly, instead of embedding another
// struct, so that it does not depend on the subtle rule of flattening an
// embedded marshaler: the body is exactly the JSON of the function plus
// the invocation token key when it is not empty.
type registerResponse struct {
	Function    registry.Function
	InvokeToken string
}

func (r registerResponse) MarshalJSON() ([]byte, error) {
	type functionOut registry.Function
	type out struct {
		functionOut
		InvokeToken string `json:"invoke_token,omitempty"`
	}
	return json.Marshal(out{
		functionOut: functionOut(r.Function),
		InvokeToken: r.InvokeToken,
	})
}

// decodeJSONBody decodes the body with a hard limit of bytes. The unknown
// JSON keys are ignored, which is the default behavior of the decoder, and
// it is a deliberate choice: the API evolves by addition without breaking
// older clients, and the strict validation lives in the known fields, with
// an invalid parameter error, never in the shape of the envelope.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64, hint string) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE",
				fmt.Sprintf("corpo da requisição excede o limite de %d bytes", maxBytes))
			return false
		}
		writeError(w, http.StatusBadRequest, "INVALID_BODY", hint)
		return false
	}
	return true
}

func parseIdleTimeout(raw string) (config.Duration, bool) {
	if raw == "" {
		return 0, true
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 || d > 24*time.Hour {
		return 0, false
	}
	return config.Duration(d), true
}

func buildRegisterOptions(req registerRequest) (registry.RegisterOptions, bool) {
	opts := registry.RegisterOptions{
		MinInstances:    req.MinInstances,
		MaxInstances:    req.MaxInstances,
		QueueMax:        req.QueueMax,
		TimeoutMS:       req.TimeoutMS,
		Rotate:          req.Rotate,
		ExpectedMetrics: req.ExpectedMetrics,
		LateReportable:  req.LateReportable,
		GPU:             req.GPU,
	}
	d, ok := parseIdleTimeout(req.IdleTimeout)
	if !ok {
		return registry.RegisterOptions{}, false
	}
	opts.IdleTimeout = d
	return opts, true
}

// decodeRegisterRequest concentrates the preamble shared by the
// registration and the update handlers: the decoding with the size limit,
// the construction of the options and the validation of the idle timeout.
// It returns the request, the options and true on success, and returns
// false after having already answered the error.
//
// It does not touch the orchestration between the pool and the registry:
// the order of a partial failure and the asymmetry between marking a
// function as degraded and unregistering it live in the handlers.
//
// The extraction into a service layer was skipped on purpose. The
// asymmetry is deliberate: an update marks the function as degraded while
// a registration undoes it by unregistering. The partial order, which
// takes a snapshot and releases the lock before terminating the
// instances, unregisters before removing the function on a creation and
// removes it before ensuring and marking as degraded on an update, is
// pinned only by the tests of the final state, since there is no fake of
// an injectable order. Moving it to a service layer would risk reordering
// it without a safety net.
func (s *Server) decodeRegisterRequest(w http.ResponseWriter, r *http.Request) (registerRequest, registry.RegisterOptions, bool) {
	var req registerRequest
	if !decodeJSONBody(w, r, &req, int64(s.maxManagementBodyBytes),
		"corpo da requisição inválido: esperado JSON com image_ref, min_instances, max_instances e timeout_ms") {
		return registerRequest{}, registry.RegisterOptions{}, false
	}

	opts, ok := buildRegisterOptions(req)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_IDLE_TIMEOUT", fmt.Sprintf("idle_timeout inválido: %q", req.IdleTimeout))
		return registerRequest{}, registry.RegisterOptions{}, false
	}
	return req, opts, true
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	req, opts, ok := s.decodeRegisterRequest(w, r)
	if !ok {
		return
	}

	fn, plainToken, err := s.registry.Register(s.host, name, req.ImageRef, opts)
	if err != nil {
		switch {
		case errors.Is(err, registry.ErrDuplicateFunction):
			writeError(w, http.StatusConflict, "DUPLICATE_FUNCTION", err.Error())
		case errors.Is(err, registry.ErrInvalidFunctionName):
			writeError(w, http.StatusBadRequest, "INVALID_FUNCTION_NAME", err.Error())
		case errors.Is(err, registry.ErrInvalidMinInstances), errors.Is(err, registry.ErrInvalidMaxInstances), errors.Is(err, registry.ErrInvalidScalingParams):
			writeError(w, http.StatusBadRequest, "INVALID_SCALING_PARAMS", err.Error())
		case errors.Is(err, registry.ErrInvalidTimeoutMS):
			writeError(w, http.StatusBadRequest, "INVALID_TIMEOUT_MS", err.Error())
		case errors.Is(err, registry.ErrInvalidIdleTimeout):
			writeError(w, http.StatusBadRequest, "INVALID_IDLE_TIMEOUT", err.Error())
		case errors.Is(err, registry.ErrInvalidImageRef):
			writeError(w, http.StatusBadRequest, "INVALID_IMAGE_REF", err.Error())
		case errors.Is(err, registry.ErrInvalidExpectedMetrics):
			writeError(w, http.StatusBadRequest, "INVALID_EXPECTED_METRICS", err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "REGISTER_FAILED", err.Error())
		}
		return
	}

	if err := s.pool.EnsureMinInstances(r.Context(), fn.Name, fn.ImageRef, fn.MinInstances); err != nil {
		if unregErr := s.registry.Unregister(fn.Name); unregErr != nil {
			log.Printf("nodeapi: falha adicional ao desfazer registro de %q após provisionamento malsucedido: %v", fn.Name, unregErr)
		}
		_ = s.pool.RemoveFunction(r.Context(), fn.Name)
		writeError(w, http.StatusInternalServerError, "POOL_PROVISION_FAILED",
			fmt.Sprintf("função %q não pôde ser registrada: %v", fn.Name, err))
		return
	}

	writeJSON(w, http.StatusCreated, registerResponse{Function: fn, InvokeToken: plainToken})
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	// The previous configuration is kept before any mutation: if the
	// reprovisioning of the new image fails, the old pool has already
	// been torn down and there is no automatic rollback, so the recorded
	// error must tell the operator exactly how to restore it.
	prevFn, exists := s.registry.Get(name)
	if !exists {
		writeError(w, http.StatusNotFound, "FUNCTION_NOT_FOUND", fmt.Sprintf("função %q não encontrada", name))
		return
	}

	var req, opts, ok = s.decodeRegisterRequest(w, r)
	if !ok {
		return
	}

	updatedFn, plainToken, err := s.registry.Update(s.host, name, req.ImageRef, opts, opts.Rotate)
	if err != nil {
		switch {
		case errors.Is(err, registry.ErrFunctionNotFound):
			writeError(w, http.StatusNotFound, "FUNCTION_NOT_FOUND", err.Error())
		case errors.Is(err, registry.ErrInvalidFunctionName):
			writeError(w, http.StatusBadRequest, "INVALID_FUNCTION_NAME", err.Error())
		case errors.Is(err, registry.ErrInvalidMinInstances), errors.Is(err, registry.ErrInvalidMaxInstances), errors.Is(err, registry.ErrInvalidScalingParams):
			writeError(w, http.StatusBadRequest, "INVALID_SCALING_PARAMS", err.Error())
		case errors.Is(err, registry.ErrInvalidTimeoutMS):
			writeError(w, http.StatusBadRequest, "INVALID_TIMEOUT_MS", err.Error())
		case errors.Is(err, registry.ErrInvalidIdleTimeout):
			writeError(w, http.StatusBadRequest, "INVALID_IDLE_TIMEOUT", err.Error())
		case errors.Is(err, registry.ErrInvalidImageRef):
			writeError(w, http.StatusBadRequest, "INVALID_IMAGE_REF", err.Error())
		case errors.Is(err, registry.ErrInvalidExpectedMetrics):
			writeError(w, http.StatusBadRequest, "INVALID_EXPECTED_METRICS", err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "UPDATE_FAILED", err.Error())
		}
		return
	}

	if err := s.pool.RemoveFunction(r.Context(), name); err != nil {
		log.Printf("nodeapi: falha ao derrubar instâncias antigas de %q durante update: %v", name, err)
	}

	if err := s.pool.EnsureMinInstances(r.Context(), updatedFn.Name, updatedFn.ImageRef, updatedFn.MinInstances); err != nil {
		// There is no automatic rollback: the old instances have already
		// been removed above and the new image does not come up. The
		// recorded error carries the previous configuration and the
		// restoration command, so that the operator can recover it
		// without guessing.
		errMsg := fmt.Sprintf("falha ao reprovisionar %q após atualização (imagem %q não subiu: %v; config anterior: imagem %q min_instances=%d max_instances=%d; para restaurar: PUT /function/%s com image_ref=%q)",
			name, updatedFn.ImageRef, err, prevFn.ImageRef, prevFn.MinInstances, prevFn.MaxInstances, name, prevFn.ImageRef)
		log.Printf("nodeapi: %s", errMsg)
		if markErr := s.registry.MarkDegraded(name, errMsg); markErr != nil {
			log.Printf("nodeapi: falha adicional ao marcar %q como degraded: %v", name, markErr)
		}
		writeError(w, http.StatusInternalServerError, "POOL_PROVISION_FAILED", errMsg)
		return
	}

	writeJSON(w, http.StatusOK, registerResponse{Function: updatedFn, InvokeToken: plainToken})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	// The read requires the same invocation credential, since the
	// metadata, such as the image reference and the internal errors, is
	// not public.
	if !checkInvokeAuthFormat(r) {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "credencial de invocação inválida ou ausente")
		return
	}
	name := r.PathValue("name")

	// The authentication comes before the lookup, to avoid enumeration:
	// a nonexistent function is denied with an unauthorized answer and
	// never with a not-found one, so the caller cannot tell a wrong name
	// from a wrong token. The binding can only be verified with the
	// function in hand, and without it the answer is closed.
	fn, ok := s.registry.Get(name)
	if !ok {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "credencial de invocação inválida ou ausente")
		return
	}
	if !checkInvokeAuth(r, fn) {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "credencial de invocação inválida ou ausente")
		return
	}
	writeJSON(w, http.StatusOK, fn)
}

// handleList returns every registered function. It is a management route
// that requires the credential of the node through the middleware and not
// the invocation token, since the listing exposes the image reference and
// the internal errors. The response is the array of the functions, empty
// rather than null.
func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.registry.List())
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	if _, ok := s.registry.Get(name); !ok {
		writeError(w, http.StatusNotFound, "FUNCTION_NOT_FOUND", fmt.Sprintf("função %q não encontrada", name))
		return
	}

	if err := s.pool.RemoveFunction(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, "POOL_TEARDOWN_FAILED", err.Error())
		return
	}
	if err := s.registry.Unregister(name); err != nil {
		// The pool has already been torn down above and the update of the
		// registry reverted the removal in memory when the persistence
		// failed, so the function remains registered but without its
		// minimum floor. The floor is restored before the answer, and
		// the deletion still fails with an internal error.
		if fn, ok := s.registry.Get(name); ok {
			if rerr := s.pool.EnsureMinInstances(r.Context(), name, fn.ImageRef, fn.MinInstances); rerr != nil {
				log.Printf("nodeapi: falha ao restaurar min_instances de %q após Unregister malsucedido: %v", name, rerr)
			} else {
				log.Printf("nodeapi: min_instances de %q restaurado após Unregister malsucedido", name)
			}
		} else {
			log.Printf("nodeapi: função %q ausente após Unregister malsucedido, piso mínimo não restaurado", name)
		}
		writeError(w, http.StatusInternalServerError, "UNREGISTER_FAILED", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// The pressure block appears only in the gateway timeouts in which
	// the function did not run, and it is optional so that the other
	// answers are untouched.
	//
	// The names differ on purpose from the capacity block of the
	// piggyback. There the field is named after the available instances
	// and carries the total number of live instances, occupied ones
	// included. The name is misleading, and the misleading name is
	// dangerous: in a discussion about whether the node had an instance
	// available when it gave up, the name suggests the free ones and
	// leads to reading zero where the real value is the number of live
	// containers.
	//
	// Here the name says what it is. The value is the same quantity as
	// the available instances of the piggyback. What does not exist is the
	// count of free slots, which the pool cannot answer, so it is not
	// published here, because a free slot count that the pool cannot
	// provide would be a lie.
	Pressure *admissionPressure `json:"pressure,omitempty"`
}

// admissionPressure is the snapshot of the pool at the instant the
// admission timed out. It is the only direct observation of admission
// pressure in the system, and it was on the path that discarded it: the
// snapshot of the successful path is filled only after the answer, so a
// request that never ran reported nothing.
//
// The wait of the acquisition was present in a small share of the
// recorded invocations, and the depth of the queue in an even smaller one,
// and all of them belonged to requests that were served.
type admissionPressure struct {
	// PoolAlive is the number of live instances of the function in the
	// pool. It is not the number of available ones.
	PoolAlive int `json:"pool_alive"`
	// QueueDepth is the number of waiters in the bounded queue at that
	// instant.
	QueueDepth int `json:"queue_depth"`
	// AcquireWaitMS is how long the request waited in the admission
	// before giving up. It applies to both kinds of gateway timeout, and
	// its value distinguishes them:
	//
	//   - when the admission consumes the whole deadline, the value is
	//     about the deadline and the number of live containers shows
	//     containers that are alive but not usable, which is the
	//     signature of an admission starvation;
	//   - when the instance was not ready in time, the admission was
	//     instantaneous and it was the readiness of a live instance that
	//     timed out, so the number of live containers is at least one and
	//     the wait was about zero.
	//
	// It is the same number that the successful path already publishes, and
	// comparing the two closes the diagnosis, since the successful path
	// never had this measurement for either case.
	AcquireWaitMS int64 `json:"acquire_wait_ms"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Code: code, Message: message})
}

// writeErrorWithPressure is the error answer plus the admission snapshot.
// It is used only for the gateway timeouts in which the function did not
// run: in the other ones the block is omitted entirely and the body is
// exactly what it was.
func writeErrorWithPressure(w http.ResponseWriter, status int, code, message string, p admissionPressure) {
	writeJSON(w, status, errorResponse{Code: code, Message: message, Pressure: &p})
}

// admissionPressure takes the snapshot of the pool now, at the instant the
// admission timed out. The count and the depth of the queue are reads
// under a mutex with contention, and the request has already been lost,
// so the worst case is spending microseconds on the lock in a path that
// has already cost the whole deadline.
func (s *Server) admissionPressure(fnName string, waited time.Duration) admissionPressure {
	var p admissionPressure
	p.AcquireWaitMS = waited.Milliseconds()
	if s.pool == nil {
		return p
	}
	p.PoolAlive = s.pool.Count(fnName)
	p.QueueDepth = s.pool.QueueLen(fnName)
	return p
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	data, err := json.Marshal(body)
	if err != nil {
		log.Printf("nodeapi: falha ao serializar resposta HTTP (status %d): %v", status, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"ENCODE_FAILED","message":"falha ao serializar a resposta"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
