package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
)

// Tokens per node.

// checkNodeToken validates the node token for the node identifier from the
// authorization header. It returns false and answers a uniform unauthorized
// on any failure, without distinguishing a nonexistent node from a wrong
// token, so that the identifiers cannot be enumerated. An unconfigured store
// preserves the legacy behavior without a check, which is the exclusive path
// of the older tests, since the production binary always configures it and
// aborts without it.
func (s *Server) checkNodeToken(w http.ResponseWriter, r *http.Request, nodeID string) bool {
	if s.tokens == nil {
		return true
	}
	if nodeID == "" || !s.tokens.Verify(nodeID, r.Header.Get("Authorization")) {
		writeErrorJSON(w, http.StatusUnauthorized, "UNAUTHORIZED", "credencial de nó inválida ou ausente", false, false)
		return false
	}
	return true
}

// handleIssueNodeToken issues the token of a node, on an operator route
// authenticated by the administrative credential: the post to the node route
// answers with the token, shown once, and the plaintext is never logged nor
// persisted. It does not require a previous entry in the store, which is how
// the bootstrap works: the operator issues the token, distributes it, and the
// agent registers.
func (s *Server) handleIssueNodeToken(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeErrorJSON(w, http.StatusNotFound, "NODE_NOT_FOUND", "node_id não pode ser vazio", false, false)
		return
	}
	if s.tokens == nil {
		writeErrorJSON(w, http.StatusInternalServerError, "TOKENS_NOT_CONFIGURED", "store de tokens por nó não configurado", false, false)
		return
	}
	plain, err := s.tokens.Issue(id)
	if err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, "TOKEN_ISSUE_FAILED", err.Error(), false, false)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"token": plain})
}

// Late reports.

// handleLateReport receives the late report of the node. It is authenticated
// by the node token, the same mechanism as the telemetry. The deduplication
// is by the pair of the request identifier and the attempt number. The body
// is limited, with the same ceiling as the management routes. The answers
// distinguish the outcome: 202 means applied to the history, 208 means
// already reported and has no effect, and 503 means the gateway is not
// configured in this server, which only happens in tests without one.
func (s *Server) handleLateReport(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxManagementBodyBytes)
	var rep gateway.LateReport
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErrorJSON(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "corpo da requisição excede o limite de 64 KiB", false, false)
		} else {
			writeErrorJSON(w, http.StatusBadRequest, "INVALID_LATE_REPORT", "corpo da requisição inválido: esperado LateReport JSON", false, false)
		}
		return
	}
	if err := gateway.ValidateLateReport(&rep); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_LATE_REPORT", err.Error(), false, false)
		return
	}
	if !s.checkNodeToken(w, r, rep.NodeID) {
		return
	}
	if s.gw == nil {
		writeErrorJSON(w, http.StatusServiceUnavailable, "LATE_NO_GATEWAY", "gateway de histórico não configurado", false, false)
		return
	}
	applied, err := s.gw.RecordLate(rep)
	if err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, "LATE_RECORD_FAILED", err.Error(), false, false)
		return
	}
	if !applied {
		w.WriteHeader(http.StatusAlreadyReported)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
