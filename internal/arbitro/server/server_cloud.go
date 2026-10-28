package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
)

const cloudStoreNotInitializedMessage = "cloud store não inicializado"

// Cloud fallback, backed by the store and the forwarding of the cloud package.

func (s *Server) handleCloudPut(w http.ResponseWriter, r *http.Request) {
	if s.cloudStore == nil {
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR", cloudStoreNotInitializedMessage, false, false)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxManagementBodyBytes)
	var cfg cloud.CloudFallbackConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErrorJSON(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "corpo da requisição excede o limite de 64 KiB", false, false)
		} else {
			writeErrorJSON(w, http.StatusBadRequest, "INVALID_CLOUD_CONFIG", "corpo da requisição inválido: esperado CloudFallbackConfig JSON", false, false)
		}
		return
	}
	if err := s.cloudStore.Set(cfg); err != nil {
		if errors.Is(err, cloud.ErrInvalidCloudConfig) {
			writeErrorJSON(w, http.StatusBadRequest, "INVALID_CLOUD_CONFIG", err.Error(), false, false)
			return
		}
		writeErrorJSON(w, http.StatusInternalServerError, "PERSIST_FAILED", err.Error(), false, false)
		return
	}
	saved, _ := s.cloudStore.Get()
	writeJSON(w, http.StatusOK, saved)
}

func (s *Server) handleCloudGet(w http.ResponseWriter, r *http.Request) {
	if s.cloudStore == nil {
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR", cloudStoreNotInitializedMessage, false, false)
		return
	}
	cfg, ok := s.cloudStore.Get()
	if !ok {
		writeErrorJSON(w, http.StatusNotFound, "CLOUD_CONFIG_NOT_FOUND", "configuração de fallback não encontrada", false, false)
		return
	}
	// Security: the route is authenticated, and the token is intentionally
	// returned so that the operator can audit or confirm the complete
	// configuration. The token is never logged.
	writeJSON(w, http.StatusOK, cfg)
}

func (s *Server) handleCloudDelete(w http.ResponseWriter, r *http.Request) {
	if s.cloudStore == nil {
		writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR", cloudStoreNotInitializedMessage, false, false)
		return
	}
	if err := s.cloudStore.Clear(); err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, "DELETE_FAILED", err.Error(), false, false)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
