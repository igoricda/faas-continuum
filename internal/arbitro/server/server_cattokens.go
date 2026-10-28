package server

import (
	"encoding/json"
	"net/http"
)

// Tokens per category, with show-once emission and a downstream map for the
// translation at the dispatch. The operator routes are authenticated by the
// administrative credential, with the same management limiter as the others.

func (s *Server) catStoreOr501(w http.ResponseWriter) bool {
	if s.catTokens == nil {
		writeErrorJSON(w, http.StatusNotImplemented, "CATEGORY_TOKENS_NOT_CONFIGURED", "store de tokens por categoria não configurado (flag -category-tokens)", false, false)
		return false
	}
	return true
}

// handleIssueCategoryToken issues the token of a category: a post to the
// category route answers with the token, shown once, and reissuing it revokes
// the previous one.
func (s *Server) handleIssueCategoryToken(w http.ResponseWriter, r *http.Request) {
	if !s.catStoreOr501(w) {
		return
	}
	cat := r.PathValue("category")
	plain, err := s.catTokens.Issue(cat)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_CATEGORY", err.Error(), false, false)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"token": plain})
}

type downstreamRequest struct {
	NodeID   string `json:"node_id"`
	Concrete string `json:"concrete"`
	Token    string `json:"token"`
}

// handleCategoryDownstreamPut records the token that the node expects in the
// concrete variant: a put to the downstream route of the category carries the
// node identifier, the concrete variant and the token, and an empty token
// removes the entry.
func (s *Server) handleCategoryDownstreamPut(w http.ResponseWriter, r *http.Request) {
	if !s.catStoreOr501(w) {
		return
	}
	cat := r.PathValue("category")
	r.Body = http.MaxBytesReader(w, r.Body, maxManagementBodyBytes)
	var req downstreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_BODY", "corpo inválido: esperado JSON com node_id, concrete e token", false, false)
		return
	}
	if err := s.catTokens.SetDownstream(cat, req.NodeID, req.Concrete, req.Token); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_DOWNSTREAM", err.Error(), false, false)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"category": cat, "node_id": req.NodeID, "concrete": req.Concrete, "mapped": req.Token != ""})
}

// handleCategoryTokensGet lists the issued categories and the mapped pairs of
// node and concrete variant, without any plaintext.
func (s *Server) handleCategoryTokensGet(w http.ResponseWriter, r *http.Request) {
	if !s.catStoreOr501(w) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"categories": s.catTokens.Snapshot()})
}
