package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/registry"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// Registration

type registerNodeRequest struct {
	NodeID             string                                        `json:"node_id"`
	Layer              string                                        `json:"layer"`
	Host               string                                        `json:"host"`
	MinInstances       int                                           `json:"min_instances"`
	PSISupported       bool                                          `json:"psi_supported"`
	Functions          []string                                      `json:"functions"`
	FunctionCategories map[string]string                             `json:"function_categories,omitempty"`
	FunctionExpected   map[string]map[string]registry.ExpectedMetric `json:"function_expected_metrics,omitempty"`
	FunctionCapacity   map[string]int                                `json:"max_instances,omitempty"`
	FunctionTimeoutMS  map[string]int                                `json:"function_timeout_ms,omitempty"`
}

func (s *Server) handleRegisterNode(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxManagementBodyBytes)
	var req registerNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErrorJSON(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "corpo da requisição excede o limite de 64 KiB", false, false)
		} else {
			writeErrorJSON(w, http.StatusBadRequest, "INVALID_NODE", "corpo da requisição inválido: esperado JSON com node_id, layer e host", false, false)
		}
		return
	}
	n := nodes.Node{
		NodeID:             req.NodeID,
		Layer:              req.Layer,
		Host:               req.Host,
		MinInstances:       req.MinInstances,
		PSISupported:       req.PSISupported,
		Functions:          req.Functions,
		FunctionCategories: req.FunctionCategories,
		FunctionExpected:   req.FunctionExpected,
		FunctionCapacity:   req.FunctionCapacity,
		FunctionTimeoutMS:  req.FunctionTimeoutMS,
	}
	// The token and the node are bound before the upsert: without a valid
	// token for this node identifier nothing is registered. The answer is a
	// uniform unauthorized, which does not distinguish a nonexistent node
	// from a wrong token.
	if !s.checkNodeToken(w, r, req.NodeID) {
		return
	}
	isNew, err := s.nodes.Upsert(n)
	if err != nil {
		if errors.Is(err, nodes.ErrTooManyNodes) {
			writeErrorJSON(w, http.StatusServiceUnavailable, "NODE_LIMIT_REACHED", err.Error(), true, false)
			return
		}
		if errors.Is(err, nodes.ErrInvalidLayer) || errors.Is(err, nodes.ErrInvalidNodeID) || errors.Is(err, nodes.ErrInvalidHost) || errors.Is(err, nodes.ErrInvalidFunctionCategories) || errors.Is(err, nodes.ErrInvalidFunctionExpected) || errors.Is(err, nodes.ErrInvalidFunctionCapacity) || errors.Is(err, nodes.ErrInvalidFunctionTimeout) {
			writeErrorJSON(w, http.StatusBadRequest, "INVALID_NODE", err.Error(), false, false)
			return
		}
		writeErrorJSON(w, http.StatusBadRequest, "INVALID_NODE", err.Error(), false, false)
		return
	}
	// Return the persisted node, with the last seen instant filled in.
	persisted, _ := s.nodes.Get(n.NodeID)
	// A re-registration inherited an exhausted occupancy for up to the time
	// to live of the capacity, and only the capacity is cleared here, since
	// the telemetry remains valid.
	if s.mon != nil {
		s.mon.DeleteCapacities(n.NodeID)
	}
	if isNew {
		writeJSON(w, http.StatusCreated, persisted)
	} else {
		writeJSON(w, http.StatusOK, persisted)
	}
}

// List

type listNodeResponse struct {
	Node            nodes.Node         `json:"node_id_embedded,omitempty"`
	Status          string             `json:"status"`
	LatestTelemetry *telemetry.Package `json:"latest_telemetry,omitempty"`
}

// HistorySummaryEntry is the observable summary of a single segment of the
// history. The count, the percentile and the recent rate are the frozen
// contract, always present, while the fields of the classes only appear when
// there is data for them:
//   - the percentiles and the mean processor usage of the solo and contended
//     classes, where the mean uses the average of the solo samples with a
//     positive average;
//   - the contention factor of the segment, which is learned only, since the
//     override of the operator is not exposed here, and which is omitted when
//     the factor is absent;
//   - the count and the mean latency of the cold class, omitted only when the
//     segment has no cold data.
//
// An empty field is omitted, which also omits a legitimate zero, and that
// behavior is accepted and documented.
type HistorySummaryEntry struct {
	Count            int     `json:"count"`
	P95MS            int64   `json:"p95_ms"`
	RecentRPS        float64 `json:"recent_rps"`
	ColdN            int     `json:"cold_count,omitempty"`
	ColdMeanMS       int64   `json:"cold_mean_ms,omitempty"`
	SoloP95MS        int64   `json:"solo_p95_ms,omitempty"`
	ContP95MS        int64   `json:"contended_p95_ms,omitempty"`
	ContentionFactor float64 `json:"contention_factor,omitempty"`
	SoloCPUPctMean   float64 `json:"solo_cpu_pct_mean,omitempty"`
	// The saturation counts the exceeded budget, the capacity rejection and
	// the admission timeout in the window. It goes separately from the count
	// and the percentile because it is pressure and neither quality nor
	// reliability: a node that only refuses has a zero count and a high
	// saturation, and that is exactly the case that used to vanish from the
	// summary.
	Saturation int `json:"saturation,omitempty"`
	// The node saturation is the subset of the saturation with the node as
	// the author, which is the capacity rejection and the admission timeout,
	// while the budget cut is a decision of the arbiter and stays out. It is
	// the signal that the absolution floor consumes, and publishing it
	// separately is what allows reading, in the node summary, why a node was
	// demoted.
	NodeSaturation int `json:"node_saturation,omitempty"`
}

type listEntry struct {
	NodeID             string                                        `json:"node_id"`
	Layer              string                                        `json:"layer"`
	Host               string                                        `json:"host"`
	MinInstances       int                                           `json:"min_instances"`
	LastSeen           time.Time                                     `json:"last_seen"`
	PSISupported       bool                                          `json:"psi_supported"`
	Functions          []string                                      `json:"functions,omitempty"`
	FunctionCategories map[string]string                             `json:"function_categories,omitempty"`
	FunctionExpected   map[string]map[string]registry.ExpectedMetric `json:"function_expected_metrics,omitempty"`
	FunctionCapacity   map[string]int                                `json:"max_instances,omitempty"`
	FunctionTimeoutMS  map[string]int                                `json:"function_timeout_ms,omitempty"`
	Status             string                                        `json:"status"`
	LatestTelemetry    *telemetry.Package                            `json:"latest_telemetry,omitempty"`
	HistorySummary     map[string]HistorySummaryEntry                `json:"history_summary,omitempty"`
	SelectionRiskStats *selection.RiskStatsSnapshot                  `json:"selection_risk_stats,omitempty"`
	SelectionRisk      []learn.ModelDump                             `json:"selection_risk,omitempty"`
}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	// Injectable clock: the derived status and the summary of the history
	// come from the same base, since the status used to read the wall clock
	// while the summary used the clock of the history.
	now := s.now()
	nodeList := s.nodes.List()
	// Dump of the selector: the global block repeated per node, the same
	// pattern as the previous statistics of the selection, and omitted when
	// there is no wiring.
	var selectionRiskStats *selection.RiskStatsSnapshot
	var selectionRisk []learn.ModelDump
	if s.selectionRiskStats != nil {
		snap := s.selectionRiskStats.Snapshot()
		selectionRiskStats = &snap
	}
	if s.selectionRiskModels != nil {
		selectionRisk = s.selectionRiskModels()
	}
	entries := make([]listEntry, 0, len(nodeList))
	for _, n := range nodeList {
		status := nodes.DerivedStatus(n, s.staleAfter, now)
		var latest *telemetry.Package
		if s.mon != nil {
			if pkg, ok := s.mon.Latest(n.NodeID); ok {
				cp := pkg
				latest = &cp
				// The threshold of the node itself, with a per-node
				// override when it exists: the status has to match what
				// the selection decides, otherwise the operator would see
				// an active node that the hard limits are dropping.
				if status == nodes.StatusAtivo && selection.Overloaded(pkg, ok, s.limits.For(n.NodeID)) {
					status = nodes.StatusSobrecarregado
				}
			}
		}
		summary := s.historySummaryForNode(n.NodeID, n.Layer)
		e := listEntry{
			NodeID:             n.NodeID,
			Layer:              n.Layer,
			Host:               n.Host,
			MinInstances:       n.MinInstances,
			LastSeen:           n.LastSeen,
			PSISupported:       n.PSISupported,
			Functions:          n.Functions,
			FunctionCategories: n.FunctionCategories,
			FunctionExpected:   n.FunctionExpected,
			FunctionCapacity:   n.FunctionCapacity,
			FunctionTimeoutMS:  n.FunctionTimeoutMS,
			Status:             status,
			LatestTelemetry:    latest,
			HistorySummary:     summary,
			SelectionRiskStats: selectionRiskStats,
			SelectionRisk:      selectionRisk,
		}
		entries = append(entries, e)
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": entries})
}

// historySummaryForNode derives the summary per segment of the node from the
// primary keys plus the dense aggregated curve of the layer, which is
// displayed verbatim for the observability of the fan-out.
func (s *Server) historySummaryForNode(nodeID, layer string) map[string]HistorySummaryEntry {
	hv := s.historyView
	if hv == nil || hv.hist == nil || hv.window <= 0 || hv.clock == nil {
		return nil
	}
	keys := append(hv.hist.KeysForNode(nodeID), hv.hist.KeysForLayer(layer)...)
	if len(keys) == 0 {
		return nil
	}
	nowClock := hv.clock()
	summary := make(map[string]HistorySummaryEntry, len(keys))
	for _, key := range keys {
		fs, ok := hv.hist.StatsFull(key, hv.window, nowClock)
		// The gate is the existence of a measurement of the node: the total
		// counts only service, so a segment with only saturation has a zero
		// total and a positive saturation and still has to appear, since it is
		// the saturated node. A segment with only cold data keeps reporting its
		// cold count and mean, and the gate is not applied to the warm
		// percentile.
		if !ok || (fs.Total == 0 && fs.Saturation == 0) {
			continue
		}
		entry := HistorySummaryEntry{
			Count:          fs.Count,
			P95MS:          fs.P95MS,
			RecentRPS:      float64(fs.Recent) / hv.window.Seconds(),
			ColdN:          fs.ColdN,
			ColdMeanMS:     fs.ColdMeanMS,
			SoloP95MS:      fs.SoloP95MS,
			ContP95MS:      fs.ContP95MS,
			SoloCPUPctMean: fs.SoloCPUPctMean,
			Saturation:     fs.Saturation,
			NodeSaturation: fs.NodeSaturation,
		}
		// The learned factor uses the same scalar as the selector, with the
		// exported minimum number of samples and never a hardcoded five here.
		// The override of the operator is an arm of the selector and does not
		// enter the summary.
		if factor, fok := selection.EffectiveContentionFactor(fs.SoloP95MS, fs.ContP95MS, fs.SoloN, fs.ContN, selection.MinClassSamples); fok {
			entry.ContentionFactor = factor
		}
		summary[key] = entry
	}
	return summary
}

// Delete

func (s *Server) handleDeleteNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// The token of the node is revoked before the record is removed, which is
	// fail-closed: if the persistence of the revocation fails, the node stays
	// and the operator can repeat, and a valid token never survives a removed
	// node. The revocation is idempotent, so a legacy node without a token
	// does not change its behavior, and a nil store preserves the exclusive
	// path of the older tests.
	if s.tokens != nil {
		if err := s.tokens.Revoke(id); err != nil {
			writeErrorJSON(w, http.StatusInternalServerError, "REVOKE_FAILED", err.Error(), false, false)
			return
		}
	}
	if err := s.nodes.Delete(id); err != nil {
		if errors.Is(err, nodes.ErrNodeNotFound) {
			writeErrorJSON(w, http.StatusNotFound, "NODE_NOT_FOUND", err.Error(), false, false)
			return
		}
		writeErrorJSON(w, http.StatusInternalServerError, "DELETE_FAILED", err.Error(), false, false)
		return
	}
	s.mon.Delete(id)
	w.WriteHeader(http.StatusNoContent)
}

// Telemetry

func (s *Server) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	r.Body = http.MaxBytesReader(w, r.Body, maxManagementBodyBytes)
	var pkg telemetry.Package
	if err := json.NewDecoder(r.Body).Decode(&pkg); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErrorJSON(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "corpo da requisição excede o limite de 64 KiB", false, false)
		} else {
			writeErrorJSON(w, http.StatusBadRequest, "INVALID_REQUEST", "corpo da requisição inválido: esperado TelemetryPackage JSON", false, false)
		}
		return
	}
	if pkg.NodeID != id {
		writeErrorJSON(w, http.StatusBadRequest, "TELEMETRY_MISMATCH", "node_id do pacote diverge do id da URL", false, false)
		return
	}
	// The token and the node are bound before the touch, since without a
	// valid token the liveness is not updated.
	if !s.checkNodeToken(w, r, id) {
		return
	}
	if err := s.nodes.Touch(pkg.NodeID); err != nil {
		if errors.Is(err, nodes.ErrNodeNotFound) {
			writeErrorJSON(w, http.StatusNotFound, "NODE_NOT_FOUND", err.Error(), false, false)
			return
		}
		writeErrorJSON(w, http.StatusInternalServerError, "TOUCH_FAILED", err.Error(), false, false)
		return
	}
	s.mon.UpdateAt(pkg, s.now())
	// Periodic ingestion of the capacity: each entry present in the map is
	// merged with the instant of reception in the arbiter, under a single
	// clock. A present key is fresh data and an absent key is unknown, with
	// nothing to merge. The timestamp of the package from the node is
	// ignored for the ordering, since it is only informative, and the merge
	// in the update orders only by the instant of reception and never goes
	// back. Each entry passes through a sanitization for the node, where an
	// unknown function is discarded, a negative value is clamped and the
	// availability is limited to the registered ceiling, and the expired
	// entries are swept at every ingestion.
	if len(pkg.Capacity) > 0 {
		recvNow := s.now()
		node, _ := s.nodes.Get(pkg.NodeID)
		for fn, cap := range pkg.Capacity {
			if clean, ok := monitor.SanitizeForNode(node.Functions, node.FunctionCapacity, fn, cap); ok {
				s.mon.UpdateCapacity(pkg.NodeID, fn, clean, recvNow)
			}
		}
		s.mon.SweepExpired(recvNow)
	}
	w.WriteHeader(http.StatusNoContent)
}
