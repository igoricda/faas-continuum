// Package server implements the HTTP server of the arbiter: it exposes the
// node manager, the monitor, the gateway and the management routes.
package server

import (
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/cattokens"
	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/auth"
)

const maxManagementBodyBytes = 64 * 1024 // 64 KiB — teto para /nodes* (PROJECT §3:2)
const contentTypeHeader = "Content-Type"
const applicationJSON = "application/json"

// Server exposes the node manager, the monitor and the gateway over HTTP.
//
// The rate limit is per IP and per class: the invocation path uses 600 requests
// per minute as the hot path, the node routes use 600 for the registration and
// the telemetry, the management uses 300 and the late reports use 600 in a
// dedicated limiter, since the late report is best-effort and bursts with
// retries and sharing the limiter of the nodes would drop telemetry and mark
// the fleet unavailable in a cascade.
type Server struct {
	nodes         *nodes.NodeStore
	mon           *monitor.Monitor
	gw            *gateway.Gateway
	cred          auth.Credential
	staleAfter    time.Duration
	limits        selection.LimitsConfig
	cloudStore    *cloud.CloudStore
	invokeLimiter *auth.RateLimiter
	nodeLimiter   *auth.RateLimiter
	mgmtLimiter   *auth.RateLimiter
	lateLimiter   *auth.RateLimiter
	tokens        *nodes.NodeTokenStore
	// The category tokens translate a category token into the tokens of the
	// nodes at the dispatch, and nil preserves the legacy verbatim path.
	catTokens   *cattokens.Store
	historyView *historyView
	// The risk statistics and the risk models are the observability of the
	// canonical selector, and nil omits both blocks from the node query. The
	// production arbiter always wires them, since the risk selector is the only
	// selector. The statistics are consumed through the port, not through the
	// concrete type, so replacing the selector does not require changing the
	// server.
	selectionRiskStats  selection.StatsSnapshooter
	selectionRiskModels func() []learn.ModelDump
	// The clock is injectable for the derived status and the telemetry, with the
	// current time in production and a deterministic fake in the tests.
	clock func() time.Time
}

type historyView struct {
	hist   *history.History
	window time.Duration
	clock  func() time.Time
}

// NewServerWithHistory creates a server with an observable history for the node
// query. The history may be nil, which omits the summary, and a positive
// window enables the recent rate. A nil clock means the current time, and it is
// injectable for deterministic tests. The normalization happens here, since a
// nil history or a non-positive window gives no history view.
func NewServerWithHistory(nodes *nodes.NodeStore, mon *monitor.Monitor, gw *gateway.Gateway, cred auth.Credential, staleAfter time.Duration, limits selection.LimitsConfig, cloudStore *cloud.CloudStore, hist *history.History, throughputWindow time.Duration, clock func() time.Time) *Server {
	if clock == nil {
		clock = time.Now
	}
	var hv *historyView
	if hist != nil && throughputWindow > 0 {
		hv = &historyView{hist: hist, window: throughputWindow, clock: clock}
	}
	return &Server{
		nodes:         nodes,
		mon:           mon,
		gw:            gw,
		cred:          cred,
		staleAfter:    staleAfter,
		limits:        limits,
		cloudStore:    cloudStore,
		invokeLimiter: auth.NewRateLimiter(600, time.Minute),
		nodeLimiter:   auth.NewRateLimiter(600, time.Minute),
		mgmtLimiter:   auth.NewRateLimiter(300, time.Minute),
		lateLimiter:   auth.NewRateLimiter(600, time.Minute),
		historyView:   hv,
		clock:         clock,
	}
}

// SetNodeTokens binds the store of tokens per node to the server and returns
// the server itself for chaining. The production binary always configures it
// and aborts without it, while a nil value preserves the legacy behavior
// without a token check, which is the exclusive path of the older tests and
// never of production.
func (s *Server) SetNodeTokens(t *nodes.NodeTokenStore) *Server {
	s.tokens = t
	return s
}

// SetCatTokens binds the store of tokens per category to the server, in the
// same pattern as the node tokens, and returns the server itself for chaining.
// A nil value preserves the legacy behavior verbatim.
func (s *Server) SetCatTokens(t *cattokens.Store) *Server {
	s.catTokens = t
	return s
}

// SetSelectionRisk binds the instrumentation of the canonical selector to the
// server. The models function returns the dump per pair of group and node, and
// nil gives only the statistics. Without any wiring both blocks are omitted
// from the node query. The statistics are consumed through the port, so a
// future selector wires its own instrumentation without changing the server.
func (s *Server) SetSelectionRisk(st selection.StatsSnapshooter, modelsFn func() []learn.ModelDump) *Server {
	s.selectionRiskStats = st
	s.selectionRiskModels = modelsFn
	return s
}

// Handler returns the mux with the exact routes of the server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// The node routes, with the self-registration and the telemetry, are
	// authenticated by the node token inside the handlers, which binds the token
	// to the node identifier, and not by the administrative credential, since
	// the agent only carries its own token. The node limiter preserves the
	// denial-of-service ceiling that the authentication gave before, of 600
	// requests per minute per IP, shared between the registration and the
	// telemetry, while the late report has its own limiter.
	mux.Handle("POST /nodes", s.nodeRateLimit(http.HandlerFunc(s.handleRegisterNode)))
	mux.Handle("GET /nodes", auth.RequireAuthWithLimiter(s.cred, s.mgmtLimiter, http.HandlerFunc(s.handleListNodes)))
	mux.Handle("DELETE /nodes/{id}", auth.RequireAuthWithLimiter(s.cred, s.mgmtLimiter, http.HandlerFunc(s.handleDeleteNode)))
	mux.Handle("POST /nodes/{id}/telemetry", s.nodeRateLimit(http.HandlerFunc(s.handleTelemetry)))
	mux.Handle("POST /nodes/{id}/token", auth.RequireAuthWithLimiter(s.cred, s.mgmtLimiter, http.HandlerFunc(s.handleIssueNodeToken)))
	mux.Handle("POST /category-tokens/{category}", auth.RequireAuthWithLimiter(s.cred, s.mgmtLimiter, http.HandlerFunc(s.handleIssueCategoryToken)))
	mux.Handle("PUT /category-tokens/{category}/downstream", auth.RequireAuthWithLimiter(s.cred, s.mgmtLimiter, http.HandlerFunc(s.handleCategoryDownstreamPut)))
	mux.Handle("GET /category-tokens", auth.RequireAuthWithLimiter(s.cred, s.mgmtLimiter, http.HandlerFunc(s.handleCategoryTokensGet)))
	mux.Handle("PUT /cloud-fallback", auth.RequireAuthWithLimiter(s.cred, s.mgmtLimiter, http.HandlerFunc(s.handleCloudPut)))
	mux.Handle("GET /cloud-fallback", auth.RequireAuthWithLimiter(s.cred, s.mgmtLimiter, http.HandlerFunc(s.handleCloudGet)))
	mux.Handle("DELETE /cloud-fallback", auth.RequireAuthWithLimiter(s.cred, s.mgmtLimiter, http.HandlerFunc(s.handleCloudDelete)))
	mux.Handle("POST /invoke", s.invokeRateLimit(http.HandlerFunc(s.gw.Handle)))
	// The late report is best-effort and has a dedicated limiter: behind the
	// same address translation as the registration and the telemetry, a burst of
	// retries of the sender cannot exhaust the quota of the telemetry, which
	// would age the last seen instant and make the whole fleet unavailable in a
	// cascade.
	mux.Handle("POST /internal/late-report", s.lateRateLimit(http.HandlerFunc(s.handleLateReport)))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

// Rate limit of the invocation path.

func (s *Server) invokeRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := ipFromRemoteAddr(r.RemoteAddr)
		if !s.invokeLimiter.Allow(ip) {
			writeErrorJSON(w, http.StatusTooManyRequests, "TOO_MANY_REQUESTS", "muitas tentativas", true, false)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// nodeRateLimit protects the registration and the telemetry of the nodes, with
// 600 requests per minute per IP shared between the two routes. The late
// report has its own limiter and never this one.
func (s *Server) nodeRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := ipFromRemoteAddr(r.RemoteAddr)
		if !s.nodeLimiter.Allow(ip) {
			writeErrorJSON(w, http.StatusTooManyRequests, "TOO_MANY_REQUESTS", "muitas tentativas", true, false)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// lateRateLimit protects the best-effort late report with an isolated quota of
// 600 requests per minute per IP, since a burst of retries of the sender shares
// the address translation with the registration and the telemetry and must not
// contaminate the quota that keeps the last seen instant.
func (s *Server) lateRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := ipFromRemoteAddr(r.RemoteAddr)
		if s.lateLimiter == nil {
			next.ServeHTTP(w, r)
			return
		}
		if !s.lateLimiter.Allow(ip) {
			writeErrorJSON(w, http.StatusTooManyRequests, "TOO_MANY_REQUESTS", "muitas tentativas", true, false)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// now returns the injectable clock of the server, and nil, from a manual
// construction in the legacy tests, means the current time.
func (s *Server) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

func ipFromRemoteAddr(remoteAddr string) string {
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return ip
}

// Local helpers, which serialize before writing the status and set the JSON content type.

func writeJSON(w http.ResponseWriter, status int, body any) {
	data, err := json.Marshal(body)
	if err != nil {
		w.Header().Set(contentTypeHeader, applicationJSON)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"ENCODE_FAILED","message":"falha ao serializar resposta","retryable":false,"fallback_applied":false}`))
		return
	}
	w.Header().Set(contentTypeHeader, applicationJSON)
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeErrorJSON(w http.ResponseWriter, status int, code, message string, retryable, fallback bool) {
	resp := gateway.ErrorResponse{
		Code:            code,
		Message:         message,
		Retryable:       retryable,
		FallbackApplied: fallback,
	}
	data, err := json.Marshal(resp)
	if err != nil {
		w.Header().Set(contentTypeHeader, applicationJSON)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"ENCODE_FAILED","message":"falha ao serializar resposta","retryable":false,"fallback_applied":false}`))
		return
	}
	w.Header().Set(contentTypeHeader, applicationJSON)
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
