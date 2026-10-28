// Package nodeapi exposes the HTTP surface of a node: the management,
// the read and the invocation of the functions in the standalone mode.
package nodeapi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/igoricda/faas-continuum/internal/auth"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// InstanceAddresser resolves the path of the Unix socket of the
// invocation and the directory that the readiness watcher must observe,
// in a fail-closed way: an error never falls back to the root or to a
// temporary directory.
type InstanceAddresser interface {
	SocketPath(instanceID string) string
	SocketWatchDir(instanceID string) (string, error)
}

// InstanceMetricsSampler samples the resource counters of an instance.
type InstanceMetricsSampler interface {
	SampleMetrics(ctx context.Context, instanceID string) (telemetry.InvocationSample, error)
}

// defaultInvokeBodyBytes is the ceiling of the body of an invocation when
// it is not configured: eight megabytes, the same as the gateway of the
// arbiter, so that both paths of an invocation accept the same payload.
const defaultInvokeBodyBytes = 8 << 20

// Server groups the dependencies needed to serve the HTTP API of the node.
type Server struct {
	host           string
	registry       *registry.Registry
	pool           *pool.Manager
	addresser      InstanceAddresser
	credential     auth.Credential
	defaultTimeout time.Duration
	collector      *telemetry.Collector
	metricsSampler InstanceMetricsSampler
	readiness      ReadinessChecker

	maxManagementBodyBytes   int
	maxFunctionOutputBytes   int
	maxInvokeBodyBytes       int
	readHeaderTimeout        time.Duration
	idleTimeout              time.Duration
	invocationSampleInterval time.Duration

	invokeTransport *http.Transport

	// mgmtLimiter is the rate limiter shared by the management routes,
	// with a per-server semantics. It is created once in the constructor
	// and injected in the authentication middleware.
	mgmtLimiter *auth.RateLimiter

	// The sender of the late reports.
	lateQueue   chan lateReportMsg
	lateDropped int64
	lateURL     string
	lateToken   string
	nodeID      string
	lateClient  *http.Client
	// lateCap limits the opted-in detached execution: the effective late
	// capacity is the minimum of the timeout of the node and this
	// capacity, and a non-positive value means no extra limit.
	lateCap time.Duration
}

func NewServer(host string, reg *registry.Registry, pl *pool.Manager, addresser InstanceAddresser, cred auth.Credential, defaultTimeout time.Duration, collector *telemetry.Collector, sampler InstanceMetricsSampler, maxManagementBodyBytes int, maxFunctionOutputBytes int, readHeaderTimeout time.Duration, idleTimeout time.Duration, invocationSampleInterval time.Duration, readiness ReadinessChecker) *Server {
	// The periodic report carries the complete occupancy map: the
	// reporter collects it and the snapshot below fills every function of
	// the registry. It is installed here because the server is the only
	// component that holds the registry, the pool and the collector
	// together. The collector still does not import the pool or the
	// registry, it only sees a function, so the agent does not change.
	if collector != nil && reg != nil && pl != nil {
		collector.SetCapacitySnapshot(func() map[string]telemetry.FunctionCapacity {
			funcs := reg.List()
			if len(funcs) == 0 {
				return nil
			}
			out := make(map[string]telemetry.FunctionCapacity, len(funcs))
			for _, f := range funcs {
				out[f.Name] = telemetry.FunctionCapacity{
					MaxInstances: f.MaxInstances,
					// The total number of live instances, not the free
					// ones, since the pool only exposes the count.
					InstancesAvailable: pl.Count(f.Name),
					QueueDepth:         pl.QueueLen(f.Name),
				}
			}
			return out
		})
	}
	return &Server{
		host:                     host,
		registry:                 reg,
		pool:                     pl,
		addresser:                addresser,
		credential:               cred,
		defaultTimeout:           defaultTimeout,
		collector:                collector,
		metricsSampler:           sampler,
		readiness:                readiness,
		maxManagementBodyBytes:   maxManagementBodyBytes,
		maxFunctionOutputBytes:   maxFunctionOutputBytes,
		maxInvokeBodyBytes:       defaultInvokeBodyBytes,
		readHeaderTimeout:        readHeaderTimeout,
		idleTimeout:              idleTimeout,
		invocationSampleInterval: invocationSampleInterval,
		mgmtLimiter:              auth.NewRateLimiter(300, time.Minute),
		invokeTransport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				path, ok := ctx.Value(socketPathCtxKey{}).(string)
				if !ok || path == "" {
					return nil, errors.New("nodeapi: requisição sem socket Unix de destino no contexto")
				}
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			},
			DisableKeepAlives: true,
			Proxy:             nil,
		},
	}
}

func (s *Server) telemetryActive() bool { return s.collector.Enabled() }

// WithInvokeBodyBytes replaces the ceiling of the body of an invocation.
// It returns the server itself so that the calls can be chained, and it
// is used by the binary with the value of the configuration.
func (s *Server) WithInvokeBodyBytes(n int) *Server {
	if n > 0 {
		s.maxInvokeBodyBytes = n
	}
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /function/{name}", auth.RequireAuthWithLimiter(s.credential, s.mgmtLimiter, http.HandlerFunc(s.handleRegister)))
	mux.Handle("GET /function", auth.RequireAuthWithLimiter(s.credential, s.mgmtLimiter, http.HandlerFunc(s.handleList)))
	mux.Handle("PUT /function/{name}", auth.RequireAuthWithLimiter(s.credential, s.mgmtLimiter, http.HandlerFunc(s.handleUpdate)))
	mux.Handle("DELETE /function/{name}", auth.RequireAuthWithLimiter(s.credential, s.mgmtLimiter, http.HandlerFunc(s.handleDelete)))
	mux.HandleFunc("GET /function/{name}", s.handleGet)
	mux.HandleFunc("POST /function/{name}/invoke", s.handleInvoke)
	return mux
}
