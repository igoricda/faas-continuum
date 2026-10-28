package sim

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// NoConfig describes a mocked node. The maximum number of instances uses the
// semantics of the real pool: zero is an open pool with unlimited and
// coalesced spawning, and a positive value is a finite ceiling. The queue
// capacity is the ceiling of waiters before the full queue rejection.
type NoConfig struct {
	NodeID       string
	Layer        string
	Funcs        map[string]string // função concreta -> categoria
	MaxInstances map[string]int    // função concreta -> teto (0 = aberto)
	QueueCap     int
	SpawnDelay   time.Duration
}

type poolState struct {
	busy     int
	alive    int
	waiters  int
	spawning bool
}

// Mock is a simulated node with a real pool and real concurrency over HTTP.
type Mock struct {
	cfg  NoConfig
	cal  *Calibracao
	rand *rand.Rand
	srv  *http.Server
	lis  net.Listener
	url  string

	mu    sync.Mutex
	cond  *sync.Cond
	pools map[string]*poolState
	down  atomic.Bool

	// The mutex serializes the random source of the node: concurrent HTTP
	// handlers call the cold start and the service in parallel, and the
	// standard generator is not safe for concurrent use.
	randMu sync.Mutex

	// observational counters
	Aceitas   atomic.Int64
	Recusadas atomic.Int64
	Frias     atomic.Int64
	EmVooPeak atomic.Int64
}

// NewMock starts the HTTP server of the node on a loopback port chosen by the
// system. A warm start pre-warms one instance per function, which is the
// batch with priming, and otherwise the pool stays empty and the first
// request pays a cold start.
func NewMock(cfg NoConfig, cal *Calibracao, seed int64, warm bool) (*Mock, error) {
	if cfg.QueueCap <= 0 {
		cfg.QueueCap = 32
	}
	if cfg.SpawnDelay <= 0 {
		cfg.SpawnDelay = 400 * time.Millisecond
	}
	m := &Mock{
		cfg:   cfg,
		cal:   cal,
		rand:  rand.New(rand.NewSource(seed)),
		pools: map[string]*poolState{},
	}
	m.cond = sync.NewCond(&m.mu)
	for fn := range cfg.Funcs {
		p := &poolState{}
		if warm {
			p.alive = 1
		}
		m.pools[fn] = p
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("sim: listen mock %s: %w", cfg.NodeID, err)
	}
	m.lis = lis
	m.url = "http://" + lis.Addr().String()
	mux := http.NewServeMux()
	mux.HandleFunc("/function/", m.handleInvoke)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	m.srv = &http.Server{Handler: mux}
	go func() { _ = m.srv.Serve(lis) }()
	return m, nil
}

// URL returns the host and the port of the mock, for the node store.
func (m *Mock) URL() string { return m.url }

// SetDown turns the mode of a node that is out on and off: every invocation
// answers with a pool exhausted rejection, which exercises the failover and
// the cloud fallback.
func (m *Mock) SetDown(v bool) { m.down.Store(v) }

// Close shuts the server down.
func (m *Mock) Close() { _ = m.srv.Close() }

func (m *Mock) maxFor(fn string) int {
	if v, ok := m.cfg.MaxInstances[fn]; ok {
		return v
	}
	return 0
}

func (m *Mock) handleInvoke(w http.ResponseWriter, r *http.Request) {
	if m.down.Load() {
		m.Recusadas.Add(1)
		writeCapacityErr(w, "POOL_EXHAUSTED")
		return
	}
	fn := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/function/"), "/invoke")
	if fn == "" || strings.Contains(fn, "/") {
		http.NotFound(w, r)
		return
	}
	cat := m.cfg.Funcs[fn]
	if cat == "" {
		cat = fn // the category is accepted verbatim, which is a dispatch without resolving a concrete variant
	}
	wait, cold, err := m.acquire(fn)
	if err != nil {
		m.Recusadas.Add(1)
		writeCapacityErr(w, "QUEUE_FULL")
		return
	}
	m.Aceitas.Add(1)
	if cold {
		m.Frias.Add(1)
	}

	// The number of executions in flight in this pool, including this one, the
	// same quantity as the meter of the requests in flight that calibrated the
	// real samples.
	m.mu.Lock()
	k := m.pools[fn].busy
	m.mu.Unlock()
	if int64(k) > m.EmVooPeak.Load() {
		m.EmVooPeak.Store(int64(k))
	}

	var servico time.Duration
	m.randMu.Lock()
	if cold {
		servico = m.cal.Cold(m.cfg.NodeID, cat, m.rand)
	} else {
		servico = m.cal.Servico(m.cfg.NodeID, cat, k, m.rand)
	}
	m.randMu.Unlock()
	time.Sleep(wait + servico)
	m.release(fn)

	m.mu.Lock()
	p := m.pools[fn]
	alive, queue := p.alive, p.waiters
	m.mu.Unlock()

	instID := fmt.Sprintf("%s-%d", fn, k-1)
	resp := map[string]any{
		"result": map[string]any{
			"plates": []map[string]any{{"text": "SIM1234", "confidence": 0.99}},
			"nodes":  m.cfg.NodeID,
		},
		"execution_metrics": map[string]any{
			"cold_start":  cold,
			"instance_id": instID,
			"resource_profile": map[string]any{
				"cpu_pct_avg":    cpuSimulada(m.cfg, k, cold),
				"cpu_pct_peak":   cpuSimulada(m.cfg, k, cold) + 12,
				"mem_peak_bytes": uint64(120<<20) + uint64(k)*24<<20,
				"sample_count":   10,
			},
			"phases": map[string]any{
				"acquire_wait_ms": wait.Milliseconds(),
				"readiness_ms":    0,
				"function_ms":     servico.Milliseconds(),
			},
			"telemetry": map[string]any{
				"capacity": map[string]telemetry.FunctionCapacity{
					fn: {
						MaxInstances:       m.maxFor(fn),
						InstancesAvailable: alive,
						QueueDepth:         queue,
					},
				},
			},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// cpuSimulada imitates the real aggregate of ten seconds: the edge rises with
// the concurrency, with values from 46 to 77 percent in the data, while the
// fog stays low.
func cpuSimulada(cfg NoConfig, k int, cold bool) float64 {
	if cfg.Layer == "borda" {
		v := 38 + 8*float64(k)
		if cold {
			v += 15
		}
		if v > 92 {
			v = 92
		}
		return v
	}
	return 8 + 2*float64(k)
}

// acquire implements the real pool, with the coalescing of the spawn and the
// queue of waiters: it returns the time in queue, whether this request paid
// the cold spawn, and a saturation error when the queue overflows.
func (m *Mock) acquire(fn string) (time.Duration, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pools[fn]
	if p == nil {
		p = &poolState{}
		m.pools[fn] = p
	}
	inicio := time.Now()
	cold := false
	for {
		if p.busy < p.alive {
			p.busy++
			return time.Since(inicio), cold, nil
		}
		max := m.maxFor(fn)
		if (max == 0 || p.alive < max) && !p.spawning {
			p.spawning = true
			cold = p.alive == 0
			fnLocal := fn
			go func() {
				time.Sleep(m.cfg.SpawnDelay)
				m.mu.Lock()
				pp := m.pools[fnLocal]
				pp.spawning = false
				pp.alive++
				m.cond.Broadcast()
				m.mu.Unlock()
			}()
		}
		if p.waiters >= m.cfg.QueueCap {
			return 0, false, fmt.Errorf("sim: fila cheia em %s", fn)
		}
		p.waiters++
		m.cond.Wait()
		p.waiters--
	}
}

func (m *Mock) release(fn string) {
	m.mu.Lock()
	if p := m.pools[fn]; p != nil {
		p.busy--
	}
	m.cond.Broadcast()
	m.mu.Unlock()
}

func writeCapacityErr(w http.ResponseWriter, code string) {
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": "lotação simulada"})
}

// CloudMock simulates the cloud endpoint with the real latencies of the
// fallback, and answers a raw success body in the format of the cloud
// contract.
type CloudMock struct {
	cal   *Calibracao
	rand  *rand.Rand
	srv   *http.Server
	url   string
	frioP float64

	mu       sync.Mutex
	Chamadas atomic.Int64
	// The mutex serializes the random source of the cloud, for the same reason as the node mock.
	randMu sync.Mutex
}

func NewCloudMock(cal *Calibracao, seed int64, frioP float64) (*CloudMock, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("sim: listen cloud: %w", err)
	}
	c := &CloudMock{cal: cal, rand: rand.New(rand.NewSource(seed)), frioP: frioP}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c.Chamadas.Add(1)
		c.randMu.Lock()
		lat := c.cal.CloudServico(c.rand, c.frioP)
		c.randMu.Unlock()
		time.Sleep(lat)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plates":[{"text":"CLOUD1234","confidence":0.98}]}`))
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(lis) }()
	c.srv = srv
	c.url = "http://" + lis.Addr().String()
	return c, nil
}

func (c *CloudMock) URL() string { return c.url }
func (c *CloudMock) Close()      { _ = c.srv.Close() }

// cloudEndpoint builds the map of endpoints per function used by the cloud store.
func (c *CloudMock) endpoints() map[string]map[string]string {
	return map[string]map[string]string{
		"lpr": {"endpoint_url": c.url + "/api/alpr", "input_content_type": "application/json", "auth_scheme": "none"},
		"ocr": {"endpoint_url": c.url + "/api/ocr", "input_content_type": "application/json", "auth_scheme": "none"},
	}
}
