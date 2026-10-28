package sim

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

const (
	// The hard-limit mode runs the pure queue selector, which is the baseline of
	// the experiment and exists only in the bench, since in production the queue
	// selector lives only as the deterministic base and shield of the risk
	// selector.
	ModoHardLimits = "hard-limits"
	// The risk mode runs the risk selector, which is the only canonical selector
	// of production. It uses the same apparatus of the self-learning, with the
	// priming and the history, and its own learner, so that the defaults are
	// fine-tuned in the simulation before production.
	ModoRisk = "risk"
)

// Cenario describes one execution of the bench.
type Cenario struct {
	Nome string `json:"nome"`
	Modo string `json:"modo"`
	// The type is either a cadence, with requests at every interval as the
	// adaptive runner, or a burst, with a number of workers in a loop as the
	// probes of six and ten workers of the session.
	Tipo           string        `json:"tipo"`
	SLO            int           `json:"slo_ms"` // 0 = sem SLO
	Intervals      []float64     `json:"intervals_s"`
	Warmup         int           `json:"warmup"`
	Reps           int           `json:"reps"`
	Imagens        int           `json:"imagens"`
	Workers        int           `json:"workers"`
	Duracao        time.Duration `json:"duracao_s"`
	DerrubarLocais bool          `json:"derruba_locais"` // borda+névoa respondem 503 (valida nuvem)
}

// Config is the configuration of the bench, shared by the scenarios.
type Config struct {
	InvLogPath  string
	OutDir      string
	HistorySeed string // "none" = histórico vazio; caminho = seed real copiado
	Severidade  float64
	Seed        int64
	MaxCPU      float64
	MaxMem      float64
	CloudFrioP  float64 // probabilidade da cauda fria/lenta da nuvem (0 = warm real)
	// The cold mode starts the pools of the nodes empty, without a pre-warm, so
	// the first request of each function and concrete variant pays a cold start,
	// with the real samples of the pair and a conservative default without a
	// sample. The warm mode primes the pools.
	Frio bool
	// The priming is the duration of a burst under the objective of the scenario
	// before the measured phase, which warms the history as the mixed priming of
	// the batch. The metrics of the client and the counters of the choices of the
	// measured phase are taken by difference, so they do not carry the priming.
	Prime time.Duration
	// The override replaces the hyperparameters of the risk selector in the risk
	// mode, for the fine-tuning and the ablations, and a nil value means the
	// defaults, while an invalid value makes the run fail.
	RiskOverride *selection.RiskConfig
}

// Resultado aggregates the metrics of a scenario.
type Resultado struct {
	Cenario string        `json:"cenario"`
	Modo    string        `json:"modo"`
	SLO     int           `json:"slo_ms"`
	Duracao time.Duration `json:"duracao_s"`

	Invocos   int            `json:"invocos_cliente"`
	OK        int            `json:"ok_cliente"`
	Erros     int            `json:"erros_cliente"`
	PorCodigo map[string]int `json:"erros_por_codigo"`
	SLOOk     int            `json:"slo_ok_cliente"`
	SLOTotal  int            `json:"slo_total_cliente"`
	P50MS     int64          `json:"p50_ms"`
	P95MS     int64          `json:"p95_ms"`
	P99MS     int64          `json:"p99_ms"`

	Tentativas        int            `json:"tentativas"`
	TentativasPorNo   map[string]int `json:"tentativas_por_no"`
	OutcomesNo        map[string]int `json:"outcomes"`
	OrcamentoExcedido int            `json:"orcamento_excedido"`
	Falhas            int            `json:"falhas_tentativa"`
	Timeouts          int            `json:"timeouts_tentativa"`
	CloudTentativas   int            `json:"cloud_tentativas"`
	FanoutCuts        int64          `json:"fanout_cuts"`
	UnviableSkips     int64          `json:"unviable_skips"`
	QueueCapSkips     int64          `json:"queue_cap_skips"`
	LateReceived      int64          `json:"late_received"`
	LateDropped       int64          `json:"late_dropped"`
	RaceCancelled     int64          `json:"race_cancelled"`
	CloudMockChamadas int64          `json:"cloud_mock_chamadas"`
	PrimeiroPorNo     map[string]int `json:"primeira_escolha_por_no"`

	// The choices of the risk selector are the difference of its counters in the
	// measured phase, which is zero in the hard-limit baseline.
	EscolhasRisk selection.RiskStatsSnapshot `json:"escolhas_risk,omitempty"`

	Alertas []string `json:"alertas"`

	invPath string
}

// Run executes a scenario of the bench with the real code of the arbiter.
func Run(cfg Config, cen Cenario, cal *Calibracao) (*Resultado, error) {
	dir, err := os.MkdirTemp("", "sim-arbitro-")
	if err != nil {
		return nil, err
	}
	if cfg.OutDir != "" {
		if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
			return nil, err
		}
	}

	seed := cfg.Seed
	if seed == 0 {
		seed = 42
	}
	// Deterministic seed per scenario, which gives the same conditions between the modes.
	h := int64(0)
	for _, r := range cen.Nome {
		h = h*31 + int64(r)
	}
	rngSeed := seed + h

	edge, err := NewMock(NoConfig{
		NodeID: "xp4", Layer: nodes.LayerBorda,
		Funcs: map[string]string{"alpr-ncnn": "lpr", "ocr": "ocr"},
		// The edge runs with an open pool, which is the decision of the session.
		MaxInstances: map[string]int{"alpr-ncnn": 0, "ocr": 0},
	}, cal, rngSeed+1, !cfg.Frio)
	if err != nil {
		return nil, err
	}
	defer edge.Close()
	fog, err := NewMock(NoConfig{
		NodeID: "ccsc-lscdesktop6", Layer: nodes.LayerNevoa,
		Funcs: map[string]string{"alpr-cpu": "lpr", "alpr-gpu": "lpr", "ocr": "ocr"},
		// The real ceilings of the session, per concrete variant.
		MaxInstances: map[string]int{"alpr-cpu": 4, "alpr-gpu": 2, "ocr": 8},
	}, cal, rngSeed+2, !cfg.Frio)
	if err != nil {
		return nil, err
	}
	defer fog.Close()
	cloudMock, err := NewCloudMock(cal, rngSeed+3, cfg.CloudFrioP)
	if err != nil {
		return nil, err
	}
	defer cloudMock.Close()
	if cen.DerrubarLocais {
		edge.SetDown(true)
		fog.SetDown(true)
	}

	ns, err := nodes.NewNodeStore("")
	if err != nil {
		return nil, err
	}
	if _, err := ns.Upsert(nodes.Node{
		NodeID: "xp4", Layer: nodes.LayerBorda, Host: hostOf(edge.URL()),
		Functions:          []string{"alpr-ncnn", "ocr"},
		FunctionCategories: map[string]string{"alpr-ncnn": "lpr", "ocr": "ocr"},
		FunctionCapacity:   map[string]int{"alpr-ncnn": 0, "ocr": 0},
	}); err != nil {
		return nil, err
	}
	if _, err := ns.Upsert(nodes.Node{
		NodeID: "ccsc-lscdesktop6", Layer: nodes.LayerNevoa, Host: hostOf(fog.URL()),
		Functions:          []string{"alpr-cpu", "alpr-gpu", "ocr"},
		FunctionCategories: map[string]string{"alpr-cpu": "lpr", "alpr-gpu": "lpr", "ocr": "ocr"},
		FunctionCapacity:   map[string]int{"alpr-cpu": 4, "alpr-gpu": 2, "ocr": 8},
	}); err != nil {
		return nil, err
	}

	mon := monitor.NewMonitor()
	stopHB := startHeartbeat(mon)
	defer stopHB()

	// History: the optional real seed, copied so that the original is not touched.
	histPath := filepath.Join(dir, "history.json")
	if cfg.HistorySeed != "" && cfg.HistorySeed != "none" {
		if data, err := os.ReadFile(cfg.HistorySeed); err == nil {
			_ = os.WriteFile(histPath, data, 0o600)
		}
	}
	hist, err := history.NewHistory(histPath, 1000)
	if err != nil {
		return nil, err
	}
	defer hist.Close()

	invPath := filepath.Join(dir, "invocation.jsonl")
	if cfg.OutDir != "" {
		invPath = filepath.Join(cfg.OutDir, "invocation-"+cen.Nome+".jsonl")
	}
	// The invocation log is append-only by design of the arbiter, so the bench
	// truncates it before, which keeps each scenario self-contained in the
	// analysis.
	_ = os.Remove(invPath)
	invLog, err := gateway.NewInvocationLog(invPath)
	if err != nil {
		return nil, err
	}

	// Limits of the simulator: the global trio from the configured processor and
	// memory ceilings. The limitation is deliberate, since the per-node override
	// is not propagated to the bench, which drives the scenarios with a single
	// synthetic topology and does not read the configuration file.
	limits := selection.LimitsConfig{
		MaxCPUPct: cfg.MaxCPU, MaxMemPct: cfg.MaxMem, MaxPSIAvg10: 25,
	}
	gauge := gateway.NewInflightGauge()
	criteria := []selection.SLOCriterion{selection.LatencyCriterion{}, selection.ThroughputCriterion{}}
	var selector selection.Selector
	var riskStats *selection.RiskStats
	if cen.Modo == ModoHardLimits {
		// The baseline of the experiment has only the rigid limits and no history,
		// and it exists only in the bench, since it was never a production default.
		selector = selection.BuildQueueSelector(mon, limits)
	} else {
		// The canonical risk selector is built in a single point, with its own
		// learner and the production defaults, fed by the replay of the seed and by
		// the live hook, and with the same priming of the self-learning, since it
		// also needs data to leave the probation.
		riskCfg := selection.DefaultRiskConfig()
		if cfg.RiskOverride != nil {
			riskCfg = *cfg.RiskOverride
		}
		sel, stats, _, err := selection.BuildRiskSelector(riskCfg, "", selection.RiskDeps{
			Mon: mon, Hist: hist, Inflight: gauge, Layers: ns, Clock: time.Now,
			Rand:     rand.New(rand.NewSource(cfg.Seed)),
			Criteria: criteria, Limits: limits, ThroughputWindow: 10 * time.Minute,
		})
		if err != nil {
			return nil, err
		}
		selector, riskStats = sel, stats
	}
	recSel := &recordingSelector{inner: selector, primeiro: map[string]int{}}

	store, err := cloud.NewCloudStore("")
	if err != nil {
		return nil, err
	}
	fns := map[string]cloud.FunctionEndpoint{}
	for fn, ep := range cloudMock.endpoints() {
		fns[fn] = cloud.FunctionEndpoint{
			EndpointURL: ep["endpoint_url"], InputContentType: ep["input_content_type"],
			AuthScheme: ep["auth_scheme"], MaxInstances: 10,
		}
	}
	if err := store.Set(cloud.CloudFallbackConfig{Connector: "azure-functions", Functions: fns}); err != nil {
		return nil, err
	}

	gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{
		Nodes: ns, Monitor: mon, Selector: recSel, RemoteExecutor: cloud.NewConnector(store),
		History: hist, Criteria: criteria, Clock: time.Now,
		DefaultBudget: 90 * time.Second, StaleAfter: 90 * time.Second,
		MinSamples: 20, MaxRaceFanout: 3, ThroughputWindow: 10 * time.Minute,
	}).WithInflight(gauge)
	gw.SetInvocationLog(invLog)

	gwLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	gwSrv := &http.Server{Handler: http.HandlerFunc(gw.Handle)}
	go func() { _ = gwSrv.Serve(gwLis) }()
	defer func() { _ = gwSrv.Close() }()

	gwURL := "http://" + gwLis.Addr().String()
	res := &Resultado{
		Cenario: cen.Nome, Modo: cen.Modo, SLO: cen.SLO,
		PorCodigo: map[string]int{}, TentativasPorNo: map[string]int{}, OutcomesNo: map[string]int{},
	}

	// The priming only exists in the risk mode: a burst under the objective of the
	// scenario warms the learner as the mixed priming of the batch, with its own
	// discarded client, and the counters of the choices of the measured phase are
	// taken by difference.
	if cfg.Prime > 0 && cen.Modo != ModoHardLimits && !cen.DerrubarLocais {
		primeWorkers := cen.Workers
		if primeWorkers <= 0 {
			primeWorkers = 3
		}
		newWorkloadClient(gwURL, cen.SLO).burst(primeWorkers, cfg.Prime)
	}
	var riskAntes selection.RiskStatsSnapshot
	if riskStats != nil {
		riskAntes = riskStats.Snapshot()
	}

	cli := newWorkloadClient(gwURL, cen.SLO)
	inicio := time.Now()
	switch cen.Tipo {
	case "burst":
		cli.burst(cen.Workers, cen.Duracao)
	default: // cadência (runner)
		cli.cadence(cen.Intervals, cen.Warmup, cen.Reps, cen.Imagens)
	}
	res.Duracao = time.Since(inicio)
	cli.fill(res)
	if riskStats != nil {
		res.EscolhasRisk = subRiskSnapshot(riskStats.Snapshot(), riskAntes)
	}

	// Persist and read back the attempts, with the same analysis trail of the real batch.
	_ = invLog.Close()
	_ = hist.FlushSync()
	parseTentativas(invPath, res)
	a, lateR, lateD, raceC := gw.Counters()
	res.Tentativas = int(a)
	res.LateReceived, res.LateDropped, res.RaceCancelled = lateR, lateD, raceC
	res.FanoutCuts = gw.FanoutCuts()
	res.UnviableSkips = gw.UnviableSkips()
	res.QueueCapSkips = gw.QueueCapSkips()
	res.CloudMockChamadas = cloudMock.Chamadas.Load()
	res.PrimeiroPorNo = recSel.snapshot()
	res.Alertas = alertas(res, cen)
	return res, nil
}

func hostOf(rawURL string) string { return strings.TrimPrefix(rawURL, "http://") }

// startHeartbeat feeds the monitor as the real agent does, with a periodic
// heartbeat and a plausible aggregate processor usage per layer, since in the
// bench the gate is the capacity of the pool and the processor value is
// secondary.
func startHeartbeat(mon *monitor.Monitor) func() {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				mon.Update(telemetry.Package{NodeID: "xp4", CPUUsagePct: 55, MemoryUsagePct: 22})
				mon.Update(telemetry.Package{NodeID: "ccsc-lscdesktop6", CPUUsagePct: 14, MemoryUsagePct: 28})
			}
		}
	}()
	return func() { close(done) }
}

// recordingSelector decorates the real selector, counting the first choice,
// which is the node that the dispatch would try first, and forwarding the three
// prediction ports, since without the forwarding the gateway would lose the
// service upper bound and the queue ceiling in the simulation.
type recordingSelector struct {
	inner    selection.Selector
	mu       sync.Mutex
	primeiro map[string]int
}

func (r *recordingSelector) Select(c []nodes.Node, fn string, slo *selection.SLO) []nodes.Node {
	out := r.inner.Select(c, fn, slo)
	r.mu.Lock()
	if len(out) > 0 {
		r.primeiro[out[0].NodeID]++
	}
	r.mu.Unlock()
	return out
}

func (r *recordingSelector) PredictLatencyMS(group string, n nodes.Node, slo *selection.SLO) (int64, bool) {
	if p, ok := r.inner.(gateway.LatencyPredictor); ok {
		return p.PredictLatencyMS(group, n, slo)
	}
	return 0, false
}

func (r *recordingSelector) PredictLatencyMSNewRequest(group string, n nodes.Node, slo *selection.SLO) (int64, bool) {
	if p, ok := r.inner.(gateway.NewRequestPredictor); ok {
		return p.PredictLatencyMSNewRequest(group, n, slo)
	}
	return 0, false
}

func (r *recordingSelector) PredictMaxQueueK(group string, n nodes.Node, slo *selection.SLO) (int, bool) {
	if p, ok := r.inner.(gateway.QueueCapPredictor); ok {
		return p.PredictMaxQueueK(group, n, slo)
	}
	return 0, false
}

func (r *recordingSelector) snapshot() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.primeiro))
	for k, v := range r.primeiro {
		out[k] = v
	}
	return out
}

func parseTentativas(path string, res *Resultado) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for sc.Scan() {
		var r linhaInvLog
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			continue
		}
		if r.NodeID == "" {
			continue
		}
		res.TentativasPorNo[r.NodeID]++
		res.OutcomesNo[r.Outcome]++
		switch r.Outcome {
		case history.OutcomeOrcamentoExcedido:
			res.OrcamentoExcedido++
		case history.OutcomeFalha:
			res.Falhas++
		case history.OutcomeTimeout:
			res.Timeouts++
		}
		if r.NodeID == "cloud-fallback" {
			res.CloudTentativas++
		}
	}
}

// subRiskSnapshot returns the difference counter by counter, of the measured phase minus the priming.
func subRiskSnapshot(after, before selection.RiskStatsSnapshot) selection.RiskStatsSnapshot {
	return selection.RiskStatsSnapshot{
		Feasible:          after.Feasible - before.Feasible,
		Probation:         after.Probation - before.Probation,
		ExploreFront:      after.ExploreFront - before.ExploreFront,
		StretchExcluded:   after.StretchExcluded - before.StretchExcluded,
		SLOExcluded:       after.SLOExcluded - before.SLOExcluded,
		CapExcluded:       after.CapExcluded - before.CapExcluded,
		ProjectedExcluded: after.ProjectedExcluded - before.ProjectedExcluded,
		BandRandom:        after.BandRandom - before.BandRandom,
		EmptyQueue:        after.EmptyQueue - before.EmptyQueue,
		ColdChosen:        after.ColdChosen - before.ColdChosen,
		Bootstrap:         after.Bootstrap - before.Bootstrap,
	}
}

func pct(v []int64, p float64) int64 {
	if len(v) == 0 {
		return 0
	}
	i := int(math.Ceil(p*float64(len(v)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(v) {
		i = len(v) - 1
	}
	return v[i]
}

// alertas materializes the validation assertions of the corrected model.
func alertas(res *Resultado, cen Cenario) []string {
	var out []string
	if cen.DerrubarLocais {
		if res.CloudTentativas == 0 {
			out = append(out, "nuvem nunca foi tentada com os dois nós locais em 503")
		}
		if res.Invocos > 0 && res.OK < res.Invocos {
			out = append(out, fmt.Sprintf("nuvem não absorveu tudo: ok=%d/%d", res.OK, res.Invocos))
		}
		return out
	}
	if res.Invocos > 0 && res.OK == 0 {
		out = append(out, "nenhum invoke concluiu com sucesso")
	}
	if cen.SLO > 0 && res.SLOTotal > 0 {
		frac := float64(res.SLOOk) / float64(res.SLOTotal)
		if frac < 0.90 {
			out = append(out, fmt.Sprintf("SLO cumprido em %.1f%% (<90%%)", frac*100))
		}
	}
	if res.PorCodigo["NO_NODE_AVAILABLE"] > 0 || res.PorCodigo["NODE_INVOCATION_TIMEOUT"] > 2 {
		out = append(out, "fila terminou sem nó/estourou orçamento além do tolerado")
	}
	return out
}
