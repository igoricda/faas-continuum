// Command arbitro is the entry point of the arbiter. It exposes the node
// manager, the monitor, the gateway and the persistence of the nodes.
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/cattokens"
	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/arbitro/server"
	"github.com/igoricda/faas-continuum/internal/auth"
	"github.com/igoricda/faas-continuum/internal/config"
)

func main() {
	configPath := flag.String("config", "./config.jsonc", "caminho do config.jsonc (JSONC com comentários; sobrepõe defaults; flags ainda vencem)")
	_ = flag.String("listen", ":8090", "endereço de escuta da API do Árbitro")
	_ = flag.String("selector", "risk", "seletor em produção: risk (RiskSelector canônico) ou hl (QueueSelector puro, baseline hard-limits; o v1 continua aposentado)")
	_ = flag.String("credential", "./faas-continuum-arbitro-credential.json", "credencial local do Árbitro (gerada no primeiro uso)")
	_ = flag.String("node-tokens", "./faas-continuum-node-tokens.json", "persistência dos tokens por nó (correcao-seguranca-i1-i7, I1)")
	_ = flag.String("nodes", "./faas-continuum-nodes.json", "persistência do registro de nós")
	_ = flag.Duration("stale-after", 90*time.Second, "tempo sem last_seen para considerar o nó indisponivel")
	maxCPU := flag.Float64("max-cpu-pct", 85, "limite de CPU em % para marcar nó como sobrecarregado")
	maxMem := flag.Float64("max-mem-pct", 90, "limite de memória em % para marcar nó como sobrecarregado")
	maxPSI := flag.Float64("max-psi-avg10", 25, "limite de pressão PSI avg10 em % para marcar nó como sobrecarregado")
	psiVetoCPU := flag.Float64("psi-veto-cpu", 28, "limiar cpu_some avg10 em % do veto full PSI no Select (todo 12; >0)")
	psiVetoMemFull := flag.Float64("psi-veto-mem-full", 12, "limiar mem_full avg10 em % do veto full PSI no Select (todo 12; >0)")
	flagInvokeTimeout := flag.Duration("invoke-timeout", 90*time.Second, "timeout padrão de invocação no gateway")
	_ = flag.String("cloud-fallback", "./faas-continuum-cloud-fallback.json", "persistência da configuração de fallback de nuvem")
	_ = flag.String("category-tokens", "./faas-continuum-category-tokens.json", "persistência dos tokens por categoria e mapa downstream (vazio desliga a tradução)")
	_ = flag.String("history", "./faas-continuum-history.json", "persistência do histórico de execuções")
	historyCap := flag.Int("history-cap", 1000, "capacidade por segmento do histórico")
	historyRetention := flag.Duration("history-retention", time.Hour, "janela de retenção por idade do histórico (0 desliga o corte; ex. 1h)")
	historySync := flag.String("history-sync", "batched", "modo de persistência do histórico: batched (ticker 500ms+coalesce) ou strict (fsync por Append)")
	_ = flag.String("invocation-log", "", "log append-only de invocações (JSONL, sem limite) para análise; vazio desliga")
	minSamples := flag.Int("selflearning-min-samples", 20, "amostras mínimas do histórico por segmento (nome da flag é histórico; o v1 que o consumia foi aposentado no ADR-0012)")
	slWindow := flag.Duration("selflearning-throughput-window", 60*time.Second, "janela do histórico para vazão/custo (RiskSelector ExpectedCost e history_summary)")
	maxRaceFanout := flag.Int("max-race-fanout", 3, "teto de tentativas por invocação no gateway")
	_ = flag.Bool("gateway-observe-armed", false, "observa pendências armadas durante a fatia (correção G1, SELETOR-V2 §8.1)")
	_ = flag.Duration("risk-halflife", 10*time.Minute, "meia-vida do decaimento temporal do seletor (RiskSelector)")
	_ = flag.Float64("risk-epsilon", 0.05, "ε_risk do RiskSelector (alvo de miscoverage do ACI)")
	_ = flag.Float64("risk-aci-gamma", 0.01, "γ do ACI do RiskSelector")
	_ = flag.Float64("risk-nmin", 3, "nmin do RiskSelector (probation abaixo disso)")
	_ = flag.Float64("risk-explore", 0.05, "probabilidade de exploração à frente da fila do RiskSelector")
	_ = flag.Float64("risk-stretch", 2.0, "stretch do T_eff sem SLO do RiskSelector")
	_ = flag.Float64("risk-stretch-slo", 0, "stretch do T_eff com SLO do RiskSelector (0 = desligado)")
	_ = flag.Float64("risk-band", 0.10, "tolerância de empate da banda do RiskSelector")
	_ = flag.Float64("risk-queue-allowance", 1.0, "teto estático de fila do RiskSelector (2c por default)")
	_ = flag.Int("risk-default-servers", 1, "servidores presumidos pelo RiskSelector sem max_instances")
	_ = flag.Float64("risk-min-success", 0.5, "P_succ mínima do RiskSelector (paridade VetoByRate)")
	_ = flag.String("risk-priors", "", "arquivo JSON de priors do seletor (vazio = sem priors)")
	pprofAddr := flag.String("pprof-addr", "", "endereço do pprof (só builds debug, desligado por padrão; ex. 127.0.0.1:6061)")
	flag.Parse()

	StartPprof(*pprofAddr)

	validateStartupFlags(*historyCap, *historyRetention, *minSamples, *slWindow, *historySync, *maxRaceFanout, *maxCPU, *maxMem, *maxPSI, *flagInvokeTimeout, *psiVetoCPU, *psiVetoMemFull)

	flagOverrides := map[string]string{}
	flag.Visit(func(f *flag.Flag) { flagOverrides[f.Name] = f.Value.String() })

	effectiveConfigPath := *configPath
	cfg, err := config.Load(effectiveConfigPath, flagOverrides)
	if err != nil {
		log.Fatalf("arbitro: falha ao carregar config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		log.Fatalf("arbitro: config inválida: %v", err)
	}
	if _, err := os.Stat(effectiveConfigPath); os.IsNotExist(err) {
		log.Printf("arbitro: config não encontrado em %q, usando flags/defaults", effectiveConfigPath)
	}

	listenAddr := cfg.Arbitro.Listen
	credPathStr := cfg.Arbitro.CredentialPath
	nodesPathStr := cfg.Arbitro.NodesPath
	nodeTokensPathStr := cfg.Arbitro.NodeTokensPath
	staleAfterDur := time.Duration(cfg.Arbitro.StaleAfter)
	historyPathStr := cfg.Arbitro.HistoryPath
	historyCapN := cfg.Arbitro.HistoryCap
	historyRetentionN := time.Duration(cfg.Arbitro.HistoryRetention)
	minSamplesN := cfg.Arbitro.MinSamples
	slWindowDur := time.Duration(cfg.Arbitro.ThroughputWindow)
	historySyncMode := cfg.Arbitro.HistorySync

	cred, plainToken, err := auth.LoadOrGenerate(credPathStr)
	if err != nil {
		log.Fatalf("arbitro: falha ao preparar credencial local: %v", err)
	}
	if plainToken != "" {
		log.Printf("arbitro: token gerado (show-once) em %q — guarde este token para configurar os agentes", credPathStr)
		fmt.Printf(`
========================================================================
ATENÇÃO: credencial do Árbitro provisionada agora (primeiro uso).

Este token em texto plano NÃO será exibido novamente. Guarde-o agora
para configurar os agentes que se registram no Árbitro.

  Cabeçalho: %s
  Token:     %s

========================================================================
`, cred.HeaderName, plainToken)
	} else {
		log.Printf("arbitro: credencial carregada de %q (cabeçalho %q)", credPathStr, cred.HeaderName)
	}

	nodeStore, err := nodes.NewNodeStore(nodesPathStr)
	if err != nil {
		log.Fatalf("arbitro: falha ao carregar registro de nós %q: %v", nodesPathStr, err)
	}
	log.Printf("arbitro: registro de nós em %q (%d nó(s) carregado(s))", nodesPathStr, len(nodeStore.List()))

	// Node tokens are mandatory in production: without the store the server
	// could not bind a registration or telemetry to an identity, which is
	// fail-closed. The received token is validated before the node is
	// registered.
	nodeTokens, err := nodes.NewNodeTokenStore(nodeTokensPathStr)
	if err != nil {
		log.Fatalf("arbitro: falha ao carregar tokens por nó %q: %v — remova o arquivo se corrompido para reemitir", nodeTokensPathStr, err)
	}
	log.Printf("arbitro: tokens por nó em %q", nodeTokensPathStr)

	limits := selection.LimitsConfig{MaxCPUPct: cfg.Arbitro.MaxCPUPct, MaxMemPct: cfg.Arbitro.MaxMemPct, MaxPSIAvg10: cfg.Arbitro.MaxPSIAvg10}
	// Hard limits per node, with a partial override of the global trio that
	// is resolved into a complete trio here at startup. The selection only
	// consults the limits for the node and never merges. It changes only the
	// stage of the hard limits, which is the reactive overload filter and the
	// overloaded status, shared by both modes of selection, and no statistic,
	// prediction or ordering of the self-learning is affected.
	limits.PerNode = resolveNodeHardLimits(cfg.Arbitro)
	if limits.PerNode != nil {
		log.Printf("arbitro: hard-limits por nó (override do global cpu=%.1f mem=%.1f psi=%.1f): %s",
			limits.MaxCPUPct, limits.MaxMemPct, limits.MaxPSIAvg10, nodeLimitsLog(limits.PerNode))
		// An override for an unknown node, from a typo or from a node that did
		// not register, drops nobody, and it warns instead of failing, since a
		// node can register after the boot and the map is read at every
		// selection.
		if unknown := unknownNodeLimitIDs(limits.PerNode, nodeStore.List()); len(unknown) > 0 {
			log.Printf("arbitro: aviso — node_limits com node_id sem nó registrado (ignorado até registro): %s", strings.Join(unknown, " "))
		}
	}
	invokeTimeout := time.Duration(cfg.Arbitro.InvokeTimeout)
	cloudPathStr := cfg.Arbitro.CloudFallbackPath

	cloudStore, cerr := cloud.NewCloudStore(cloudPathStr)
	if cerr != nil {
		log.Printf("arbitro: aviso — falha ao carregar cloud fallback %q: %v (seguindo sem fallback)", cloudPathStr, cerr)
		cloudStore, _ = cloud.NewCloudStore("")
	}

	// Tokens per category: an empty value disables the translation and
	// preserves the legacy verbatim path, and a corrupted store aborts, which
	// is fail-closed, since it carries downstream secrets in plaintext and the
	// arbiter does not start without it intact.
	catTokensPathStr := cfg.Arbitro.CategoryTokensPath
	catTokens, caterr := cattokens.NewStore(catTokensPathStr)
	if caterr != nil {
		log.Fatalf("arbitro: falha ao carregar tokens por categoria %q: %v — remova o arquivo se corrompido para reemitir", catTokensPathStr, caterr)
	}
	if catTokensPathStr != "" {
		log.Printf("arbitro: tokens por categoria em %q", catTokensPathStr)
	}

	hist, herr := history.NewHistoryWithRetention(historyPathStr, historyCapN, historyRetentionN)
	if herr != nil {
		log.Fatalf("arbitro: falha ao carregar histórico %q: %v — remova o arquivo se corrompido ou ajuste history-cap", historyPathStr, herr)
	}
	hist.SetStrict(historySyncMode == "strict")
	log.Printf("arbitro: history path=%s cap=%d retention=%s minSamples=%d window=%s strict=%v", historyPathStr, historyCapN, historyRetentionN, minSamplesN, slWindowDur, historySyncMode == "strict")

	defer hist.Close()

	// Append-only logger of the invocations, which is optional and disabled by
	// default. It carries the same entry as the history, as JSON lines without
	// a limit, for analysis.
	invLog, invErr := gateway.NewInvocationLog(cfg.Arbitro.InvocationLogPath)
	if invErr != nil {
		log.Fatalf("arbitro: falha ao abrir invocation log %q: %v", cfg.Arbitro.InvocationLogPath, invErr)
	}
	if invLog != nil {
		log.Printf("arbitro: invocation log em %q (append-only, sem limite)", cfg.Arbitro.InvocationLogPath)
	}
	defer invLog.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		hist.Close()
		invLog.Close()
		os.Exit(0)
	}()

	// Cloud connector: it dispatches by the current format, either the legacy
	// plan or a per-function plan, reading the store at every invocation, so a
	// configuration update changes the format without rebuilding the gateway.
	// It is injected into the port of the remote executor, so the consumer
	// declares the interface and the producer satisfies it without a reverse
	// import.
	cloudConnector := cloud.NewConnector(cloudStore)
	mon := monitor.NewMonitor()
	// A single shared meter: the gateway mutates it per attempt and the
	// selector reads the same data through function fields, which avoids a
	// cross import from the selection to the gateway and the cycle it would
	// create.
	gauge := gateway.NewInflightGauge()
	criteria := []selection.SLOCriterion{selection.LatencyCriterion{}, selection.ThroughputCriterion{}}
	// Selector in production: either the canonical risk selector or the pure
	// queue selector, which is the baseline of the hard limits and has no
	// learning. The queue selector also remains as the deterministic base and
	// shield of the risk selector.
	//
	// The construction happens in a single point: the replay of the history
	// and the observation hook feed the learner only in the risk mode, and the
	// dump goes to the node query and is omitted when there is no wiring.
	var selector selection.Selector
	var riskStats *selection.RiskStats
	var riskLearner *learn.Learner
	switch cfg.Arbitro.Selector {
	case "hl":
		selector = selection.BuildQueueSelector(mon, limits)
		log.Printf("arbitro: selection modo=hl (QueueSelector puro, baseline hard-limits, sem aprendizado)")
	case "risk":
		riskCfg := selection.RiskConfig{
			HalfLife: time.Duration(cfg.Arbitro.RiskHalfLife), RiskEpsilon: cfg.Arbitro.RiskEpsilon,
			ACIGamma: cfg.Arbitro.RiskACIGamma, NMin: cfg.Arbitro.RiskNMin,
			Explore: cfg.Arbitro.RiskExplore, Stretch: cfg.Arbitro.RiskStretch,
			StretchSLO: cfg.Arbitro.RiskStretchSLO, Band: cfg.Arbitro.RiskBand,
			QueueAllowance: cfg.Arbitro.RiskQueueAllowance,
			DefaultServers: cfg.Arbitro.RiskDefaultServers, MinSuccess: cfg.Arbitro.RiskMinSuccess,
		}
		var err error
		selector, riskStats, riskLearner, err = selection.BuildRiskSelector(riskCfg, cfg.Arbitro.RiskPriorsPath, selection.RiskDeps{
			Mon: mon, Hist: hist, Inflight: gauge, Layers: nodeStore, Clock: time.Now,
			Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
			Criteria: criteria, Limits: limits, ThroughputWindow: slWindowDur,
			PSIVeto: selection.PSIVetoThresholds{CPUSomeAvg10: cfg.Arbitro.PSIVetoCPUSomeAvg10, MemFullAvg10: cfg.Arbitro.PSIVetoMemFullAvg10},
		})
		if err != nil {
			log.Fatalf("arbitro: %v", err)
		}
		log.Printf("arbitro: selection modo=risk (risk calibrado) — seletor canônico (v1 aposentado, ADR-0012)")
	default:
		log.Fatalf("arbitro: selector %q inválido (want 'risk' ou 'hl')", cfg.Arbitro.Selector)
	}
	gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{Nodes: nodeStore, Monitor: mon, Selector: selector, RemoteExecutor: cloudConnector, History: hist, Criteria: criteria, Clock: time.Now, DefaultBudget: invokeTimeout, StaleAfter: staleAfterDur, MinSamples: minSamplesN, MaxRaceFanout: cfg.Arbitro.MaxRaceFanout, ThroughputWindow: slWindowDur, CatTokens: catTokens, ObserveArmed: cfg.Arbitro.ObserveArmed}).WithInflight(gauge)
	gw.SetInvocationLog(invLog)
	arb := server.NewServerWithHistory(nodeStore, mon, gw, cred, staleAfterDur, limits, cloudStore, hist, slWindowDur, time.Now)
	arb.SetNodeTokens(nodeTokens)
	arb.SetCatTokens(catTokens)
	if riskStats != nil {
		arb.SetSelectionRisk(riskStats, func() []learn.ModelDump { return riskLearner.Snapshot(time.Now()) })
	}
	log.Printf("arbitro: limits cpu=%.1f mem=%.1f psi=%.1f invoke=%s cloud=%s", limits.MaxCPUPct, limits.MaxMemPct, limits.MaxPSIAvg10, invokeTimeout, cloudPathStr)

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           arb.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("arbitro: credencial em %s | nodes em %s | stale-after=%s", credPathStr, nodesPathStr, staleAfterDur)
	log.Printf("arbitro: escutando em %q (stale-after=%s)", listenAddr, staleAfterDur)
	log.Fatal(srv.ListenAndServe())
}

// The validation of the startup flags duplicates the validation of the
// resolved configuration, and the duplication can be unified once the raw
// flags are validated. A probe showed that every raw bad flag is rejected
// after the resolution, except for an explicitly empty history synchronization
// interval: the configuration validation allows the empty value as unset,
// while this check aborts, so removing it today would turn that input from an
// abort into a boot. It stays until that edge is closed in the configuration.
func validateStartupFlags(historyCap int, historyRetention time.Duration, minSamples int, throughputWindow time.Duration, historySync string, maxRaceFanout int, maxCPU, maxMem, maxPSI float64, invokeTimeout time.Duration, psiVetoCPU, psiVetoMemFull float64) {
	if historyCap <= 0 || minSamples <= 0 || throughputWindow <= 0 {
		log.Fatalf("arbitro: history-cap/min-samples/throughput-window must be >0 (got cap=%d samples=%d window=%s)", historyCap, minSamples, throughputWindow)
	}

	// Retention of the history: zero disables it, and a positive value has to
	// cover the largest reading window, otherwise the cut could delete entries
	// still used by the self-learning. The same pair is validated again after
	// the resolution of the flags, the environment and the file.
	if historyRetention < 0 {
		log.Fatalf("arbitro: history-retention must be >= 0 (0 disables age trimming; got %s)", historyRetention)
	}
	if historyRetention > 0 && historyRetention < throughputWindow {
		log.Fatalf("arbitro: history-retention (%s) must cover selflearning-throughput-window (%s) or be 0", historyRetention, throughputWindow)
	}

	if maxRaceFanout <= 0 {
		log.Fatalf("arbitro: max-race-fanout must be >0 (got %d)", maxRaceFanout)
	}

	// Hard limits: fail fast here over the raw flag values and again through
	// the validation of the configuration after the resolution of the flags,
	// the environment, the file and the defaults.
	if maxCPU <= 0 || maxMem <= 0 || maxPSI <= 0 {
		log.Fatalf("arbitro: max-cpu-pct/max-mem-pct/max-psi-avg10 must be >0 (got cpu=%.2f mem=%.2f psi=%.2f)", maxCPU, maxMem, maxPSI)
	}
	if invokeTimeout <= 0 {
		log.Fatalf("arbitro: invoke-timeout must be >0 (got %s)", invokeTimeout)
	}

	// Full pressure veto: the same single rule as the validation of the
	// configuration and of the veto thresholds, where a non-positive threshold
	// is invalid, with a fail-fast here over the raw flag values and again
	// through the validated configuration after the resolution of the flags,
	// the environment, the file and the defaults.
	if psiVetoCPU <= 0 || psiVetoMemFull <= 0 {
		log.Fatalf("arbitro: psi-veto-cpu/psi-veto-mem-full must be >0 (got cpu=%.2f mem_full=%.2f)", psiVetoCPU, psiVetoMemFull)
	}

	if historySync != "batched" && historySync != "strict" {
		log.Fatalf("arbitro: history-sync must be 'batched' or 'strict' (got %q)", historySync)
	}
}
