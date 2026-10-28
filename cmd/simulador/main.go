// Command simulador runs the Arbiter offline against mocked nodes, which
// lets a recorded invocation log be replayed without any hardware
// hardware.
//
// Usage:
//
//	go run ./cmd/simulador -invlog invocation.jsonl -intervals 5,2 -reps 1 -burst 20s -out results
//
// The gateway, selectors, history and monitor are the production code; only
// the node side of the wire is mocked. The -frio flag starts with empty
// node pools so the first request pays a real cold start, -cloudfrio adds a
// cold cloud tail, and -prime warms the history before the measured stage.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/sim"
)

func main() {
	var (
		invLog       = flag.String("invlog", filepath.Join(os.Getenv("HOME"), "Unioeste/ic/experimentos/dados-sessao-20260924/BRUTOS/arbitro/invocation.jsonl"), "invocation.jsonl real da sessão (calibração)")
		out          = flag.String("out", filepath.Join(os.Getenv("HOME"), "Unioeste/ic/experimentos/simulacao-2026-09-24"), "diretório de saída (invLogs por cenário + resultado.json)")
		historySeed  = flag.String("history", "none", "seed do histórico: none | caminho do history.json real")
		cenariosFlag = flag.String("cenarios", "cad-risk-slo,cad-risk,cad-hl-slo,cad-hl,burst-risk-slo,burst-hl-slo,burst-risk,burst-hl,cloud-risk-slo", "cenários separados por vírgula")
		intervals    = flag.String("intervals", "30,15,10,5,2", "intervalos (s) da cadência do runner")
		warmup       = flag.Int("warmup", 1, "disparos de warmup por intervalo/imagem")
		reps         = flag.Int("reps", 2, "repetições válidas por intervalo/imagem")
		imagens      = flag.Int("imagens", 1, "imagens por intervalo")
		workers      = flag.Int("workers", 6, "workers do cenário burst (sondas da sessão: 6/10)")
		burst        = flag.Duration("burst", 30*time.Second, "duração do burst")
		prime        = flag.Duration("prime", 0, "priming (burst com o SLO do cenário) antes da fase medida nos cenários de risco; 0 desliga")
		sloMS        = flag.Int("slo", 5000, "SLO de latência (ms) dos cenários com SLO")
		seed         = flag.Int64("seed", 42, "seed base do RNG (mesmas condições entre modos)")
		severidade   = flag.Float64("severidade", 1.0, "escala da probabilidade de execução lenta da borda (1.0 = fiel aos dados)")
		cloudFrio    = flag.Float64("cloudfrio", 0.0, "probabilidade da cauda fria/lenta da nuvem (5–14 s); 0 = warm real (1,0–1,9 s)")
		frio         = flag.Bool("frio", false, "inicia os pools dos nós vazios: a 1ª requisição de cada função paga cold start real (amostras da sessão)")
		maxCPU       = flag.Float64("maxcpu", 300, "teto de CPU% do Árbitro (300 desliga o guard; a sessão usou 300)")
		maxMem       = flag.Float64("maxmem", 90, "teto de memória% do Árbitro")
	)
	flag.Parse()

	cal, err := sim.LoadCalibracao(*invLog, *severidade)
	if err != nil {
		fmt.Fprintf(os.Stderr, "simulador: %v\n", err)
		os.Exit(1)
	}

	ival := parseIntervals(*intervals)
	cenarios := buildCenarios(*cenariosFlag, ival, *warmup, *reps, *imagens, *workers, *burst, *sloMS)

	cfg := sim.Config{
		InvLogPath: *invLog, OutDir: *out, HistorySeed: *historySeed,
		Severidade: *severidade, Seed: *seed, MaxCPU: *maxCPU, MaxMem: *maxMem,
		CloudFrioP: *cloudFrio, Prime: *prime, Frio: *frio,
	}

	fmt.Println("=== calibração (dados reais) ===")
	for _, l := range cal.Resumo() {
		fmt.Println(l)
	}
	fmt.Printf("cenários: %s\n\n", strings.Join(nomes(cenarios), ", "))

	var resultados []*sim.Resultado
	for _, cen := range cenarios {
		fmt.Printf("=== %s (modo=%s slo=%d) ===\n", cen.Nome, cen.Modo, cen.SLO)
		res, err := sim.Run(cfg, cen, cal)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cenário %s: %v\n", cen.Nome, err)
			os.Exit(1)
		}
		resultados = append(resultados, res)
		imprime(res)
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "simulador: %v\n", err)
		os.Exit(1)
	}
	rel := map[string]any{
		"gerado_em":    time.Now().Format(time.RFC3339),
		"invlog_real":  *invLog,
		"calibracao":   cal.Resumo(),
		"config":       cfg,
		"cenarios_cfg": cenarios,
		"resultados":   resultados,
	}
	data, _ := json.MarshalIndent(rel, "", "  ")
	dest := filepath.Join(*out, "resultado.json")
	if err := os.WriteFile(dest, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "simulador: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("\nrelatório: %s\n", dest)
}

func buildCenarios(sel string, ival []float64, warmup, reps, imagens, workers int, burst time.Duration, slo int) []sim.Cenario {
	cad := func(nome, modo string, sloMS int) sim.Cenario {
		return sim.Cenario{
			Nome: nome, Modo: modo, Tipo: "cadencia", SLO: sloMS,
			Intervals: ival, Warmup: warmup, Reps: reps, Imagens: imagens,
		}
	}
	brst := func(nome, modo string, sloMS int) sim.Cenario {
		return sim.Cenario{
			Nome: nome, Modo: modo, Tipo: "burst", SLO: sloMS,
			Workers: workers, Duracao: burst,
		}
	}
	todos := map[string]sim.Cenario{
		"cad-hl-slo":     cad("cad-hl-slo", sim.ModoHardLimits, slo),
		"cad-hl":         cad("cad-hl", sim.ModoHardLimits, 0),
		"burst-hl-slo":   brst("burst-hl-slo", sim.ModoHardLimits, slo),
		"burst-hl":       brst("burst-hl", sim.ModoHardLimits, 0),
		"cad-risk-slo":   cad("cad-risk-slo", sim.ModoRisk, slo),
		"cad-risk":       cad("cad-risk", sim.ModoRisk, 0),
		"burst-risk-slo": brst("burst-risk-slo", sim.ModoRisk, slo),
		"burst-risk":     brst("burst-risk", sim.ModoRisk, 0),
		"cloud-risk-slo": {
			Nome: "cloud-risk-slo", Modo: sim.ModoRisk, Tipo: "burst", SLO: slo,
			Workers: 3, Duracao: 12 * time.Second, DerrubarLocais: true,
		},
	}
	var out []sim.Cenario
	for _, nome := range strings.Split(sel, ",") {
		nome = strings.TrimSpace(nome)
		if c, ok := todos[nome]; ok {
			out = append(out, c)
		}
	}
	return out
}

func nomes(cs []sim.Cenario) []string {
	var out []string
	for _, c := range cs {
		out = append(out, c.Nome)
	}
	return out
}

func parseIntervals(s string) []float64 {
	var out []float64
	for _, p := range strings.Split(s, ",") {
		var v float64
		if _, err := fmt.Sscanf(strings.TrimSpace(p), "%g", &v); err == nil && v > 0 {
			out = append(out, v)
		}
	}
	return out
}

func imprime(r *sim.Resultado) {
	inv := r.Invocos
	if inv <= 0 {
		inv = 1
	}
	sloT := r.SLOTotal
	if sloT <= 0 {
		sloT = 1
	}
	fmt.Printf("  invocos=%d ok=%d erros=%d (%.1f%% ok) | p50=%dms p95=%dms p99=%dms\n",
		r.Invocos, r.OK, r.Erros, 100*float64(r.OK)/float64(inv),
		r.P50MS, r.P95MS, r.P99MS)
	if r.SLOTotal > 0 {
		fmt.Printf("  SLO<=%dms: %d/%d (%.1f%%)\n", r.SLO, r.SLOOk, r.SLOTotal,
			100*float64(r.SLOOk)/float64(sloT))
	}
	fmt.Printf("  tentativas=%d orcamento_excedido=%d falhas=%d timeouts=%d cloud=%d\n",
		r.Tentativas, r.OrcamentoExcedido, r.Falhas, r.Timeouts, r.CloudTentativas)
	fmt.Printf("  tentativas por nó: %v\n", r.TentativasPorNo)
	fmt.Printf("  primeira escolha:  %v\n", r.PrimeiroPorNo)
	if e := r.EscolhasRisk; e.Feasible > 0 || e.Probation > 0 {
		fmt.Printf("  escolhas risk: viaveis=%d probation=%d exploracao=%d bootstrap=%d frio=%d\n",
			e.Feasible, e.Probation, e.ExploreFront, e.Bootstrap, e.ColdChosen)
		fmt.Printf("  exclusoes: stretch=%d slo=%d teto=%d projetado=%d banda=%d fila_vazia=%d\n",
			e.StretchExcluded, e.SLOExcluded, e.CapExcluded, e.ProjectedExcluded, e.BandRandom, e.EmptyQueue)
	}
	if len(r.PorCodigo) > 0 {
		fmt.Printf("  erros do cliente:  %v\n", r.PorCodigo)
	}
	if len(r.Alertas) > 0 {
		fmt.Printf("  ALERTAS: %v\n", r.Alertas)
	}
	fmt.Println()
}
