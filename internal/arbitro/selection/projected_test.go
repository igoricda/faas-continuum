package selection

import (
	"math"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// appendClassEntries grava n entradas de sucesso com classe explícita
// (inflight), amostra de CPU e PSI em ambos os eixos — o invariante
// solo+PSI do ExpectedCost (psiSoloEligible: Inflight==1, CPUPctAvg>0,
// !ColdStart, sucesso e PSI>0 nos dois eixos).
func appendClassEntries(t *testing.T, h *history.History, fn, nodeID, bucket string, lat int64, inflight int, cpuPct float64, n int, now time.Time) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := h.AppendWithGroup(history.Entry{
			FunctionName:      fn,
			NodeID:            nodeID,
			Timestamp:         now.Add(-time.Second),
			ScenarioContext:   history.ScenarioContext{LoadBucket: bucket},
			ObservedLatencyMS: lat,
			Outcome:           history.OutcomeSucesso,
			Inflight:          inflight,
			CPUPctAvg:         cpuPct,
			PSICpuSomeAvg10:   5,
			PSIMemFullAvg10:   5,
		}, fn); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
}

// TestEffectiveContentionFactor pina o predicado do fator APRENDIDO
// (clamp[1,8], pisos por classe): o history_summary do server consome o
// MESMO predicado via MinClassSamples exportado.
func TestEffectiveContentionFactor(t *testing.T) {
	cases := []struct {
		name                          string
		soloP95, contP95              int64
		soloN, contN, minClassSamples int
		wantOK                        bool
		wantFactor                    float64
	}{
		{name: "zero solo p95", soloP95: 0, contP95: 2300, soloN: 5, contN: 5, minClassSamples: 5, wantOK: false},
		{name: "zero cont p95", soloP95: 1000, contP95: 0, soloN: 5, contN: 5, minClassSamples: 5, wantOK: false},
		{name: "thin solo class", soloP95: 1000, contP95: 2300, soloN: 4, contN: 5, minClassSamples: 5, wantOK: false},
		{name: "thin cont class", soloP95: 1000, contP95: 2300, soloN: 5, contN: 4, minClassSamples: 5, wantOK: false},
		{name: "learned 2.3x", soloP95: 1000, contP95: 2300, soloN: 5, contN: 5, minClassSamples: 5, wantOK: true, wantFactor: 2.3},
		{name: "clamp low to 1", soloP95: 1000, contP95: 500, soloN: 5, contN: 5, minClassSamples: 5, wantOK: true, wantFactor: 1},
		{name: "clamp high to 8", soloP95: 100, contP95: 10000, soloN: 5, contN: 5, minClassSamples: 5, wantOK: true, wantFactor: 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := EffectiveContentionFactor(tc.soloP95, tc.contP95, tc.soloN, tc.contN, tc.minClassSamples)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v want=%v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				if got != 0 {
					t.Fatalf("factor=%v want 0 when !ok", got)
				}
				return
			}
			if math.Abs(got-tc.wantFactor) > 1e-9 {
				t.Fatalf("factor=%v want %v", got, tc.wantFactor)
			}
		})
	}
}

// TestProjectedLoadFormula pinna a fórmula ÚNICA de carga projetada
// (observed + custo em voo desde recvAt + custo próprio): a mesma do
// record do gateway e do SHIELD do RiskSelector. Nil-safe em todos os
// leitores (Mon/Hist/gauges nil => projected 0 => bucket unknown).
func TestProjectedLoadFormula(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	group, nodeID := "myfn", "n1"

	h, _ := history.NewHistory("", 500)
	// Custo próprio: média de CPUPctAvg sobre entradas solo (Inflight==1).
	appendClassEntries(t, h, group, nodeID, history.Bucket0_10, 100, 1, 20, 3, now)

	mon := monitor.NewMonitor()
	mon.UpdateAt(telemetry.Package{NodeID: nodeID, CPUUsagePct: 30}, now)
	projCost := func(id string, _ time.Time) float64 {
		if id == nodeID {
			return 10
		}
		return 0
	}

	pkg, recvAt, monOK, projected, sc := projectedLoad(mon, h, projCost, group, nodeID, time.Minute, now)
	if !monOK {
		t.Fatal("monOK deveria ser true com telemetria fresca")
	}
	if pkg.CPUUsagePct != 30 {
		t.Fatalf("pkg.CPUUsagePct=%v want 30", pkg.CPUUsagePct)
	}
	if recvAt.IsZero() {
		t.Fatal("recvAt deveria vir do monitor (relógio do servidor na ingestão)")
	}
	// 30 (observada) + 10 (voo desde recvAt) + 20 (custo próprio) = 60.
	if projected != 60 {
		t.Fatalf("projected=%v want 60 (30+10+20)", projected)
	}
	if sc.LoadBucket != history.LoadBucket(60) {
		t.Fatalf("bucket=%q want %q (derivado de projected)", sc.LoadBucket, history.LoadBucket(60))
	}

	// Nil-safe: sem Mon/Hist/gauge => projected 0 e bucket unknown.
	_, _, _, projected, sc = projectedLoad(nil, nil, nil, group, nodeID, time.Minute, now)
	if projected != 0 || sc.LoadBucket != history.BucketUnknown {
		t.Fatalf("nil-safe: projected=%v bucket=%q want 0/unknown", projected, sc.LoadBucket)
	}

	// Só telemetria, sem histórico/gauge: observed puro.
	_, _, _, projected, _ = projectedLoad(mon, nil, nil, group, nodeID, time.Minute, now)
	if projected != 30 {
		t.Fatalf("só observada: projected=%v want 30", projected)
	}
}

// TestProjectedLoadSoloNuncaEntraNoCusto confirma o invariante do
// ExpectedCost: entradas contendidas (Inflight>1) NÃO entram na média do
// custo próprio (o estimador seria circular).
func TestProjectedLoadSoloNuncaEntraNoCusto(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	group, nodeID := "myfn", "n1"
	h, _ := history.NewHistory("", 500)
	// 5 solos a 20% e 5 contidos a 90%: a média só pode ver os solos.
	appendClassEntries(t, h, group, nodeID, history.Bucket0_10, 100, 1, 20, 5, now)
	appendClassEntries(t, h, group, nodeID, history.Bucket0_10, 900, 2, 90, 5, now)
	cost, _, _, _ := h.ExpectedCost(group, nodeID, time.Minute, now)
	if cost != 20 {
		t.Fatalf("ExpectedCost=%v want 20 (só solos)", cost)
	}
}

// TestProjectedLoadNegativeClamped garante que custo de voo negativo
// (relógio/recvAt degenerado) nunca produz projected negativo.
func TestProjectedLoadNegativeClamped(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	group, nodeID := "myfn", "n1"
	h, _ := history.NewHistory("", 500)
	appendClassEntries(t, h, group, nodeID, history.Bucket0_10, 100, 1, 20, 3, now)
	mon := monitor.NewMonitor()
	mon.UpdateAt(telemetry.Package{NodeID: nodeID, CPUUsagePct: 10}, now)
	projCost := func(string, time.Time) float64 { return -100 }
	_, _, _, projected, _ := projectedLoad(mon, h, projCost, group, nodeID, time.Minute, now)
	if projected < 0 {
		t.Fatalf("projected=%v não pode ser negativo", projected)
	}
}

// TestSelectBootstrapOrdersByCPU é uma regressão de integração do SHIELD:
// com learner frio (tudo probation) o RiskSelector devolve a fila por
// bootstrap (layerRank, k_fn, cpu) — o nó com CPU menor lidera.
func TestSelectBootstrapOrdersByCPU(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	fn := "myfn"
	mon := monitor.NewMonitor()
	mon.UpdateAt(telemetry.Package{NodeID: "b1", CPUUsagePct: 10}, now)
	mon.UpdateAt(telemetry.Package{NodeID: "b2", CPUUsagePct: 50}, now)
	n1 := nodes.Node{NodeID: "b1", Layer: nodes.LayerBorda, Host: "h1"}
	n2 := nodes.Node{NodeID: "b2", Layer: nodes.LayerBorda, Host: "h2"}
	cfg := DefaultRiskConfig()
	sel := &RiskSelector{
		Base:             QueueSelector{Mon: mon, Cfg: defaultLimits},
		ThroughputWindow: time.Minute,
		Clock:            fixedClock(now),
		Learner:          learn.NewLearner(cfg.LearnConfig()),
		Cfg:              cfg,
	}
	out := sel.Select([]nodes.Node{n2, n1}, fn, nil)
	if ids := nodeIDs(out); len(ids) != 2 || ids[0] != "b1" {
		t.Fatalf("bootstrap deveria ordenar por cpu (b1 primeiro), obteve %v", ids)
	}
}
