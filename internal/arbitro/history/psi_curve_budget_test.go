package history

// Budgets do caminho novo PSI/curva/lookup (task 17,
// selflearning-telemetry-seeded-history).
//
// Cobre o que os benches pré-existentes (history_bench_test.go) não mediam:
// PSICurveFor hit (O(1) cacheado) vs miss (scan windowed + fit + publish) e
// a perna de seed do ExpectedCost (grupo-wide quando o par não tem amostra
// local). Bench/miss determinísticos por construção:
//
//   - hit: mesma chave + mesma janela + now parado => sempre cache;
//   - miss: janelas alternadas (60s/61s) — o guard exige window igual, então
//     TODA iteração é scan+fit+publish, sem append (sem custo estranho);
//   - seed: nó fantasma sem entries locais + doadores na camada => perna de
//     seed a cada miss (janelas alternadas).
//
// Asserts são DETERMINÍSTICOS (alocações, igualdade de curva, tamanho do
// cache) — nenhum assert de wall-time.
//

import (
	"testing"
	"time"
)

// budgetSpread cobre os dois eixos de pressão (nunca colinear), mesmo shape
// de psiFitSpread — duplicado aqui porque aquele helper recebe *testing.T
// (não reutilizável do caminho *testing.B).
var budgetSpread = [][2]float64{
	{1, 2}, {2, 1}, {3, 4}, {4, 3},
	{5, 1}, {1, 5}, {6, 6}, {2, 7},
}

// seedBudgetLaw grava len(budgetSpread) solos elegíveis (Inflight==1,
// CPUPctAvg>0, warm, PSI>0) seguindo lat=100*(1+.05cpu+.03mem).
func seedBudgetLaw(b *testing.B, h *History, group, node, layer string, cpuPct float64, ts time.Time) {
	b.Helper()
	for _, s := range budgetSpread {
		lat := int64(100 * (1 + 0.05*s[0] + 0.03*s[1]))
		e := Entry{
			FunctionName:      group,
			NodeID:            node,
			Layer:             layer,
			Timestamp:         ts,
			ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
			ObservedLatencyMS: lat,
			Outcome:           OutcomeSucesso,
			Inflight:          1,
			CPUPctAvg:         cpuPct,
			PSICpuSomeAvg10:   s[0],
			PSIMemFullAvg10:   s[1],
		}
		if err := h.AppendWithGroup(e, group); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
	}
}

// BenchmarkPSICurveForHit mede o caminho quente cacheado: mesma chave,
// mesma janela, now parado => O(1) sob RLock.
func BenchmarkPSICurveForHit(b *testing.B) {
	h, err := NewHistory("", 1000)
	if err != nil {
		b.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	seedBudgetLaw(b, h, "bench-fn", "bench-node", "borda", 30, now)
	window := 60 * time.Second
	if _, ok := h.PSICurveFor("bench-fn", "bench-node", "borda", window, now); !ok {
		b.Fatalf("PSICurveFor preflight: sem curva")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := h.PSICurveFor("bench-fn", "bench-node", "borda", window, now); !ok {
			b.Fatalf("PSICurveFor hit: sem curva")
		}
	}
}

// BenchmarkPSICurveForMiss mede o caminho de miss: janelas alternadas
// burlam o guard (window faz parte da chave de validade) => scan windowed
// + fit OLS + publish a cada iteração, sem nenhum append no caminho.
func BenchmarkPSICurveForMiss(b *testing.B) {
	h, err := NewHistory("", 1000)
	if err != nil {
		b.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	seedBudgetLaw(b, h, "bench-fn", "bench-node", "borda", 30, now)
	windows := []time.Duration{60 * time.Second, 61 * time.Second}
	if _, ok := h.PSICurveFor("bench-fn", "bench-node", "borda", windows[0], now); !ok {
		b.Fatalf("PSICurveFor preflight: sem curva")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := h.PSICurveFor("bench-fn", "bench-node", "borda", windows[i%2], now); !ok {
			b.Fatalf("PSICurveFor miss: sem curva")
		}
	}
}

// BenchmarkExpectedCostSeededMiss mede a perna de seed: o nó fantasma não
// tem entries locais (count==0) => scan dos segmentos agregados da camada a
// cada miss (janelas alternadas, sem append no caminho).
func BenchmarkExpectedCostSeededMiss(b *testing.B) {
	h, err := NewHistory("", 1000)
	if err != nil {
		b.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	for _, donor := range []string{"donor-1", "donor-2"} {
		seedBudgetLaw(b, h, "bench-fn", donor, "borda", 30, now)
	}
	windows := []time.Duration{60 * time.Second, 61 * time.Second}
	if mean, n, ok, seeded := h.ExpectedCost("bench-fn", "ghost", windows[0], now); !ok || !seeded || n == 0 || mean <= 0 {
		b.Fatalf("ExpectedCost preflight: mean=%v n=%d ok=%v seeded=%v", mean, n, ok, seeded)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, n, ok, seeded := h.ExpectedCost("bench-fn", "ghost", windows[i%2], now); !ok || !seeded || n == 0 {
			b.Fatalf("ExpectedCost seeded miss: n=%d ok=%v seeded=%v", n, ok, seeded)
		}
	}
}

// TestPSICurveForHitBudget: o hit aloca ~0 (só RLock + lookup) e devolve a
// curva bit-idêntica — O(1) por construção, sem wall-time no assert.
func TestPSICurveForHitBudget(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	for _, s := range budgetSpread {
		lat := int64(100 * (1 + 0.05*s[0] + 0.03*s[1]))
		e := Entry{
			FunctionName:      "budget-fn",
			NodeID:            "budget-node",
			Layer:             "borda",
			Timestamp:         now,
			ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
			ObservedLatencyMS: lat,
			Outcome:           OutcomeSucesso,
			Inflight:          1,
			CPUPctAvg:         30,
			PSICpuSomeAvg10:   s[0],
			PSIMemFullAvg10:   s[1],
		}
		if err := h.AppendWithGroup(e, "budget-fn"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	window := 60 * time.Second
	want, ok := h.PSICurveFor("budget-fn", "budget-node", "borda", window, now)
	if !ok {
		t.Fatalf("PSICurveFor preflight: sem curva")
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, ok := h.PSICurveFor("budget-fn", "budget-node", "borda", window, now); !ok {
			t.Errorf("PSICurveFor hit: sem curva")
		}
	}); allocs > 1 {
		t.Fatalf("PSICurveFor hit aloca %v/run, want <=1 (O(1) cacheado)", allocs)
	}
	got, ok := h.PSICurveFor("budget-fn", "budget-node", "borda", window, now)
	if !ok || got != want {
		t.Fatalf("PSICurveFor hit devolveu %+v (ok=%v), want bit-idêntico a %+v", got, ok, want)
	}
}

// TestPSICurveForMissBounded: misses repetidos (janelas alternadas) sempre
// resolvem, convergem para a mesma curva por janela e NÃO crescem o cache —
// a chave é (grupo,nó,camada), a janela vive no entry (overwrite, sem leak).
func TestPSICurveForMissBounded(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	for _, s := range budgetSpread {
		lat := int64(100 * (1 + 0.05*s[0] + 0.03*s[1]))
		e := Entry{
			FunctionName:      "budget-fn",
			NodeID:            "budget-node",
			Layer:             "borda",
			Timestamp:         now,
			ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
			ObservedLatencyMS: lat,
			Outcome:           OutcomeSucesso,
			Inflight:          1,
			CPUPctAvg:         30,
			PSICpuSomeAvg10:   s[0],
			PSIMemFullAvg10:   s[1],
		}
		if err := h.AppendWithGroup(e, "budget-fn"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	windows := []time.Duration{60 * time.Second, 61 * time.Second}
	seen := map[time.Duration]PSICurve{}
	for i := 0; i < 20; i++ {
		w := windows[i%2]
		got, ok := h.PSICurveFor("budget-fn", "budget-node", "borda", w, now)
		if !ok {
			t.Fatalf("PSICurveFor miss iter %d: sem curva", i)
		}
		if prev, dup := seen[w]; dup {
			if got != prev {
				t.Fatalf("PSICurveFor miss iter %d: curva instável %+v vs %+v", i, got, prev)
			}
		} else {
			seen[w] = got
		}
	}
	h.mu.RLock()
	n := len(h.psiCache)
	h.mu.RUnlock()
	if n != 1 {
		t.Fatalf("psiCache tem %d entries após 20 misses, want 1 (overwrite por chave)", n)
	}
}

// TestExpectedCostSeededBudget: fantasma sem local resolve via seed
// (ok+seeded, média dos doadores), o hit aloca ~0 e rescans alternados
// devolvem a mesma média — sem wall-time no assert.
func TestExpectedCostSeededBudget(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	for _, donor := range []string{"donor-1", "donor-2"} {
		for _, s := range budgetSpread {
			lat := int64(100 * (1 + 0.05*s[0] + 0.03*s[1]))
			e := Entry{
				FunctionName:      "budget-fn",
				NodeID:            donor,
				Layer:             "borda",
				Timestamp:         now,
				ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
				ObservedLatencyMS: lat,
				Outcome:           OutcomeSucesso,
				Inflight:          1,
				CPUPctAvg:         30,
				PSICpuSomeAvg10:   s[0],
				PSIMemFullAvg10:   s[1],
			}
			if err := h.AppendWithGroup(e, "budget-fn"); err != nil {
				t.Fatalf("AppendWithGroup: %v", err)
			}
		}
	}
	window := 60 * time.Second
	mean, n, ok, seeded := h.ExpectedCost("budget-fn", "ghost", window, now)
	if !ok || !seeded || n == 0 || mean != 30 {
		t.Fatalf("ExpectedCost fantasma = (%v,%d,%v,%v), want (30,16,true,true)", mean, n, ok, seeded)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, _, ok, _ := h.ExpectedCost("budget-fn", "ghost", window, now); !ok {
			t.Errorf("ExpectedCost seeded hit: !ok")
		}
	}); allocs > 1 {
		t.Fatalf("ExpectedCost seeded hit aloca %v/run, want <=1 (O(1) cacheado)", allocs)
	}
	for _, w := range []time.Duration{61 * time.Second, 60 * time.Second} {
		m, _, ok, seeded := h.ExpectedCost("budget-fn", "ghost", w, now)
		if !ok || !seeded || m != mean {
			t.Fatalf("ExpectedCost rescan %v = (%v,%v,%v), want (%v,true,true)", w, m, ok, seeded, mean)
		}
	}
}
