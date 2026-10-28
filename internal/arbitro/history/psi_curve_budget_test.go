package history

// Budgets of the path of pressure, curve and lookup.
//
// It covers what the existing benchmarks do not measure: a cache hit of the
// pressure curve, which is constant time, against a miss, which scans the
// window, fits the curve and publishes it, and the seeding branch of the
// expected cost, which is group wide when the pair has no local sample. The
// benchmarks are deterministic by construction:
//
//   - the hit uses the same key, the same window and a frozen instant, so the
//     result is always cached;
//   - the miss alternates between two windows, and the guard requires an equal
//     window, so every iteration is a scan, a fit and a publication, with no
//     append and no unexpected cost;
//   - the seed uses a ghost node without local entries and with donors in the
//     layer, so the seeding branch runs on every miss.
//
// The assertions are deterministic, about allocations, the equality of the curve
// and the size of the cache, with no assertion on the wall time.

import (
	"testing"
	"time"
)

// budgetSpread covers both pressure axes, which are never collinear, with the
// same shape as the spread of the fit. It is duplicated here because that
// helper receives a test pointer and cannot be reused in a benchmark.
var budgetSpread = [][2]float64{
	{1, 2}, {2, 1}, {3, 4}, {4, 3},
	{5, 1}, {1, 5}, {6, 6}, {2, 7},
}

// seedBudgetLaw records eligible solo samples, with one request in flight, a
// positive processor usage, a warm instance and a positive pressure, following a
// latency that grows with the processor and the memory usage.
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

// BenchmarkPSICurveForHit measures the hot cached path, with the same key, the
// same window and a frozen instant, which is constant time under the read lock.
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

// BenchmarkPSICurveForMiss measures the miss path: the alternating windows
// defeat the guard, since the window is part of the validity key, so every
// iteration scans the window, fits the curve with the ordinary least squares
// and publishes it, with no append in the path.
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

// BenchmarkExpectedCostSeededMiss measures the seeding branch: the ghost node
// has no local entries, so every miss scans the aggregated segments of the
// layer, with the alternating windows and no append in the path.
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

// TestPSICurveForHitBudget: the hit allocates almost nothing, with only the read
// lock and the lookup, and returns a bit identical curve, which is constant time
// by construction, with no assertion on the wall time.
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

// TestPSICurveForMissBounded: the repeated misses always resolve, converge to
// the same curve per window and do not grow the cache, since the key is the
// triple of group, node and layer, and the window lives in the entry, which is
// overwritten without a leak.
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

// TestExpectedCostSeededBudget: the ghost node without local entries resolves
// through the seed, with the average of the donors, the hit allocates almost
// nothing, and the alternating rescans return the same average, with no assertion
// on the wall time.
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
