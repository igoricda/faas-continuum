package history

// Tests for the stateful pooled PSI estimator (see psi_curve_cache.go).
//
// No mocks, no mirrors: every test appends real solo Entries with PSI via
// AppendWithGroup into a real History and queries PSICurveFor. Expectations
// derive from the data-generating law lat = b*(1+alpha*cpu+beta*mem), never
// from the estimator's internals.
//
// Names use the TestPSICurveFit* prefix (plan acceptance name); existing
// TestPSICurveFitRecoversKnownB (pure fit, psi_curve_test.go) also matches
// -run 'TestPSICurveFit' and stays green — no collisions, each name below
// is new.

import (
	"math"
	"sync"
	"testing"
	"time"
)

// psiFitSpread covers both PSI axes independently (never collinear) so the
// fit is identified — same shape as knownCurvePoints in psi_curve_test.go,
// replayed here as real history Entries.
var psiFitSpread = [][2]float64{
	{1, 2}, {2, 1}, {3, 4}, {4, 3},
	{5, 1}, {1, 5}, {6, 6}, {2, 7},
}

// appendPSISolo appends one solo-eligible Entry with explicit PSI through
// the production path (primary + AggLayerKey fan-out when layer != "").
func appendPSISolo(t *testing.T, h *History, group, node, layer string, latencyMS int64, cpuSome, memFull float64, ts time.Time) {
	t.Helper()
	e := Entry{
		FunctionName:      group,
		NodeID:            node,
		Layer:             layer,
		Timestamp:         ts,
		ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
		ObservedLatencyMS: latencyMS,
		Outcome:           OutcomeSucesso,
		Inflight:          1,
		CPUPctAvg:         10,
		PSICpuSomeAvg10:   cpuSome,
		PSIMemFullAvg10:   memFull,
	}
	if err := h.AppendWithGroup(e, group); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
}

// appendPSILaw appends len(spread) solo Entries following
// lat = b*(1+alpha*cpu+beta*mem) (int64 truncation is sub-ms noise).
func appendPSILaw(t *testing.T, h *History, group, node, layer string, b, alpha, beta float64, ts time.Time) {
	t.Helper()
	for _, s := range psiFitSpread {
		lat := int64(b * (1 + alpha*s[0] + beta*s[1]))
		appendPSISolo(t, h, group, node, layer, lat, s[0], s[1], ts)
	}
}

func psiRelErr(got, want float64) float64 {
	return math.Abs(got-want) / math.Abs(want)
}

// Pooled prior + per-node posterior against the REAL history: a dense
// donor node builds the layer prior (b=100); a thin node (2 points at the
// b=130 level) stays glued to the prior; the dense node converges to its
// local fit; a node with no local entries gets the prior verbatim.
func TestPSICurveFitPooledPosteriorAgainstHistory(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	const bPrior, alpha, beta = 100.0, 0.5, 0.3
	// Dense layer: 6x spread so the pooled prior resists the thin node's
	// own hot points (the prior pools every node in the layer, thin node
	// included — the motivating dense-layer/thin-node scenario).
	for i := 0; i < 6; i++ {
		appendPSILaw(t, h, "g", "donor", "borda", bPrior, alpha, beta, ts)
	}
	appendPSISolo(t, h, "g", "thin", "borda", int64(130*(1+0.5*1+0.3*2)), 1, 2, ts)
	appendPSISolo(t, h, "g", "thin", "borda", int64(130*(1+0.5*4+0.3*1)), 4, 1, ts)

	// No local entries at all: posterior IS the pooled prior.
	pooled, ok := h.PSICurveFor("g", "ghost", "borda", window, now)
	if !ok {
		t.Fatal("PSICurveFor ghost: ok=false, want prior fallback")
	}
	if psiRelErr(pooled.B, bPrior) >= 0.10 {
		t.Fatalf("pooled prior b=%.3f, want ~%.3f (<10%%)", pooled.B, bPrior)
	}

	thin, ok := h.PSICurveFor("g", "thin", "borda", window, now)
	if !ok {
		t.Fatal("PSICurveFor thin: ok=false, want posterior shrunk to prior")
	}
	if thin.N != 2 {
		t.Fatalf("thin N=%d, want 2 (local eligible count)", thin.N)
	}
	if psiRelErr(thin.B, pooled.B) > 0.20 {
		t.Fatalf("thin posterior b=%.3f drifted >20%% from pooled prior b=%.3f", thin.B, pooled.B)
	}

	dense, ok := h.PSICurveFor("g", "donor", "borda", window, now)
	if !ok {
		t.Fatal("PSICurveFor dense: ok=false, want local convergence")
	}
	if dense.N != 48 {
		t.Fatalf("dense N=%d, want 48", dense.N)
	}
	if psiRelErr(dense.B, bPrior) >= 0.10 {
		t.Fatalf("dense b=%.3f, want ~%.3f (<10%%)", dense.B, bPrior)
	}

	// Stable data + frozen now => O(1) hit, bit-identical.
	thinAgain, ok := h.PSICurveFor("g", "thin", "borda", window, now)
	if !ok || thinAgain != thin {
		t.Fatalf("cache hit: got %+v,%v want %+v,true", thinAgain, ok, thin)
	}
}

// ok=false with a zero curve (never silent zeros): empty history, only
// PSI==0 solo entries, only cold/contended entries.
func TestPSICurveFitNoDataNotOK(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	empty, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	if curve, ok := empty.PSICurveFor("g", "n1", "borda", window, now); ok || curve != (PSICurve{}) {
		t.Fatalf("empty: got %+v,%v want zero,false", curve, ok)
	}
	if curve, ok := empty.PSICurveFor("g", "n1", "borda", 0, now); ok || curve != (PSICurve{}) {
		t.Fatalf("window<=0: got %+v,%v want zero,false", curve, ok)
	}

	unknown, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	for i := 0; i < 6; i++ {
		appendPSISolo(t, unknown, "g", "n1", "borda", 120+int64(i), 0, 0, ts)
	}
	if curve, ok := unknown.PSICurveFor("g", "n1", "borda", window, now); ok || curve != (PSICurve{}) {
		t.Fatalf("PSI==0 only: got %+v,%v want zero,false", curve, ok)
	}

	nonSolo, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	cold := Entry{FunctionName: "g", NodeID: "n1", Layer: "borda", Timestamp: ts,
		ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 5000,
		Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 10,
		PSICpuSomeAvg10: 2, PSIMemFullAvg10: 3, ColdStart: true}
	if err := nonSolo.AppendWithGroup(cold, "g"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	cont := cold
	cont.ColdStart = false
	cont.Inflight = 2
	if err := nonSolo.AppendWithGroup(cont, "g"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	if curve, ok := nonSolo.PSICurveFor("g", "n1", "borda", window, now); ok || curve != (PSICurve{}) {
		t.Fatalf("cold/contended only: got %+v,%v want zero,false", curve, ok)
	}
}

// PSI==0 entries never pull the fit: valid law plus huge-latency solo
// outliers with unknown PSI must leave the fit on the valid law.
func TestPSICurveFitZeroPSINeverPulls(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	const bReal, alpha, beta = 100.0, 0.5, 0.3
	appendPSILaw(t, h, "g", "n1", "borda", bReal, alpha, beta, ts)
	appendPSISolo(t, h, "g", "n1", "borda", 9999, 0, 3, ts) // cpu unknown
	appendPSISolo(t, h, "g", "n1", "borda", 9999, 2, 0, ts) // mem unknown
	appendPSISolo(t, h, "g", "n1", "borda", 9999, 0, 0, ts) // both unknown

	curve, ok := h.PSICurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("PSICurveFor: ok=false, want true")
	}
	if curve.N != 8 {
		t.Fatalf("N=%d, want 8 (PSI==0 excluded)", curve.N)
	}
	if psiRelErr(curve.B, bReal) >= 0.10 {
		t.Fatalf("b=%.3f pulled by PSI==0 outliers, want ~%.3f", curve.B, bReal)
	}
}

// Cache invalidation on append: the pair gen bump must yield a fresh fit,
// never a stale hit. Also pins the cache key + gen linkage.
func TestPSICurveFitCacheInvalidatedOnAppend(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	appendPSILaw(t, h, "g", "n1", "borda", 100, 0.5, 0.3, ts)
	c1, ok := h.PSICurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("first query: ok=false, want true")
	}
	h.mu.RLock()
	e, hit := h.psiCache["g\x00n1\x00borda"]
	pairGen := h.costGen["g\x00n1"]
	h.mu.RUnlock()
	if !hit {
		t.Fatal("psiCache not populated after miss")
	}
	if e.gen != pairGen {
		t.Fatalf("cached gen=%d != pair gen=%d", e.gen, pairGen)
	}

	appendPSISolo(t, h, "g", "n1", "borda", 5000, 1, 2, ts)
	c2, ok := h.PSICurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("post-append query: ok=false, want true")
	}
	if c2.B == c1.B {
		t.Fatalf("stale hit: b unchanged at %.3f after append", c2.B)
	}
	if c2.B <= c1.B {
		t.Fatalf("fresh fit b=%.3f should exceed pre-append b=%.3f (hot sample added)", c2.B, c1.B)
	}
	h.mu.RLock()
	e2, hit := h.psiCache["g\x00n1\x00borda"]
	pairGen2 := h.costGen["g\x00n1"]
	h.mu.RUnlock()
	if !hit || e2.gen != pairGen2 || pairGen2 != pairGen+1 {
		t.Fatalf("post-append cache linkage broken: hit=%v gen=%d pairGen=%d->%d", hit, e2.gen, pairGen, pairGen2)
	}
}

// No prior without layer fan-out: entries appended with Layer=="" write
// only the primary segment, so a query for any layer falls back to the raw
// local fit.
func TestPSICurveFitNoPriorFallsBackToLocal(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	const bLocal = 110.0
	appendPSILaw(t, h, "g", "n1", "", bLocal, 0.5, 0.3, ts)
	for _, k := range h.Keys() {
		if IsAggLayerKey(k) {
			t.Fatalf("layerless append fanned out to agg key %q", k)
		}
	}
	curve, ok := h.PSICurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("PSICurveFor: ok=false, want local-only fit")
	}
	if psiRelErr(curve.B, bLocal) >= 0.10 {
		t.Fatalf("b=%.3f, want local ~%.3f (<10%%)", curve.B, bLocal)
	}
}

// Concurrent AppendWithGroup during curve queries: correct values, no
// stale hits after gen invalidation, clean under -race.
func TestPSICurveFitConcurrentAppendRace(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	appendPSILaw(t, h, "g", "n1", "borda", 100, 0.5, 0.3, ts)
	base, ok := h.PSICurveFor("g", "n1", "borda", window, now)
	if !ok || base.B <= 0 {
		t.Fatalf("seed query: %+v,%v", base, ok)
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				appendPSISolo(t, h, "g", "n1", "borda", 100+int64(i), 1, 2, ts)
			}
		}()
	}
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				curve, ok := h.PSICurveFor("g", "n1", "borda", window, now)
				if !ok {
					t.Error("concurrent query: ok=false, want true")
					return
				}
				if curve.B <= 0 {
					t.Errorf("concurrent query: b=%.3f, want >0", curve.B)
					return
				}
			}
		}()
	}
	wg.Wait()

	final, ok := h.PSICurveFor("g", "n1", "borda", window, now)
	if !ok || final.B <= 0 {
		t.Fatalf("final query: %+v,%v", final, ok)
	}
	h.mu.RLock()
	e, hit := h.psiCache["g\x00n1\x00borda"]
	pairGen := h.costGen["g\x00n1"]
	h.mu.RUnlock()
	if !hit || e.gen != pairGen {
		t.Fatalf("final cache linkage broken: hit=%v gen=%d pairGen=%d", hit, e.gen, pairGen)
	}
}
