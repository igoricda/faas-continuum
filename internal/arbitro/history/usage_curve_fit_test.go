package history

// Tests for the stateful pooled no-PSI usage estimator (see
// usage_curve_cache.go).
//
// Mirror of psi_curve_fit_test.go: no mocks, no mirrors — every test
// appends real solo Entries with usage features via AppendWithGroup into a
// real History and queries UsageCurveFor. Expectations derive from the
// data-generating law lat = b*(1+alpha*x+beta*y), never from the
// estimator's internals. psiRelErr is reused from psi_curve_fit_test.go
// (same package): one helper, not two.
//
// Names use the TestUsageCurveFit* prefix (plan acceptance shape); no
// collisions with the TestPSICurveFit* names — each name below is new.

import (
	"sync"
	"testing"
	"time"
)

// usageFitSpread covers both usage axes independently (never collinear) so
// the fit is identified — same shape as knownUsageCurvePoints in
// usage_curve_test.go, replayed here as real history Entries.
var usageFitSpread = [][2]float64{
	{10, 20}, {20, 10}, {30, 40}, {40, 30},
	{50, 10}, {10, 50}, {60, 60}, {20, 70},
}

// appendUsageSolo appends one solo-eligible Entry with usage features
// through the production path (primary + AggLayerKey fan-out when layer !=
// ""). X rides ProjectedLoadPct (the dispatch-time load the decision acted
// on), Y rides ObservedMemPct.
func appendUsageSolo(t *testing.T, h *History, group, node, layer string, latencyMS int64, x, y float64, ts time.Time) {
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
		ProjectedLoadPct:  x,
		ObservedMemPct:    y,
	}
	if err := h.AppendWithGroup(e, group); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
}

// appendUsageLaw appends len(spread) solo Entries following
// lat = b*(1+alpha*x+beta*y) (int64 truncation is sub-ms noise).
func appendUsageLaw(t *testing.T, h *History, group, node, layer string, b, alpha, beta float64, ts time.Time) {
	t.Helper()
	for _, s := range usageFitSpread {
		lat := int64(b * (1 + alpha*s[0] + beta*s[1]))
		appendUsageSolo(t, h, group, node, layer, lat, s[0], s[1], ts)
	}
}

// Pooled prior + per-node posterior against the REAL history: a dense
// donor node builds the layer prior (b=100); a thin node (2 points at the
// b=130 level) stays glued to the prior; the dense node converges to its
// local fit; a node with no local entries gets the prior verbatim.
func TestUsageCurveFitPooledPosteriorAgainstHistory(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	const bPrior, alpha, beta = 100.0, 0.01, 0.005
	thinB := 130.0 // variable (not const): keeps int64(...) a runtime conversion
	// Dense layer: 6x spread so the pooled prior resists the thin node's
	// own hot points (the prior pools every node in the layer, thin node
	// included — the motivating dense-layer/thin-node scenario).
	for i := 0; i < 6; i++ {
		appendUsageLaw(t, h, "g", "donor", "borda", bPrior, alpha, beta, ts)
	}
	appendUsageSolo(t, h, "g", "thin", "borda", int64(thinB*(1+0.01*10+0.005*20)), 10, 20, ts)
	appendUsageSolo(t, h, "g", "thin", "borda", int64(thinB*(1+0.01*40+0.005*10)), 40, 10, ts)

	// No local entries at all: posterior IS the pooled prior.
	pooled, ok := h.UsageCurveFor("g", "ghost", "borda", window, now)
	if !ok {
		t.Fatal("UsageCurveFor ghost: ok=false, want prior fallback")
	}
	if psiRelErr(pooled.B, bPrior) >= 0.10 {
		t.Fatalf("pooled prior b=%.3f, want ~%.3f (<10%%)", pooled.B, bPrior)
	}

	thin, ok := h.UsageCurveFor("g", "thin", "borda", window, now)
	if !ok {
		t.Fatal("UsageCurveFor thin: ok=false, want posterior shrunk to prior")
	}
	if thin.N != 2 {
		t.Fatalf("thin N=%d, want 2 (local eligible count)", thin.N)
	}
	if psiRelErr(thin.B, pooled.B) > 0.20 {
		t.Fatalf("thin posterior b=%.3f drifted >20%% from pooled prior b=%.3f", thin.B, pooled.B)
	}

	dense, ok := h.UsageCurveFor("g", "donor", "borda", window, now)
	if !ok {
		t.Fatal("UsageCurveFor dense: ok=false, want local convergence")
	}
	if dense.N != 48 {
		t.Fatalf("dense N=%d, want 48", dense.N)
	}
	if psiRelErr(dense.B, bPrior) >= 0.10 {
		t.Fatalf("dense b=%.3f, want ~%.3f (<10%%)", dense.B, bPrior)
	}

	// Stable data + frozen now => O(1) hit, bit-identical.
	thinAgain, ok := h.UsageCurveFor("g", "thin", "borda", window, now)
	if !ok || thinAgain != thin {
		t.Fatalf("cache hit: got %+v,%v want %+v,true", thinAgain, ok, thin)
	}
}

// ok=false with a zero curve (never silent zeros): empty history, only
// usage-unknown solo entries, only cold/contended entries.
func TestUsageCurveFitNoDataNotOK(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	empty, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	if curve, ok := empty.UsageCurveFor("g", "n1", "borda", window, now); ok || curve != (UsageCurve{}) {
		t.Fatalf("empty: got %+v,%v want zero,false", curve, ok)
	}
	if curve, ok := empty.UsageCurveFor("g", "n1", "borda", 0, now); ok || curve != (UsageCurve{}) {
		t.Fatalf("window<=0: got %+v,%v want zero,false", curve, ok)
	}
	if curve, ok := empty.UsageCurveFor("g", "n1", "borda", window, now); ok || curve != (UsageCurve{}) {
		t.Fatalf("empty map miss path: got %+v,%v want zero,false", curve, ok)
	}
	var nilH *History
	if curve, ok := nilH.UsageCurveFor("g", "n1", "borda", window, now); ok || curve != (UsageCurve{}) {
		t.Fatalf("nil receiver: got %+v,%v want zero,false", curve, ok)
	}

	unknown, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	for i := 0; i < 6; i++ {
		appendUsageSolo(t, unknown, "g", "n1", "borda", 120+int64(i), 0, 0, ts)
	}
	if curve, ok := unknown.UsageCurveFor("g", "n1", "borda", window, now); ok || curve != (UsageCurve{}) {
		t.Fatalf("usage-unknown only: got %+v,%v want zero,false", curve, ok)
	}

	nonSolo, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	cold := Entry{FunctionName: "g", NodeID: "n1", Layer: "borda", Timestamp: ts,
		ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 5000,
		Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 10,
		ProjectedLoadPct: 20, ObservedMemPct: 30, ColdStart: true}
	if err := nonSolo.AppendWithGroup(cold, "g"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	cont := cold
	cont.ColdStart = false
	cont.Inflight = 2
	if err := nonSolo.AppendWithGroup(cont, "g"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	if curve, ok := nonSolo.UsageCurveFor("g", "n1", "borda", window, now); ok || curve != (UsageCurve{}) {
		t.Fatalf("cold/contended only: got %+v,%v want zero,false", curve, ok)
	}
}

// Usage-unknown entries never pull the fit: valid law plus huge-latency
// solo outliers with (0,0) usage must leave the fit on the valid law.
func TestUsageCurveFitUnknownNeverPulls(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	const bReal, alpha, beta = 100.0, 0.01, 0.005
	appendUsageLaw(t, h, "g", "n1", "borda", bReal, alpha, beta, ts)
	appendUsageSolo(t, h, "g", "n1", "borda", 9999, 0, 0, ts) // both unknown
	appendUsageSolo(t, h, "g", "n1", "borda", 9999, 0, 0, ts) // both unknown

	curve, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("UsageCurveFor: ok=false, want true")
	}
	if curve.N != 8 {
		t.Fatalf("N=%d, want 8 (usage-unknown excluded)", curve.N)
	}
	if psiRelErr(curve.B, bReal) >= 0.10 {
		t.Fatalf("b=%.3f pulled by usage-unknown outliers, want ~%.3f", curve.B, bReal)
	}
}

// Either-axis gate through the full path: points with X>0 but Y==0 train.
// A history with ONLY X-known points still fits.
func TestUsageCurveFitEitherAxisEligible(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	const bReal, alpha, beta = 100.0, 0.01, 0.005
	// Full-spread base on the law plane (both axes vary: fit identified).
	for _, s := range [][2]float64{{10, 20}, {20, 10}, {30, 40}, {40, 30}} {
		appendUsageSolo(t, h, "g", "n1", "borda", int64(bReal*(1+alpha*s[0]+beta*s[1])), s[0], s[1], ts)
	}
	// Either-axis tail: Y unknown (0), X on the SAME law plane at y=0, so
	// the joint set stays identified while proving Y==0 points train.
	for _, x := range []float64{50, 10, 60, 20} {
		appendUsageSolo(t, h, "g", "n1", "borda", int64(bReal*(1+alpha*x)), x, 0, ts)
	}
	curve, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("UsageCurveFor: ok=false, want true (X-known points must train)")
	}
	if curve.N != 8 {
		t.Fatalf("N=%d, want 8", curve.N)
	}
	if psiRelErr(curve.B, bReal) >= 0.10 {
		t.Fatalf("b=%.3f, want ~%.3f (<10%%)", curve.B, bReal)
	}
}

// ProjectedLoadPct==0 falls back to ObservedCPUPct through the full path:
// entries carrying X only in the observed field still train on the law.
func TestUsageCurveFitProjectedFallback(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	const bReal, alpha, beta = 100.0, 0.01, 0.005
	for _, s := range usageFitSpread {
		lat := int64(bReal * (1 + alpha*s[0] + beta*s[1]))
		e := Entry{
			FunctionName: "g", NodeID: "n1", Layer: "borda", Timestamp: ts,
			ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
			ObservedLatencyMS: lat, Outcome: OutcomeSucesso,
			Inflight: 1, CPUPctAvg: 10,
			ObservedCPUPct: s[0], ObservedMemPct: s[1], // no projection
		}
		if err := h.AppendWithGroup(e, "g"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	curve, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("UsageCurveFor: ok=false, want true (observed fallback must train)")
	}
	if curve.N != 8 {
		t.Fatalf("N=%d, want 8", curve.N)
	}
	if psiRelErr(curve.B, bReal) >= 0.10 {
		t.Fatalf("b=%.3f, want ~%.3f (<10%%)", curve.B, bReal)
	}
}

// Cache invalidation on append: the pair gen bump must yield a fresh fit,
// never a stale hit. Also pins the cache key + gen linkage on the SEPARATE
// usageCache map.
func TestUsageCurveFitCacheInvalidatedOnAppend(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	appendUsageLaw(t, h, "g", "n1", "borda", 100, 0.01, 0.005, ts)
	c1, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("first query: ok=false, want true")
	}
	h.mu.RLock()
	e, hit := h.usageCache["g\x00n1\x00borda"]
	pairGen := h.costGen["g\x00n1"]
	h.mu.RUnlock()
	if !hit {
		t.Fatal("usageCache not populated after miss")
	}
	if e.gen != pairGen {
		t.Fatalf("cached gen=%d != pair gen=%d", e.gen, pairGen)
	}

	appendUsageSolo(t, h, "g", "n1", "borda", 5000, 10, 20, ts)
	c2, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
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
	e2, hit := h.usageCache["g\x00n1\x00borda"]
	pairGen2 := h.costGen["g\x00n1"]
	h.mu.RUnlock()
	if !hit || e2.gen != pairGen2 || pairGen2 != pairGen+1 {
		t.Fatalf("post-append cache linkage broken: hit=%v gen=%d pairGen=%d->%d", hit, e2.gen, pairGen, pairGen2)
	}
}

// No prior without layer fan-out: entries appended with Layer=="" write
// only the primary segment, so a query for any layer falls back to the raw
// local fit.
func TestUsageCurveFitNoPriorFallsBackToLocal(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	const bLocal = 110.0
	appendUsageLaw(t, h, "g", "n1", "", bLocal, 0.01, 0.005, ts)
	for _, k := range h.Keys() {
		if IsAggLayerKey(k) {
			t.Fatalf("layerless append fanned out to agg key %q", k)
		}
	}
	curve, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("UsageCurveFor: ok=false, want local-only fit")
	}
	if psiRelErr(curve.B, bLocal) >= 0.10 {
		t.Fatalf("b=%.3f, want local ~%.3f (<10%%)", curve.B, bLocal)
	}
}

// Cache separation: the usage and PSI caches never serve each other's data.
// PSI-only entries (PSI known, usage unknown) fit a PSI curve but no usage
// curve; usage-only entries (usage known, PSI zero) fit a usage curve but
// no PSI curve.
func TestUsageCurveFitCacheSeparatesPSICache(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	for _, s := range usageFitSpread {
		e := Entry{
			FunctionName: "gp", NodeID: "psi-only", Layer: "borda", Timestamp: ts,
			ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
			ObservedLatencyMS: int64(100 * (1 + 0.5*s[0] + 0.3*s[1])),
			Outcome:           OutcomeSucesso,
			Inflight:          1,
			CPUPctAvg:         10,
			PSICpuSomeAvg10:   s[0],
			PSIMemFullAvg10:   s[1],
		}
		if err := h.AppendWithGroup(e, "gp"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	// Separate group ("gu") so the usage layer prior cannot leak into the
	// PSI node's query and vice versa — the prior pools per (grupo,camada).
	appendUsageLaw(t, h, "gu", "usage-only", "borda", 100, 0.01, 0.005, ts)

	if _, ok := h.UsageCurveFor("gp", "psi-only", "borda", window, now); ok {
		t.Fatal("UsageCurveFor psi-only node: ok=true, want false (usage unknown)")
	}
	if _, ok := h.PSICurveFor("gp", "psi-only", "borda", window, now); !ok {
		t.Fatal("PSICurveFor psi-only node: ok=false, want true")
	}
	if _, ok := h.UsageCurveFor("gu", "usage-only", "borda", window, now); !ok {
		t.Fatal("UsageCurveFor usage-only node: ok=false, want true")
	}
	if _, ok := h.PSICurveFor("gu", "usage-only", "borda", window, now); ok {
		t.Fatal("PSICurveFor usage-only node: ok=true, want false (PSI unknown)")
	}
}

// futureFrom guard: a solo eligible entry with ts > computedAt is excluded
// from the fit, and advancing now past it refreshes (never a stale hit
// omitting entered data).
func TestUsageCurveFitFutureFromGuard(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	const bReal, alpha, beta = 100.0, 0.01, 0.005
	appendUsageLaw(t, h, "g", "n1", "borda", bReal, alpha, beta, ts)
	futureTS := now.Add(10 * time.Minute)
	appendUsageSolo(t, h, "g", "n1", "borda", 5000, 10, 20, futureTS)

	before, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("UsageCurveFor at now: ok=false, want true")
	}
	if psiRelErr(before.B, bReal) >= 0.10 {
		t.Fatalf("b=%.3f includes future hot sample, want ~%.3f", before.B, bReal)
	}
	if before.N != 8 {
		t.Fatalf("N=%d, want 8 (future entry excluded)", before.N)
	}

	later := futureTS.Add(time.Minute)
	after, ok := h.UsageCurveFor("g", "n1", "borda", window, later)
	if !ok {
		t.Fatal("UsageCurveFor past futureFrom: ok=false, want true")
	}
	if after.N != 9 {
		t.Fatalf("N=%d, want 9 (future entry entered)", after.N)
	}
	if after.B <= before.B {
		t.Fatalf("refit b=%.3f should exceed pre-entry b=%.3f (hot sample entered)", after.B, before.B)
	}
}

// oldest guard: a positive hit expires once the oldest included entry ages
// out of the window — the recompute goes negative instead of serving stale
// latency, and the negative stays exact until mutation.
func TestUsageCurveFitOldestGuard(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-50 * time.Minute)

	appendUsageLaw(t, h, "g", "n1", "borda", 100, 0.01, 0.005, ts)
	if _, ok := h.UsageCurveFor("g", "n1", "borda", window, now); !ok {
		t.Fatal("UsageCurveFor at now: ok=false, want true")
	}
	// Frozen now reuses the positive entry (oldest+window not exceeded).
	if _, ok := h.UsageCurveFor("g", "n1", "borda", window, now); !ok {
		t.Fatal("UsageCurveFor frozen now: ok=false, want cached positive hit")
	}

	aged := ts.Add(window + time.Minute)
	if curve, ok := h.UsageCurveFor("g", "n1", "borda", window, aged); ok || curve != (UsageCurve{}) {
		t.Fatalf("aged-out: got %+v,%v want zero,false (no stale positive)", curve, ok)
	}
	// Negative stays exact until pair mutation.
	if curve, ok := h.UsageCurveFor("g", "n1", "borda", window, aged); ok || curve != (UsageCurve{}) {
		t.Fatalf("negative re-query: got %+v,%v want zero,false", curve, ok)
	}
}

// Concurrent AppendWithGroup during curve queries: correct values, no
// stale hits after gen invalidation, clean under -race.
func TestUsageCurveFitConcurrentAppendRace(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	ts := now.Add(-time.Minute)

	appendUsageLaw(t, h, "g", "n1", "borda", 100, 0.01, 0.005, ts)
	base, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
	if !ok || base.B <= 0 {
		t.Fatalf("seed query: %+v,%v", base, ok)
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				appendUsageSolo(t, h, "g", "n1", "borda", 100+int64(i), 10, 20, ts)
			}
		}()
	}
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				curve, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
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

	final, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
	if !ok || final.B <= 0 {
		t.Fatalf("final query: %+v,%v", final, ok)
	}
	h.mu.RLock()
	e, hit := h.usageCache["g\x00n1\x00borda"]
	pairGen := h.costGen["g\x00n1"]
	h.mu.RUnlock()
	if !hit || e.gen != pairGen {
		t.Fatalf("final cache linkage broken: hit=%v gen=%d pairGen=%d", hit, e.gen, pairGen)
	}
}
