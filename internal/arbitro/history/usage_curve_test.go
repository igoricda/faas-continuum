package history

// TDD tests for the no-PSI usage curve estimator (see usage_curve.go).
//
// Mirror of psi_curve_test.go over usage-continuous features: all tests use
// synthetic points with independently known answers — no fixture coupling,
// no mocks, no pinned implementation constants. Each test derives its
// expectation from the data-generating parameters (b/alpha/beta), never
// from the estimator's internals. relErr is reused from psi_curve_test.go
// (same package): one helper, not two.

import (
	"testing"
)

// soloUsagePoint builds a solo-eligible observation carrying usage features.
// Gate mirrored: Inflight==1 && CPUPctAvg>0 && !ColdStart &&
// Outcome==sucesso (see ExpectedCost in history_stats.go).
func soloUsagePoint(latencyMS, x, y float64) UsagePoint {
	return UsagePoint{
		LatencyMS: latencyMS,
		X:         x,
		Y:         y,
		Inflight:  1,
		CPUPctAvg: 10,
		Outcome:   OutcomeSucesso,
	}
}

// knownUsageCurvePoints generates n spread (x,y) observations from the exact
// law lat = b*(1+alpha*x+beta*y) on a CPU-load-pct scale. Spread covers both
// axes independently (never collinear) so the fit is identified.
func knownUsageCurvePoints(b, alpha, beta float64) []UsagePoint {
	spread := [][2]float64{
		{10, 20}, {20, 10}, {30, 40}, {40, 30},
		{50, 10}, {10, 50}, {60, 60}, {20, 70},
	}
	pts := make([]UsagePoint, 0, len(spread))
	for _, s := range spread {
		lat := b * (1 + alpha*s[0] + beta*s[1])
		pts = append(pts, soloUsagePoint(lat, s[0], s[1]))
	}
	return pts
}

// (a) 8 synthetic solo spread points with known b => |b-b_real|/b_real<10%.
func TestUsageCurveFitRecoversKnownB(t *testing.T) {
	const bReal, alphaReal, betaReal = 100.0, 0.01, 0.005
	curve, ok := FitUsageCurve(knownUsageCurvePoints(bReal, alphaReal, betaReal))
	if !ok {
		t.Fatal("FitUsageCurve: ok=false on 8 clean solo points")
	}
	if curve.N != 8 {
		t.Fatalf("FitUsageCurve: N=%d, want 8", curve.N)
	}
	if relErr(curve.B, bReal) >= 0.10 {
		t.Fatalf("|b-b_real|/b_real = %.4f (b=%.3f, want %.3f), need <10%%",
			relErr(curve.B, bReal), curve.B, bReal)
	}
	if relErr(curve.Alpha, alphaReal) >= 0.15 {
		t.Fatalf("alpha=%.4f, want %.4f (rel err %.4f)", curve.Alpha, alphaReal, relErr(curve.Alpha, alphaReal))
	}
	if relErr(curve.Beta, betaReal) >= 0.15 {
		t.Fatalf("beta=%.4f, want %.4f (rel err %.4f)", curve.Beta, betaReal, relErr(curve.Beta, betaReal))
	}
}

// (b) points with BOTH axes unknown are ignored (either-axis gate: X>0 ||
// Y>0 trains; X==0 && Y==0 does not). N unchanged, fit identical.
func TestUsageCurveIgnoresUnknownUsage(t *testing.T) {
	base := knownUsageCurvePoints(100, 0.01, 0.005)[:4]
	withUnknown := append(append([]UsagePoint{}, base...),
		soloUsagePoint(9999, 0, 0),  // both unknown
		soloUsagePoint(9999, -1, 0), // negative X = unknown, Y unknown
		soloUsagePoint(9999, 0, -2), // X unknown, negative Y = unknown
	)
	cBase, okBase := FitUsageCurve(base)
	cWith, okWith := FitUsageCurve(withUnknown)
	if !okBase || !okWith {
		t.Fatal("FitUsageCurve: ok=false, want true")
	}
	if cWith.N != cBase.N {
		t.Fatalf("N changed with unknown usage: base=%d with=%d", cBase.N, cWith.N)
	}
	if cWith != cBase {
		t.Fatalf("fit moved with unknown usage: base=%+v with=%+v", cBase, cWith)
	}
}

// (b2) either-axis gate: a point with X>0 but Y==0 (or vice versa) TRAINS —
// unlike the PSI both-axes gate, one known axis carries signal.
func TestUsageCurveEitherAxisTrains(t *testing.T) {
	base := knownUsageCurvePoints(100, 0.01, 0.005)[:4]
	cBase, _ := FitUsageCurve(base)
	withEither := append(append([]UsagePoint{}, base...),
		soloUsagePoint(150, 30, 0), // X known, Y unknown
		soloUsagePoint(150, 0, 40), // X unknown, Y known
	)
	cWith, ok := FitUsageCurve(withEither)
	if !ok {
		t.Fatal("FitUsageCurve: ok=false, want true")
	}
	if cWith.N != cBase.N+2 {
		t.Fatalf("N=%d, want %d (either-axis points must train)", cWith.N, cBase.N+2)
	}
}

// (c) cold/contended points do not move (alpha,beta).
func TestUsageCurveExcludesColdAndContended(t *testing.T) {
	base := knownUsageCurvePoints(100, 0.01, 0.005)
	poison := []UsagePoint{
		{LatencyMS: 5000, X: 10, Y: 20, Inflight: 1, CPUPctAvg: 10, ColdStart: true, Outcome: OutcomeSucesso},
		{LatencyMS: 6000, X: 20, Y: 10, Inflight: 1, CPUPctAvg: 10, ColdStart: true, Outcome: OutcomeSucesso},
		{LatencyMS: 7000, X: 30, Y: 40, Inflight: 2, CPUPctAvg: 10, Outcome: OutcomeSucesso},
		{LatencyMS: 8000, X: 40, Y: 30, Inflight: 2, CPUPctAvg: 10, Outcome: OutcomeSucesso},
		{LatencyMS: 9000, X: 50, Y: 10, Inflight: 3, CPUPctAvg: 10, Outcome: OutcomeSucesso},
		{LatencyMS: 9000, X: 50, Y: 10, Inflight: 0, CPUPctAvg: 10, Outcome: OutcomeSucesso},
		{LatencyMS: 9000, X: 50, Y: 10, Inflight: 1, CPUPctAvg: 0, Outcome: OutcomeSucesso},
		{LatencyMS: 9000, X: 50, Y: 10, Inflight: 1, CPUPctAvg: 10, Outcome: OutcomeFalha},
	}
	cBase, _ := FitUsageCurve(base)
	cMix, ok := FitUsageCurve(append(append([]UsagePoint{}, base...), poison...))
	if !ok {
		t.Fatal("FitUsageCurve: ok=false, want true")
	}
	if cMix.N != cBase.N {
		t.Fatalf("N changed with cold/contended: base=%d mix=%d", cBase.N, cMix.N)
	}
	if cMix.Alpha != cBase.Alpha || cMix.Beta != cBase.Beta || cMix.B != cBase.B {
		t.Fatalf("fit moved with cold/contended: base=%+v mix=%+v", cBase, cMix)
	}
}

// (d) per-(group,node) posterior: 2 points stay within 20% of the layer
// prior (shrinkage); 6+ points converge to the local fit.
func TestUsageCurveShrinkageToPrior(t *testing.T) {
	prior, ok := FitUsageCurve(knownUsageCurvePoints(100, 0.01, 0.005))
	if !ok {
		t.Fatal("prior fit failed")
	}

	// Thin node: only 2 solo points at a ~30% hotter level.
	thin := []UsagePoint{
		soloUsagePoint(130*(1+0.01*10+0.005*20), 10, 20),
		soloUsagePoint(130*(1+0.01*40+0.005*10), 40, 10),
	}
	postThin, ok := PosteriorUsageCurve(prior, true, thin)
	if !ok {
		t.Fatal("PosteriorUsageCurve: ok=false on thin node with valid prior")
	}
	if relErr(postThin.B, prior.B) > 0.20 {
		t.Fatalf("thin posterior b=%.3f drifted >20%% from prior b=%.3f", postThin.B, prior.B)
	}

	// Dense node: 6+ solo points — posterior must converge to the local fit.
	dense := knownUsageCurvePoints(130, 0.01, 0.005)[:6]
	local, ok := FitUsageCurve(dense)
	if !ok {
		t.Fatal("local fit failed")
	}
	postDense, ok := PosteriorUsageCurve(prior, true, dense)
	if !ok {
		t.Fatal("PosteriorUsageCurve: ok=false on dense node")
	}
	if relErr(postDense.B, local.B) > 1e-9 {
		t.Fatalf("dense posterior b=%.6f did not converge to local b=%.6f", postDense.B, local.B)
	}
	if relErr(postDense.B, prior.B) < 0.20 {
		t.Fatalf("dense posterior b=%.3f still glued to prior b=%.3f, want convergence to local b=%.3f",
			postDense.B, prior.B, local.B)
	}
}

// Failure case: dataset with only unknown usage (X==0 && Y==0 on every
// point) => ok=false (never silent b==0).
func TestUsageCurveAllUnknownNotOK(t *testing.T) {
	only := []UsagePoint{
		soloUsagePoint(120, 0, 0),
		soloUsagePoint(130, 0, 0),
		soloUsagePoint(140, -1, -2),
	}
	curve, ok := FitUsageCurve(only)
	if ok {
		t.Fatalf("FitUsageCurve: ok=true on all-unknown usage, want false (got %+v)", curve)
	}
	if curve.B != 0 {
		t.Fatalf("FitUsageCurve: silent b=%.3f with ok=false, want zero value", curve.B)
	}
	if _, ok := FitUsageCurve(nil); ok {
		t.Fatal("FitUsageCurve(nil): ok=true, want false")
	}
}

// Predict is the closed-form law p95 = b*(1+alpha*x+beta*y), O(1).
func TestUsageCurvePredictFormula(t *testing.T) {
	c := UsageCurve{B: 100, Alpha: 0.01, Beta: 0.005, N: 8}
	if got, want := c.Predict(20, 40), 100*(1+0.01*20+0.005*40); got != want {
		t.Fatalf("Predict(20,40)=%.3f, want %.3f", got, want)
	}
	if got := c.Predict(0, 0); got != 100 {
		t.Fatalf("Predict(0,0)=%.3f, want 100 (baseline b)", got)
	}
}

// X resolution: ProjectedLoadPct wins when >0, ObservedCPUPct is the
// fallback, both-zero stays unknown.
func TestUsageXOfPrefersProjected(t *testing.T) {
	if got := usageXOf(Entry{ProjectedLoadPct: 42, ObservedCPUPct: 10}); got != 42 {
		t.Fatalf("usageXOf with both set = %.3f, want projected 42", got)
	}
	if got := usageXOf(Entry{ObservedCPUPct: 10}); got != 10 {
		t.Fatalf("usageXOf with projected==0 = %.3f, want observed 10", got)
	}
	if got := usageXOf(Entry{}); got != 0 {
		t.Fatalf("usageXOf empty = %.3f, want 0 (unknown)", got)
	}
}

// Seam: fit over Entries via usageOf (X = ProjectedLoadPct when >0 else
// ObservedCPUPct, Y = ObservedMemPct). The last entry rides the observed
// fallback (ProjectedLoadPct==0); the cold entry must not move the fit.
func TestUsageCurveEntrySeam(t *testing.T) {
	usage := map[string][2]float64{
		"a": {10, 20}, "b": {20, 10}, "c": {30, 40}, "d": {40, 30},
		"e": {50, 10}, "f": {10, 50}, "g": {60, 60}, "h": {20, 70},
	}
	const bReal, alphaReal, betaReal = 100.0, 0.01, 0.005
	var entries []Entry
	i := 0
	for id, s := range usage {
		e := Entry{
			FunctionName:      id,
			NodeID:            "n1",
			Outcome:           OutcomeSucesso,
			ObservedLatencyMS: int64(bReal * (1 + alphaReal*s[0] + betaReal*s[1])),
			Inflight:          1,
			CPUPctAvg:         10,
			ObservedMemPct:    s[1],
		}
		if i == 0 {
			// First entry rides the ObservedCPUPct fallback (no projection).
			e.ObservedCPUPct = s[0]
		} else {
			e.ProjectedLoadPct = s[0]
			e.ObservedCPUPct = s[0] + 25 // divergent decoy: projection must win
		}
		entries = append(entries, e)
		i++
	}
	// One cold entry must not move the fit even with known usage.
	entries = append(entries, Entry{
		FunctionName: "cold", NodeID: "n1", Outcome: OutcomeSucesso,
		ObservedLatencyMS: 9000, Inflight: 1, CPUPctAvg: 10, ColdStart: true,
		ProjectedLoadPct: 10, ObservedMemPct: 20,
	})
	curve, ok := FitUsageCurveFromEntries(entries)
	if !ok {
		t.Fatal("FitUsageCurveFromEntries: ok=false, want true")
	}
	if curve.N != 8 {
		t.Fatalf("N=%d, want 8 (cold excluded)", curve.N)
	}
	if relErr(curve.B, bReal) >= 0.10 {
		t.Fatalf("|b-b_real|/b_real=%.4f, need <10%% (b=%.3f)", relErr(curve.B, bReal), curve.B)
	}
}
