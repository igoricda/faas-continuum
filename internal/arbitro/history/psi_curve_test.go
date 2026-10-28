package history

// TDD tests for the PSI pressure curve estimator (see psi_curve.go).
//
// All tests use synthetic points with independently known answers — no
// fixture coupling, no mocks, no pinned implementation constants. Each
// test derives its expectation from the data-generating parameters
// (b/alpha/beta), never from the estimator's internals.

import (
	"math"
	"testing"
)

// soloPSIPoint builds a solo-eligible observation carrying explicit PSI.
// Gate mirrored: Inflight==1 && CPUPctAvg>0 && !ColdStart &&
// Outcome==sucesso (see ExpectedCost in history_stats.go).
func soloPSIPoint(latencyMS, cpuSome, memFull float64) PSIPoint {
	return PSIPoint{
		LatencyMS: latencyMS,
		CPUSome:   cpuSome,
		MemFull:   memFull,
		Inflight:  1,
		CPUPctAvg: 10,
		Outcome:   OutcomeSucesso,
	}
}

// knownCurvePoints generates n spread (cpu,mem) observations from the exact
// law lat = b*(1+alpha*cpu+beta*mem). Spread covers both axes independently
// (never collinear) so the fit is identified.
func knownCurvePoints(b, alpha, beta float64) []PSIPoint {
	spread := [][2]float64{
		{1, 2}, {2, 1}, {3, 4}, {4, 3},
		{5, 1}, {1, 5}, {6, 6}, {2, 7},
	}
	pts := make([]PSIPoint, 0, len(spread))
	for _, s := range spread {
		lat := b * (1 + alpha*s[0] + beta*s[1])
		pts = append(pts, soloPSIPoint(lat, s[0], s[1]))
	}
	return pts
}

func relErr(got, want float64) float64 {
	return math.Abs(got-want) / math.Abs(want)
}

// (a) 8 synthetic solo spread points with known b => |b-b_real|/b_real<10%.
func TestPSICurveFitRecoversKnownB(t *testing.T) {
	const bReal, alphaReal, betaReal = 100.0, 0.5, 0.3
	curve, ok := FitPSICurve(knownCurvePoints(bReal, alphaReal, betaReal))
	if !ok {
		t.Fatal("FitPSICurve: ok=false on 8 clean solo points")
	}
	if curve.N != 8 {
		t.Fatalf("FitPSICurve: N=%d, want 8", curve.N)
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

// (b) points with PSI==0 are ignored (n does not change, fit identical).
func TestPSICurveIgnoresZeroPSI(t *testing.T) {
	base := knownCurvePoints(100, 0.5, 0.3)[:4]
	withUnknown := append(append([]PSIPoint{}, base...),
		soloPSIPoint(9999, 0, 3),  // cpu unknown
		soloPSIPoint(9999, 2, 0),  // mem unknown
		soloPSIPoint(9999, 0, 0),  // both unknown
		soloPSIPoint(9999, -1, 2), // negative = unknown
	)
	cBase, okBase := FitPSICurve(base)
	cWith, okWith := FitPSICurve(withUnknown)
	if !okBase || !okWith {
		t.Fatal("FitPSICurve: ok=false, want true")
	}
	if cWith.N != cBase.N {
		t.Fatalf("N changed with unknown PSI: base=%d with=%d", cBase.N, cWith.N)
	}
	if cWith != cBase {
		t.Fatalf("fit moved with unknown PSI: base=%+v with=%+v", cBase, cWith)
	}
}

// (c) cold/contended points do not move (alpha,beta).
func TestPSICurveExcludesColdAndContended(t *testing.T) {
	base := knownCurvePoints(100, 0.5, 0.3)
	poison := []PSIPoint{
		{LatencyMS: 5000, CPUSome: 1, MemFull: 1, Inflight: 1, CPUPctAvg: 10, ColdStart: true, Outcome: OutcomeSucesso},
		{LatencyMS: 6000, CPUSome: 2, MemFull: 2, Inflight: 1, CPUPctAvg: 10, ColdStart: true, Outcome: OutcomeSucesso},
		{LatencyMS: 7000, CPUSome: 3, MemFull: 3, Inflight: 2, CPUPctAvg: 10, Outcome: OutcomeSucesso},
		{LatencyMS: 8000, CPUSome: 4, MemFull: 4, Inflight: 2, CPUPctAvg: 10, Outcome: OutcomeSucesso},
		{LatencyMS: 9000, CPUSome: 5, MemFull: 5, Inflight: 3, CPUPctAvg: 10, Outcome: OutcomeSucesso},
		{LatencyMS: 9000, CPUSome: 5, MemFull: 5, Inflight: 0, CPUPctAvg: 10, Outcome: OutcomeSucesso},
		{LatencyMS: 9000, CPUSome: 5, MemFull: 5, Inflight: 1, CPUPctAvg: 0, Outcome: OutcomeSucesso},
		{LatencyMS: 9000, CPUSome: 5, MemFull: 5, Inflight: 1, CPUPctAvg: 10, Outcome: OutcomeFalha},
	}
	cBase, _ := FitPSICurve(base)
	cMix, ok := FitPSICurve(append(append([]PSIPoint{}, base...), poison...))
	if !ok {
		t.Fatal("FitPSICurve: ok=false, want true")
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
func TestPSICurveShrinkageToPrior(t *testing.T) {
	prior, ok := FitPSICurve(knownCurvePoints(100, 0.5, 0.3))
	if !ok {
		t.Fatal("prior fit failed")
	}

	// Thin node: only 2 solo points at a ~30% hotter level.
	thin := []PSIPoint{
		soloPSIPoint(130*(1+0.5*1+0.3*2), 1, 2),
		soloPSIPoint(130*(1+0.5*4+0.3*1), 4, 1),
	}
	postThin, ok := PosteriorPSICurve(prior, true, thin)
	if !ok {
		t.Fatal("PosteriorPSICurve: ok=false on thin node with valid prior")
	}
	if relErr(postThin.B, prior.B) > 0.20 {
		t.Fatalf("thin posterior b=%.3f drifted >20%% from prior b=%.3f", postThin.B, prior.B)
	}

	// Dense node: 6+ solo points — posterior must converge to the local fit.
	dense := knownCurvePoints(130, 0.5, 0.3)[:6]
	local, ok := FitPSICurve(dense)
	if !ok {
		t.Fatal("local fit failed")
	}
	postDense, ok := PosteriorPSICurve(prior, true, dense)
	if !ok {
		t.Fatal("PosteriorPSICurve: ok=false on dense node")
	}
	if relErr(postDense.B, local.B) > 1e-9 {
		t.Fatalf("dense posterior b=%.6f did not converge to local b=%.6f", postDense.B, local.B)
	}
	if relErr(postDense.B, prior.B) < 0.20 {
		t.Fatalf("dense posterior b=%.3f still glued to prior b=%.3f, want convergence to local b=%.3f",
			postDense.B, prior.B, local.B)
	}
}

// Failure case: dataset with only PSI==0 => ok=false (never silent b==0).
func TestPSICurveAllUnknownNotOK(t *testing.T) {
	only := []PSIPoint{
		soloPSIPoint(120, 0, 0),
		soloPSIPoint(130, 0, 5),
		soloPSIPoint(140, 4, 0),
	}
	curve, ok := FitPSICurve(only)
	if ok {
		t.Fatalf("FitPSICurve: ok=true on all-unknown PSI, want false (got %+v)", curve)
	}
	if curve.B != 0 {
		t.Fatalf("FitPSICurve: silent b=%.3f with ok=false, want zero value", curve.B)
	}
	if _, ok := FitPSICurve(nil); ok {
		t.Fatal("FitPSICurve(nil): ok=true, want false")
	}
}

// Predict is the closed-form law p95 = b*(1+alpha*cpu+beta*mem), O(1).
func TestPSICurvePredictFormula(t *testing.T) {
	c := PSICurve{B: 100, Alpha: 0.5, Beta: 0.25, N: 8}
	if got, want := c.Predict(2, 4), 100*(1+0.5*2+0.25*4); got != want {
		t.Fatalf("Predict(2,4)=%.3f, want %.3f", got, want)
	}
	if got := c.Predict(0, 0); got != 100 {
		t.Fatalf("Predict(0,0)=%.3f, want 100 (baseline b)", got)
	}
}

// Seam for todo 5: fit over Entries via an explicit PSI accessor, so no PSI
// fields on Entry are needed yet. The accessor is the only PSI source.
func TestPSICurveEntrySeam(t *testing.T) {
	psi := map[string][2]float64{
		"a": {1, 2}, "b": {2, 1}, "c": {3, 4}, "d": {4, 3},
		"e": {5, 1}, "f": {1, 5}, "g": {6, 6}, "h": {2, 7},
	}
	const bReal, alphaReal, betaReal = 100.0, 0.5, 0.3
	var entries []Entry
	for id, s := range psi {
		entries = append(entries, Entry{
			FunctionName:      id,
			NodeID:            "n1",
			Outcome:           OutcomeSucesso,
			ObservedLatencyMS: int64(bReal * (1 + alphaReal*s[0] + betaReal*s[1])),
			Inflight:          1,
			CPUPctAvg:         10,
		})
	}
	// One cold entry must not move the fit even though the accessor has PSI.
	entries = append(entries, Entry{
		FunctionName: "cold", NodeID: "n1", Outcome: OutcomeSucesso,
		ObservedLatencyMS: 9000, Inflight: 1, CPUPctAvg: 10, ColdStart: true,
	})
	psiOf := func(e Entry) (cpu, mem float64) {
		if s, found := psi[e.FunctionName]; found {
			return s[0], s[1]
		}
		return 1, 1 // cold entry: valid PSI, but cold gate excludes it
	}
	curve, ok := FitPSICurveFromEntries(entries, psiOf)
	if !ok {
		t.Fatal("FitPSICurveFromEntries: ok=false, want true")
	}
	if curve.N != 8 {
		t.Fatalf("N=%d, want 8 (cold excluded)", curve.N)
	}
	if relErr(curve.B, bReal) >= 0.10 {
		t.Fatalf("|b-b_real|/b_real=%.4f, need <10%% (b=%.3f)", relErr(curve.B, bReal), curve.B)
	}
}
