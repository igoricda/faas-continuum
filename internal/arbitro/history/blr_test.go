package history

// Unit tests for the conjugate Normal-Gamma BLR posterior (see blr.go).
//
// All expectations derive from the data-generating law
// lat = b*(1+alpha*x+beta*y) or from the documented conjugacy identities
// (strong-data mean = batch OLS, n=0 = prior, band widening with smaller n
// / larger residual, determinism) — never from the estimator's internals.
// Synthetic points reuse soloUsagePoint / knownUsageCurvePoints from
// usage_curve_test.go (same package): one helper, not two.

import (
	"math"
	"testing"
)

// blrLawPoints generates n deterministic spread observations from the
// exact law lat = b*(1+alpha*x+beta*y) on a fixed non-collinear grid
// (both axes vary independently so the fit is identified). No RNG, no
// truncation: OLS recovers the law up to float rounding.
func blrLawPoints(b, alpha, beta float64, n int) []UsagePoint {
	grid := [][2]float64{
		{10, 20}, {20, 10}, {30, 40}, {40, 30},
		{50, 10}, {10, 50}, {60, 60}, {20, 70},
		{15, 35}, {35, 15}, {45, 55}, {55, 45},
	}
	pts := make([]UsagePoint, 0, n)
	for i := 0; i < n; i++ {
		s := grid[i%len(grid)]
		pts = append(pts, soloUsagePoint(b*(1+alpha*s[0]+beta*s[1]), s[0], s[1]))
	}
	return pts
}

// blrNoisyPoints adds a deterministic alternating perturbation (±frac) to
// the exact law: fixed residual scale, no RNG, order-stable.
func blrNoisyPoints(b, alpha, beta, frac float64, n int) []UsagePoint {
	pts := blrLawPoints(b, alpha, beta, n)
	for i := range pts {
		if i%2 == 0 {
			pts[i].LatencyMS *= 1 + frac
		} else {
			pts[i].LatencyMS *= 1 - frac
		}
	}
	return pts
}

// (a) Conjugacy: on strong synthetic data the posterior point estimate
// matches batch OLS within tight tolerance, and at/above the
// min-class-samples threshold the posterior trusts local data fully.
func TestBLRConjugacyStrongDataMatchesOLS(t *testing.T) {
	const bReal, alphaReal, betaReal = 100.0, 0.01, 0.005
	prior, ok := FitUsageCurve(blrLawPoints(130, 0.02, 0.008, 8))
	if !ok {
		t.Fatal("prior fit failed")
	}
	local := blrLawPoints(bReal, alphaReal, betaReal, 12)
	ols, ok := FitUsageCurve(local)
	if !ok {
		t.Fatal("batch OLS fit failed")
	}
	post, ok := PosteriorUsageCurve(prior, true, local)
	if !ok {
		t.Fatal("PosteriorUsageCurve: ok=false on strong data with valid prior")
	}
	if post.N != len(local) {
		t.Fatalf("N=%d, want %d", post.N, len(local))
	}
	for name, pair := range map[string][2]float64{
		"B":     {post.B, ols.B},
		"Alpha": {post.Alpha, ols.Alpha},
		"Beta":  {post.Beta, ols.Beta},
	} {
		got, want := pair[0], pair[1]
		if relErr(got, want) > 1e-9 {
			t.Fatalf("strong-data posterior %s=%.9f != batch OLS %.9f (rel err %.3g)",
				name, got, want, relErr(got, want))
		}
	}
	if relErr(post.B, bReal) >= 0.10 {
		t.Fatalf("posterior b=%.3f, want law b=%.3f (<10%%)", post.B, bReal)
	}
	// The BLR-level posterior agrees with the batch OLS coefficients too.
	bp := blrPosteriorOf(prior, true, local)
	if !bp.ok {
		t.Fatal("blrPosteriorOf: ok=false on strong data")
	}
	if math.Abs(bp.mean[0]-ols.B) > 1e-9*math.Abs(ols.B) {
		t.Fatalf("BLR mean c0=%.9f != OLS b=%.9f", bp.mean[0], ols.B)
	}
}

// (b) n=0 returns the prior verbatim: point estimate, N=0, ok=true — and
// the band exists above the point (max uncertainty, never a veto).
func TestBLRNoDataReturnsPrior(t *testing.T) {
	prior := UsageCurve{B: 100, Alpha: 0.01, Beta: 0.005, N: 48}
	for name, local := range map[string][]UsagePoint{
		"nil":   nil,
		"empty": {},
		"all-ineligible": {
			{LatencyMS: 5000, X: 10, Y: 20, Inflight: 2, CPUPctAvg: 10, Outcome: OutcomeSucesso},
			{LatencyMS: 6000, X: 0, Y: 0, Inflight: 1, CPUPctAvg: 10, Outcome: OutcomeSucesso},
		},
	} {
		post, ok := PosteriorUsageCurve(prior, true, local)
		if !ok {
			t.Fatalf("%s: ok=false, want prior fallback", name)
		}
		if post.B != prior.B || post.Alpha != prior.Alpha || post.Beta != prior.Beta {
			t.Fatalf("%s: got %+v, want prior verbatim %+v", name, post, prior)
		}
		if post.N != 0 {
			t.Fatalf("%s: N=%d, want 0 (no local data)", name, post.N)
		}
		if got := post.Upper90(20, 40); !(got >= post.Predict(20, 40)) {
			t.Fatalf("%s: Upper90=%.3f below Predict=%.3f", name, got, post.Predict(20, 40))
		}
	}
	if _, ok := PosteriorUsageCurve(prior, false, nil); ok {
		t.Fatal("n=0 without prior: ok=true, want false")
	}
}

// (c) The band widens with smaller n (same law, fewer points) and with a
// larger residual (same n, noisier points) at a fixed query.
func TestBLRBandWidensWithUncertainty(t *testing.T) {
	const bReal, alphaReal, betaReal = 100.0, 0.01, 0.005
	prior, ok := FitUsageCurve(blrLawPoints(130, 0.02, 0.008, 8))
	if !ok {
		t.Fatal("prior fit failed")
	}
	const qx, qy = 25.0, 30.0
	width := func(c UsageCurve) float64 { return c.Upper90(qx, qy) - c.Predict(qx, qy) }

	dense, ok := PosteriorUsageCurve(prior, true, blrLawPoints(bReal, alphaReal, betaReal, 12))
	if !ok {
		t.Fatal("dense posterior failed")
	}
	thin, ok := PosteriorUsageCurve(prior, true, blrLawPoints(bReal, alphaReal, betaReal, 12)[:2])
	if !ok {
		t.Fatal("thin posterior failed")
	}
	wDense, wThin := width(dense), width(thin)
	if !(wDense > 0) || math.IsNaN(wDense) || math.IsInf(wDense, 0) {
		t.Fatalf("dense band width invalid: %.6f", wDense)
	}
	if !(wThin > wDense) {
		t.Fatalf("thin (n=2) width %.3f should exceed dense (n=12) width %.3f", wThin, wDense)
	}

	clean, ok := PosteriorUsageCurve(prior, true, blrLawPoints(bReal, alphaReal, betaReal, 8))
	if !ok {
		t.Fatal("clean posterior failed")
	}
	noisy, ok := PosteriorUsageCurve(prior, true, blrNoisyPoints(bReal, alphaReal, betaReal, 0.05, 8))
	if !ok {
		t.Fatal("noisy posterior failed")
	}
	wClean, wNoisy := width(clean), width(noisy)
	if !(wNoisy > wClean) {
		t.Fatalf("noisy width %.3f should exceed clean width %.3f (same n)", wNoisy, wClean)
	}
}

// (c2) Zero band state collapses Upper90 to Predict exactly (fail-open for
// hand-built curves with no posterior behind them).
func TestBLRBandAbsentCollapsesToPoint(t *testing.T) {
	c := UsageCurve{B: 100, Alpha: 0.01, Beta: 0.005, N: 8}
	for _, q := range [][2]float64{{0, 0}, {20, 40}, {85, 60}} {
		if got := c.Upper90(q[0], q[1]); got != c.Predict(q[0], q[1]) {
			t.Fatalf("Upper90(%.0f,%.0f)=%.6f without band state, want Predict=%.6f",
				q[0], q[1], got, c.Predict(q[0], q[1]))
		}
	}
}

// (d) Determinism: same input yields bit-identical posterior and band.
func TestBLRDeterministic(t *testing.T) {
	prior, _ := FitUsageCurve(blrLawPoints(130, 0.02, 0.008, 8))
	local := blrNoisyPoints(100, 0.01, 0.005, 0.05, 8)
	first, ok1 := PosteriorUsageCurve(prior, true, local)
	second, ok2 := PosteriorUsageCurve(prior, true, local)
	if !ok1 || !ok2 {
		t.Fatal("posterior failed")
	}
	if first != second {
		t.Fatalf("posterior not bit-identical:\n%+v\n%+v", first, second)
	}
	for _, q := range [][2]float64{{0, 0}, {25, 30}, {85, 60}} {
		a, b := first.Upper90(q[0], q[1]), second.Upper90(q[0], q[1])
		if math.Float64bits(a) != math.Float64bits(b) {
			t.Fatalf("Upper90(%.0f,%.0f) bits differ: %x vs %x", q[0], q[1],
				math.Float64bits(a), math.Float64bits(b))
		}
	}
}

// The t_{0.95} quantile helper is sane: monotone decreasing in dof,
// pinned spot values, converging to z_{0.95} from above.
func TestBLRT95QuantileSane(t *testing.T) {
	if got := blrT95(2); math.Abs(got-2.91999) > 1e-4 {
		t.Fatalf("blrT95(2)=%.5f, want 2.91999", got)
	}
	if got := blrT95(30); math.Abs(got-1.69726) > 1e-4 {
		t.Fatalf("blrT95(30)=%.5f, want 1.69726", got)
	}
	prev := math.Inf(1)
	for _, dof := range []float64{1, 2, 4, 8, 16, 30, 60, 200} {
		got := blrT95(dof)
		if !(got < prev) {
			t.Fatalf("blrT95 not strictly decreasing: t(%v)=%.5f after %.5f", dof, got, prev)
		}
		prev = got
	}
	if got := blrT95(10000); math.Abs(got-1.64485) > 1e-3 {
		t.Fatalf("blrT95(10000)=%.5f, want ~z_0.95=1.64485", got)
	}
}
