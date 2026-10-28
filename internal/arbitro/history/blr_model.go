// Conjugate Normal-Gamma BLR posterior over lat = c0+c1*x+c2*y with a
// Student-t Upper90 band for the no-PSI usage-curve path (ADR-0007 D32).
// Full calibration (tiers, floors, conformal) lives in docs/ADR-0007-algoritmo-selecao-dispositivo.md.
package history

// Tier0 noise prior (ADR-0007 D32): a0=3, b0=a0*sigma0^2, sigma0=30ms warm jitter.
const (
	blrPriorA0 = 3.0
	blrPriorB0 = 2700.0
)

// Tier0 width floor (ADR-0007 D32): halfwidth = max(t_halfwidth,
// conformal_halfwidth, floorAbs, floorRel*|point|). floorRel = 7% (in the
// 5-10% band) kills degenerate sub-percent bands (Track C tail: a 5ms
// miss on a 1.1% band — floored to 7% it covers); floorAbs = 2ms is twice
// the int64-millisecond timer granularity of ObservedLatencyMS, so the
// band never claims sub-tick precision.
const (
	blrFloorAbs = 2.0
	blrFloorRel = 0.07
)

// blrSuffStats are the sufficient statistics of the linearized model
// lat = c0+c1*x+c2*y: count, 3x3 second moment XtX, cross moment Xty, and
// yty for the residual sum of squares. Accumulated in ONE pass with the
// same gate (UsagePoint.eligible) and the same summation order as
// FitUsageCurve, plus yty — O(1) update per point, O(1) memory.
type blrSuffStats struct {
	n    int
	xtx  [3][3]float64
	xty  [3]float64
	yty  float64
	sumY float64
}

// blrSuffOf accumulates sufficient statistics over the eligible points.
// Summation order mirrors FitUsageCurve exactly (plus yty last in the body)
// so the shared sums are bit-identical to the OLS path.
func blrSuffOf(points []UsagePoint) blrSuffStats {
	var s blrSuffStats
	for _, p := range points {
		if !p.eligible() {
			continue
		}
		y, x1, x2 := p.LatencyMS, p.X, p.Y
		s.n++
		s.sumY += y
		s.yty += y * y
		s.xtx[0][0] += 1
		s.xtx[0][1] += x1
		s.xtx[1][0] += x1
		s.xtx[0][2] += x2
		s.xtx[2][0] += x2
		s.xtx[1][1] += x1 * x1
		s.xtx[2][2] += x2 * x2
		s.xtx[1][2] += x1 * x2
		s.xtx[2][1] += x1 * x2
		s.xty[0] += y
		s.xty[1] += x1 * y
		s.xty[2] += x2 * y
	}
	return s
}

// blrOLS solves the local OLS estimate from sufficient statistics: the
// same 3x3 normal equations + partial-pivot solve3 + intercept-only
// fallback on singularity or non-positive intercept as FitUsageCurve.
// ok=false when n == 0 — never a silent zero vector.
func blrOLS(s blrSuffStats) (coef [3]float64, ok bool) {
	if s.n == 0 {
		return coef, false
	}
	aug := [3][4]float64{
		{s.xtx[0][0], s.xtx[0][1], s.xtx[0][2], s.xty[0]},
		{s.xtx[1][0], s.xtx[1][1], s.xtx[1][2], s.xty[1]},
		{s.xtx[2][0], s.xtx[2][1], s.xtx[2][2], s.xty[2]},
	}
	c, solved := solve3(aug)
	if !solved || c[0] <= 0 {
		return [3]float64{s.sumY / float64(s.n), 0, 0}, true
	}
	return c, true
}

// blrPriorMean maps a prior UsageCurve to coefficient space
// mu0 = [B, alpha*B, beta*B]. ok=false unless the prior is usable
// (B>0 — the same guard as the curve mapping).
func blrPriorMean(prior UsageCurve, priorOK bool) (mu0 [3]float64, ok bool) {
	if !priorOK || !(prior.B > 0) {
		return mu0, false
	}
	return [3]float64{prior.B, prior.Alpha * prior.B, prior.Beta * prior.B}, true
}

// blrShrinkWeight is the posterior shrinkage weight
// w = clamp(n/UsageMinClassSamples, 0, 1): thin nodes stay glued to the
// prior, nodes at/above the threshold trust local data fully. Identical
// semantics to ShrinkUsageToPrior / ShrinkToPrior over the count.
func blrShrinkWeight(n int) float64 {
	w := float64(n) / float64(UsageMinClassSamples)
	if w < 0 {
		w = 0
	}
	if w > 1 {
		w = 1
	}
	return w
}
