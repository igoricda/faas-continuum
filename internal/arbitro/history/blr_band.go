// BLR Student-t Upper90 band — see blr_model.go for overview.
// Pure stdlib, no locks/I-O (ADR-0007 D32).
package history

import (
	"math"
)

// blrT95 returns the one-sided 95% Student-t quantile (the Upper90
// credibility choice: P(y* <= Upper90) = 0.90). Exact tabulated values
// for integer dof 1..30, linear interpolation between them for
// non-integer dof, Cornish-Fisher expansion beyond 30; dof<1 clamps to
// the dof=1 value. Deterministic, O(1), stdlib only.
func blrT95(dof float64) float64 {
	// Standard t_{0.95} quantiles, dof 1..30.
	table := [...]float64{
		6.31375, 2.91999, 2.35336, 2.13185, 2.01505, 1.94318, 1.89458,
		1.85955, 1.83311, 1.81246, 1.79588, 1.78229, 1.77093, 1.76131,
		1.75305, 1.74588, 1.73961, 1.73406, 1.72913, 1.72472, 1.72074,
		1.71714, 1.71387, 1.71088, 1.70814, 1.70562, 1.70329, 1.70113,
		1.69913, 1.69726,
	}
	if math.IsNaN(dof) || dof <= 1 {
		return table[0]
	}
	if dof <= 30 {
		lo := int(math.Floor(dof))
		if lo < 1 {
			lo = 1
		}
		if lo >= 30 {
			return table[29]
		}
		hi := lo + 1
		frac := dof - float64(lo)
		return table[lo-1]*(1-frac) + table[hi-1]*frac
	}
	// Cornish-Fisher: t = z + (z^3+z)/4nu + (5z^5+16z^3+3z)/96nu^2,
	// z = Phi^{-1}(0.95).
	const z = 1.6448536269514729
	nu := dof
	return z + (z*z*z+z)/(4*nu) + (5*z*z*z*z*z+16*z*z*z+3*z)/(96*nu*nu)
}

// blrScaleOf returns the posterior predictive scale sigma(x, y) =
// sqrt((b_n/a_n)*(1 + x*^T Lambda_n^{-1} x*)) — the FULL conjugate
// predictive (leverage factor present, dof = 2*a_n carried by the caller).
// ok=false without band state or on non-finite/non-positive intermediates.
func blrScaleOf(c UsageCurve, x, y float64) (float64, bool) {
	if !c.blrHasBand || !(c.blrA > 0) {
		return 0, false
	}
	scale2 := c.blrB / c.blrA
	if !(scale2 > 0) {
		return 0, false
	}
	xs := [3]float64{1, x, y}
	lev := xs[0]*(c.blrInv[0][0]*xs[0]+c.blrInv[0][1]*xs[1]+c.blrInv[0][2]*xs[2]) +
		xs[1]*(c.blrInv[1][0]*xs[0]+c.blrInv[1][1]*xs[1]+c.blrInv[1][2]*xs[2]) +
		xs[2]*(c.blrInv[2][0]*xs[0]+c.blrInv[2][1]*xs[1]+c.blrInv[2][2]*xs[2])
	if math.IsNaN(lev) || lev < 0 {
		lev = 0
	}
	s := math.Sqrt(scale2 * (1 + lev))
	if math.IsNaN(s) || math.IsInf(s, 0) || !(s > 0) {
		return 0, false
	}
	return s, true
}

// blrHalfT is the parametric Student-t halfwidth t_{0.95}(dof)*sigma.
// ok=false without band state.
func blrHalfT(c UsageCurve, x, y float64) (float64, bool) {
	sigma, ok := blrScaleOf(c, x, y)
	if !ok {
		return 0, false
	}
	dof := 2 * c.blrA
	if math.IsNaN(dof) || math.IsInf(dof, 0) {
		return 0, false
	}
	return blrT95(dof) * sigma, true
}

// upper90Half composes the Tier0+Tier1 halfwidth:
// max(student_t_halfwidth, qhat*sigma, floorAbs, floorRel*|point|).
// Every term only widens: the band stays monotone-cautious (>= point) and
// Tier1 can never loosen a veto. useQ=false skips the conformal term
// (audit comparisons and parametric fallback).
func upper90Half(c UsageCurve, x, y float64, useQ bool) (float64, bool) {
	pred := c.Predict(x, y)
	halfT, ok := blrHalfT(c, x, y)
	if !ok {
		return 0, false
	}
	half := halfT
	if useQ && c.blrHasQ {
		sigma, ok := blrScaleOf(c, x, y)
		if ok {
			if halfQ := c.blrQ * sigma; halfQ > half {
				half = halfQ
			}
		}
	}
	if f := blrFloorAbs; f > half {
		half = f
	}
	if f := blrFloorRel * math.Abs(pred); f > half {
		half = f
	}
	if math.IsNaN(half) || math.IsInf(half, 0) || half < 0 {
		return 0, false
	}
	return half, true
}

// Upper90 returns the calibrated one-sided 90% upper predictive band at
// observed usage (x, y): point + max(Student-t halfwidth, conformal
// q-hat*sigma, Tier0 floor). O(1), pure, deterministic. Without posterior
// band state (zero value, hand-built curves) it collapses to Predict —
// fail-open: no uncertainty model, no band, never a veto on unknown.
// Non-finite intermediates likewise fall back to the point prediction.
func (c UsageCurve) Upper90(x, y float64) float64 {
	pred := c.Predict(x, y)
	half, ok := upper90Half(c, x, y, true)
	if !ok {
		return pred
	}
	return pred + half
}

// Upper90Parametric returns the Tier0 parametric band (Student-t +
// floor, WITHOUT the Tier1 conformal term): the audit comparator for
// with-vs-without-qhat decision deltas. Same fail-open collapse as
// Upper90 when band state is absent.
func (c UsageCurve) Upper90Parametric(x, y float64) float64 {
	pred := c.Predict(x, y)
	half, ok := upper90Half(c, x, y, false)
	if !ok {
		return pred
	}
	return pred + half
}

// ConformalQ reports the pooled Tier1 conformal correction carried by the
// curve (q-hat from the UsageCurveFor miss path). ok=false below the
// calibration minimum — pure parametric band, current behavior.
func (c UsageCurve) ConformalQ() (float64, bool) {
	if !c.blrHasQ {
		return 0, false
	}
	return c.blrQ, true
}
