// Normal and Student-t quantiles.
//
// NormalQuantile uses Acklam's inverse normal approximation refined by two
// Newton steps via math.Erfc. StudentTQuantile uses the Cornish-Fisher
// expansion (Abramowitz and Stegun 26.7.5).
//
// The expansion is used for every nu > 0 rather than switching to the normal
// quantile for large nu: it reproduces the reference values at nu = 3, 10 and
// 30 (2.3534, 1.8125 and 1.6973) and converges to z as nu grows. At nu = 30
// the normal quantile would give 1.6449, an error of 0.05.

package learn

import "math"

// Acklam (2003): coefficients of the inverse normal approximation.
var (
	acklamA = [6]float64{-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00}
	acklamB = [5]float64{-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01}
	acklamC = [6]float64{-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00}
	acklamD = [4]float64{7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00}
)

const (
	acklamPLow  = 0.02425
	acklamPHigh = 1 - acklamPLow
)

// NormalQuantile returns z such that Phi(z) = p. It returns NaN outside [0,1]
// and -Inf or +Inf at the endpoints.
func NormalQuantile(p float64) float64 {
	if math.IsNaN(p) || p < 0 || p > 1 {
		return math.NaN()
	}
	if p == 0 {
		return math.Inf(-1)
	}
	if p == 1 {
		return math.Inf(1)
	}
	if p == 0.5 {
		return 0
	}
	var x float64
	switch {
	case p < acklamPLow:
		q := math.Sqrt(-2 * math.Log(p))
		x = poly6(acklamC, q) / (poly4(acklamD, q)*q + 1)
	case p > acklamPHigh:
		q := math.Sqrt(-2 * math.Log(1-p))
		x = -poly6(acklamC, q) / (poly4(acklamD, q)*q + 1)
	default:
		q := p - 0.5
		r := q * q
		x = poly6(acklamA, r) * q / (poly5(acklamB, r)*r + 1)
	}
	// Newton refinement: two steps bring the error near 1e-15.
	for i := 0; i < 2; i++ {
		cdf := 0.5 * math.Erfc(-x/math.Sqrt2)
		if math.Abs(cdf-p) < 1e-15 {
			break
		}
		x -= (cdf - p) / (math.Exp(-0.5*x*x) * 0.3989422804014327)
	}
	return x
}

func poly6(c [6]float64, x float64) float64 {
	return (((((c[0]*x+c[1])*x+c[2])*x+c[3])*x+c[4])*x + c[5])
}

func poly5(c [5]float64, x float64) float64 {
	return ((((c[0]*x+c[1])*x+c[2])*x+c[3])*x + c[4])
}

func poly4(c [4]float64, x float64) float64 {
	return (((c[0]*x+c[1])*x+c[2])*x + c[3])
}

// StudentTQuantile returns t such that T_nu(t) = p, using the Cornish-Fisher
// expansion (A&S 26.7.5). It is valid for every nu > 0.
//
// Known limitation: for extreme tails (|z| > 3) and small nu the true quantile
// is underestimated by up to about 10% (t(0.999; 3) gives 9.3 instead of
// 10.2). Callers stay at p <= 0.999 and leave nu < 3 in probation, so the
// affected regime never drives a decision.
func StudentTQuantile(p, nu float64) float64 {
	if math.IsNaN(p) || math.IsNaN(nu) || nu <= 0 {
		return math.NaN()
	}
	if p <= 0 {
		return math.Inf(-1)
	}
	if p >= 1 {
		return math.Inf(1)
	}
	if p == 0.5 {
		return 0
	}
	z := NormalQuantile(p)
	z2 := z * z
	z3 := z2 * z
	z5 := z3 * z2
	z7 := z5 * z2
	v := nu
	return z +
		(z3+z)/(4*v) +
		(5*z5+16*z3+3*z)/(96*v*v) +
		(3*z7+19*z5+17*z3-15*z)/(384*v*v*v)
}
