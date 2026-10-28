// Bayesian log-normal regression with temporal decay (NIG, fixed d=2).
//
// Model: ln S | x ~ N(theta^T x, sigma^2) with a Normal-Inverse-Gamma prior.
// The regressor stores decayed sufficient statistics (A 2x2, c 2, Q, N) and
// decays lazily, so old data loses weight continuously and the state forgets
// towards the prior.
//
// Fixed [2]/[2][2] arrays avoid allocation and keep the code specific to two
// regressors.

package learn

import (
	"math"
	"time"
)

// NIGPrior is the Normal-Inverse-Gamma prior:
// theta | sigma^2 ~ N(m0, sigma^2 L0^-1) and sigma^2 ~ Inv-Gamma(a0, b0).
type NIGPrior struct {
	M0     [2]float64
	L0     [2][2]float64
	A0, B0 float64
}

// DefaultWarmPrior returns m0 = [ln(priorMS), 0], L0 = diag(0.5, 1),
// a0 = 1.5 (nu0 = 3, heavy tails) and b0 = a0*sigma0^2 with sigma0 = sigma
// (0.5 when sigma <= 0).
func DefaultWarmPrior(priorMS, sigma float64) NIGPrior {
	if sigma <= 0 || math.IsNaN(sigma) || math.IsInf(sigma, 0) {
		sigma = 0.5
	}
	m0 := [2]float64{0, 0}
	if priorMS > 0 && !math.IsNaN(priorMS) && !math.IsInf(priorMS, 0) {
		m0[0] = math.Log(priorMS)
	}
	return NIGPrior{
		M0: m0,
		L0: [2][2]float64{{0.5, 0}, {0, 1}},
		A0: 1.5, B0: 1.5 * sigma * sigma,
	}
}

// WeakWarmPrior returns a prior without priorMS: L0[0][0] = 1e-3 leaves the
// intercept essentially unpenalized. The model stays in probation until
// n_eff >= nmin.
func WeakWarmPrior() NIGPrior {
	return NIGPrior{
		M0: [2]float64{0, 0},
		L0: [2][2]float64{{1e-3, 0}, {0, 1}},
		A0: 1.5, B0: 1.5 * 0.25,
	}
}

// ColdPrior reuses the NIG with x = [1, 0] and L0[1][1] = 1e6, which pins the
// slope at zero instead of introducing a second type. coldMS > 0 sets the
// intercept to ln(coldMS); otherwise it falls back to serviceMS, and then to
// the weak prior (L0[0][0] = 1e-3).
func ColdPrior(serviceMS, sigma, coldMS float64) NIGPrior {
	if sigma <= 0 || math.IsNaN(sigma) || math.IsInf(sigma, 0) {
		sigma = 0.5
	}
	ms := coldMS
	if !(ms > 0) {
		ms = serviceMS
	}
	m0 := [2]float64{0, 0}
	l00 := 1e-3
	if ms > 0 && !math.IsNaN(ms) && !math.IsInf(ms, 0) {
		m0[0] = math.Log(ms)
		l00 = 0.5
	}
	return NIGPrior{
		M0: m0,
		L0: [2][2]float64{{l00, 0}, {0, 1e6}},
		A0: 1.5, B0: 1.5 * sigma * sigma,
	}
}

// NIG is the regressor with temporal decay (fixed d=2).
type NIG struct {
	prior   NIGPrior
	a       [2][2]float64
	c       [2]float64
	q, n    float64
	last    time.Time
	touched bool
}

// NewNIG returns a regressor with the given prior.
func NewNIG(p NIGPrior) *NIG { return &NIG{prior: p} }

// decay applies the lazy decay up to now (delta = 0.5^(dt/H); H <= 0 disables
// decay). A non-monotonic clock (dt <= 0) leaves the state unchanged.
func (n *NIG) decay(now time.Time, halflife time.Duration) {
	if !n.touched {
		n.touched = true
		n.last = now
		return
	}
	dt := now.Sub(n.last)
	if dt <= 0 {
		return
	}
	n.last = now
	if halflife <= 0 {
		return
	}
	d := math.Exp2(-float64(dt) / float64(halflife))
	if d >= 1 {
		return
	}
	if d <= 0 {
		d = 0
	}
	for i := 0; i < 2; i++ {
		n.c[i] *= d
		for j := 0; j < 2; j++ {
			n.a[i][j] *= d
		}
	}
	n.q *= d
	n.n *= d
}

// Observe adds (x, y) with weight 1 after decaying up to now. Non-finite
// inputs are dropped without touching the state.
func (n *NIG) Observe(x [2]float64, y float64, now time.Time, halflife time.Duration) {
	if math.IsNaN(y) || math.IsInf(y, 0) ||
		math.IsNaN(x[0]) || math.IsInf(x[0], 0) ||
		math.IsNaN(x[1]) || math.IsInf(x[1], 0) {
		return
	}
	n.decay(now, halflife)
	n.a[0][0] += x[0] * x[0]
	n.a[0][1] += x[0] * x[1]
	n.a[1][0] += x[1] * x[0]
	n.a[1][1] += x[1] * x[1]
	n.c[0] += x[0] * y
	n.c[1] += x[1] * y
	n.q += y * y
	n.n++
}

// Neff returns N, the effective number of observations decayed up to the last
// touch.
func (n *NIG) Neff() float64 { return n.n }

// Posterior returns the posterior decayed up to now: mean m, a, b and the
// inverse of Lambda, used for the predictive variance. b is clamped to at
// least 1e-9.
func (n *NIG) Posterior(now time.Time, halflife time.Duration) (m [2]float64, a, b float64, lamInv [2][2]float64) {
	n.decay(now, halflife)
	l00 := n.prior.L0[0][0] + n.a[0][0]
	l01 := n.prior.L0[0][1] + n.a[0][1]
	l10 := n.prior.L0[1][0] + n.a[1][0]
	l11 := n.prior.L0[1][1] + n.a[1][1]
	eta0 := n.prior.L0[0][0]*n.prior.M0[0] + n.prior.L0[0][1]*n.prior.M0[1] + n.c[0]
	eta1 := n.prior.L0[1][0]*n.prior.M0[0] + n.prior.L0[1][1]*n.prior.M0[1] + n.c[1]
	det := l00*l11 - l01*l10
	if !(det > 0) || math.IsNaN(det) || math.IsInf(det, 0) {
		det = 1e-12
	}
	m[0] = (l11*eta0 - l01*eta1) / det
	m[1] = (-l10*eta0 + l00*eta1) / det
	if math.IsNaN(m[0]) || math.IsNaN(m[1]) {
		m = n.prior.M0
	}
	lamInv = [2][2]float64{{l11 / det, -l01 / det}, {-l10 / det, l00 / det}}
	a = n.prior.A0 + n.n/2
	// m0^T L0 m0 - eta^T m, using eta^T L^-1 eta = eta^T m.
	m0l0m0 := n.prior.M0[0]*(n.prior.L0[0][0]*n.prior.M0[0]+n.prior.L0[0][1]*n.prior.M0[1]) +
		n.prior.M0[1]*(n.prior.L0[1][0]*n.prior.M0[0]+n.prior.L0[1][1]*n.prior.M0[1])
	etam := eta0*m[0] + eta1*m[1]
	b = n.prior.B0 + 0.5*(n.q+m0l0m0-etam)
	if !(b >= 1e-9) || math.IsNaN(b) || math.IsInf(b, 0) {
		b = 1e-9
	}
	return m, a, b, lamInv
}

// predictive returns the location, squared scale and degrees of freedom of
// the Student-t predictive distribution for x.
func (n *NIG) predictive(x [2]float64, now time.Time, halflife time.Duration) (loc, scale2, nu float64) {
	m, a, b, lamInv := n.Posterior(now, halflife)
	loc = x[0]*m[0] + x[1]*m[1]
	// v = xᵀΛ⁻¹x.
	y0 := lamInv[0][0]*x[0] + lamInv[0][1]*x[1]
	y1 := lamInv[1][0]*x[0] + lamInv[1][1]*x[1]
	v := x[0]*y0 + x[1]*y1
	if !(v >= 0) || math.IsNaN(v) || math.IsInf(v, 0) {
		v = 0
	}
	scale2 = (b / a) * (1 + v)
	if !(scale2 > 0) || math.IsNaN(scale2) || math.IsInf(scale2, 0) {
		scale2 = b / a
		if !(scale2 > 0) {
			scale2 = 1e-9
		}
	}
	return loc, scale2, 2 * a
}

// Median returns the predictive median (the location) for x.
func (n *NIG) Median(x [2]float64, now time.Time, halflife time.Duration) float64 {
	loc, _, _ := n.predictive(x, now, halflife)
	return loc
}

// Quantile returns the predictive p-quantile (location + scale*t) for x.
func (n *NIG) Quantile(x [2]float64, p float64, now time.Time, halflife time.Duration) (loc, q float64) {
	loc, scale2, nu := n.predictive(x, now, halflife)
	if nu < 3 {
		nu = 3 // guaranteed by a0 = 1.5; defensive clamp
	}
	return loc, loc + math.Sqrt(scale2)*StudentTQuantile(p, nu)
}
