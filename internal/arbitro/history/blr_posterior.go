// Conjugate Normal-Gamma posterior of the Bayesian linear regression. See
// blr_model.go for the overview. It uses only the standard library, without
// locks or input and output.
package history

// blrPosterior is the conjugate Normal-Gamma posterior: posterior mean
// coefficients, shape/rate, and the posterior precision inverse for the
// predictive leverage. ok=false only when both prior and local are empty
// (same fail-closed shape as PosteriorUsageCurve).
type blrPosterior struct {
	mean [3]float64
	a    float64
	b    float64
	inv  [3][3]float64
	n    int
	ok   bool
}

// blrInvertSym inverts a symmetric 3x3 via three partial-pivot solve3
// passes (shared singularity notion with the OLS path). ok=false on
// singular input — the caller falls back to a diagonal ridge.
func blrInvertSym(m [3][3]float64) (inv [3][3]float64, ok bool) {
	for col := 0; col < 3; col++ {
		var rhs [3]float64
		rhs[col] = 1
		aug := [3][4]float64{
			{m[0][0], m[0][1], m[0][2], rhs[0]},
			{m[1][0], m[1][1], m[1][2], rhs[1]},
			{m[2][0], m[2][1], m[2][2], rhs[2]},
		}
		sol, solved := solve3(aug)
		if !solved {
			return inv, false
		}
		inv[0][col], inv[1][col], inv[2][col] = sol[0], sol[1], sol[2]
	}
	return inv, true
}

// blrPosteriorOf computes the conjugate Normal-Gamma posterior from a
// pooled layer prior and the node's local points. Branch shape mirrors
// PosteriorUsageCurve exactly (prior-only / raw local / anchored n<3 /
// shrunk fit); the posterior mean in coefficient space is the
// linear-weight shrinkage mu_n = (1-w)*mu0 + w*c_local, mapped by the
// caller through the existing (B,alpha,beta) mapping + clamps.
//
// Uncertainty: a_n = a0 + n/2; b_n = b0 + SSE(mu_n)/2 + penalty/2 with
// SSE from (n, XtX, Xty, yty) and the power-prior penalty
// ((1-w)/w)*(mu_n-mu0)^T XtX (mu_n-mu0)/2 (zero at w=1 and without prior).
// Lambda_n^{-1} = w*XtX^{-1} (w=1: OLS information; w<1: prior-inflated);
// singular XtX or n<3 anchored fits use a diagonal ridge (w/n)*I with unit
// scales (deliberately vague: leverage grows away from the data);
// n=0 prior-only uses (1/n0)*I (max uncertainty — never veto on unknown).
// Pure, deterministic, O(1) in the posterior size.
func blrPosteriorOf(prior UsageCurve, priorOK bool, local []UsagePoint) blrPosterior {
	var elig []UsagePoint
	for _, p := range local {
		if p.eligible() {
			elig = append(elig, p)
		}
	}
	mu0, havePrior := blrPriorMean(prior, priorOK)
	n := len(elig)
	if n == 0 {
		if !havePrior {
			return blrPosterior{}
		}
		ridge := 1 / float64(UsageMinClassSamples)
		return blrPosterior{
			mean: mu0,
			a:    blrPriorA0,
			b:    blrPriorB0,
			inv:  [3][3]float64{{ridge, 0, 0}, {0, ridge, 0}, {0, 0, ridge}},
			n:    0,
			ok:   true,
		}
	}
	s := blrSuffOf(elig)
	w := blrShrinkWeight(n)
	// Local estimate in coefficient space: anchored baseline + prior
	// slopes for n<3 (slopes unidentifiable — same anchored solve as
	// PosteriorUsageCurve, denom<=0 guarded to 1), else OLS.
	var loc [3]float64
	if havePrior && n < 3 {
		var sum float64
		for _, p := range elig {
			denom := 1 + prior.Alpha*p.X + prior.Beta*p.Y
			if denom <= 0 {
				denom = 1
			}
			sum += p.LatencyMS / denom
		}
		loc = [3]float64{sum / float64(n), mu0[1], mu0[2]}
	} else {
		var ok bool
		loc, ok = blrOLS(s)
		if !ok {
			return blrPosterior{}
		}
	}
	var mean [3]float64
	if !havePrior {
		mean = loc
	} else {
		for i := 0; i < 3; i++ {
			mean[i] = (1-w)*mu0[i] + w*loc[i]
		}
	}
	// Residual sum of squares around the posterior mean from suff stats.
	a := blrPriorA0 + float64(n)/2
	b := blrPriorB0 + 0.5*(s.yty-2*(mean[0]*s.xty[0]+mean[1]*s.xty[1]+mean[2]*s.xty[2])+
		(mean[0]*(s.xtx[0][0]*mean[0]+s.xtx[0][1]*mean[1]+s.xtx[0][2]*mean[2])+
			mean[1]*(s.xtx[1][0]*mean[0]+s.xtx[1][1]*mean[1]+s.xtx[1][2]*mean[2])+
			mean[2]*(s.xtx[2][0]*mean[0]+s.xtx[2][1]*mean[1]+s.xtx[2][2]*mean[2])))
	if !(b >= blrPriorB0) {
		// Float rounding on exact fits can dip SSE marginally below zero;
		// the weak-prior floor keeps the predictive scale finite.
		b = blrPriorB0
	}
	if havePrior && w > 0 && w < 1 {
		c := (1 - w) / w
		var d [3]float64
		for i := 0; i < 3; i++ {
			d[i] = mean[i] - mu0[i]
		}
		pen := d[0]*(s.xtx[0][0]*d[0]+s.xtx[0][1]*d[1]+s.xtx[0][2]*d[2]) +
			d[1]*(s.xtx[1][0]*d[0]+s.xtx[1][1]*d[1]+s.xtx[1][2]*d[2]) +
			d[2]*(s.xtx[2][0]*d[0]+s.xtx[2][1]*d[1]+s.xtx[2][2]*d[2])
		if pen > 0 {
			b += 0.5 * c * pen
		}
	}
	// Posterior precision inverse: Lambda_n^{-1} = w*XtX^{-1}; diagonal
	// ridge (w/n)*I when XtX is singular (anchored/collinear fits).
	var inv [3][3]float64
	if xtxInv, ok := blrInvertSym(s.xtx); ok {
		for i := 0; i < 3; i++ {
			for j := 0; j < 3; j++ {
				inv[i][j] = w * xtxInv[i][j]
			}
		}
	} else {
		ridge := w / float64(n)
		inv = [3][3]float64{{ridge, 0, 0}, {0, ridge, 0}, {0, 0, ridge}}
	}
	return blrPosterior{mean: mean, a: a, b: b, inv: inv, n: n, ok: true}
}
