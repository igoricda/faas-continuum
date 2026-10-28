// No-PSI usage curve estimator, a pooled solo-only fit with shrinkage to a
// layer prior.
//
// Some deployments expose no PSI at all, so the PSI fields of the Entry are
// always zero there and PSICurveFor never fires. This file is the
// usage-continuous fallback: it trains on the observed CPU and memory load
// instead of the PSI pressure.
//
// Model (closed form, O(1) predict — NO GP/Bayes on the hot path):
//
//	p95_pred = b * (1 + alpha*cpu_load + beta*mem)
//
// Fit gate mirrors the ExpectedCost solo-only invariant
// (history_stats.go): Inflight==1 && CPUPctAvg>0 && !ColdStart &&
// Outcome==sucesso, PLUS usage known on EITHER axis (X>0 || Y>0; 0 =
// unknown, same convention as PSICpuSomeAvg10/PSIMemFullAvg10 in
// history.go). Either-axis (not both-axes like PSI) because CPU and memory
// load are independently informative continuous features — a hot-CPU point
// with unknown mem still carries signal. Cold and contended (Inflight==2)
// / deep-queue (Inflight>=3) points never train — they validate downstream.
//
// Math mirrors psi_curve.go EXACTLY (ordinary least squares on the
// linearized form lat = c0+c1*x+c2*y, partial-pivot 3x3 solve3, negative
// slopes clamped to 0, intercept-only fallback on singular systems,
// ShrinkToPrior-style linear shrinkage with the shared PSIMinClassSamples
// threshold): one law shape, two feature sources, no drift between the
// estimators.
//
// Pure functions, no I/O, no locks, no global state (nothing to cap).
package history

// UsageMinClassSamples is the solo-sample threshold at which a per-node
// posterior stops shrinking to the layer prior and trusts its local fit.
// Aliases PSIMinClassSamples (psi_curve.go) — one threshold, not two, so
// the PSI and usage posteriors densify at the same rate. Defined as an
// alias (not a copy) so the two can never drift out of lockstep.
const UsageMinClassSamples = PSIMinClassSamples

// UsagePoint is one training observation with usage-continuous features.
// X = CPU load (ProjectedLoadPct when >0 else ObservedCPUPct — resolved by
// the caller via usageXOf, see FitUsageCurveFromEntries); Y = mem load
// continuous (ObservedMemPct). The gate fields
// (Inflight/CPUPctAvg/ColdStart/Outcome) carry the solo-only filter so
// FitUsageCurve can enforce it without touching Entry.
type UsagePoint struct {
	LatencyMS float64
	X         float64
	Y         float64
	CPUPctAvg float64
	Inflight  int
	Outcome   string
	ColdStart bool
}

// eligible reports whether the point may train the estimator: the
// ExpectedCost solo-only gate plus usage known on either axis.
func (p UsagePoint) eligible() bool {
	return p.Inflight == 1 &&
		p.CPUPctAvg > 0 &&
		!p.ColdStart &&
		p.Outcome == OutcomeSucesso &&
		(p.X > 0 || p.Y > 0)
}

// UsageCurve holds the fitted usage law p95 = B*(1+Alpha*x+Beta*y).
// N counts the eligible training points behind it.
//
// The blr* fields carry the conjugate Normal-Gamma posterior state behind
// the point estimate (see blr.go): blrA/blrB are the posterior shape/rate,
// blrInv the posterior precision inverse for the predictive leverage, and
// blrHasBand marks a populated band. They feed ONLY UsageCurve.Upper90
// (the Student-t predictive upper band); Predict, kernel behavior, and the
// JSON wire format (curves are never serialized — history persists Entry
// segments) are untouched. Zero value => Upper90 collapses to Predict.
type UsageCurve struct {
	B     float64
	Alpha float64
	Beta  float64
	N     int

	blrA       float64
	blrB       float64
	blrInv     [3][3]float64
	blrHasBand bool
	// blrQ/blrHasQ carry the pooled Tier1 one-sided normalized conformal
	// correction (q-hat from the UsageCurveFor miss path, cached alongside
	// the curve). They feed ONLY the conformal term of Upper90; absent
	// (zero value) the pure parametric Tier0 band rules.
	blrQ    float64
	blrHasQ bool
}

// Predict evaluates the curve at observed usage. O(1), pure.
func (c UsageCurve) Predict(x, y float64) float64 {
	return c.B * (1 + c.Alpha*x + c.Beta*y)
}

// FitUsageCurve fits lat = b*(1+a*x+b*y) over the eligible points via
// ordinary least squares on the linearized form lat = c0+c1*x+c2*y
// (b=c0, alpha=c1/c0, beta=c2/c0), solved in closed form (3x3 normal
// equations, partial-pivot elimination via the shared solve3 in
// psi_curve.go). Negative slopes (noise artefact — load never speeds work
// up) clamp to 0; a singular system (fewer than 3 identified directions,
// collinear or constant usage) falls back to an intercept-only fit (mean
// latency, slopes 0) instead of NaN. ok=false with a zero curve when NO
// point is eligible — never a silent b==0.
func FitUsageCurve(points []UsagePoint) (UsageCurve, bool) {
	var n int
	var sumY, sumX1, sumX2 float64
	var sumX1Y, sumX2Y, sumX1X1, sumX2X2, sumX1X2 float64
	for _, p := range points {
		if !p.eligible() {
			continue
		}
		n++
		y, x1, x2 := p.LatencyMS, p.X, p.Y
		sumY += y
		sumX1 += x1
		sumX2 += x2
		sumX1Y += x1 * y
		sumX2Y += x2 * y
		sumX1X1 += x1 * x1
		sumX2X2 += x2 * x2
		sumX1X2 += x1 * x2
	}
	if n == 0 {
		return UsageCurve{}, false
	}
	attachBand := func(curve UsageCurve) UsageCurve {
		// Local-only BLR uncertainty (noninformative-prior limit) for the
		// Tier0 band and for Tier1 temp-fit scoring (blrScaleOf needs band
		// state on temp curves). Point estimate untouched.
		if post := blrPosteriorOf(UsageCurve{}, false, points); post.ok {
			curve.blrA, curve.blrB, curve.blrInv, curve.blrHasBand = post.a, post.b, post.inv, true
		}
		return curve
	}
	aug := [3][4]float64{
		{float64(n), sumX1, sumX2, sumY},
		{sumX1, sumX1X1, sumX1X2, sumX1Y},
		{sumX2, sumX1X2, sumX2X2, sumX2Y},
	}
	coef, ok := solve3(aug)
	if !ok || coef[0] <= 0 {
		return attachBand(UsageCurve{B: sumY / float64(n), N: n}), true
	}
	alpha, beta := coef[1]/coef[0], coef[2]/coef[0]
	if alpha < 0 {
		alpha = 0
	}
	if beta < 0 {
		beta = 0
	}
	return attachBand(UsageCurve{B: coef[0], Alpha: alpha, Beta: beta, N: n}), true
}

// ShrinkUsageToPrior blends a per-node local fit toward the pooled layer
// prior with linear weight w = clamp(localN/UsageMinClassSamples, 0, 1):
// thin nodes stay glued to the prior, nodes at/above the threshold trust
// local data fully (w=1 returns local untouched). Pure, O(1). Identical
// semantics to ShrinkToPrior (psi_curve.go) over the UsageCurve type.
func ShrinkUsageToPrior(local, prior UsageCurve, localN int) UsageCurve {
	w := float64(localN) / float64(UsageMinClassSamples)
	if w < 0 {
		w = 0
	}
	if w > 1 {
		w = 1
	}
	return UsageCurve{
		B:     (1-w)*prior.B + w*local.B,
		Alpha: (1-w)*prior.Alpha + w*local.Alpha,
		Beta:  (1-w)*prior.Beta + w*local.Beta,
		N:     localN,
	}
}

// PosteriorUsageCurve builds the per-(group,node) posterior from a pooled
// layer prior and the node's local points. The point estimate is the
// conjugate Normal-Gamma posterior mean (see blr.go: coefficient-space
// linear-weight shrinkage with prior strength UsageMinClassSamples,
// mapped through the existing (B,alpha,beta) mapping + clamps),
// reproducing the three-branch shape of PosteriorPSICurve (psi_curve.go):
//
//   - no eligible local point: posterior IS the prior (fully shrunk);
//     ok=false only when the prior is invalid too.
//   - invalid prior: raw local fit (nothing to shrink to) — FitUsageCurve
//     verbatim, then the local-only BLR uncertainty is attached.
//   - fewer than 3 local points (slopes unidentifiable): borrow the prior
//     slopes and solve only the baseline b = mean(lat/(1+a*x+b*y)) —
//     attributing all latency to b would inflate it by the load pressure
//     the prior already explains — then shrink that anchored fit.
//   - otherwise: shrink the OLS local fit to the prior.
//
// The returned curve additionally carries the BLR posterior uncertainty
// for UsageCurve.Upper90 (Student-t predictive upper band); the
// (B,Alpha,Beta,N,ok) observables keep the branch semantics above.
// ok=false (zero curve) only when BOTH sources are empty.
func PosteriorUsageCurve(prior UsageCurve, priorOK bool, local []UsagePoint) (UsageCurve, bool) {
	if !priorOK {
		curve, ok := FitUsageCurve(local)
		if !ok {
			return UsageCurve{}, false
		}
		if post := blrPosteriorOf(prior, false, local); post.ok {
			curve.blrA, curve.blrB, curve.blrInv, curve.blrHasBand = post.a, post.b, post.inv, true
		}
		return curve, true
	}
	post := blrPosteriorOf(prior, true, local)
	if !post.ok {
		return UsageCurve{}, false
	}
	if post.n == 0 {
		curve := UsageCurve{B: prior.B, Alpha: prior.Alpha, Beta: prior.Beta}
		curve.blrA, curve.blrB, curve.blrInv, curve.blrHasBand = post.a, post.b, post.inv, true
		return curve, true
	}
	if post.mean[0] <= 0 {
		var sum float64
		for _, p := range local {
			if p.eligible() {
				sum += p.LatencyMS
			}
		}
		curve := UsageCurve{B: sum / float64(post.n), N: post.n}
		curve.blrA, curve.blrB, curve.blrInv, curve.blrHasBand = post.a, post.b, post.inv, true
		return curve, true
	}
	alpha, beta := post.mean[1]/post.mean[0], post.mean[2]/post.mean[0]
	if alpha < 0 {
		alpha = 0
	}
	if beta < 0 {
		beta = 0
	}
	curve := UsageCurve{B: post.mean[0], Alpha: alpha, Beta: beta, N: post.n}
	curve.blrA, curve.blrB, curve.blrInv, curve.blrHasBand = post.a, post.b, post.inv, true
	return curve, true
}

// usageXOf resolves the training X for one Entry: ProjectedLoadPct when
// known (>0) else ObservedCPUPct (0 = unknown on both axes). The projection
// is preferred because it is the dispatch-time load the decision acted on;
// the observation is the fallback when no projection was recorded.
func usageXOf(e Entry) float64 {
	if e.ProjectedLoadPct > 0 {
		return e.ProjectedLoadPct
	}
	return e.ObservedCPUPct
}

// usageOf resolves the training features for one Entry: (X, Y) =
// (usageXOf, ObservedMemPct). 0 on an axis = unknown.
func usageOf(e Entry) (x, y float64) {
	return usageXOf(e), e.ObservedMemPct
}

// FitUsageCurveFromEntries fits over history Entries with usage features
// supplied through usageOf (X = ProjectedLoadPct when >0 else
// ObservedCPUPct, Y = ObservedMemPct). The solo-only + usage-known gate
// still applies inside FitUsageCurve.
func FitUsageCurveFromEntries(entries []Entry) (UsageCurve, bool) {
	pts := make([]UsagePoint, 0, len(entries))
	for _, e := range entries {
		x, y := usageOf(e)
		pts = append(pts, UsagePoint{
			LatencyMS: float64(e.ObservedLatencyMS),
			X:         x,
			Y:         y,
			CPUPctAvg: e.CPUPctAvg,
			Inflight:  e.Inflight,
			Outcome:   e.Outcome,
			ColdStart: e.ColdStart,
		})
	}
	return FitUsageCurve(pts)
}
