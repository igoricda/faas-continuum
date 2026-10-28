// PSI pressure curve estimator: a pooled fit of the solo entries with
// shrinkage to a prior of the layer.
//
// The model has a closed form, with a constant-time prediction and no
// Gaussian process and no Bayesian inference in the hot path:
//
//	p95_pred = b * (1 + alpha*cpu_some + beta*mem_full)
//
// The fit gate mirrors the solo-only invariant of the expected cost: exactly
// one request in flight, a positive average processor usage, no cold start, a
// success and a known pressure on both axes, where zero means unknown and is
// excluded from the fit. Cold entries and contended or deep-queue entries never
// train and validate the model downstream.
//
// The pressure is taken as explicit input, either as points or through entries
// plus an accessor, since the entry schema carries the pressure values that the
// accessor reads.
//
// The functions are pure, without input or output, locks or global state.
//
// Calibration: the pooled linear law errs 0.41 against the percentile targets
// in the synthetic overlay, where the knee of the bucket doubles the local
// error and the gaps between the mean and the percentile put a floor on any
// estimator of central tendency. An offline quantile isotonic fit reaches 0.06,
// and it is the registered fallback for a re-gate with real pressure data,
// with a future implementation outside the hot path of the selection.
package history

import "math"

// PSIMinClassSamples is the threshold of solo samples above which a per-node
// posterior stops shrinking to the prior of the layer and trusts its local
// fit. It mirrors the equivalent threshold of the selection package, and it is
// defined here instead of imported because the selection imports the history
// and importing it back would close a cycle. The two values are kept in
// lockstep, and a cross-check test would face the same cycle.
const PSIMinClassSamples = 5

// PSIPoint is one training observation with EXPLICIT PSI values. The gate
// fields (Inflight/CPUPctAvg/ColdStart/Outcome) carry the solo-only filter
// so FitPSICurve can enforce it without touching Entry.
type PSIPoint struct {
	LatencyMS float64
	CPUSome   float64
	MemFull   float64
	CPUPctAvg float64
	Inflight  int
	Outcome   string
	ColdStart bool
}

// eligible reports whether the point may train the estimator: the
// ExpectedCost solo-only gate plus PSI known on both axes.
func (p PSIPoint) eligible() bool {
	return p.Inflight == 1 &&
		p.CPUPctAvg > 0 &&
		!p.ColdStart &&
		p.Outcome == OutcomeSucesso &&
		p.CPUSome > 0 &&
		p.MemFull > 0
}

// PSICurve holds the fitted pressure law p95 = B*(1+Alpha*cpu+Beta*mem).
// N counts the eligible training points behind it.
type PSICurve struct {
	B     float64
	Alpha float64
	Beta  float64
	N     int
}

// Predict evaluates the curve at observed PSI. O(1), pure.
func (c PSICurve) Predict(cpuSome, memFull float64) float64 {
	return c.B * (1 + c.Alpha*cpuSome + c.Beta*memFull)
}

// FitPSICurve fits lat = b*(1+alpha*x+beta*y) over the eligible points via
// ordinary least squares on the linearized form lat = c0+c1*x+c2*y
// (b=c0, alpha=c1/c0, beta=c2/c0), solved in closed form (3x3 normal
// equations, partial-pivot elimination). Negative slopes (noise artefact —
// pressure never speeds work up) clamp to 0; a singular system (fewer than
// 3 identified directions, collinear or constant PSI) falls back to an
// intercept-only fit (mean latency, slopes 0) instead of NaN. ok=false
// with a zero curve when NO point is eligible — never a silent b==0.
func FitPSICurve(points []PSIPoint) (PSICurve, bool) {
	var n int
	var sumY, sumX1, sumX2 float64
	var sumX1Y, sumX2Y, sumX1X1, sumX2X2, sumX1X2 float64
	for _, p := range points {
		if !p.eligible() {
			continue
		}
		n++
		y, x1, x2 := p.LatencyMS, p.CPUSome, p.MemFull
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
		return PSICurve{}, false
	}
	aug := [3][4]float64{
		{float64(n), sumX1, sumX2, sumY},
		{sumX1, sumX1X1, sumX1X2, sumX1Y},
		{sumX2, sumX1X2, sumX2X2, sumX2Y},
	}
	coef, ok := solve3(aug)
	if !ok || coef[0] <= 0 {
		return PSICurve{B: sumY / float64(n), N: n}, true
	}
	alpha, beta := coef[1]/coef[0], coef[2]/coef[0]
	if alpha < 0 {
		alpha = 0
	}
	if beta < 0 {
		beta = 0
	}
	return PSICurve{B: coef[0], Alpha: alpha, Beta: beta, N: n}, true
}

// solve3 solves a 3x4 augmented system by Gaussian elimination with partial
// pivoting. ok=false when the matrix is singular (pivot ~0).
func solve3(a [3][4]float64) (coef [3]float64, ok bool) {
	const eps = 1e-9
	for col := 0; col < 3; col++ {
		pivot := col
		for row := col + 1; row < 3; row++ {
			if math.Abs(a[row][col]) > math.Abs(a[pivot][col]) {
				pivot = row
			}
		}
		if math.Abs(a[pivot][col]) < eps {
			return coef, false
		}
		a[col], a[pivot] = a[pivot], a[col]
		for row := col + 1; row < 3; row++ {
			f := a[row][col] / a[col][col]
			for k := col; k < 4; k++ {
				a[row][k] -= f * a[col][k]
			}
		}
	}
	for row := 2; row >= 0; row-- {
		s := a[row][3]
		for k := row + 1; k < 3; k++ {
			s -= a[row][k] * coef[k]
		}
		coef[row] = s / a[row][row]
	}
	return coef, true
}

// ShrinkToPrior blends a per-node local fit toward the pooled layer prior
// with linear weight w = clamp(localN/PSIMinClassSamples, 0, 1): thin nodes
// stay glued to the prior, nodes at/above the threshold trust local data
// fully (w=1 returns local untouched). Pure, O(1).
func ShrinkToPrior(local, prior PSICurve, localN int) PSICurve {
	w := float64(localN) / float64(PSIMinClassSamples)
	if w < 0 {
		w = 0
	}
	if w > 1 {
		w = 1
	}
	return PSICurve{
		B:     (1-w)*prior.B + w*local.B,
		Alpha: (1-w)*prior.Alpha + w*local.Alpha,
		Beta:  (1-w)*prior.Beta + w*local.Beta,
		N:     localN,
	}
}

// PosteriorPSICurve builds the per-(group,node) posterior from a pooled
// layer prior and the node's local points. Semantics:
//
//   - no eligible local point: posterior IS the prior (fully shrunk);
//     ok=false only when the prior is invalid too.
//   - invalid prior: raw local fit (nothing to shrink to).
//   - fewer than 3 local points (slopes unidentifiable): borrow the prior
//     slopes and solve only the baseline b = mean(lat/(1+a*x+b*y)) —
//     attributing all latency to b would inflate it by the pressure the
//     prior already explains — then shrink that anchored fit.
//   - otherwise: shrink the OLS local fit to the prior.
//
// ok=false (zero curve) only when BOTH sources are empty.
func PosteriorPSICurve(prior PSICurve, priorOK bool, local []PSIPoint) (PSICurve, bool) {
	var elig []PSIPoint
	for _, p := range local {
		if p.eligible() {
			elig = append(elig, p)
		}
	}
	if len(elig) == 0 {
		if !priorOK {
			return PSICurve{}, false
		}
		return PSICurve{B: prior.B, Alpha: prior.Alpha, Beta: prior.Beta}, true
	}
	if !priorOK {
		return FitPSICurve(local)
	}
	if len(elig) < 3 {
		var sum float64
		for _, p := range elig {
			denom := 1 + prior.Alpha*p.CPUSome + prior.Beta*p.MemFull
			if denom <= 0 {
				denom = 1
			}
			sum += p.LatencyMS / denom
		}
		anchored := PSICurve{
			B:     sum / float64(len(elig)),
			Alpha: prior.Alpha,
			Beta:  prior.Beta,
			N:     len(elig),
		}
		return ShrinkToPrior(anchored, prior, len(elig)), true
	}
	loc, _ := FitPSICurve(local)
	return ShrinkToPrior(loc, prior, len(elig)), true
}

// FitPSICurveFromEntries fits over history Entries with PSI supplied by the
// caller through psiOf, which is the accessor that reads
// PSICpuSomeAvg10 and PSIMemFullAvg10 from the Entry.
// The solo-only + PSI>0 gate still applies inside FitPSICurve. nil psiOf
// is fail-closed (ok=false).
func FitPSICurveFromEntries(entries []Entry, psiOf func(Entry) (cpuSome, memFull float64)) (PSICurve, bool) {
	if psiOf == nil {
		return PSICurve{}, false
	}
	pts := make([]PSIPoint, 0, len(entries))
	for _, e := range entries {
		cpu, mem := psiOf(e)
		pts = append(pts, PSIPoint{
			LatencyMS: float64(e.ObservedLatencyMS),
			CPUSome:   cpu,
			MemFull:   mem,
			CPUPctAvg: e.CPUPctAvg,
			Inflight:  e.Inflight,
			Outcome:   e.Outcome,
			ColdStart: e.ColdStart,
		})
	}
	return FitPSICurve(pts)
}
