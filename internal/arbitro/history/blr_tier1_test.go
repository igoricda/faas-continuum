package history

// Unit tests for Tier0/Tier1 calibration (ADR-0007 D32, see blr.go):
// q-hat math on hand-computed residuals (incl. below-minimum unavailability
// and one-sidedness vs absolute residuals), floor binding on tiny
// residuals, spec ranges of the Tier0 constants, and determinism.
// Expectations derive from the documented formulas — never from the
// implementation's internals. blrLawPoints/soloUsagePoint are reused from
// blr_test.go / usage_curve_test.go (same package).

import (
	"math"
	"testing"
	"time"
)

// Hand-computed q-hat: m=10 scores, k=ceil(11*0.9)=ceil(9.9)=10, so q-hat
// is the largest = 3.0.
func TestBLRConformalQHandComputed(t *testing.T) {
	scores := []float64{0.5, -1.0, 2.0, 1.5, 0.0, 3.0, -0.5, 1.0, 2.5, -2.0}
	q, ok := blrConformalQ(scores, 5)
	if !ok {
		t.Fatal("blrConformalQ: ok=false on m=10 >= min 5")
	}
	if q != 3.0 {
		t.Fatalf("q-hat=%.6f, want exactly 3.0 (10th smallest of 10)", q)
	}
	// Caller's slice untouched (sorts a copy).
	if scores[0] != 0.5 || scores[9] != -2.0 {
		t.Fatalf("input slice mutated: %+v", scores)
	}
}

// One-sidedness: a huge NEGATIVE score must not inflate the upper band.
// Absolute-residual (two-sided) scoring would return 100 here; the signed
// one-sided quantile returns 9.0.
func TestBLRConformalQOneSidedNotAbsolute(t *testing.T) {
	scores := []float64{-100.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}
	q, ok := blrConformalQ(scores, 5)
	if !ok {
		t.Fatal("blrConformalQ: ok=false")
	}
	if q != 9.0 {
		t.Fatalf("q-hat=%.6f, want 9.0 (upper tail only; -100 must not count)", q)
	}
}

// Below-minimum and empty pools: q-hat unavailable -> parametric band.
func TestBLRConformalQBelowMinimumUnavailable(t *testing.T) {
	scores := []float64{0.5, -1.0, 2.0, 1.5, 0.0, 3.0, -0.5, 1.0, 2.5, -2.0}
	if _, ok := blrConformalQ(scores, 11); ok {
		t.Fatal("m=10 < min 11: ok=true, want unavailable")
	}
	if _, ok := blrConformalQ(nil, 5); ok {
		t.Fatal("empty pool: ok=true, want unavailable")
	}
	if _, ok := blrConformalQ([]float64{1.0}, 1); ok {
		t.Fatal("m=1: k=ceil(1.8)=2>m=1 must be unavailable, got ok=true")
	}
}

// Determinism: same multiset in any order yields bit-identical q-hat
// (sort-based quantile, no float summation order involved).
func TestBLRConformalQDeterministic(t *testing.T) {
	fwd := []float64{0.5, -1.0, 2.0, 1.5, 0.0, 3.0, -0.5, 1.0, 2.5, -2.0, 4.0, -3.0}
	rev := make([]float64, len(fwd))
	for i, v := range fwd {
		rev[len(fwd)-1-i] = v
	}
	qa, oka := blrConformalQ(fwd, 5)
	qb, okb := blrConformalQ(rev, 5)
	if !oka || !okb {
		t.Fatal("blrConformalQ: ok=false")
	}
	if math.Float64bits(qa) != math.Float64bits(qb) {
		t.Fatalf("order-dependent q-hat: %x vs %x", math.Float64bits(qa), math.Float64bits(qb))
	}
}

// Tier0 constant spec ranges: 2*a0 in 5-10 pseudo-obs, floorRel in 5-10%,
// floorAbs from timer granularity (>0), calibration minimum 30 shipped.
func TestBLRTier0ConstantRanges(t *testing.T) {
	if dof := 2 * blrPriorA0; dof < 5 || dof > 10 {
		t.Fatalf("2*a0=%.1f, want 5-10 pseudo-observations", dof)
	}
	if blrFloorRel < 0.05 || blrFloorRel > 0.10 {
		t.Fatalf("floorRel=%.3f, want 5-10%%", blrFloorRel)
	}
	if !(blrFloorAbs > 0) {
		t.Fatalf("floorAbs=%.3f, want >0 (timer granularity)", blrFloorAbs)
	}
	if blrCalMinSamples != 30 {
		t.Fatalf("calibration minimum=%d, want shipped 30 (knob 30->50->100)", blrCalMinSamples)
	}
	// b0 = a0*sigma0^2 with sigma0 = 30ms documented jitter.
	if sigma0 := math.Sqrt(blrPriorB0 / blrPriorA0); math.Abs(sigma0-30) > 1e-9 {
		t.Fatalf("prior jitter sigma0=%.6f, want 30ms (r3 warm-solo)", sigma0)
	}
}

// Floor binds on a near-exact fit: with a big-base exact law the Student-t
// halfwidth (~35ms) sits far below floorRel*|point| (~7% of ~2000ms), so
// the width must EQUAL the relative floor bit-exactly (max picks it).
func TestBLRFloorBindsOnTinyResidual(t *testing.T) {
	prior, ok := FitUsageCurve(blrLawPoints(2500, 0.02, 0.008, 8))
	if !ok {
		t.Fatal("prior fit failed")
	}
	post, ok := PosteriorUsageCurve(prior, true, blrLawPoints(2000, 0.01, 0.005, 12))
	if !ok {
		t.Fatal("posterior failed")
	}
	const qx, qy = 25.0, 30.0
	pred := post.Predict(qx, qy)
	want := blrFloorRel * math.Abs(pred)
	got := post.Upper90(qx, qy) - pred
	// max() returns the floor term bit-exactly, but Upper90-pred rounds in
	// the last ulp — near-equality plus proof the t-term is far smaller.
	if relErr(got, want) > 1e-9 {
		t.Fatalf("width=%.9f, want floorRel*|pred|=%.9f (floor must bind)", got, want)
	}
	if halfT, ok := blrHalfT(post, qx, qy); !ok || !(halfT < want/2) {
		t.Fatalf("t-halfwidth=%.6f not far below floor %.6f (floor must bind)", halfT, want)
	}
	if w := post.Upper90(qx, qy) - pred; w < blrFloorAbs {
		t.Fatalf("width=%.6f below absolute floor %.6f", w, blrFloorAbs)
	}
}

// Tier1 end-to-end through UsageCurveFor: a dense windowed history yields
// a q-hat-carrying curve with Upper90 >= Upper90Parametric >= Predict;
// a thin history stays parametric (ConformalQ unavailable).
func TestBLRTier1ThroughCache(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	newHist := func(t *testing.T) *History {
		t.Helper()
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		return h
	}
	appendAt := func(t *testing.T, h *History, node string, lat int64, x, y float64, ts time.Time) {
		t.Helper()
		e := Entry{
			FunctionName: "g", NodeID: node, Layer: "borda", Timestamp: ts,
			ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
			ObservedLatencyMS: lat, Outcome: OutcomeSucesso,
			Inflight: 1, CPUPctAvg: 10,
			ProjectedLoadPct: x, ObservedMemPct: y,
		}
		if err := h.AppendWithGroup(e, "g"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	// Dense: 80 solo points over the full window (40 older / 40 newer at
	// mid-window) on a noisy law -> m=40+ calibration scores available.
	h := newHist(t)
	grid := [][2]float64{{10, 20}, {20, 10}, {30, 40}, {40, 30}, {50, 10}, {10, 50}, {60, 60}, {20, 70}}
	for i := 0; i < 80; i++ {
		s := grid[i%len(grid)]
		lat := 200 * (1 + 0.01*s[0] + 0.005*s[1])
		if i%2 == 0 {
			lat *= 1.1
		} else {
			lat *= 0.9
		}
		appendAt(t, h, "n1", int64(lat), s[0], s[1], now.Add(-window+time.Duration(i)*window/80))
	}
	dense, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
	if !ok {
		t.Fatal("dense UsageCurveFor: ok=false")
	}
	q, hasQ := dense.ConformalQ()
	if !hasQ {
		t.Fatal("dense curve: ConformalQ unavailable, want pooled q-hat (m>=30)")
	}
	if math.IsNaN(q) || math.IsInf(q, 0) {
		t.Fatalf("q-hat invalid: %v", q)
	}
	pred, upper, param := dense.Predict(25, 30), dense.Upper90(25, 30), dense.Upper90Parametric(25, 30)
	if !(upper >= param && param >= pred) {
		t.Fatalf("band order broken: pred=%.3f param=%.3f upper=%.3f (q-hat=%.3f)", pred, param, upper, q)
	}
	// Determinism through the cache path: same query, bit-identical curve.
	again, ok := h.UsageCurveFor("g", "n1", "borda", window, now)
	if !ok || again != dense {
		t.Fatalf("cache re-query not bit-identical:\n%+v\n%+v", again, dense)
	}

	// Thin: 4 points -> newer-half scores < 30 -> parametric only.
	h2 := newHist(t)
	ts := now.Add(-time.Minute)
	for i := 0; i < 4; i++ {
		s := grid[i]
		appendAt(t, h2, "n2", int64(200*(1+0.01*s[0]+0.005*s[1])), s[0], s[1], ts)
	}
	thin, ok := h2.UsageCurveFor("g", "n2", "borda", window, now)
	if !ok {
		t.Fatal("thin UsageCurveFor: ok=false")
	}
	if _, hasQ := thin.ConformalQ(); hasQ {
		t.Fatal("thin curve: ConformalQ available below minimum, want parametric")
	}
	if got := thin.Upper90(25, 30); !(got >= thin.Predict(25, 30)) {
		t.Fatalf("thin Upper90=%.3f below Predict=%.3f", got, thin.Predict(25, 30))
	}
}

// blrUsageCalQ determinism on fixed entry slices (same input -> identical
// bits), and unavailability with an empty newer half.
func TestBLRUsageCalQDeterministic(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Hour
	var prior, local []Entry
	grid := [][2]float64{{10, 20}, {20, 10}, {30, 40}, {40, 30}, {50, 10}, {10, 50}, {60, 60}, {20, 70}}
	for i := 0; i < 80; i++ {
		s := grid[i%len(grid)]
		lat := 200 * (1 + 0.01*s[0] + 0.005*s[1])
		if i%2 == 0 {
			lat *= 1.1
		} else {
			lat *= 0.9
		}
		e := Entry{
			FunctionName: "g", NodeID: "donor", Layer: "borda",
			Timestamp:         now.Add(-window + time.Duration(i)*window/80),
			ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
			ObservedLatencyMS: int64(lat), Outcome: OutcomeSucesso,
			Inflight: 1, CPUPctAvg: 10,
			ProjectedLoadPct: s[0], ObservedMemPct: s[1],
		}
		prior = append(prior, e)
		if i%2 == 0 {
			e.NodeID = "n1"
			local = append(local, e)
		}
	}
	qa, oka := blrUsageCalQ(prior, local, "n1", window, now, blrCalMinSamples)
	qb, okb := blrUsageCalQ(prior, local, "n1", window, now, blrCalMinSamples)
	if !oka || !okb {
		t.Fatal("blrUsageCalQ: ok=false on dense windowed entries")
	}
	if math.Float64bits(qa) != math.Float64bits(qb) {
		t.Fatalf("q-hat not bit-identical: %x vs %x", math.Float64bits(qa), math.Float64bits(qb))
	}
	// Empty newer half (all entries older than mid-window) -> unavailable.
	var oldOnly []Entry
	for _, e := range prior {
		e.Timestamp = now.Add(-45 * time.Minute)
		oldOnly = append(oldOnly, e)
	}
	if _, ok := blrUsageCalQ(oldOnly, nil, "n1", window, now, blrCalMinSamples); ok {
		t.Fatal("empty newer half: ok=true, want unavailable")
	}
}
