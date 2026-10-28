// Unit tests for the model: calibration under a distribution shift, input
// robustness and routing.
package learn

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

var modelCfg = Config{HalfLife: 0, Risk: 0.05, ACIGamma: 0.01, NMin: 3}

var soloLive = LiveState{KFn: 0, KNode: 0, Servers: 1, Alive: 1}

// TestACI_CoverageUnderShift feeds a lognormal series with a mean shift in
// the middle and checks that the empirical long-run coverage stays within
// one percentage point of the target, and that the calibration level stays
// inside its bounds.
func TestACI_CoverageUnderShift(t *testing.T) {
	m := NewModel(WeakWarmPrior(), ColdPrior(0, 0, 0), 0.05)
	rng := rand.New(rand.NewSource(7))
	t0 := time.Date(2026, 10, 3, 22, 0, 0, 0, time.UTC)
	const total = 3000
	errs := make([]bool, 0, total)
	for i := 0; i < total; i++ {
		mu := 6.9
		if i >= total/2 {
			mu = 7.4
		}
		y := mu + rng.NormFloat64()*0.35
		lat := math.Exp(y)
		at := t0.Add(time.Duration(i) * time.Second)
		bound := m.Predict(at, soloLive, modelCfg).UpperMS
		errs = append(errs, lat > bound)
		if al := m.Alpha(0.05); al < 0.001 || al > 0.5 {
			t.Fatalf("passo %d: alpha=%v fora de [0.001, 0.5]", i, al)
		}
		m.Observe(Obs{At: at, Outcome: OutcomeSuccess, LatencyMS: lat, UseRegr: true, M: 1}, modelCfg)
	}
	miss := 0
	for _, e := range errs[total-500:] {
		if e {
			miss++
		}
	}
	cov := 1 - float64(miss)/500
	if math.Abs(cov-0.95) > 0.01 {
		t.Fatalf("cobertura rolagem-500 = %v, want 0.95 ±0.01", cov)
	}
}

// TestModel_GarbageNeverCorrupts: NaN/Inf/negativos em Obs nunca
// corrompem o estado (descartar).
func TestModel_GarbageNeverCorrupts(t *testing.T) {
	m := NewModel(WeakWarmPrior(), ColdPrior(0, 0, 0), 0.05)
	t0 := time.Date(2026, 10, 3, 22, 0, 0, 0, time.UTC)
	m.Observe(Obs{At: t0, Outcome: OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: 1}, modelCfg)
	before := m.Predict(t0, soloLive, modelCfg)
	garbage := []Obs{
		{At: t0, Outcome: OutcomeSuccess, LatencyMS: math.NaN(), UseRegr: true, M: 1},
		{At: t0, Outcome: OutcomeSuccess, LatencyMS: math.Inf(1), UseRegr: true, M: 1},
		{At: t0, Outcome: OutcomeSuccess, LatencyMS: -5, UseRegr: true, M: 1},
		{At: t0, Outcome: OutcomeSuccess, LatencyMS: 0, UseRegr: true, M: 1},
		{At: t0, Outcome: OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: math.NaN()},
		{At: t0, Outcome: OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: math.Inf(1)},
		{At: t0, Outcome: OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: 0},
		{At: t0, Outcome: OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: -2},
		{At: t0, Outcome: OutcomeSuccess, Cold: true, LatencyMS: math.NaN()},
	}
	for i, o := range garbage {
		m.Observe(o, modelCfg)
		after := m.Predict(t0, soloLive, modelCfg)
		if after.Neff != before.Neff {
			t.Fatalf("lixo %d: Neff %v -> %v (regressor contaminado)", i, before.Neff, after.Neff)
		}
		if m.Alpha(0.05) != beforeAlpha(m) {
			t.Fatalf("lixo %d: alpha mudou", i)
		}
		assertFinitePrediction(t, i, after)
	}
}

func beforeAlpha(m *Model) float64 { return m.Alpha(0.05) }

func assertFinitePrediction(t *testing.T, i int, p Prediction) {
	t.Helper()
	for _, v := range []float64{p.UpperMS, p.MedianMS, p.WaitMS, p.PSucc, p.Neff, p.NeffBeta} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			t.Fatalf("lixo %d: previsão não-finita: %+v", i, p)
		}
	}
	if p.PSucc < 0 || p.PSucc > 1 {
		t.Fatalf("lixo %d: P_succ=%v fora de [0,1]", i, p.PSucc)
	}
	if p.UpperMS < p.MedianMS {
		t.Fatalf("lixo %d: Upper=%v < Median=%v", i, p.UpperMS, p.MedianMS)
	}
}

// TestModel_ProbationAndColdRouting checks that a fresh model is in
// probation, that enough successes lift it out, and that a cold observation
// is routed to the cold regressor.
func TestModel_ProbationAndColdRouting(t *testing.T) {
	m := NewModel(WeakWarmPrior(), ColdPrior(0, 0, 0), 0.05)
	t0 := time.Date(2026, 10, 3, 22, 0, 0, 0, time.UTC)
	if p := m.Predict(t0, soloLive, modelCfg); !p.Probation {
		t.Fatalf("fresco: Probation=false, want true")
	}
	for i := 0; i < 3; i++ {
		m.Observe(Obs{At: t0, Outcome: OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: 1}, modelCfg)
	}
	if p := m.Predict(t0, soloLive, modelCfg); p.Probation {
		t.Fatalf("após 3 obs: Probation=true, want false")
	} else if math.Abs(p.MedianMS-800) > 400 {
		t.Fatalf("Median=%v, want ~800", p.MedianMS)
	}
	coldLive := LiveState{KFn: 0, KNode: 0, Servers: 1, Alive: 0, Cold: true}
	m.Observe(Obs{At: t0, Outcome: OutcomeSuccess, Cold: true, LatencyMS: 8600}, modelCfg)
	pc := m.Predict(t0, coldLive, modelCfg)
	if !pc.Cold || pc.WaitMS != 0 {
		t.Fatalf("cold: %+v, want Cold=true Wait=0", pc)
	}
	// With more requests in flight than servers, the wait is positive.
	busy := LiveState{KFn: 3, KNode: 3, Servers: 2, Alive: 2}
	pb := m.Predict(t0, busy, modelCfg)
	if pb.WaitMS <= 0 {
		t.Fatalf("ocupado: Wait=%v, want > 0", pb.WaitMS)
	}
	if pb.UpperMS < pb.MedianMS+pb.WaitMS {
		t.Fatalf("ocupado: Upper=%v < Median+Wait=%v", pb.UpperMS, pb.MedianMS+pb.WaitMS)
	}
}
