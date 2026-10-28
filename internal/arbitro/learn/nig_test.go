// Unit tests for the Normal-Inverse-Gamma regressor.
package learn

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

var testT0 = time.Date(2026, 10, 3, 22, 0, 0, 0, time.UTC)

// TestNIG_PosteriorMatchesBatch checks that, without decay, the sequential
// posterior equals the batch posterior for the same prior. The batch case is
// computed by hand from two points.
func TestNIG_PosteriorMatchesBatch(t *testing.T) {
	prior := NIGPrior{M0: [2]float64{0, 0}, L0: [2][2]float64{{1, 0}, {0, 1}}, A0: 1.5, B0: 0.375}
	n := NewNIG(prior)
	n.Observe([2]float64{1, 0}, 1, testT0, 0)
	n.Observe([2]float64{1, 1}, 2, testT0, 0)
	m, a, b, _ := n.Posterior(testT0, 0)
	if math.Abs(m[0]-0.8) > 1e-9 || math.Abs(m[1]-0.6) > 1e-9 {
		t.Fatalf("m = %v, want [0.8 0.6]", m)
	}
	if math.Abs(a-2.5) > 1e-9 {
		t.Fatalf("a = %v, want 2.5", a)
	}
	if math.Abs(b-1.075) > 1e-9 {
		t.Fatalf("b = %v, want 1.075", b)
	}
	if n.Neff() != 2 {
		t.Fatalf("Neff = %v, want 2", n.Neff())
	}
}

// TestNIG_DecayHalfLife checks that the effective sample size halves after
// one half-life and that the posterior returns to the prior after a long
// time without observations.
func TestNIG_DecayHalfLife(t *testing.T) {
	n := NewNIG(WeakWarmPrior())
	h := time.Minute
	for i := 0; i < 10; i++ {
		n.Observe([2]float64{1, 0}, 7.0, testT0, h)
	}
	if n.Neff() != 10 {
		t.Fatalf("Neff = %v, want 10", n.Neff())
	}
	m, _, _, _ := n.Posterior(testT0.Add(h), h)
	if math.Abs(n.Neff()-5) > 1e-9 {
		t.Fatalf("Neff após H = %v, want 5", n.Neff())
	}
	// With data, the mean leaves the prior.
	if !(m[0] > 1) {
		t.Fatalf("m[0] = %v, want > 1 com 5 obs efetivas de y=7", m[0])
	}
	m2, _, _, _ := n.Posterior(testT0.Add(1000*h), h)
	if n.Neff() > 1e-9 {
		t.Fatalf("Neff após 1000H = %v, want ~0", n.Neff())
	}
	if math.Abs(m2[0]) > 1e-6 || math.Abs(m2[1]) > 1e-6 {
		t.Fatalf("m após 1000H = %v, want ~prior [0 0]", m2)
	}
}

// TestNIG_SlopeRecovered checks that synthetic data of the form
// ln S = 6.9 + 0.4 ln m recovers the slope within 0.05 with at least 50
// points.
func TestNIG_SlopeRecovered(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	n := NewNIG(WeakWarmPrior())
	const total = 500
	for i := 0; i < total; i++ {
		m := 1 + rng.Intn(8)
		y := 6.9 + 0.4*math.Log(float64(m)) + rng.NormFloat64()*0.3
		n.Observe([2]float64{1, math.Log(float64(m))}, y, testT0, 0)
	}
	m, _, _, _ := n.Posterior(testT0, 0)
	if math.Abs(m[1]-0.4) > 0.05 {
		t.Fatalf("slope = %v, want 0.4 ±0.05", m[1])
	}
	if math.Abs(m[0]-6.9) > 0.1 {
		t.Fatalf("intercept = %v, want 6.9 ±0.1", m[0])
	}
}
