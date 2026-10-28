// Unit tests for the reliability counter.
package learn

import (
	"math"
	"testing"
	"time"
)

// TestBeta_DecayAndLaplace checks that the estimate starts at 0.5, agrees
// with the Laplace formula when there is no decay, and halves after one
// half-life.
func TestBeta_DecayAndLaplace(t *testing.T) {
	var b Beta
	if p, n := b.PSucc(); p != 0.5 || n != 0 {
		t.Fatalf("vazio: P=%v Neff=%v, want 0.5/0", p, n)
	}
	t0 := time.Date(2026, 10, 3, 22, 0, 0, 0, time.UTC)
	for i := 0; i < 7; i++ {
		b.Observe(true, t0, 0)
	}
	for i := 0; i < 3; i++ {
		b.Observe(false, t0, 0)
	}
	if p, n := b.PSucc(); math.Abs(p-8.0/12.0) > 1e-12 || n != 10 {
		t.Fatalf("P=%v Neff=%v, want %v/10", p, n, 8.0/12.0)
	}

	var b2 Beta
	for i := 0; i < 4; i++ {
		b2.Observe(true, t0, time.Minute)
	}
	b2.decay(t0.Add(time.Minute), time.Minute)
	if _, n := b2.PSucc(); math.Abs(n-2) > 1e-9 {
		t.Fatalf("Neff após H = %v, want 2", n)
	}
}
