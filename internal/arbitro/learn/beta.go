// Beta-Bernoulli reliability with temporal decay.
//
// S and F decay with the same delta as the NIG, and P_succ = (S+1)/(S+F+2)
// (the Laplace estimate). Callers require P_succ >= p_min (default 0.5) only
// when n_eff_beta >= 5.

package learn

import (
	"math"
	"time"
)

// Beta is the discounted reliability counter.
type Beta struct {
	s, f    float64
	last    time.Time
	touched bool
}

func (b *Beta) decay(now time.Time, halflife time.Duration) {
	if !b.touched {
		b.touched = true
		b.last = now
		return
	}
	dt := now.Sub(b.last)
	if dt <= 0 {
		return
	}
	b.last = now
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
	b.s *= d
	b.f *= d
}

// Observe records a success or a failure after decaying up to now.
func (b *Beta) Observe(success bool, now time.Time, halflife time.Duration) {
	b.decay(now, halflife)
	if success {
		b.s++
	} else {
		b.f++
	}
}

// PSucc returns (P_succ, n_eff_beta), decayed up to the last touch. With no
// data it returns P = 0.5, from the Laplace estimate (0+1)/(0+2).
func (b *Beta) PSucc() (p, neff float64) {
	return (b.s + 1) / (b.s + b.f + 2), b.s + b.f
}
