// Unit tests for the learner.
package learn

import (
	"sync"
	"testing"
	"time"
)

func TestLearner_ObservePredictSnapshot(t *testing.T) {
	l := NewLearner(Config{HalfLife: 0, Risk: 0.05, ACIGamma: 0.01, NMin: 3})
	now := time.Date(2026, 10, 3, 22, 0, 0, 0, time.UTC)
	solo := LiveState{Servers: 1, Alive: 1}
	if p := l.Predict(now, "g", "n1", solo); !p.Probation || p.PSucc != 0.5 {
		t.Fatalf("fresco: %+v, want probation + P=0.5", p)
	}
	for i := 0; i < 5; i++ {
		l.Observe("g", "n1", Obs{At: now, Outcome: OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: 1})
	}
	p := l.Predict(now, "g", "n1", solo)
	if p.Probation {
		t.Fatalf("após 5 obs: probation, want false")
	}
	if p.UpperMS < p.MedianMS {
		t.Fatalf("Upper=%v < Median=%v", p.UpperMS, p.MedianMS)
	}
	// An ignored observation does not change the effective sample size; a
	// failure changes only the reliability counter.
	l.Observe("g", "n1", Obs{At: now, Outcome: OutcomeIgnore})
	if p2 := l.Predict(now, "g", "n1", solo); p2.Neff != p.Neff {
		t.Fatalf("ignore moveu Neff: %v -> %v", p.Neff, p2.Neff)
	}
	l.Observe("g", "n1", Obs{At: now, Outcome: OutcomeFailure})
	if p3 := l.Predict(now, "g", "n1", solo); p3.Neff != p.Neff || !(p3.PSucc < p.PSucc) {
		t.Fatalf("failure: Neff=%v P=%v, want Neff=%v e P<%v", p3.Neff, p3.PSucc, p.Neff, p.PSucc)
	}
	dump := l.Snapshot(now)
	if len(dump) != 1 || dump[0].Group != "g" || dump[0].Node != "n1" {
		t.Fatalf("snapshot = %+v, want 1 modelo g/n1", dump)
	}
	if dump[0].Neff != p.Neff || dump[0].Alpha <= 0 {
		t.Fatalf("snapshot = %+v, want Neff=%v e alpha>0", dump, p.Neff)
	}
}

func TestLearner_ConcurrentSmoke(t *testing.T) {
	l := NewLearner(Config{HalfLife: time.Minute, Risk: 0.05, ACIGamma: 0.01, NMin: 3})
	now := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			node := string(rune('a' + w))
			for i := 0; i < 100; i++ {
				l.Observe("g", node, Obs{At: now, Outcome: OutcomeSuccess, LatencyMS: 100 + float64(i), UseRegr: true, M: 1})
				_ = l.Predict(now, "g", node, LiveState{Servers: 2, Alive: 2, KFn: 1, KNode: 1})
			}
		}(w)
	}
	wg.Wait()
	if got := len(l.Snapshot(now)); got != 8 {
		t.Fatalf("modelos = %d, want 8", got)
	}
}

func BenchmarkPredict(b *testing.B) {
	l := NewLearner(Config{HalfLife: time.Minute, Risk: 0.05, ACIGamma: 0.01, NMin: 3})
	now := time.Now()
	for i := 0; i < 50; i++ {
		l.Observe("g", "n1", Obs{At: now, Outcome: OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: 1})
	}
	s := LiveState{KFn: 1, KNode: 1, Servers: 2, Alive: 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l.Predict(now, "g", "n1", s)
	}
}

func TestPredictZeroAllocs(t *testing.T) {
	l := NewLearner(Config{HalfLife: time.Minute, Risk: 0.05, ACIGamma: 0.01, NMin: 3})
	now := time.Now()
	for i := 0; i < 50; i++ {
		l.Observe("g", "n1", Obs{At: now, Outcome: OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: 1})
	}
	s := LiveState{KFn: 1, KNode: 1, Servers: 2, Alive: 2}
	if n := testing.AllocsPerRun(100, func() { _ = l.Predict(now, "g", "n1", s) }); n != 0 {
		t.Fatalf("Predict allocs = %v, want 0", n)
	}
}
