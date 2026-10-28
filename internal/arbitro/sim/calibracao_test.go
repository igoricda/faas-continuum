package sim

import (
	"math/rand"
	"testing"
	"time"
)

// TestColdDefaults pins the band of the cold defaults without a real sample,
// since the old bug returned ranges that were not the documented ones, and the
// precedence of the real samples.
func TestColdDefaults(t *testing.T) {
	cal := &Calibracao{PorNo: map[string]map[string]*Amostras{}}
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 50; i++ {
		if d := cal.Cold("xp4", "lpr", rng); d < 5*time.Second || d > 8*time.Second {
			t.Fatalf("xp4/lpr cold=%v, want [5s,8s]", d)
		}
		if d := cal.Cold("xp4", "ocr", rng); d < 19*time.Second || d > 38*time.Second {
			t.Fatalf("xp4/ocr cold=%v, want [19s,38s]", d)
		}
		if d := cal.Cold("ccsc-lscdesktop6", "lpr", rng); d < 800*time.Millisecond || d > 1400*time.Millisecond {
			t.Fatalf("ccsc/lpr cold=%v, want [800ms,1.4s]", d)
		}
	}

	// A real sample of the pair wins over the default, with the exact value and without a band.
	cal.PorNo["xp4"] = map[string]*Amostras{
		"ocr": {Fast: map[string][]int64{}, Censuradas: map[string][]int64{}, Cold: []int64{20541}},
	}
	if d := cal.Cold("xp4", "ocr", rng); d != 20541*time.Millisecond {
		t.Fatalf("cold real=%v, want 20.541s", d)
	}
}
