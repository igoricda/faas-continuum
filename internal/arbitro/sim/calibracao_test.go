package sim

import (
	"math/rand"
	"testing"
	"time"
)

// TestColdDefaults trava a faixa dos defaults de cold sem amostra real
// (o bug antigo devolvia 5 ms–3 s para xp4/lpr e 19 ms–19 s para xp4/ocr,
// não as faixas documentadas) e a precedência das amostras reais.
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

	// Amostra real do par vence o default (valor exato, sem faixa).
	cal.PorNo["xp4"] = map[string]*Amostras{
		"ocr": {Fast: map[string][]int64{}, Censuradas: map[string][]int64{}, Cold: []int64{20541}},
	}
	if d := cal.Cold("xp4", "ocr", rng); d != 20541*time.Millisecond {
		t.Fatalf("cold real=%v, want 20.541s", d)
	}
}
