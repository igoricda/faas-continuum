// Coverage of the support for the tuning and the ablation of the selector: the
// configuration of the risk override swaps the knobs of the risk selector in
// the risk mode, and an invalid override makes the run fail fast. The grids of
// the tuning were a temporary harness, and they were consolidated and removed.
package sim

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

func overrideTestCal(t *testing.T) *Calibracao {
	t.Helper()
	home, _ := os.UserHomeDir()
	inv := filepath.Join(home, "Unioeste/ic/experimentos/dados-sessao-20261002/ANALISES/arb8-campanha/invocation.jsonl")
	cal, err := LoadCalibracao(inv, 1.0)
	if err != nil {
		t.Skipf("calibração indisponível: %v", err)
	}
	return cal
}

// TestRiskOverrideInvalidFails: the knobs outside the valid domain abort the run.
func TestRiskOverrideInvalidFails(t *testing.T) {
	cal := overrideTestCal(t)
	bad := selection.DefaultRiskConfig()
	bad.Stretch = 0.5 // the valid domain requires a value of at least one
	_, err := Run(Config{Seed: 7, MaxCPU: 300, MaxMem: 90, RiskOverride: &bad}, Cenario{
		Nome: "ov-invalido", Modo: ModoRisk, Tipo: "burst", SLO: 0,
		Workers: 2, Duracao: 2 * time.Second,
	}, cal)
	if err == nil {
		t.Fatalf("override inválido: erro=nil, want fail-fast")
	}
	t.Logf("erro esperado: %v", err)
}

// TestRiskOverrideSmoke: a valid override runs and feeds the risk choices.
func TestRiskOverrideSmoke(t *testing.T) {
	cal := overrideTestCal(t)
	ov := selection.DefaultRiskConfig()
	ov.Stretch = 1.5
	res, err := Run(Config{Seed: 7, MaxCPU: 300, MaxMem: 90, Prime: 5 * time.Second, OutDir: t.TempDir(), RiskOverride: &ov}, Cenario{
		Nome: "ov-smoke", Modo: ModoRisk, Tipo: "burst", SLO: 0,
		Workers: 4, Duracao: 8 * time.Second,
	}, cal)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Invocos == 0 || res.OK == 0 {
		t.Fatalf("sem carga medida: %+v", res)
	}
	if res.EscolhasRisk.Feasible == 0 {
		t.Fatalf("EscolhasRisk zerado com override válido: %+v", res.EscolhasRisk)
	}
}
