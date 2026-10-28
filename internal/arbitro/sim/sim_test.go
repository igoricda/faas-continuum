package sim

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSmokeBancada validates the wiring of the bench end to end with the real
// invocation log of the session, and it is skipped when the fixture is not
// present. It is not an assertion of policy but a guard of the infrastructure:
// the mocks start, the real gateway accepts the load, the cloud answers and the
// metrics come out consistent.
func TestSmokeBancada(t *testing.T) {
	home, _ := os.UserHomeDir()
	inv := filepath.Join(home, "Unioeste/ic/experimentos/dados-sessao-20260924/BRUTOS/arbitro/invocation.jsonl")
	if _, err := os.Stat(inv); err != nil {
		t.Skipf("fixture real %s indisponível: %v", inv, err)
	}
	cal, err := LoadCalibracao(inv, 1.0)
	if err != nil {
		t.Fatalf("LoadCalibracao: %v", err)
	}
	if len(cal.Cloud) == 0 {
		t.Fatalf("calibração sem amostras de nuvem (fixture inesperado)")
	}
	res, err := Run(Config{Seed: 7, MaxCPU: 300, MaxMem: 90}, Cenario{
		Nome: "smoke", Modo: ModoRisk, Tipo: "burst", SLO: 5000,
		Workers: 2, Duracao: 3 * time.Second,
	}, cal)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Invocos == 0 || res.OK == 0 {
		t.Fatalf("bancada não produziu invocações: %+v", res)
	}
	if res.TentativasPorNo["xp4"] == 0 {
		t.Fatalf("borda não recebeu tentativa: %v", res.TentativasPorNo)
	}
}
