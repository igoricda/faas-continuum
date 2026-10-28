package sim

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSmokeBancada valida o encanamento da bancada ponta a ponta com o
// invocation.jsonl REAL da sessão (pula se o fixture não existir nesta
// máquina). Não é uma asserção de política — é guarda de infraestrutura:
// mocks sobem, o gateway real aceita carga, a nuvem responde e as métricas
// saem consistentes.
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
