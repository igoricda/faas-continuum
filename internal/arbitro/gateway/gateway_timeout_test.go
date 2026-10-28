package gateway

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// Gate test-audit: protege o opt-in do best-effort (D1) — presença do anúncio
// liga, ausência/desconhecido/degenerado desliga (caminho síncrono). Regressão
// crível: tratar nó antigo como cap 0 (armaria pendência com prazo zero) ou
// ler função errada. Sem cobertura existente (acessor novo). Sem seam novo
// (função pura, testada direto).
func TestNodeFunctionTimeout(t *testing.T) {
	announced := nodes.Node{NodeID: "n1", FunctionTimeoutMS: map[string]int{"sleep": 15000}}
	if got, ok := nodeFunctionTimeout(announced, "sleep"); !ok || got != 15*time.Second {
		t.Fatalf("anunciado: got=%v ok=%v, want 15s true", got, ok)
	}
	if _, ok := nodeFunctionTimeout(announced, "outra"); ok {
		t.Fatalf("função não anunciada: ok=true, want false (sem best-effort)")
	}
	legacy := nodes.Node{NodeID: "n2"}
	if _, ok := nodeFunctionTimeout(legacy, "sleep"); ok {
		t.Fatalf("nó antigo sem anúncio: ok=true, want false (caminho síncrono)")
	}
	degenerate := nodes.Node{NodeID: "n3", FunctionTimeoutMS: map[string]int{"sleep": 0}}
	if _, ok := nodeFunctionTimeout(degenerate, "sleep"); ok {
		t.Fatalf("valor degenerado: ok=true, want false (defensivo)")
	}
}
