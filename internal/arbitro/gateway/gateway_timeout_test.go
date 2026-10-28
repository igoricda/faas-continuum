package gateway

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// Gate test: it protects the opt-in of the best-effort delivery, where the
// presence of the announcement enables it and its absence, an unknown value or a
// degenerate one disables it and keeps the synchronous path. The credible
// regression is to treat an older node as a zero ceiling, which would arm a
// pending report with a zero deadline, or to read the wrong function.
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
