package gateway

// Edges of the parse of the capacity rejection: it covers what the other tests
// do not touch, which is the exact boundary of the body ceiling, a valid JSON
// that is not an object, a truncation, the case of the code and the identity
// through a wrapped error with the propagation of the fields.

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/igoricda/faas-continuum/internal/pool"
)

func TestCapacityParseBoundaryExact(t *testing.T) {
	prefix := `{"code":"POOL_EXHAUSTED","pad":"`
	suffix := `"}`
	padLen := maxCapacityErrorBodyBytes - len(prefix) - len(suffix)
	if padLen <= 0 {
		t.Fatalf("padLen=%d, esperado positivo", padLen)
	}
	exact := []byte(prefix + strings.Repeat("x", padLen) + suffix)
	if len(exact) != maxCapacityErrorBodyBytes {
		t.Fatalf("corpo exato len=%d, esperado %d", len(exact), maxCapacityErrorBodyBytes)
	}
	got := capacityAttemptErr(exact, "n1", "busy")
	if got == nil {
		t.Fatal("corpo exatamente 64KiB válido = nil, esperado capacity error (só >64KiB trunca)")
	}
	if !errors.Is(got, pool.ErrPoolExhausted) {
		t.Fatalf("exato: errors.Is = false: %v", got)
	}
	if !IsCapacityRejection(got) {
		t.Fatalf("exato: IsCapacityRejection = false: %v", got)
	}

	over := []byte(prefix + strings.Repeat("x", padLen+1) + suffix)
	if len(over) != maxCapacityErrorBodyBytes+1 {
		t.Fatalf("corpo +1 len=%d, esperado %d", len(over), maxCapacityErrorBodyBytes+1)
	}
	if got := capacityAttemptErr(over, "n1", "busy"); got != nil {
		t.Fatalf("corpo 64KiB+1 = %v, esperado nil (caminho genérico)", got)
	}
}

func TestCapacityParseNonObjectShapes(t *testing.T) {
	// A valid JSON without an extractable code follows the generic path, which gives no capacity error.
	for name, body := range map[string][]byte{
		"null":            []byte(`null`),
		"array":           []byte(`["POOL_EXHAUSTED"]`),
		"number":          []byte(`123`),
		"string":          []byte(`"POOL_EXHAUSTED"`),
		"bool":            []byte(`true`),
		"code_numerico":   []byte(`{"code":123}`),
		"code_null":       []byte(`{"code":null}`),
		"minusculo":       []byte(`{"code":"pool_exhausted"}`),
		"code_com_espaco": []byte(`{"code":" POOL_EXHAUSTED"}`),
		"whitespace":      []byte(`   `),
		"truncado":        []byte(`{"code":"POOL_EXHAUSTED"`),
		"truncado_meio":   []byte(`{"code":"POOL_EXHAUST`),
	} {
		if got := capacityAttemptErr(body, "n1", "fallback"); got != nil {
			t.Fatalf("%s: got %v, want nil (generic path)", name, got)
		}
	}
}

func TestCapacityRejectionWrappedIdentity(t *testing.T) {
	capErr := capacityAttemptErr([]byte(`{"code":"QUEUE_FULL","message":"full"}`), "n9", "busy-msg")
	if capErr == nil {
		t.Fatal("QUEUE_FULL válido = nil, esperado capacity error")
	}
	// The error unwrapping crosses the wrap and the fields propagate.
	wrapped := fmt.Errorf("tentativa 2/3 no nó n9: %w", capErr)
	if !IsCapacityRejection(wrapped) {
		t.Fatalf("wrapped: IsCapacityRejection = false: %v", wrapped)
	}
	var as *CapacityError
	if !errors.As(wrapped, &as) {
		t.Fatalf("wrapped: errors.As *CapacityError = false: %v", wrapped)
	}
	if as.nodeID != "n9" || as.code != "QUEUE_FULL" || as.msg != "busy-msg" {
		t.Fatalf("campos = {%q %q %q}, esperado {n9 QUEUE_FULL busy-msg}", as.nodeID, as.code, as.msg)
	}
	if as.Error() != "busy-msg" {
		t.Fatalf("Error() = %q, esperado %q", as.Error(), "busy-msg")
	}
	if !errors.Is(wrapped, pool.ErrQueueFull) {
		t.Fatalf("wrapped: errors.Is pool.ErrQueueFull = false: %v", wrapped)
	}
}
