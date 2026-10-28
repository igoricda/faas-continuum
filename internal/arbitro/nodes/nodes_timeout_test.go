package nodes

// Validation of the per-function execution timeout: the keys must belong
// to the hosted functions, the values must lie in the valid range and the
// package reports its own error.
//
// The test protects the validation contract of the announcement, which is
// the entry point of the hard deadline. Accepting a zero or negative
// timeout, or a key outside the hosted functions, would let the gateway arm
// a request with a degenerate deadline.
import (
	"errors"
	"testing"
)

func TestUpsertRejectsTimeoutOutsideFunctions(t *testing.T) {
	s := mustTestStore(t)
	n := Node{NodeID: "n1", Layer: LayerBorda, Host: "h", Functions: []string{"sleep"}, FunctionTimeoutMS: map[string]int{"alpr": 5000}}
	if _, err := s.Upsert(n); !errors.Is(err, ErrInvalidFunctionTimeout) {
		t.Fatalf("Upsert(timeout fora de functions) = %v, esperado %v", err, ErrInvalidFunctionTimeout)
	}
}

func TestUpsertRejectsTimeoutOutOfRange(t *testing.T) {
	s := mustTestStore(t)
	for _, ms := range []int{0, -1, 86400001} {
		n := Node{NodeID: "n1", Layer: LayerBorda, Host: "h", Functions: []string{"sleep"}, FunctionTimeoutMS: map[string]int{"sleep": ms}}
		if _, err := s.Upsert(n); !errors.Is(err, ErrInvalidFunctionTimeout) {
			t.Fatalf("Upsert(timeout_ms=%d) = %v, esperado %v", ms, err, ErrInvalidFunctionTimeout)
		}
	}
}

func TestUpsertAcceptsTimeoutSubsetAndWildcard(t *testing.T) {
	s := mustTestStore(t)
	n := Node{NodeID: "n1", Layer: LayerBorda, Host: "h", Functions: []string{"sleep"}, FunctionTimeoutMS: map[string]int{"sleep": 15000}}
	if _, err := s.Upsert(n); err != nil {
		t.Fatalf("Upsert(subconjunto válido) = %v, esperado nil", err)
	}
	got, ok := s.Get("n1")
	if !ok || got.FunctionTimeoutMS["sleep"] != 15000 {
		t.Fatalf("Get após Upsert = %v, esperado map[sleep:15000]", got.FunctionTimeoutMS)
	}
	// An empty list of functions makes the node eligible for everything,
	// so the subset rule does not apply.
	n2 := Node{NodeID: "n2", Layer: LayerBorda, Host: "h", FunctionTimeoutMS: map[string]int{"sleep": 15000}}
	if _, err := s.Upsert(n2); err != nil {
		t.Fatalf("Upsert(functions vazio + timeout) = %v, esperado nil (wildcard ignora)", err)
	}
}

func TestLoadRejectsInvalidTimeout(t *testing.T) {
	path := writeNodesFile(t, `{"n1":{"node_id":"n1","layer":"borda","host":"h","functions":["sleep"],"function_timeout_ms":{"sleep":0}}}`)
	if _, err := NewNodeStore(path); !errors.Is(err, ErrInvalidFunctionTimeout) {
		t.Fatalf("NewNodeStore(timeout_ms=0) err = %v, esperado %v", err, ErrInvalidFunctionTimeout)
	}
}
