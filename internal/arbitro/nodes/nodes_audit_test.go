package nodes

// Regression tests for the validation on load, which must match the
// validation on registration, and for the subset rule of the declared
// ceilings: a file edited by hand with an invalid layer, a negative
// ceiling or a key outside the hosted functions fails at startup instead
// of being accepted silently.

import (
	"os"
	"path/filepath"
	"testing"
)

func writeNodesFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nodes.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp nodes: %v", err)
	}
	return path
}

func TestLoadRejectsInvalidLayer(t *testing.T) {
	// An invalid layer in the file fails at startup, because otherwise it
	// would silently fall back to the permissive default and never be
	// filtered by the ceiling.
	path := writeNodesFile(t, `{"n1":{"node_id":"n1","layer":"nuvem","host":"h"}}`)
	if _, err := NewNodeStore(path); err == nil {
		t.Fatal("NewNodeStore(layer=nuvem) = nil, esperado erro fail-fast")
	}
}

func TestLoadRejectsNegativeCapacity(t *testing.T) {
	// A negative ceiling in the file fails at startup, as on registration.
	path := writeNodesFile(t, `{"n1":{"node_id":"n1","layer":"borda","host":"h","functions":["sleep"],"max_instances":{"sleep":-5}}}`)
	if _, err := NewNodeStore(path); err == nil {
		t.Fatal("NewNodeStore(max_instances=-5) = nil, esperado erro fail-fast")
	}
}

func TestUpsertRejectsCapacityOutsideFunctions(t *testing.T) {
	// A ceiling key outside the hosted functions violates the subset rule,
	// the same rule that applies to the categories.
	s := mustTestStore(t)
	n := Node{NodeID: "n1", Layer: LayerBorda, Host: "h", Functions: []string{"sleep"}, FunctionCapacity: map[string]int{"alpr": 2}}
	if _, err := s.Upsert(n); err == nil {
		t.Fatal("Upsert(max_instances fora de functions) = nil, esperado ErrInvalidFunctionCapacity")
	}
}

func TestUpsertAcceptsCapacitySubsetAndWildcard(t *testing.T) {
	s := mustTestStore(t)
	// A valid subset passes.
	n := Node{NodeID: "n1", Layer: LayerBorda, Host: "h", Functions: []string{"sleep"}, FunctionCapacity: map[string]int{"sleep": 2}}
	if _, err := s.Upsert(n); err != nil {
		t.Fatalf("Upsert(subconjunto válido) = %v, esperado nil", err)
	}
	// An empty list of functions makes the node eligible for everything,
	// so the ceiling is ignored and accepted.
	n2 := Node{NodeID: "n2", Layer: LayerBorda, Host: "h", FunctionCapacity: map[string]int{"sleep": 2}}
	if _, err := s.Upsert(n2); err != nil {
		t.Fatalf("Upsert(functions vazio + capacity) = %v, esperado nil (wildcard ignora)", err)
	}
}

func mustTestStore(t *testing.T) *NodeStore {
	t.Helper()
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore(\"\") = %v", err)
	}
	return s
}
