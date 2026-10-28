package nodes

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestNodeCapacityValidRoundTrip(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	n := mustNode("cap-1", LayerBorda, "10.0.0.1:8080")
	n.Functions = []string{"sleep", "other"}
	n.FunctionCapacity = map[string]int{"sleep": 2, "other": 0}
	if _, err := s.Upsert(n); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, ok := s.Get("cap-1")
	if !ok {
		t.Fatalf("Get após Upsert: não encontrado")
	}
	if !reflect.DeepEqual(got.FunctionCapacity, map[string]int{"sleep": 2, "other": 0}) {
		t.Fatalf("FunctionCapacity = %v, want map[sleep:2 other:0]", got.FunctionCapacity)
	}
	// The canonical JSON key of the ceiling.
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	mi, ok := m["max_instances"].(map[string]any)
	if !ok {
		t.Fatalf("max_instances ausente ou não-objeto no JSON: %s", string(raw))
	}
	if mi["sleep"] != float64(2) || mi["other"] != float64(0) {
		t.Fatalf("max_instances = %v, want {sleep:2 other:0}", mi)
	}
}

func TestNodeCapacityPersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes-cap.json")
	s1, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s1: %v", err)
	}
	n := mustNode("cap-persist", LayerNevoa, "10.0.0.2:8080")
	n.Functions = []string{"sleep"}
	n.FunctionCapacity = map[string]int{"sleep": 3}
	if _, err := s1.Upsert(n); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	s2, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s2 (reabrir): %v", err)
	}
	got, ok := s2.Get("cap-persist")
	if !ok {
		t.Fatalf("Get após reabrir: não encontrado")
	}
	if !reflect.DeepEqual(got.FunctionCapacity, map[string]int{"sleep": 3}) {
		t.Fatalf("FunctionCapacity após reload = %v, want map[sleep:3]", got.FunctionCapacity)
	}
}

func TestNodeCapacityNegativeRejected(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	base := mustNode("cap-neg", LayerBorda, "10.0.0.1:8080")
	base.Functions = []string{"sleep"}
	base.FunctionCapacity = map[string]int{"sleep": 2}
	if _, err := s.Upsert(base); err != nil {
		t.Fatalf("Upsert base: %v", err)
	}
	bad := mustNode("cap-neg", LayerBorda, "10.0.0.1:8080")
	bad.Functions = []string{"sleep"}
	bad.FunctionCapacity = map[string]int{"sleep": -1}
	if _, err := s.Upsert(bad); !errors.Is(err, ErrInvalidFunctionCapacity) {
		t.Fatalf("Upsert negativo err=%v, want ErrInvalidFunctionCapacity", err)
	}
	// The store is unchanged.
	got, ok := s.Get("cap-neg")
	if !ok {
		t.Fatalf("Get após Upsert rejeitado: não encontrado")
	}
	if !reflect.DeepEqual(got.FunctionCapacity, map[string]int{"sleep": 2}) {
		t.Fatalf("store alterado após rejeição: %v", got.FunctionCapacity)
	}
}

func TestNodeCapacityZeroAndAbsentNoCeiling(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	zero := mustNode("cap-zero", LayerBorda, "10.0.0.1:8080")
	zero.Functions = []string{"sleep"}
	zero.FunctionCapacity = map[string]int{"sleep": 0}
	if _, err := s.Upsert(zero); err != nil {
		t.Fatalf("Upsert com 0: %v", err)
	}
	absent := mustNode("cap-absent", LayerBorda, "10.0.0.2:8080")
	absent.Functions = []string{"sleep"}
	absent.FunctionCapacity = nil
	if _, err := s.Upsert(absent); err != nil {
		t.Fatalf("Upsert ausente: %v", err)
	}
	if got, _ := s.Get("cap-absent"); got.FunctionCapacity != nil {
		t.Fatalf("FunctionCapacity ausente virou %v, want nil", got.FunctionCapacity)
	}
	// An absent field is omitted from the serialization.
	raw, _ := json.Marshal(absent)
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, ok := m["max_instances"]; ok {
		t.Fatalf("max_instances deveria ser omitida quando nil, got %s", string(raw))
	}
}

func TestNodeCapacityEmptyFunctionsNoGating(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	for _, tc := range []struct {
		name string
		node Node
	}{
		{"nil", Node{NodeID: "cap-e1", Layer: LayerBorda, Host: "10.0.0.1:8080", FunctionCapacity: map[string]int{"sleep": 2}}},
		{"vazio", Node{NodeID: "cap-e2", Layer: LayerBorda, Host: "10.0.0.2:8080", Functions: []string{}, FunctionCapacity: map[string]int{"sleep": 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.Upsert(tc.node); err != nil {
				t.Fatalf("Upsert com Functions %s err=%v, want nil (elegível-para-tudo)", tc.name, err)
			}
			got, ok := s.Get(tc.node.NodeID)
			if !ok {
				t.Fatalf("Get: não encontrado")
			}
			if !reflect.DeepEqual(got.FunctionCapacity, map[string]int{"sleep": 2}) {
				t.Fatalf("FunctionCapacity = %v, want map[sleep:2]", got.FunctionCapacity)
			}
		})
	}
}

func TestNodeCapacityUpsertConcurrent(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n := mustNode("cap-race", LayerBorda, "10.0.0.1:8080")
			n.Functions = []string{"sleep"}
			n.FunctionCapacity = map[string]int{"sleep": i}
			_, errs[i] = s.Upsert(n)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d err=%v", i, err)
		}
	}
	got, ok := s.Get("cap-race")
	if !ok {
		t.Fatalf("Get após Upserts concorrentes: não encontrado")
	}
	if len(got.FunctionCapacity) != 1 {
		t.Fatalf("FunctionCapacity = %v, want 1 entrada", got.FunctionCapacity)
	}
}
