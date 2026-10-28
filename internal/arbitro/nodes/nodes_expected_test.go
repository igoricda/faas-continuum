package nodes

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/igoricda/faas-continuum/internal/registry"
)

// A valid map is persisted and reloaded.
func TestNodeFunctionExpectedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes-exp.json")

	s1, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s1: %v", err)
	}
	want := Node{
		NodeID:    "n1",
		Layer:     LayerBorda,
		Host:      "10.0.0.1:8080",
		Functions: []string{"alpr-yolo-tiny"},
		FunctionExpected: map[string]map[string]registry.ExpectedMetric{
			"alpr-yolo-tiny": {"accuracy": {Value: 0.9, HigherIsBetter: true}},
		},
	}
	if _, err := s1.Upsert(want); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), `"function_expected_metrics"`) {
		t.Fatalf("arquivo persistido não contém function_expected_metrics: %s", data)
	}

	s2, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s2: %v", err)
	}
	got, ok := s2.Get("n1")
	if !ok {
		t.Fatalf("Get após reabrir: não encontrado")
	}
	if !reflect.DeepEqual(got.FunctionExpected, want.FunctionExpected) {
		t.Fatalf("FunctionExpected mismatch: got %v want %v", got.FunctionExpected, want.FunctionExpected)
	}
}

// An outer key outside the hosted functions, an empty outer key, an empty
// metric name and non-finite values are rejected.
func TestNodeFunctionExpectedValidate(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}

	cases := []struct {
		name string
		node Node
	}{
		{"chave externa fora de functions", Node{NodeID: "n1", Layer: LayerBorda, Host: "h:1", Functions: []string{"a"}, FunctionExpected: map[string]map[string]registry.ExpectedMetric{"ghost": {"accuracy": {Value: 0.9}}}}},
		{"chave externa vazia", Node{NodeID: "n1", Layer: LayerBorda, Host: "h:1", Functions: []string{"a"}, FunctionExpected: map[string]map[string]registry.ExpectedMetric{"": {"accuracy": {Value: 0.9}}}}},
		{"nome de métrica vazio", Node{NodeID: "n1", Layer: LayerBorda, Host: "h:1", Functions: []string{"a"}, FunctionExpected: map[string]map[string]registry.ExpectedMetric{"a": {"": {Value: 0.9}}}}},
		{"valor NaN", Node{NodeID: "n1", Layer: LayerBorda, Host: "h:1", Functions: []string{"a"}, FunctionExpected: map[string]map[string]registry.ExpectedMetric{"a": {"accuracy": {Value: math.NaN()}}}}},
		{"valor +Inf", Node{NodeID: "n1", Layer: LayerBorda, Host: "h:1", Functions: []string{"a"}, FunctionExpected: map[string]map[string]registry.ExpectedMetric{"a": {"accuracy": {Value: math.Inf(1)}}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Upsert(tc.node)
			if !errors.Is(err, ErrInvalidFunctionExpected) {
				t.Fatalf("Upsert err=%v, want ErrInvalidFunctionExpected", err)
			}
			if !strings.Contains(err.Error(), "function_expected_metrics") {
				t.Fatalf("erro %q não contém function_expected_metrics", err)
			}
		})
	}
}

// A nil map and an empty inner map keep the previous behavior: they are
// accepted and valid.
func TestNodeFunctionExpectedNilMap(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	n := mustNode("n1", LayerBorda, "10.0.0.1:8080")
	n.Functions = []string{"a"}
	n.FunctionExpected = nil
	if _, err := s.Upsert(n); err != nil {
		t.Fatalf("Upsert nil map: %v", err)
	}
	n.FunctionExpected = map[string]map[string]registry.ExpectedMetric{"a": nil}
	if _, err := s.Upsert(n); err != nil {
		t.Fatalf("Upsert empty inner map: %v", err)
	}
	got, ok := s.Get("n1")
	if !ok {
		t.Fatalf("Get: não encontrado")
	}
	if !reflect.DeepEqual(got.FunctionExpected, map[string]map[string]registry.ExpectedMetric{"a": nil}) {
		t.Fatalf("FunctionExpected=%v, want map with nil inner", got.FunctionExpected)
	}
}

// A file written before the field existed loads with a nil map.
func TestNodeFunctionExpectedOldFileCompat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes-old.json")
	old := `{"n1":{"last_seen":"2026-01-01T00:00:00Z","node_id":"n1","layer":"borda","host":"h:1","functions":["a"],"min_instances":0,"psi_supported":false}}` + "\n"
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	s, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	got, ok := s.Get("n1")
	if !ok {
		t.Fatalf("Get após reabrir: não encontrado")
	}
	if got.FunctionExpected != nil {
		t.Fatalf("FunctionExpected=%v, want nil", got.FunctionExpected)
	}
}
