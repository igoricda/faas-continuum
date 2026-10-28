package nodes

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// A valid map is persisted and reloaded byte for byte.
func TestNodeFunctionCategoriesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes-cat.json")

	s1, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s1: %v", err)
	}
	want := Node{
		NodeID:             "n1",
		Layer:              LayerBorda,
		Host:               "10.0.0.1:8080",
		Functions:          []string{"alpr-yolo-tiny", "alpr-yolo-full"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo", "alpr-yolo-full": "alpr-yolo"},
	}
	if _, err := s1.Upsert(want); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), `"function_categories"`) {
		t.Fatalf("arquivo persistido não contém function_categories: %s", data)
	}

	s2, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s2: %v", err)
	}
	got, ok := s2.Get("n1")
	if !ok {
		t.Fatalf("Get após reabrir: não encontrado")
	}
	if !reflect.DeepEqual(got.FunctionCategories, want.FunctionCategories) {
		t.Fatalf("FunctionCategories mismatch: got %v want %v", got.FunctionCategories, want.FunctionCategories)
	}
	if !reflect.DeepEqual(got.Functions, want.Functions) {
		t.Fatalf("Functions mismatch: got %v want %v", got.Functions, want.Functions)
	}
}

// A key outside the hosted functions, an empty value and an empty key are
// rejected.
func TestNodeFunctionCategoriesValidate(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}

	cases := []struct {
		name string
		node Node
	}{
		{"chave fora de functions", Node{NodeID: "n1", Layer: LayerBorda, Host: "h:1", Functions: []string{"a"}, FunctionCategories: map[string]string{"ghost": "alpr-yolo"}}},
		{"valor vazio", Node{NodeID: "n1", Layer: LayerBorda, Host: "h:1", Functions: []string{"a"}, FunctionCategories: map[string]string{"a": ""}}},
		{"chave vazia", Node{NodeID: "n1", Layer: LayerBorda, Host: "h:1", Functions: []string{"a"}, FunctionCategories: map[string]string{"": "alpr-yolo"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Upsert(tc.node)
			if !errors.Is(err, ErrInvalidFunctionCategories) {
				t.Fatalf("Upsert err=%v, want ErrInvalidFunctionCategories", err)
			}
			if !strings.Contains(err.Error(), "function_categories") {
				t.Fatalf("erro %q não contém function_categories", err)
			}
		})
	}
}

// A nil map keeps the previous behavior: it is accepted, omitted from
// the JSON and loaded back as nil.
func TestNodeFunctionCategoriesUpsertNilMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes-nilmap.json")

	s1, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s1: %v", err)
	}
	n := mustNode("n1", LayerBorda, "10.0.0.1:8080")
	n.FunctionCategories = nil
	if _, err := s1.Upsert(n); err != nil {
		t.Fatalf("Upsert nil map: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if strings.Contains(string(data), "function_categories") {
		t.Fatalf("nil map deveria ser omitido (omitempty): %s", data)
	}

	// An older file without the field loads with a nil map.
	s2, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s2: %v", err)
	}
	got, ok := s2.Get("n1")
	if !ok {
		t.Fatalf("Get após reabrir: não encontrado")
	}
	if got.FunctionCategories != nil {
		t.Fatalf("FunctionCategories=%v, want nil", got.FunctionCategories)
	}
	var raw map[string]map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, ok := raw["n1"]["function_categories"]; ok {
		t.Fatalf("chave function_categories presente para nil map")
	}
}
