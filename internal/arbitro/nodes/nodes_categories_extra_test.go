// Validation and persistence of the routing categories.
//
// These tests cover the size bound of the map, the exact names of the JSON
// fields, the tolerance of a downgrade when an older binary reads a newer
// file, and the exact round trip of the persisted file with the map
// present.
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

// The subset rule already bounds the size of the map by the number of
// hosted functions, so a larger map is rejected as a key outside the
// subset.
func TestNodeFunctionCategoriesLenBound(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	// The node hosts one function and the map has two keys, so the second
	// key lies outside the subset.
	n := Node{
		NodeID:             "n1",
		Layer:              LayerBorda,
		Host:               "h:1",
		Functions:          []string{"a"},
		FunctionCategories: map[string]string{"a": "g", "b": "g"},
	}
	if _, err := s.Upsert(n); !errors.Is(err, ErrInvalidFunctionCategories) {
		t.Fatalf("Upsert com mapa maior que Functions: err=%v, want ErrInvalidFunctionCategories", err)
	}
	// A map with the same size as the list of functions and only valid
	// keys passes.
	n2 := Node{
		NodeID:             "n2",
		Layer:              LayerBorda,
		Host:               "h:1",
		Functions:          []string{"a", "b"},
		FunctionCategories: map[string]string{"a": "g", "b": "g"},
	}
	if _, err := s.Upsert(n2); err != nil {
		t.Fatalf("Upsert mapa válido de tamanho igual: %v", err)
	}
}

// The name of the JSON field is the same in the node, in the registration
// request and in the listing, and it is omitted when empty. This test
// pins the name in the node; the server tests pin the other two through
// the endpoints.
func TestNodeFunctionCategoriesJSONTag(t *testing.T) {
	typ := reflect.TypeOf(Node{})
	f, ok := typ.FieldByName("FunctionCategories")
	if !ok {
		t.Fatal("Node sem campo FunctionCategories")
	}
	tag := f.Tag.Get("json")
	if tag != "function_categories,omitempty" {
		t.Fatalf("Node.FunctionCategories json tag = %q, want function_categories,omitempty", tag)
	}
	if f.Type != reflect.TypeOf(map[string]string(nil)) {
		t.Fatalf("Node.FunctionCategories type = %v, want map[string]string", f.Type)
	}
}

// Reading a downgrade: a newer file with the categories read by an older
// binary that does not know the field drops the map silently, without an
// error.
func TestNodeFunctionCategoriesDowngradeTolerance(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes-downgrade.json")

	// Writes a newer file with the categories.
	s1, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := s1.Upsert(Node{
		NodeID:             "n1",
		Layer:              LayerBorda,
		Host:               "h:1",
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo"},
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// A structure equivalent to an older binary, without the field.
	var oldFormat map[string]struct {
		NodeID    string   `json:"node_id"`
		Layer     string   `json:"layer"`
		Host      string   `json:"host"`
		Functions []string `json:"functions,omitempty"`
	}
	if err := json.Unmarshal(data, &oldFormat); err != nil {
		t.Fatalf("downgrade unmarshal: %v", err)
	}
	if len(oldFormat) != 1 || oldFormat["n1"].NodeID != "n1" {
		t.Fatalf("downgrade read: %+v", oldFormat)
	}
}

// The round trip of the persisted file is exact: with the map present the
// stored JSON contains exactly the expected keys, compact and without extra
// fields, and the reload returns the same map.
func TestNodeFunctionCategoriesSaveByteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes-bytes.json")

	s1, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := s1.Upsert(Node{
		NodeID:             "n1",
		Layer:              LayerBorda,
		Host:               "h:1",
		Functions:          []string{"alpr-yolo-tiny", "alpr-yolo-full"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo", "alpr-yolo-full": "alpr-yolo"},
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Compact, without spaces, and ending with a newline.
	if len(data) == 0 || data[len(data)-1] != '\n' {
		t.Fatalf("arquivo deve terminar em newline, got %q", data)
	}
	for _, want := range []string{
		`"function_categories"`,
		`"alpr-yolo-tiny":"alpr-yolo"`,
		`"alpr-yolo-full":"alpr-yolo"`,
	} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("arquivo persistido não contém %q: %s", want, data)
		}
	}

	s2, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore reload: %v", err)
	}
	got, ok := s2.Get("n1")
	if !ok {
		t.Fatalf("reload: nó não encontrado")
	}
	want := map[string]string{"alpr-yolo-tiny": "alpr-yolo", "alpr-yolo-full": "alpr-yolo"}
	if !reflect.DeepEqual(got.FunctionCategories, want) {
		t.Fatalf("reload categories = %v, want %v", got.FunctionCategories, want)
	}
}
