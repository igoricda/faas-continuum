package main

import (
	"encoding/json"
	"testing"
)

func TestBuildRegistrationPayload(t *testing.T) {
	t.Run("completo com functions", func(t *testing.T) {
		data, err := buildRegistrationPayload("node-1", "borda", "10.0.0.1:8080", true, 2, []string{"hello", "world"}, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("buildRegistrationPayload failed: %v", err)
		}
		var got map[string]any
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if got["node_id"] != "node-1" {
			t.Fatalf("node_id = %v, want node-1", got["node_id"])
		}
		if got["layer"] != "borda" {
			t.Fatalf("layer = %v, want borda", got["layer"])
		}
		if got["host"] != "10.0.0.1:8080" {
			t.Fatalf("host = %v, want 10.0.0.1:8080", got["host"])
		}
		if got["psi_supported"] != true {
			t.Fatalf("psi_supported = %v, want true", got["psi_supported"])
		}
		if got["min_instances"] != float64(2) {
			t.Fatalf("min_instances = %v, want 2", got["min_instances"])
		}
		fns, ok := got["functions"].([]any)
		if !ok || len(fns) != 2 {
			t.Fatalf("functions = %v, want [hello world]", got["functions"])
		}
		if fns[0] != "hello" || fns[1] != "world" {
			t.Fatalf("functions = %v, want [hello world]", fns)
		}
		golden := `{"node_id":"node-1","layer":"borda","host":"10.0.0.1:8080","min_instances":2,"psi_supported":true,"functions":["hello","world"]}`
		var want map[string]any
		if err := json.Unmarshal([]byte(golden), &want); err != nil {
			t.Fatal(err)
		}
		var got2 map[string]any
		_ = json.Unmarshal(data, &got2)
		goldenBytes, _ := json.Marshal(want)
		gotBytes, _ := json.Marshal(got2)
		if string(goldenBytes) != string(gotBytes) {
			t.Fatalf("golden mismatch\n got: %s\nwant: %s", string(gotBytes), string(goldenBytes))
		}
		if string(data) != golden {
			t.Fatalf("raw golden mismatch\n got: %s\nwant: %s", string(data), golden)
		}
	})

	t.Run("sem functions omite campo", func(t *testing.T) {
		data, err := buildRegistrationPayload("n2", "nevoa", "host:9090", false, 0, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["functions"]; ok {
			t.Fatalf("functions should be omitted when nil, got %v", m["functions"])
		}
		if m["node_id"] != "n2" || m["layer"] != "nevoa" {
			t.Fatalf("unexpected fields: %v", m)
		}
		if m["psi_supported"] != false {
			t.Fatalf("psi_supported = %v, want false", m["psi_supported"])
		}
		if m["min_instances"] != float64(0) {
			t.Fatalf("min_instances = %v, want 0", m["min_instances"])
		}
	})

	t.Run("functions vazia omite campo", func(t *testing.T) {
		data, err := buildRegistrationPayload("n3", "borda", "h:8080", true, 0, []string{}, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["functions"]; ok {
			t.Fatalf("functions should be omitted when empty, got %v", m["functions"])
		}
	})

	t.Run("categories presentes vao no corpo", func(t *testing.T) {
		cats := map[string]string{"alpr-yolo-tiny": "alpr-yolo", "alpr-yolo-full": "alpr-yolo"}
		data, err := buildRegistrationPayload("n4", "borda", "h:8080", true, 0, []string{"alpr-yolo-tiny", "alpr-yolo-full"}, cats, nil, nil, nil)
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		raw, ok := m["function_categories"]
		if !ok {
			t.Fatalf("function_categories ausente no corpo: %s", string(data))
		}
		got, ok := raw.(map[string]any)
		if !ok || got["alpr-yolo-tiny"] != "alpr-yolo" || got["alpr-yolo-full"] != "alpr-yolo" {
			t.Fatalf("function_categories = %v, want %v", raw, cats)
		}
	})

	t.Run("categories nil omite chave", func(t *testing.T) {
		data, err := buildRegistrationPayload("n5", "borda", "h:8080", true, 0, []string{"hello"}, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["function_categories"]; ok {
			t.Fatalf("function_categories deveria ser omitida quando nil, got %v", m["function_categories"])
		}
	})
}
