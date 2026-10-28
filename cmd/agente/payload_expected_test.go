package main

import (
	"encoding/json"
	"testing"

	"github.com/igoricda/faas-continuum/internal/registry"
)

func TestBuildRegistrationPayloadExpected(t *testing.T) {
	t.Run("expected presente vai no corpo", func(t *testing.T) {
		getExpected := func() map[string]map[string]registry.ExpectedMetric {
			return map[string]map[string]registry.ExpectedMetric{
				"alpr-yolo-tiny": {"accuracy": {Value: 0.9, HigherIsBetter: true}},
			}
		}
		data, err := buildRegistrationPayload("n1", "borda", "h:8080", true, 0, []string{"alpr-yolo-tiny"}, nil, getExpected, nil, nil)
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		raw, ok := m["function_expected_metrics"]
		if !ok {
			t.Fatalf("function_expected_metrics ausente no corpo: %s", string(data))
		}
		outer, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("function_expected_metrics não é objeto: %v", raw)
		}
		inner, ok := outer["alpr-yolo-tiny"].(map[string]any)
		if !ok {
			t.Fatalf("variante ausente: %v", outer)
		}
		metric, ok := inner["accuracy"].(map[string]any)
		if !ok {
			t.Fatalf("métrica ausente: %v", inner)
		}
		if metric["value"] != 0.9 || metric["higher_is_better"] != true {
			t.Fatalf("métrica = %v, want {value:0.9 higher_is_better:true}", metric)
		}
	})

	t.Run("getter nil omite chave", func(t *testing.T) {
		data, err := buildRegistrationPayload("n2", "borda", "h:8080", true, 0, []string{"hello"}, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["function_expected_metrics"]; ok {
			t.Fatalf("function_expected_metrics deveria ser omitida quando getter é nil, got %v", m["function_expected_metrics"])
		}
	})

	t.Run("getter que devolve nil omite chave", func(t *testing.T) {
		data, err := buildRegistrationPayload("n3", "borda", "h:8080", true, 0, []string{"hello"}, nil, func() map[string]map[string]registry.ExpectedMetric {
			return nil
		}, nil, nil)
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["function_expected_metrics"]; ok {
			t.Fatalf("function_expected_metrics deveria ser omitida quando getter devolve nil, got %v", m["function_expected_metrics"])
		}
	})
}
