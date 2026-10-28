package registry

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestValidateExpectedMetrics(t *testing.T) {
	cases := []struct {
		name    string
		metrics map[string]ExpectedMetric
		wantErr error
	}{
		{"nil map advertises nothing", nil, nil},
		{"empty map advertises nothing", map[string]ExpectedMetric{}, nil},
		{
			"valid higher-is-better",
			map[string]ExpectedMetric{"accuracy": {Value: 0.97, HigherIsBetter: true}},
			nil,
		},
		{
			"missing polarity key means lower-is-better",
			map[string]ExpectedMetric{"latency_ms": {Value: 120}},
			nil,
		},
		{
			"case-sensitive names are distinct bytes",
			map[string]ExpectedMetric{"Accuracy": {Value: 0.9}, "accuracy": {Value: 0.8}},
			nil,
		},
		{
			"whitespace-only name is kept verbatim",
			map[string]ExpectedMetric{" accuracy ": {Value: 0.9}},
			nil,
		},
		{
			"empty name rejected",
			map[string]ExpectedMetric{"": {Value: 0.9}},
			ErrInvalidExpectedMetrics,
		},
		{
			"NaN value rejected",
			map[string]ExpectedMetric{"accuracy": {Value: math.NaN()}},
			ErrInvalidExpectedMetrics,
		},
		{
			"+Inf value rejected",
			map[string]ExpectedMetric{"accuracy": {Value: math.Inf(1)}},
			ErrInvalidExpectedMetrics,
		},
		{
			"-Inf value rejected",
			map[string]ExpectedMetric{"latency_ms": {Value: math.Inf(-1)}},
			ErrInvalidExpectedMetrics,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateExpectedMetrics(tc.metrics)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("validateExpectedMetrics() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("validateExpectedMetrics() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestRegisterRejectsInvalidExpectedMetrics(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	if _, _, err := r.Register("host", "fn-bad", "img:1.0", RegisterOptions{
		ExpectedMetrics: map[string]ExpectedMetric{"": {Value: 0.9}},
	}); !errors.Is(err, ErrInvalidExpectedMetrics) {
		t.Fatalf("Register() with empty metric name = %v, want ErrInvalidExpectedMetrics", err)
	}

	if _, _, err := r.Register("host", "fn-nan", "img:1.0", RegisterOptions{
		ExpectedMetrics: map[string]ExpectedMetric{"accuracy": {Value: math.NaN()}},
	}); !errors.Is(err, ErrInvalidExpectedMetrics) {
		t.Fatalf("Register() with NaN value = %v, want ErrInvalidExpectedMetrics", err)
	}
}

func TestRegisterPersistsExpectedMetrics(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	want := map[string]ExpectedMetric{
		"accuracy":   {Value: 0.97, HigherIsBetter: true},
		"latency_ms": {Value: 120},
	}
	fn, _, err := r.Register("host", "fn-q", "img:1.0", RegisterOptions{ExpectedMetrics: want})
	if err != nil {
		t.Fatalf("Register() erro inesperado: %v", err)
	}
	if !reflect.DeepEqual(fn.ExpectedMetrics, want) {
		t.Fatalf("Register() ExpectedMetrics = %v, want %v", fn.ExpectedMetrics, want)
	}
	got, ok := r.Get("fn-q")
	if !ok {
		t.Fatal("Get() não encontrou função registrada")
	}
	if !reflect.DeepEqual(got.ExpectedMetrics, want) {
		t.Fatalf("Get() ExpectedMetrics = %v, want %v", got.ExpectedMetrics, want)
	}
}

func TestUpdateReplacesExpectedMetricsWholesale(t *testing.T) {
	r, _ := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	if _, _, err := r.Register("host", "fn-u", "img:1.0", RegisterOptions{
		ExpectedMetrics: map[string]ExpectedMetric{"accuracy": {Value: 0.9, HigherIsBetter: true}},
	}); err != nil {
		t.Fatalf("Register() erro inesperado: %v", err)
	}

	replaced := map[string]ExpectedMetric{"latency_ms": {Value: 50}}
	updated, _, err := r.Update("host", "fn-u", "img:1.0", RegisterOptions{ExpectedMetrics: replaced}, false)
	if err != nil {
		t.Fatalf("Update() erro inesperado: %v", err)
	}
	if !reflect.DeepEqual(updated.ExpectedMetrics, replaced) {
		t.Fatalf("Update() ExpectedMetrics = %v, want %v", updated.ExpectedMetrics, replaced)
	}

	// An omitted field clears the map wholesale.
	cleared, _, err := r.Update("host", "fn-u", "img:1.0", RegisterOptions{}, false)
	if err != nil {
		t.Fatalf("Update() erro inesperado: %v", err)
	}
	if len(cleared.ExpectedMetrics) != 0 {
		t.Fatalf("Update() omitted ExpectedMetrics = %v, want cleared", cleared.ExpectedMetrics)
	}
}

func TestExpectedMetricsPersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "functions.json")
	r, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatalf("NewPersistentRegistry() erro inesperado: %v", err)
	}

	want := map[string]ExpectedMetric{"accuracy": {Value: 0.97, HigherIsBetter: true}}
	if _, _, err := r.Register("host", "fn-p", "img:1.0", RegisterOptions{ExpectedMetrics: want}); err != nil {
		t.Fatalf("Register() erro inesperado: %v", err)
	}

	r2, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatalf("reload NewPersistentRegistry() erro inesperado: %v", err)
	}
	got, ok := r2.Get("fn-p")
	if !ok {
		t.Fatal("reload Get() não encontrou função persistida")
	}
	if !reflect.DeepEqual(got.ExpectedMetrics, want) {
		t.Fatalf("reload ExpectedMetrics = %v, want %v", got.ExpectedMetrics, want)
	}
}

func TestOldFileWithoutExpectedMetricsLoadsNil(t *testing.T) {
	path := filepath.Join(t.TempDir(), "functions.json")
	legacy := `{"fn-old":{"function_name":"fn-old","resource_url":"host/function/fn-old","image_ref":"img:1.0","min_instances":0,"max_instances":0,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatalf("WriteFile() erro inesperado: %v", err)
	}

	r, err := NewPersistentRegistry(path)
	if err != nil {
		t.Fatalf("NewPersistentRegistry() erro inesperado: %v", err)
	}
	got, ok := r.Get("fn-old")
	if !ok {
		t.Fatal("Get() não encontrou função legada")
	}
	if got.ExpectedMetrics != nil {
		t.Fatalf("legacy ExpectedMetrics = %v, want nil", got.ExpectedMetrics)
	}
}

func TestExpectedMetricsJSONOmitempty(t *testing.T) {
	data, err := json.Marshal(Function{Name: "fn"})
	if err != nil {
		t.Fatalf("Marshal() erro inesperado: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal() erro inesperado: %v", err)
	}
	if _, present := raw["expected_metrics"]; present {
		t.Fatalf("nil ExpectedMetrics deve ser omitido do JSON, obtido %s", data)
	}

	var fn Function
	if err := json.Unmarshal([]byte(`{"function_name":"fn"}`), &fn); err != nil {
		t.Fatalf("Unmarshal() erro inesperado: %v", err)
	}
	if fn.ExpectedMetrics != nil {
		t.Fatalf("campo ausente deve carregar nil, obtido %v", fn.ExpectedMetrics)
	}
}
