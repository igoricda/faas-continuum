package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/registry"
)

// Gate test: it protects the computation of the effective ceiling, which is the
// own timeout against the default of the node against the minimum of the ceiling
// and the cap of the late report. The credible regressions are dropping the
// minimum with the cap of the late report or the fallback to the default. The
// function is new and pure, so it is tested directly without a seam.
func TestEffectiveFunctionTimeouts(t *testing.T) {
	def := 90 * time.Second
	lateCap := 45 * time.Second
	cases := []struct {
		name  string
		funcs []registry.Function
		want  map[string]int
	}{
		{
			name:  "vazio devolve nil (chave omitida)",
			funcs: nil,
			want:  nil,
		},
		{
			name: "timeout próprio vence o default",
			funcs: []registry.Function{
				{Name: "a", TimeoutMS: 5000},
			},
			want: map[string]int{"a": 5000},
		},
		{
			name: "zero usa o default do nó",
			funcs: []registry.Function{
				{Name: "b"},
			},
			want: map[string]int{"b": 90000},
		},
		{
			name: "lateReportable com teto acima do lateCap é cortado",
			funcs: []registry.Function{
				{Name: "c", LateReportable: true},
			},
			want: map[string]int{"c": 45000},
		},
		{
			name: "lateReportable com timeout próprio acima do lateCap é cortado",
			funcs: []registry.Function{
				{Name: "d", TimeoutMS: 60000, LateReportable: true},
			},
			want: map[string]int{"d": 45000},
		},
		{
			name: "lateReportable com teto abaixo do lateCap passa intacto",
			funcs: []registry.Function{
				{Name: "e", TimeoutMS: 10000, LateReportable: true},
			},
			want: map[string]int{"e": 10000},
		},
		{
			name: "não-lateReportable ignora o lateCap",
			funcs: []registry.Function{
				{Name: "f", TimeoutMS: 60000},
			},
			want: map[string]int{"f": 60000},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := effectiveFunctionTimeouts(tc.funcs, def, lateCap)
			if len(got) != len(tc.want) {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("got[%q]=%d want %d (map=%v)", k, got[k], v, got)
				}
			}
		})
	}
}

func TestEffectiveFunctionTimeoutsSemTetoExtra(t *testing.T) {
	def := 90 * time.Second
	funcs := []registry.Function{{Name: "c", LateReportable: true}}
	got := effectiveFunctionTimeouts(funcs, def, 0)
	if got["c"] != 90000 {
		t.Fatalf("lateCap<=0: got=%v want map[c:90000]", got)
	}
}

// Gate test: it protects the wire, where the timeout of the function is announced
// in the post of the node, with the presence of the effective values and the
// omission when the getter is absent. The credible regression is forgetting to
// wire the getter into the payload. The legacy tests do not cover the new key,
// so there is no duplication.
func TestBuildRegistrationPayloadTimeoutMS(t *testing.T) {
	getTimeouts := func() map[string]int { return map[string]int{"hello": 15000} }
	data, err := buildRegistrationPayload("n1", "borda", "h:8080", true, 0, []string{"hello"}, nil, nil, nil, getTimeouts)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	raw, ok := m["function_timeout_ms"]
	if !ok {
		t.Fatalf("function_timeout_ms ausente no corpo: %s", string(data))
	}
	got, ok := raw.(map[string]any)
	if !ok || got["hello"] != float64(15000) {
		t.Fatalf("function_timeout_ms = %v, want map[hello:15000]", raw)
	}

	data, err = buildRegistrationPayload("n2", "borda", "h:8080", true, 0, []string{"hello"}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	m = nil
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["function_timeout_ms"]; ok {
		t.Fatalf("function_timeout_ms deveria ser omitida quando getter é nil, got %v", m["function_timeout_ms"])
	}
}
