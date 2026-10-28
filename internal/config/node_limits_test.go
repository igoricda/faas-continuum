package config

// Per-node hard limits (`arbitro.node_limits`) override the global triple and
// are keyed by node_id.
//
// These tests cover:
//   - the JSON key names (node_limits, max_cpu_pct, max_mem_pct,
//     max_psi_avg10); the loader rejects unknown fields, so renaming a key
//     breaks startup instead of being ignored silently;
//   - the zero-means-inherit rule: zero or absent inherits the global value,
//     and the default map is absent (nil), so behavior without node_limits is
//     unchanged;
//   - the fail-fast checks in Validate (negative, NaN, Inf, empty node_id),
//     which prevent a node limit from being silently disabled.

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

// TestNodeLimitsDefaultAusente checks that without node_limits the map is nil
// and the global values remain the only source.
func TestNodeLimitsDefaultAusente(t *testing.T) {
	cfg := Defaults()
	if cfg.Arbitro.NodeLimits != nil {
		t.Fatalf("default NodeLimits = %v, want nil (sem override por padrão)", cfg.Arbitro.NodeLimits)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(defaults) = %v, want nil", err)
	}
}

// TestNodeLimitsFromFile decodes the map from a file, including a partial entry
// (only max_cpu_pct). The two absent fields become zero, which the merge treats
// as "inherit the global value".
func TestNodeLimitsFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{"arbitro": {"node_limits": {"borda-01": {"max_cpu_pct": 60}, "nevoa-01": {"max_cpu_pct": 95, "max_mem_pct": 70, "max_psi_avg10": 10}}}}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	borda, ok := cfg.Arbitro.NodeLimits["borda-01"]
	if !ok {
		t.Fatalf("node_limits sem borda-01: %+v", cfg.Arbitro.NodeLimits)
	}
	if borda.MaxCPUPct != 60 || borda.MaxMemPct != 0 || borda.MaxPSIAvg10 != 0 {
		t.Fatalf("borda-01 = %+v, want cpu 60 e mem/psi 0 (herdados)", borda)
	}
	nevoa := cfg.Arbitro.NodeLimits["nevoa-01"]
	if nevoa.MaxCPUPct != 95 || nevoa.MaxMemPct != 70 || nevoa.MaxPSIAvg10 != 10 {
		t.Fatalf("nevoa-01 = %+v, want 95/70/10", nevoa)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(node_limits válido) = %v, want nil", err)
	}
}

// TestNodeLimitsValidateFailFast checks that negative values, NaN, Inf and an
// empty node_id abort. NaN and Inf have their own cases: they pass a <= 0 check
// and would make the node limit always false in Overloaded, so the node would
// never be considered overloaded.
func TestNodeLimitsValidateFailFast(t *testing.T) {
	cases := map[string]func(*Config){
		"cpu negativo": func(c *Config) {
			c.Arbitro.NodeLimits = map[string]NodeHardLimits{"n1": {MaxCPUPct: -1}}
		},
		"mem negativa": func(c *Config) {
			c.Arbitro.NodeLimits = map[string]NodeHardLimits{"n1": {MaxMemPct: -0.5}}
		},
		"psi NaN": func(c *Config) {
			c.Arbitro.NodeLimits = map[string]NodeHardLimits{"n1": {MaxPSIAvg10: math.NaN()}}
		},
		"cpu +Inf": func(c *Config) {
			c.Arbitro.NodeLimits = map[string]NodeHardLimits{"n1": {MaxCPUPct: math.Inf(1)}}
		},
		"node_id vazio": func(c *Config) {
			c.Arbitro.NodeLimits = map[string]NodeHardLimits{"  ": {MaxCPUPct: 60}}
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := Defaults()
			mut(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate(%s) = nil, want erro fail-fast", name)
			}
		})
	}

	// Zero is valid on every axis: it means "inherit the global value".
	cfg := Defaults()
	cfg.Arbitro.NodeLimits = map[string]NodeHardLimits{"n1": {}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(entrada toda zero) = %v, want nil (herda o global)", err)
	}
}
