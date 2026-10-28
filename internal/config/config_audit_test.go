package config

// Tests for the gateway hard limits: defaults, fail-fast validation, and the
// flags > env > file > defaults precedence. A limit set in the file or in an
// environment variable must not be ignored, and a negative flag must not cause
// a total outage.

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestArbitroLimitsDefaults(t *testing.T) {
	cfg := Defaults()
	if cfg.Arbitro.MaxCPUPct != 85 || cfg.Arbitro.MaxMemPct != 90 || cfg.Arbitro.MaxPSIAvg10 != 25 {
		t.Fatalf("defaults pct = %.1f/%.1f/%.1f, esperado 85/90/25", cfg.Arbitro.MaxCPUPct, cfg.Arbitro.MaxMemPct, cfg.Arbitro.MaxPSIAvg10)
	}
	if time.Duration(cfg.Arbitro.InvokeTimeout) != 90*time.Second {
		t.Fatalf("InvokeTimeout = %v, esperado 90s", cfg.Arbitro.InvokeTimeout)
	}
	if cfg.Arbitro.CloudFallbackPath != "./faas-continuum-cloud-fallback.json" {
		t.Fatalf("CloudFallbackPath = %q, esperado ./faas-continuum-cloud-fallback.json", cfg.Arbitro.CloudFallbackPath)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(defaults) = %v, esperado nil", err)
	}
}

func TestArbitroLimitsValidatePositive(t *testing.T) {
	bad := func(mut func(*Config)) Config {
		cfg := Defaults()
		mut(&cfg)
		return cfg
	}
	cases := map[string]Config{
		"max_cpu_pct":    bad(func(c *Config) { c.Arbitro.MaxCPUPct = -1 }),
		"max_mem_pct":    bad(func(c *Config) { c.Arbitro.MaxMemPct = 0 }),
		"max_psi_avg10":  bad(func(c *Config) { c.Arbitro.MaxPSIAvg10 = 0 }),
		"invoke_timeout": bad(func(c *Config) { c.Arbitro.InvokeTimeout = Duration(-time.Second) }),
		"cloud_fallback": bad(func(c *Config) { c.Arbitro.CloudFallbackPath = "" }),
	}
	for name, cfg := range cases {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("Validate(%s inválido) = nil, esperado erro fail-fast", name)
		}
	}
}

func TestArbitroLimitsFileEnvFlagPrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{"arbitro": {"max_cpu_pct": 70, "invoke_timeout": "45s", "cloud_fallback_path": "/tmp/file-cloud.json"}}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	// file > defaults.
	cfg, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load file: %v", err)
	}
	if cfg.Arbitro.MaxCPUPct != 70 {
		t.Fatalf("file não aplicado: cpu=%.1f", cfg.Arbitro.MaxCPUPct)
	}
	if time.Duration(cfg.Arbitro.InvokeTimeout) != 45*time.Second {
		t.Fatalf("invoke_timeout file = %v, esperado 45s", cfg.Arbitro.InvokeTimeout)
	}
	// env > file; both prefixes are accepted.
	t.Setenv("FAAS_ARBITRO_MAX_CPU_PCT", "60")
	cfg, err = Load(path, nil)
	if err != nil {
		t.Fatalf("Load env: %v", err)
	}
	if cfg.Arbitro.MaxCPUPct != 60 {
		t.Fatalf("env FAAS_ARBITRO_MAX_CPU_PCT não venceu file: %.1f", cfg.Arbitro.MaxCPUPct)
	}
	t.Setenv("ARBITRO_MAX_MEM_PCT", "80")
	cfg, err = Load(path, nil)
	if err != nil {
		t.Fatalf("Load env alias: %v", err)
	}
	if cfg.Arbitro.MaxMemPct != 80 {
		t.Fatalf("env ARBITRO_MAX_MEM_PCT não venceu file: %.1f", cfg.Arbitro.MaxMemPct)
	}
	// flags > env.
	cfg, err = Load(path, map[string]string{"max-cpu-pct": "50"})
	if err != nil {
		t.Fatalf("Load flag: %v", err)
	}
	if cfg.Arbitro.MaxCPUPct != 50 {
		t.Fatalf("flag max-cpu-pct não venceu env: %.1f", cfg.Arbitro.MaxCPUPct)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(precedência) = %v, esperado nil", err)
	}
}
