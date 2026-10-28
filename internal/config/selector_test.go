package config

// Selector in production (`arbitro.selector`): "risk" (default) or "hl" (plain
// queue selector).
//
// These tests cover:
//   - the default "risk" passes Validate;
//   - "hl" from the file passes Validate;
//   - any other value ("", "v1", "self-learning", ...) aborts startup in
//     Validate;
//   - the JSON key is named "selector"; the loader rejects unknown fields.

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSelectorDefaultRisk checks that the default selector is "risk" and passes
// Validate.
func TestSelectorDefaultRisk(t *testing.T) {
	cfg := Defaults()
	if cfg.Arbitro.Selector != "risk" {
		t.Fatalf("default Selector = %q, want %q", cfg.Arbitro.Selector, "risk")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(defaults) = %v, want nil", err)
	}
}

// TestSelectorHLFromFile checks that "hl" from the file passes Validate.
func TestSelectorHLFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"arbitro": {"selector": "hl"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Arbitro.Selector != "hl" {
		t.Fatalf("Selector = %q, want %q", cfg.Arbitro.Selector, "hl")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(hl) = %v, want nil", err)
	}
}

// TestSelectorInvalidos checks that "", "v1" and any other value abort startup.
func TestSelectorInvalidos(t *testing.T) {
	for _, v := range []string{"", "v1", "self-learning", "hard-limits", "RISK"} {
		cfg := Defaults()
		cfg.Arbitro.Selector = v
		if err := cfg.Validate(); err == nil {
			t.Fatalf("Validate(selector=%q) = nil, want erro (fail-fast)", v)
		}
	}
}
