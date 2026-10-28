package cloud

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func validCloudConfig() CloudFallbackConfig {
	return CloudFallbackConfig{
		EndpointURL:      "https://minha-funcao-cloud.exemplo.com/invoke",
		InputContentType: "application/json",
		RequiredHeaders:  []string{"X-Client-Id"},
		AuthScheme:       "Bearer",
		HeaderValues:     map[string]string{"X-Client-Id": "abc"},
		AuthToken:        "tok123",
	}
}

func TestCloudStore_SetGetClear(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cloud.json")

	s, err := NewCloudStore(path)
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	if _, ok := s.Get(); ok {
		t.Fatalf("Get vazio deveria retornar false")
	}

	cfg := validCloudConfig()
	if err := s.Set(cfg); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Get returns a copy, not the internal pointer.
	got, ok := s.Get()
	if !ok {
		t.Fatalf("Get após Set: não encontrado")
	}
	if got.EndpointURL != cfg.EndpointURL || got.InputContentType != cfg.InputContentType || got.AuthScheme != cfg.AuthScheme || got.AuthToken != cfg.AuthToken {
		t.Fatalf("Get mismatch: got %+v want %+v", got, cfg)
	}

	// External mutation must not affect the store.
	got.HeaderValues["X-Client-Id"] = "mutated"
	got.RequiredHeaders[0] = "mutated"
	got2, _ := s.Get()
	if got2.HeaderValues["X-Client-Id"] != "abc" {
		t.Fatalf("HeaderValues mutação externa vazou: %v", got2.HeaderValues)
	}
	if got2.RequiredHeaders[0] != "X-Client-Id" {
		t.Fatalf("RequiredHeaders mutação externa vazou: %v", got2.RequiredHeaders)
	}

	// Verify the file has 0600 permissions and compact JSON.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("perm=%o, want 0600", perm)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Compact output contains no two-space indentation.
	for i := 0; i+2 < len(data); i++ {
		if data[i] == '\n' && data[i+1] == ' ' && data[i+2] == ' ' {
			t.Fatalf("arquivo contém MarshalIndent, esperado Marshal compacto")
		}
	}

	// Round-trip: reopen and compare.
	s2, err := NewCloudStore(path)
	if err != nil {
		t.Fatalf("NewCloudStore s2: %v", err)
	}
	got3, ok := s2.Get()
	if !ok {
		t.Fatalf("Get s2 após reabrir: não encontrado")
	}
	if got3.EndpointURL != cfg.EndpointURL {
		t.Fatalf("round-trip mismatch: %v", got3)
	}

	// A corrupted file must fail.
	corruptPath := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(corruptPath, []byte("{corrompido"), 0o600); err != nil {
		t.Fatalf("WriteFile corrupt: %v", err)
	}
	if _, err := NewCloudStore(corruptPath); err == nil {
		t.Fatalf("NewCloudStore com arquivo corrompido deveria falhar")
	}

	// Clear removes the file and nils the configuration.
	if err := s2.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, ok := s2.Get(); ok {
		t.Fatalf("Get após Clear deveria retornar false")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("arquivo deveria ter sido removido após Clear, err=%v", err)
	}

	// Reopening after Clear stays empty.
	s3, err := NewCloudStore(path)
	if err != nil {
		t.Fatalf("NewCloudStore s3: %v", err)
	}
	if _, ok := s3.Get(); ok {
		t.Fatalf("Get s3 após Clear deveria ser vazio")
	}

	// path == "" keeps the store in memory.
	sMem, err := NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore \"\": %v", err)
	}
	if err := sMem.Set(cfg); err != nil {
		t.Fatalf("Set in-memory: %v", err)
	}
	if _, ok := sMem.Get(); !ok {
		t.Fatalf("Get in-memory após Set: não encontrado")
	}
	if err := sMem.Clear(); err != nil {
		t.Fatalf("Clear in-memory: %v", err)
	}
	if _, ok := sMem.Get(); ok {
		t.Fatalf("Get in-memory após Clear deveria ser vazio")
	}
}

func TestCloudStore_Validacao(t *testing.T) {
	s, err := NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}

	cases := []struct {
		name string
		cfg  CloudFallbackConfig
	}{
		{"endpoint vazio", CloudFallbackConfig{EndpointURL: "", InputContentType: "application/json", AuthScheme: "Bearer"}},
		{"endpoint sem scheme", CloudFallbackConfig{EndpointURL: "minha-funcao/invoke", InputContentType: "application/json", AuthScheme: "Bearer"}},
		{"endpoint scheme ftp", CloudFallbackConfig{EndpointURL: "ftp://example.com/invoke", InputContentType: "application/json", AuthScheme: "Bearer"}},
		{"endpoint sem host", CloudFallbackConfig{EndpointURL: "https://", InputContentType: "application/json", AuthScheme: "Bearer"}},
		{"input_content_type vazio", CloudFallbackConfig{EndpointURL: "https://example.com/invoke", InputContentType: "", AuthScheme: "Bearer"}},
		{"input_content_type inválido", CloudFallbackConfig{EndpointURL: "https://example.com/invoke", InputContentType: "///", AuthScheme: "Bearer"}},
		{"auth_scheme vazio com token", CloudFallbackConfig{EndpointURL: "https://example.com/invoke", InputContentType: "application/json", AuthScheme: "", AuthToken: "tok"}},
		{"auth_scheme vazio sem token", CloudFallbackConfig{EndpointURL: "https://example.com/invoke", InputContentType: "application/json", AuthScheme: ""}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.Set(tc.cfg)
			if !errors.Is(err, ErrInvalidCloudConfig) {
				t.Fatalf("Set(%q) err=%v, want ErrInvalidCloudConfig", tc.name, err)
			}
			// Ensure nothing was persisted.
			if _, ok := s.Get(); ok {
				t.Fatalf("Get após validação falhada deveria permanecer vazio para caso %q", tc.name)
			}
		})
	}

	// A valid case with a non-Bearer auth_scheme and no token also passes.
	valid := CloudFallbackConfig{EndpointURL: "http://example.com/invoke", InputContentType: "application/json", AuthScheme: "Api-Key", AuthToken: ""}
	if err := s.Set(valid); err != nil {
		t.Fatalf("Set válido com http e Api-Key: %v", err)
	}
	if _, ok := s.Get(); !ok {
		t.Fatalf("Get após Set válido deveria retornar true")
	}
	// Clear before the next case.
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	// http is valid.
	valid2 := CloudFallbackConfig{EndpointURL: "http://example.com/invoke", InputContentType: "application/json", AuthScheme: "Bearer", AuthToken: ""}
	if err := s.Set(valid2); err != nil {
		t.Fatalf("Set http Bearer sem token: %v", err)
	}
}
