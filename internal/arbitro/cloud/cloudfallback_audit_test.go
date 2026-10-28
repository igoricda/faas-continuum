package cloud

// load validates like Set: a file with an invalid endpoint fails at boot
// instead of being kept as valid configuration.

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRejectsInvalidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cloud.json")
	if err := os.WriteFile(path, []byte(`{"endpoint_url":"ftp://invalido","input_content_type":"application/json","auth_scheme":"Bearer"}`), 0o600); err != nil {
		t.Fatalf("write temp cloud: %v", err)
	}
	if _, err := NewCloudStore(path); err == nil {
		t.Fatal("NewCloudStore(endpoint ftp) = nil, esperado erro fail-fast")
	}
}

func TestLoadAcceptsValidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cloud.json")
	if err := os.WriteFile(path, []byte(`{"endpoint_url":"https://nuvem.example.com/invoke","input_content_type":"application/json","auth_scheme":"Bearer"}`), 0o600); err != nil {
		t.Fatalf("write temp cloud: %v", err)
	}
	s, err := NewCloudStore(path)
	if err != nil {
		t.Fatalf("NewCloudStore(válida) = %v, esperado nil", err)
	}
	if _, ok := s.Get(); !ok {
		t.Fatal("Get após load válido = false, esperado true")
	}
}
