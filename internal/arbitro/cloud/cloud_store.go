package cloud

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/igoricda/faas-continuum/internal/fsutil"
)

// CloudStore persists a single global fallback configuration using a
// temporary file, rename, 0600 permissions and fsync. It is safe for concurrent
// use.
type CloudStore struct {
	mu   sync.RWMutex
	path string
	cfg  *CloudFallbackConfig
}

// NewCloudStore creates a CloudStore. When path is non-empty and the file
// exists, it loads the saved state; a missing file starts empty and a corrupted
// file is an error.
func NewCloudStore(path string) (*CloudStore, error) {
	s := &CloudStore{
		path: path,
	}
	if path == "" {
		return s, nil
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *CloudStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cloud: falha ao ler %q: %w", s.path, err)
	}
	var cfg CloudFallbackConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("cloud: arquivo de persistência %q corrompido: %w", s.path, err)
	}
	// Validate on load exactly as Set does, failing fast at boot.
	if err := validateCloudConfig(cfg); err != nil {
		return fmt.Errorf("cloud: arquivo de persistência %q inválido: %w", s.path, err)
	}
	cp := deepCopyConfig(cfg)
	s.cfg = &cp
	return nil
}

// Get returns a copy of the configuration under lock, without exposing the
// internal pointer. It returns (zero, false) when no configuration is set.
func (s *CloudStore) Get() (CloudFallbackConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.cfg == nil {
		return CloudFallbackConfig{}, false
	}
	cp := deepCopyConfig(*s.cfg)
	return cp, true
}

// Set validates and persists the configuration. On a persistence failure it
// rolls back the in-memory state and returns the error. Validation is strict:
//   - endpoint_url through url.Parse, with scheme http or https and host != ""
//   - input_content_type through mime.ParseMediaType, and != ""
//   - auth_scheme != "" always
//
// Validation errors return ErrInvalidCloudConfig (wrapped with %w for
// errors.Is).
func (s *CloudStore) Set(cfg CloudFallbackConfig) error {
	if err := validateCloudConfig(cfg); err != nil {
		return err
	}

	// Deep copy to isolate the caller.
	copied := deepCopyConfig(cfg)

	s.mu.Lock()
	old := s.cfg
	s.cfg = &copied
	s.mu.Unlock()

	if err := s.saveSnapshot(copied); err != nil {
		s.mu.Lock()
		s.cfg = old
		s.mu.Unlock()
		return err
	}
	return nil
}

// Clear removes the configuration and the persistence file, leaving the stored
// state empty.
func (s *CloudStore) Clear() error {
	s.mu.Lock()
	old := s.cfg
	s.cfg = nil
	s.mu.Unlock()

	if s.path == "" {
		return nil
	}
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.mu.Lock()
		s.cfg = old
		s.mu.Unlock()
		return fmt.Errorf("cloud: falha ao remover %q: %w", s.path, err)
	}
	// Make the removal durable by syncing the directory.
	dir, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return fmt.Errorf("cloud: falha ao abrir diretório de %q: %w", s.path, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("cloud: falha ao sincronizar diretório de %q: %w", s.path, err)
	}
	return nil
}

func (s *CloudStore) saveSnapshot(cfg CloudFallbackConfig) error {
	if s.path == "" {
		return nil
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("cloud: falha ao serializar: %w", err)
	}
	data = append(data, '\n')
	tmp := s.path + ".tmp"
	if err := fsutil.WriteFileDurable(tmp, data); err != nil {
		return fmt.Errorf("cloud: falha ao gravar %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("cloud: falha ao renomear %q para %q: %w", tmp, s.path, err)
	}
	return nil
}

// canonicalizeHeaderNames returns a copy of names with canonical keys
// (http.CanonicalHeaderKey). It is applied at the Set/load boundary and keeps
// case-insensitive matching without changing validation semantics.
func canonicalizeHeaderNames(names []string) []string {
	if names == nil {
		return nil
	}
	out := make([]string, len(names))
	for i, h := range names {
		out[i] = http.CanonicalHeaderKey(h)
	}
	return out
}

// canonicalizeHeaderValues returns a copy of m with canonical keys. Nil is
// preserved to distinguish absent from empty.
func canonicalizeHeaderValues(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[http.CanonicalHeaderKey(k)] = v
	}
	return out
}

func deepCopyConfig(cfg CloudFallbackConfig) CloudFallbackConfig {
	if cfg.RequiredHeaders == nil && cfg.HeaderValues == nil && cfg.Functions == nil {
		return cfg
	}
	cp := cfg
	cp.RequiredHeaders = canonicalizeHeaderNames(cfg.RequiredHeaders)
	// Copy plus canonicalization is O(n) per entry and kept simple: the
	// configuration holds few functions and a few dozen headers, so no
	// shared cache or map is used; sharing one would hide case-insensitive
	// mismatches.
	if cfg.HeaderValues != nil {
		cp.HeaderValues = canonicalizeHeaderValues(cfg.HeaderValues)
	}
	if cfg.Functions != nil {
		cp.Functions = make(map[string]FunctionEndpoint, len(cfg.Functions))
		for fn, ep := range cfg.Functions {
			cp.Functions[fn] = deepCopyEndpoint(ep)
		}
	}
	return cp
}

// deepCopyEndpoint copies an endpoint with header canonicalization, applying
// the same boundary rule as deepCopyConfig per function.
func deepCopyEndpoint(ep FunctionEndpoint) FunctionEndpoint {
	cp := ep
	cp.RequiredHeaders = canonicalizeHeaderNames(ep.RequiredHeaders)
	if ep.HeaderValues != nil {
		cp.HeaderValues = canonicalizeHeaderValues(ep.HeaderValues)
	}
	return cp
}
