package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/igoricda/faas-continuum/internal/fsutil"
)

// load reads the persisted functions into memory and rebuilds the invoke
// token cache. A missing file is not an error; a corrupt file is.
func (r *Registry) load() error {
	data, err := os.ReadFile(r.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("registry: falha ao ler %q: %w", r.path, err)
	}

	var functions map[string]Function
	if err := json.Unmarshal(data, &functions); err != nil {
		return fmt.Errorf("registry: arquivo de persistência %q corrompido: %w", r.path, err)
	}
	for name, fn := range functions {
		fn.setInvokeTokenCache(fn.InvokeTokenSHA256)
		functions[name] = fn
	}
	r.functions = functions
	return nil
}

// save writes the registry through a temporary file and an atomic rename.
func (r *Registry) save() error {
	if r.path == "" {
		return nil
	}

	// Compact encoding keeps each rewrite small on flash storage.
	data, err := json.Marshal(r.functions)
	if err != nil {
		return fmt.Errorf("registry: falha ao serializar: %w", err)
	}
	data = append(data, '\n')
	if err := fsutil.WriteFileDurable(r.path+".tmp", data); err != nil {
		return fmt.Errorf("registry: falha ao gravar %q: %w", r.path+".tmp", err)
	}
	if err := os.Rename(r.path+".tmp", r.path); err != nil {
		return fmt.Errorf("registry: falha ao renomear %q para %q: %w", r.path+".tmp", r.path, err)
	}
	return nil
}
