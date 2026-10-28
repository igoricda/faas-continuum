// Package registry stores the functions activated on a node and enforces
// unique resource addressing: every function owns a URL of the form
// host/function/<name>, and duplicate names are rejected.
//
// The registry can persist itself to disk, so a restart keeps the registered
// functions; instance pool state is not persisted and is rebuilt at startup
// by internal/reconcile.
package registry

import (
	"sync"
)

// Registry holds the registered functions and optionally persists them to
// disk. It is safe for concurrent use.
type Registry struct {
	mu                sync.RWMutex
	functions         map[string]Function // keyed by function name
	path              string              // empty: no persistence (for example, tests)
	allowedRegistries []string            // empty: any registry is allowed
}

// NewPersistentRegistry returns a registry persisted to path.
//
// Writes are write-through to a single JSON file with 0600 permissions. An
// existing path is loaded, a missing one starts empty, and a corrupt file
// returns an error instead of silently discarding data.
func NewPersistentRegistry(path string, allowedRegistries ...string) (*Registry, error) {
	r := &Registry{functions: make(map[string]Function), path: path, allowedRegistries: allowedRegistries}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

// Get returns the registered function with the given name.
func (r *Registry) Get(name string) (Function, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.functions[name]
	return fn, ok
}

// List returns all registered functions.
func (r *Registry) List() []Function {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]Function, 0, len(r.functions))
	for _, fn := range r.functions {
		out = append(out, fn)
	}
	return out
}

// apply owns the mutex, runs the map mutation, saves the persisted state on
// success, and rolls the mutation back through undo if saving fails.
//
// A mutation error is returned unchanged and skips the save. A nil undo with
// a nil error is an idempotent no-op that also skips the save. A non-nil undo
// requires a save: if it fails, the mutation is undone and the raw error is
// returned for the caller to wrap with the operation name.
func (r *Registry) apply(mut func(map[string]Function) (undo func(), err error)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	undo, err := mut(r.functions)
	if err != nil {
		return err
	}
	if undo == nil {
		return nil
	}
	if serr := r.save(); serr != nil {
		undo()
		return serr
	}
	return nil
}
