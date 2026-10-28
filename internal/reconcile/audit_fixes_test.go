package reconcile

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// Two functions whose names differ by a suffix are registered on the same
// node: every survivor is adopted by the right owner, and a leftover
// identifier that only looks like one of them is discarded instead of
// adopted.
func TestAuditRunAdoptsFnAndFn1ToRightOwners(t *testing.T) {
	rt := newFakeRuntime()
	rt.running["fn-0"] = true
	rt.running["fn-1-0"] = true

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "fn", "img:1.0", registry.RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reg.Register("host", "fn-1", "img:1.0", registry.RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}
	pl := pool.NewManagerWithQueueCap(&fakeProvisioner{}, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() erro inesperado: %v", err)
	}
	if got := pl.Instances("fn"); len(got) != 1 || got[0] != "fn-0" {
		t.Errorf("Instances(fn) = %v, esperado [fn-0] (adoção cruzada?)", got)
	}
	if got := pl.Instances("fn-1"); len(got) != 1 || got[0] != "fn-1-0" {
		t.Errorf("Instances(fn-1) = %v, esperado [fn-1-0] (adoção cruzada?)", got)
	}
	if len(rt.removed) != 0 {
		t.Errorf("removed = %v, esperado vazio (nada era órfão)", rt.removed)
	}
}

func TestAuditRunDiscardsZombieTextualCollision(t *testing.T) {
	rt := newFakeRuntime()
	// The identifier here belongs to the removed function, while a function
	// with the same name exists: that function has its own identifiers
	// with a numeric suffix.
	rt.running["fn-1"] = true
	rt.running["fn-1-0"] = true

	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if _, _, err := reg.Register("host", "fn-1", "img:1.0", registry.RegisterOptions{MinInstances: 1}); err != nil {
		t.Fatal(err)
	}
	pl := pool.NewManagerWithQueueCap(&fakeProvisioner{}, nil)

	if err := Run(context.Background(), rt, reg, pl); err != nil {
		t.Fatalf("Run() erro inesperado: %v", err)
	}
	if !slices.Contains(rt.removed, "fn-1") {
		t.Errorf("zumbi textual fn-1 deveria ser removido; removed = %v", rt.removed)
	}
	if got := pl.Instances("fn-1"); len(got) != 1 || got[0] != "fn-1-0" {
		t.Errorf("Instances(fn-1) = %v, esperado [fn-1-0]", got)
	}
}
