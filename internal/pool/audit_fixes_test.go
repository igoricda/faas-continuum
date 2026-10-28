package pool

import (
	"context"
	"testing"
)

// Ownership is explicit: the parser and the registry decide together, so a
// leftover identifier of a removed function is never adopted by a function
// whose name merely looks the same, and two such names coexist without
// collision.
func TestAuditResolveOwner(t *testing.T) {
	registered := map[string]bool{"fn": true, "fn-1": true}
	isReg := func(name string) bool { return registered[name] }

	cases := []struct {
		id       string
		wantFn   string
		wantOK   bool
		scenario string
	}{
		{"fn-0", "fn", true, "contêiner da fn"},
		{"fn-1-0", "fn-1", true, "contêiner da fn-1 (greedy correto)"},
		{"fn-1", "fn", true, "ID textual de função, mas contêiner da fn idx 1"},
		{"outra-9", "", false, "dono não registrado = órfão"},
		{"semindice", "", false, "fora da convenção"},
		{"", "", false, "vazio"},
	}
	for _, tc := range cases {
		fn, ok := ResolveOwner(tc.id, isReg)
		if ok != tc.wantOK || fn != tc.wantFn {
			t.Errorf("ResolveOwner(%q) = (%q, %v), esperado (%q, %v) [%s]", tc.id, fn, ok, tc.wantFn, tc.wantOK, tc.scenario)
		}
	}

	// Only the second function is registered, and the leftover identifier
	// belongs to the removed one: without a registered owner there is no
	// adoption by the wrong function.
	solo := map[string]bool{"fn-1": true}
	if fn, ok := ResolveOwner("fn-1", func(n string) bool { return solo[n] }); ok || fn != "" {
		t.Errorf("ResolveOwner(fn-1 sem fn registrada) = (%q, %v), esperado (\"\", false)", fn, ok)
	}
}

// Two functions whose names differ by a suffix coexist in the same pool: the
// generated identifiers do not collide and every acquisition returns the
// right owner.
func TestAuditFnAndFn1Coexist(t *testing.T) {
	m := NewManagerWithQueueCap(&fakeProvisioner{}, nil)
	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "fn", "img:1.0", 1); err != nil {
		t.Fatal(err)
	}
	if err := m.EnsureMinInstances(ctx, "fn-1", "img:1.0", 1); err != nil {
		t.Fatal(err)
	}
	if got := m.Instances("fn"); len(got) != 1 || got[0] != "fn-0" {
		t.Errorf("Instances(fn) = %v, esperado [fn-0]", got)
	}
	if got := m.Instances("fn-1"); len(got) != 1 || got[0] != "fn-1-0" {
		t.Errorf("Instances(fn-1) = %v, esperado [fn-1-0]", got)
	}

	id1, _, err := m.AcquireWaitCold(ctx, "fn", "img:1.0", 0)
	if err != nil || id1 != "fn-0" {
		t.Errorf("Acquire(fn) = (%q, %v), esperado (fn-0, nil)", id1, err)
	}
	id2, _, err := m.AcquireWaitCold(ctx, "fn-1", "img:1.0", 0)
	if err != nil || id2 != "fn-1-0" {
		t.Errorf("Acquire(fn-1) = (%q, %v), esperado (fn-1-0, nil)", id2, err)
	}
	id3, _, err := m.AcquireWaitCold(ctx, "fn", "img:1.0", 0)
	if err != nil || id3 == id2 {
		t.Errorf("novo Acquire(fn) = (%q, %v): colidiu com a fn-1", id3, err)
	}
}
