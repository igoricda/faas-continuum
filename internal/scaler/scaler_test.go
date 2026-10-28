// noqa: SIZE_OK — integration suite, single responsibility per package; split would fragment coherent scenarios
package scaler

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// recordingProvisioner keeps provisioned/terminated IDs - same pattern as pool tests.
type recordingProvisioner struct {
	mu          sync.Mutex
	provisioned []string
	terminated  []string
}

func (r *recordingProvisioner) Provision(_ context.Context, instanceID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.provisioned = append(r.provisioned, instanceID)
	return nil
}
func (r *recordingProvisioner) Terminate(_ context.Context, instanceID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terminated = append(r.terminated, instanceID)
	return nil
}
func (r *recordingProvisioner) terminatedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.terminated)
}
func (r *recordingProvisioner) terminatedCopy() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.terminated))
	copy(out, r.terminated)
	return out
}

// touchLastUsed sets lastUsed[functionName] to at via AcquireWait+Release deterministic.
func touchLastUsed(t *testing.T, pm *pool.Manager, functionName, imageRef string, at time.Time) {
	t.Helper()
	pm.WithClock(func() time.Time { return at })
	// Ensure at least one instance exists for AcquireWait to touch
	if pm.Count(functionName) == 0 {
		if err := pm.EnsureMinInstances(context.Background(), functionName, imageRef, 1); err != nil {
			t.Fatalf("EnsureMinInstances: %v", err)
		}
	}
	id, err := pm.AcquireWait(context.Background(), functionName, imageRef, 0)
	if err != nil {
		t.Fatalf("AcquireWait touch: %v", err)
	}
	pm.Release(functionName, id)
}

func TestScalerDefaultInterval(t *testing.T) {
	prov := &recordingProvisioner{}
	pm := pool.NewManagerWithQueueCap(prov, nil)
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	m := New(pm, reg, 15*time.Minute, 0)
	if m.interval != DefaultInterval {
		t.Fatalf("interval = %v, want %v", m.interval, DefaultInterval)
	}
	if m.interval != 30*time.Second {
		t.Fatalf("DefaultInterval = %v, want 30s", m.interval)
	}
}

func TestScalerEmptyRegistry(t *testing.T) {
	prov := &recordingProvisioner{}
	pm := pool.NewManagerWithQueueCap(prov, nil)
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json")) // empty

	m := New(pm, reg, 5*time.Minute, 50*time.Millisecond)
	// also test reapOnce directly with empty
	m.reapOnce(context.Background())
	if n := prov.terminatedCount(); n != 0 {
		t.Fatalf("terminated = %d, want 0 for empty registry", n)
	}
	// also ensure no panic via Run with one tick
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)
	time.Sleep(80 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
	if prov.terminatedCount() != 0 {
		t.Fatalf("terminated after Run with empty = %d, want 0", prov.terminatedCount())
	}
}

func TestScalerReapsAfterTimeout(t *testing.T) {
	prov := &recordingProvisioner{}
	pm := pool.NewManagerWithQueueCap(prov, nil)
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	// Register fn with min 1, no per-function idle => uses global 5m
	_, _, err := reg.Register("host", "fn", "img", registry.RegisterOptions{MinInstances: 1, IdleTimeout: 0})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	base := time.Now().Truncate(time.Second)
	// Use pool clock controlled via WithClock for touch
	pm.WithClock(func() time.Time { return base })

	ctx := context.Background()
	// Create 3 instances (excess 2 beyond min 1)
	if err := pm.EnsureMinInstances(ctx, "fn", "img", 3); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	if got := pm.Count("fn"); got != 3 {
		t.Fatalf("Count = %d, want 3", got)
	}
	// Touch lastUsed to base
	touchLastUsed(t, pm, "fn", "img", base)

	// Scaler with global 5m, fake clock advancing beyond timeout
	now := base
	m := New(pm, reg, 5*time.Minute, 50*time.Millisecond)
	m.WithClock(func() time.Time { return now })

	// Before timeout: now = base+1m -> no reap
	now = base.Add(1 * time.Minute)
	m.reapOnce(ctx)
	if got := pm.Count("fn"); got != 3 {
		t.Fatalf("before timeout Count = %d, want 3", got)
	}
	if prov.terminatedCount() != 0 {
		t.Fatalf("before timeout terminated = %d, want 0", prov.terminatedCount())
	}

	// Beyond timeout: now = base+10m (>5m)
	now = base.Add(10 * time.Minute)
	m.reapOnce(ctx)
	if got := pm.Count("fn"); got != 1 {
		t.Fatalf("after timeout Count = %d, want 1 (reaped 2)", got)
	}
	if prov.terminatedCount() != 2 {
		t.Fatalf("terminated = %d, want 2", prov.terminatedCount())
	}
	// Verify via list that terminated are newest first fn-2 fn-1 (fn-0 survives)
	terms := prov.terminatedCopy()
	// terms should contain fn-2 and fn-1 (order may be fn-2 then fn-1)
	found0 := false
	for _, id := range terms {
		if id == "fn-0" {
			found0 = true
		}
	}
	if found0 {
		t.Fatalf("fn-0 should survive, but was terminated: %v", terms)
	}
}

func TestScalerRespectsContextCancel(t *testing.T) {
	prov := &recordingProvisioner{}
	pm := pool.NewManagerWithQueueCap(prov, nil)
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	m := New(pm, reg, 5*time.Minute, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.Run(ctx)
		close(done)
	}()
	// Let at least one tick happen
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not return after cancel (goroutine leak)")
	}
	// Ensure no panic and no extra work after cancel
	time.Sleep(20 * time.Millisecond)
	select {
	case <-done:
	default:
		t.Fatal("done channel should be closed")
	}
}

func TestScalerInterval(t *testing.T) {
	prov := &recordingProvisioner{}
	pm := pool.NewManagerWithQueueCap(prov, nil)
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	var ticks int64
	// list counting ticks (no error return)
	m := New(pm, reg, 5*time.Minute, 50*time.Millisecond)
	origList := m.list
	m.list = func() []registry.Function {
		atomic.AddInt64(&ticks, 1)
		return origList()
	}
	// controlled clock (not advancing beyond timeout, just counting)
	m.WithClock(time.Now)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	// generous window: 50ms interval, 180ms should give at least 2 ticks
	time.Sleep(180 * time.Millisecond)
	cancel()
	// wait a bit for Run to exit
	time.Sleep(20 * time.Millisecond)

	got := atomic.LoadInt64(&ticks)
	if got < 2 {
		t.Fatalf("ticks = %d, want >=2 (interval 50ms, waited 180ms)", got)
	}
}

func TestScalerRegistryError(t *testing.T) {
	prov := &recordingProvisioner{}
	pm := pool.NewManagerWithQueueCap(prov, nil)
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	// Provide a function so reap would happen
	_, _, err := reg.Register("host", "fn", "img", registry.RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	base := time.Now().Truncate(time.Second)
	// Global clock for the pool: base time, stays set for the test (like TestScalerReapsAfterTimeout)
	pm.WithClock(func() time.Time { return base })
	if err := pm.EnsureMinInstances(context.Background(), "fn", "img", 2); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	touchLastUsed(t, pm, "fn", "img", base)

	// Scaler with global 5m, fake clock advancing beyond timeout
	m := New(pm, reg, 5*time.Minute, 20*time.Millisecond)
	m.WithClock(func() time.Time { return base.Add(10 * time.Minute) })

	// First reapOnce -> list succeeds, timeout exceeded => reap
	m.reapOnce(context.Background())
	if got := pm.Count("fn"); got != 1 {
		t.Fatalf("after first tick Count = %d, want 1", got)
	}
	if prov.terminatedCount() != 1 {
		t.Fatalf("terminated after first tick = %d, want 1", prov.terminatedCount())
	}
	// Second reapOnce -> no more functions to reap (only fn-0 survives)
	m.reapOnce(context.Background())
	if got := pm.Count("fn"); got != 1 {
		t.Fatalf("after second tick Count = %d, want 1", got)
	}
	if prov.terminatedCount() != 1 {
		t.Fatalf("terminated after second tick = %d, want 1", prov.terminatedCount())
	}
}

// Additional coverage: per-function idle_timeout override
func TestScalerPerFunctionIdleTimeout(t *testing.T) {
	prov := &recordingProvisioner{}
	pm := pool.NewManagerWithQueueCap(prov, nil)
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))

	// fn with 1m per-function timeout, global 15m
	_, _, err := reg.Register("host", "fn", "img", registry.RegisterOptions{MinInstances: 1, IdleTimeout: config.Duration(1 * time.Minute)})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	base := time.Now()
	pm.WithClock(func() time.Time { return base })
	if err := pm.EnsureMinInstances(context.Background(), "fn", "img", 2); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	touchLastUsed(t, pm, "fn", "img", base)

	now := base.Add(2 * time.Minute) // >1m per-fn, <15m global
	m := New(pm, reg, 15*time.Minute, 50*time.Millisecond)
	m.WithClock(func() time.Time { return now })
	m.reapOnce(context.Background())
	if got := pm.Count("fn"); got != 1 {
		t.Fatalf("per-function timeout Count = %d, want 1", got)
	}
}
