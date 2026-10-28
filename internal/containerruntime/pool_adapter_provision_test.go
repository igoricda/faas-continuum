package containerruntime

// Regression test: a provisioning that failed at the creation step
// cleaned nothing and leaked the socket directory, while the branch of
// the start already cleaned it.
//
// The fake below satisfies the consumer interface of the adapter without a
// container daemon. Every test exercises real concurrency and runs under
// the race detector.

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/containerd/containerd"
)

// fakeAdapterContainer is a minimal container, since only its identifier
// is read by the adapter and by the listing. The rest inherits the nil
// values of the embedded interface.
type fakeAdapterContainer struct {
	containerd.Container
	id string
}

func (c fakeAdapterContainer) ID() string { return c.id }

// fakeAdapterRuntime fails on demand at a given phase, the creation or the
// start, and counts the removals so that the best-effort cleanup of each
// branch can be pinned.
type fakeAdapterRuntime struct {
	mu                 sync.Mutex
	failCreateIDs      map[string]bool
	failStart          bool
	created            []string
	stopAndRemoveCalls int
	handles            int
}

func (f *fakeAdapterRuntime) ctx(parent context.Context) context.Context { return parent }

func (f *fakeAdapterRuntime) PullImage(_ context.Context, _ string) (containerd.Image, error) {
	return nil, nil
}

func (f *fakeAdapterRuntime) Create(_ context.Context, _ containerd.Image, opts CreateOptions) (containerd.Container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCreateIDs[opts.ID] {
		return nil, errors.New("Create simulado")
	}
	f.created = append(f.created, opts.ID)
	return fakeAdapterContainer{id: opts.ID}, nil
}

func (f *fakeAdapterRuntime) Start(_ context.Context, c containerd.Container) (*Handle, error) {
	if f.failStart {
		return nil, errors.New("Start simulado")
	}
	return &Handle{Container: c}, nil
}

func (f *fakeAdapterRuntime) Stop(_ context.Context, _ *Handle, _ time.Duration) error { return nil }

func (f *fakeAdapterRuntime) Remove(_ context.Context, _ containerd.Container) error { return nil }

func (f *fakeAdapterRuntime) StopAndRemove(_ context.Context, _ containerd.Container) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopAndRemoveCalls++
	return nil
}

func (f *fakeAdapterRuntime) List(_ context.Context) ([]containerd.Container, error) {
	return nil, nil
}

func (f *fakeAdapterRuntime) LoadContainer(_ context.Context, id string) (containerd.Container, error) {
	return fakeAdapterContainer{id: id}, nil
}

func (f *fakeAdapterRuntime) LoadRunningHandle(_ context.Context, c containerd.Container) (*Handle, error) {
	return &Handle{Container: c}, nil
}

func (f *fakeAdapterRuntime) stopAndRemoves() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopAndRemoveCalls
}

func (f *fakeAdapterRuntime) handlesOf(a *PoolAdapter) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.handles)
}

// socketDirExists reports whether the socket directory of the instance is
// still on the disk. It takes no testing handle, so it can be called from
// a goroutine, and the verdict is reported through the error method.
func socketDirExists(a *PoolAdapter, id string) bool {
	_, err := os.Stat(a.socketDir(id))
	return err == nil
}

// dirExiste reports whether the socket directory of the instance is still
// on the disk.
func dirExiste(t *testing.T, a *PoolAdapter, id string) bool {
	t.Helper()
	_, err := os.Stat(a.socketDir(id))
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	t.Fatalf("stat do socket-dir: %v", err)
	return false
}

// A failure at the creation must not leave an orphan socket directory,
// since the handle is never registered and no later termination would
// remove it.
func TestProvisionCreateFailureCleansSocketDir(t *testing.T) {
	root := t.TempDir()
	f := &fakeAdapterRuntime{failCreateIDs: map[string]bool{"fn-0": true}}
	a := NewPoolAdapterWithSocketRoot(f, root)

	if err := a.Provision(context.Background(), "fn-0", "img"); err == nil {
		t.Fatal("esperava erro de Create")
	}
	if dirExiste(t, a, "fn-0") {
		t.Fatalf("socket-dir de fn-0 vazou após Create falho: %q", a.socketDir("fn-0"))
	}
	if got := f.handlesOf(a); got != 0 {
		t.Fatalf("handles = %d após Create falho, esperado 0", got)
	}
}

// The branch of the start, which already cleaned up, is pinned to
// guarantee the symmetry with the creation.
func TestProvisionStartFailureCleansSocketDir(t *testing.T) {
	root := t.TempDir()
	f := &fakeAdapterRuntime{failStart: true}
	a := NewPoolAdapterWithSocketRoot(f, root)

	if err := a.Provision(context.Background(), "fn-0", "img"); err == nil {
		t.Fatal("esperava erro de Start")
	}
	if dirExiste(t, a, "fn-0") {
		t.Fatalf("socket-dir de fn-0 vazou após Start falho: %q", a.socketDir("fn-0"))
	}
	if got := f.stopAndRemoves(); got != 1 {
		t.Fatalf("StopAndRemove chamado %dx após Start falho, esperado 1", got)
	}
	if got := f.handlesOf(a); got != 0 {
		t.Fatalf("handles = %d após Start falho, esperado 0", got)
	}
}

// The happy path: the directory persists until the termination, which
// removes it together with the handle.
func TestProvisionSuccessKeepsSocketDirUntilTerminate(t *testing.T) {
	root := t.TempDir()
	f := &fakeAdapterRuntime{}
	a := NewPoolAdapterWithSocketRoot(f, root)
	ctx := context.Background()

	if err := a.Provision(ctx, "fn-0", "img"); err != nil {
		t.Fatalf("Provision(): %v", err)
	}
	if !dirExiste(t, a, "fn-0") {
		t.Fatal("socket-dir sumiu após Provision ok")
	}
	if err := a.Terminate(ctx, "fn-0"); err != nil {
		t.Fatalf("Terminate(): %v", err)
	}
	if dirExiste(t, a, "fn-0") {
		t.Fatal("socket-dir vazou após Terminate ok")
	}
}

// Concurrent provisionings with mixed failures at the creation: no
// orphan directory and no phantom handle at the end, with the race
// detector covering the handles and the mutex.
func TestProvisionConcurrentMixedCreateFailures(t *testing.T) {
	root := t.TempDir()
	f := &fakeAdapterRuntime{failCreateIDs: map[string]bool{}}
	for i := 0; i < 16; i += 2 {
		f.failCreateIDs["fn-"+itoa2(i)] = true
	}
	a := NewPoolAdapterWithSocketRoot(f, root)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "fn-" + itoa2(i)
			if err := a.Provision(ctx, id, "img"); err != nil {
				// The failure is expected on the even pairs, whose
				// directory must never leak.
				if socketDirExists(a, id) {
					t.Errorf("socket-dir de %q vazou após Create falho concorrente", id)
				}
				return
			}
		}(i)
	}
	wg.Wait()

	// The odd instances were provisioned and have a directory, the even
	// ones failed and have none.
	for i := 0; i < 16; i++ {
		id := "fn-" + itoa2(i)
		want := i%2 == 1
		if got := socketDirExists(a, id); got != want {
			t.Errorf("socket-dir de %q existe=%v, esperado %v", id, got, want)
		}
	}
	if got := f.handlesOf(a); got != 8 {
		t.Errorf("handles = %d, esperado 8 (só os ímpares)", got)
	}

	// The termination of all the odd instances cleans everything, and the
	// root ends up empty.
	for i := 1; i < 16; i += 2 {
		if err := a.Terminate(ctx, "fn-"+itoa2(i)); err != nil {
			t.Errorf("Terminate(fn-%d): %v", i, err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir(root): %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("raiz de sockets com %d órfão(s) após Terminate de todos", len(entries))
	}
}

// itoa2 formats a number from 0 to 99 with two digits, which avoids the
// formatting package on the hot path of the test.
func itoa2(n int) string {
	return string([]byte{byte('0' + n/10), byte('0' + n%10)})
}
