//go:build linux

package nodeapi

// Minimal tests of the event-driven wait through the notification
// mechanism. The complete matrix, with the race, the overflow, the burst
// and the traversal, is covered below. Here only the acceptance is
// checked: the event-driven hit, the ceiling of four dials, the deadline
// and the release of the descriptors.

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ctr "github.com/igoricda/faas-continuum/internal/containerruntime"
)

// TestWaitReady_EventDriven: the socket appears 100 ms after the start,
// so the wait succeeds with at most four attempts and a duration close to
// the delay. The number of attempts does not grow with the delay.
func TestWaitReady_EventDriven(t *testing.T) {
	if readinessSocketName != "handle.sock" {
		t.Fatalf("readinessSocketName = %q, esperado \"handle.sock\" (must match containerruntime.socketFileName)", readinessSocketName)
	}
	dir := t.TempDir()
	sockPath := filepath.Join(dir, readinessSocketName)
	addr := &fakeAddresser{path: sockPath}
	checker := &SocketReadinessChecker{Addresser: addr}
	go func() {
		time.Sleep(100 * time.Millisecond)
		l, err := net.Listen("unix", sockPath)
		if err != nil {
			t.Errorf("listen falhou: %v", err)
			return
		}
		defer l.Close()
		time.Sleep(2 * time.Second)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	attempts, err := checker.WaitReady(ctx, "inst-event")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("WaitReady event-driven falhou: %v (attempts=%d, elapsed=%v)", err, attempts, elapsed)
	}
	if attempts < 1 || attempts > maxReadinessDials {
		t.Errorf("attempts = %d, esperado 1..%d (teto total)", attempts, maxReadinessDials)
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("elapsed = %v, esperado ≈100ms (retornou antes do evento?)", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("elapsed = %v, esperado ≈100ms (não acordou no evento?)", elapsed)
	}
	t.Logf("event-driven hit: attempts=%d elapsed=%v", attempts, elapsed)
}

// TestWaitReady_ConfirmCapFour: the dial always fails but the event
// arrives, since the file is created without a listen, so the wait fails
// after exactly four dials: the immediate one, the check-watch-check
// pair, the confirmation and a single redial.
func TestWaitReady_ConfirmCapFour(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, readinessSocketName)
	addr := &fakeAddresser{path: sockPath}
	origDial := unixDial
	var dials int32
	unixDial = func(network, address string, timeout time.Duration) (net.Conn, error) {
		atomic.AddInt32(&dials, 1)
		return nil, errors.New("nodeapi: stub sempre-falha (recusa persistente pós-evento)")
	}
	defer func() { unixDial = origDial }()
	go func() {
		time.Sleep(50 * time.Millisecond)
		if err := os.WriteFile(sockPath, []byte{}, 0o644); err != nil {
			t.Errorf("criação do arquivo para o evento falhou: %v", err)
		}
	}()
	checker := &SocketReadinessChecker{Addresser: addr}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempts, err := checker.WaitReady(ctx, "inst-cap")
	if err == nil {
		t.Fatal("WaitReady com recusa persistente deveria falhar, retornou nil")
	}
	if attempts != maxReadinessDials {
		t.Errorf("attempts = %d, esperado EXATAMENTE %d (teto total)", attempts, maxReadinessDials)
	}
	if got := atomic.LoadInt32(&dials); got != maxReadinessDials {
		t.Errorf("dials reais no stub = %d, esperado %d", got, maxReadinessDials)
	}
	t.Logf("cap-four: attempts=%d err=%v", attempts, err)
}

// countOpenFDs counts the open descriptors of the process. The owner of
// the watcher must release everything before it returns.
func countOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("sem /proc/self/fd: %v", err)
	}
	return len(entries)
}

// TestWaitReady_DeadlineClosesWatcher: a short context without a socket
// gives the deadline error with the watcher closed and no change in the
// number of descriptors.
func TestWaitReady_DeadlineClosesWatcher(t *testing.T) {
	dir := t.TempDir()
	addr := &fakeAddresser{path: filepath.Join(dir, readinessSocketName)}
	checker := &SocketReadinessChecker{Addresser: addr}
	before := countOpenFDs(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	attempts, err := checker.WaitReady(ctx, "inst-no-socket")
	elapsed := time.Since(start)
	if err != context.DeadlineExceeded {
		t.Fatalf("esperado context.DeadlineExceeded, obtido %v (attempts=%d, elapsed=%v)", err, attempts, elapsed)
	}
	if attempts < 1 || attempts > maxReadinessDials {
		t.Errorf("attempts = %d, esperado 1..%d", attempts, maxReadinessDials)
	}
	if elapsed > 800*time.Millisecond {
		t.Errorf("elapsed = %v, esperado ≈300ms (poll não acordou no ctx?)", elapsed)
	}
	// It gives a short window for the deferred close to release the
	// descriptor before the measurement, since the return already
	// happened and the close is synchronous in the defer.
	time.Sleep(50 * time.Millisecond)
	if after := countOpenFDs(t); after != before {
		t.Errorf("delta de FD = %d (antes=%d depois=%d): watcher vazou fd", after-before, before, after)
	}
	t.Logf("deadline: attempts=%d elapsed=%v fds=%d", attempts, elapsed, before)
}

// ---------------------------------------------------------------------------
// Complete matrix of the event-driven wait, with a real watcher in a
// temporary directory, timeouts of milliseconds and no network nor
// container daemon. The naming conventions group the tests by outcome:
//   - TestWaitReady_Event*               covers the success path
//   - TestWaitReady_FallbackOrDeadline*  covers the failure path
// ---------------------------------------------------------------------------

// listenUnixHeld opens a Unix listener at the given path and keeps it
// until the end of the test, removing any previous file first.
func listenUnixHeld(t *testing.T, sockPath string) net.Listener {
	t.Helper()
	_ = os.Remove(sockPath)
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix em %s falhou: %v", sockPath, err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// withCountingDial wraps the dial with a counter and restores the
// original through the cleanup of the test. With no failure it delegates
// to the real dial; with a failure it forces the failure, which models a
// persistent refusal after the event.
func withCountingDial(t *testing.T, dials *int32, fail error) {
	t.Helper()
	orig := unixDial
	unixDial = func(network, address string, timeout time.Duration) (net.Conn, error) {
		atomic.AddInt32(dials, 1)
		if fail != nil {
			return nil, fail
		}
		return orig(network, address, timeout)
	}
	t.Cleanup(func() { unixDial = orig })
}

// syncLogBuffer is a buffer with a mutex: the standard logger is global,
// so a goroutine leaked by another test can write to it while this test
// reads it. Without the mutex that would be a data race.
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureNodeLog redirects the standard logger, where the fallback writes
// its contractual line, to a buffer and restores it at the end of the
// test.
func captureNodeLog(t *testing.T) *syncLogBuffer {
	t.Helper()
	buf := &syncLogBuffer{}
	orig := log.Writer()
	log.SetOutput(buf)
	t.Cleanup(func() { log.SetOutput(orig) })
	return buf
}

// assertNoFdLeak requires the absence of growth in the number of
// descriptors: a real leak of the watcher persists, while a background
// drift, such as the asynchronous cleanup of another test of the package,
// can close unrelated descriptors in the window and give a negative
// change, or open transient ones. That is why a second measurement is
// taken before the failure.
func assertNoFdLeak(t *testing.T, before int) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	after := countOpenFDs(t)
	if after > before {
		time.Sleep(200 * time.Millisecond)
		after = countOpenFDs(t)
	}
	if after > before {
		t.Errorf("delta de FD = +%d (antes=%d depois=%d): watcher vazou fd", after-before, before, after)
	} else if after != before {
		t.Logf("delta de FD = %d (antes=%d depois=%d): deriva de fundo, sem crescimento", after-before, before, after)
	}
}

// TestWaitReady_EventDelays: the socket appears after 50, 150 and 200
// milliseconds, so the wait succeeds with at most four attempts and a
// duration close to the delay. The number of attempts does not grow with
// the delay.
func TestWaitReady_EventDelays(t *testing.T) {
	for _, delay := range []time.Duration{50 * time.Millisecond, 150 * time.Millisecond, 200 * time.Millisecond} {
		delay := delay
		t.Run(delay.String(), func(t *testing.T) {
			dir := t.TempDir()
			sockPath := filepath.Join(dir, readinessSocketName)
			checker := &SocketReadinessChecker{Addresser: &fakeAddresser{path: sockPath}}
			go func() {
				time.Sleep(delay)
				l, err := net.Listen("unix", sockPath)
				if err != nil {
					t.Errorf("listen unix falhou: %v", err)
					return
				}
				defer l.Close()
				time.Sleep(4 * time.Second)
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			start := time.Now()
			attempts, err := checker.WaitReady(ctx, "inst-delay")
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("WaitReady delay=%v falhou: %v (attempts=%d, elapsed=%v)", delay, err, attempts, elapsed)
			}
			if attempts < 1 || attempts > maxReadinessDials {
				t.Errorf("attempts = %d, esperado 1..%d (teto total)", attempts, maxReadinessDials)
			}
			if elapsed+30*time.Millisecond < delay {
				t.Errorf("elapsed = %v, delay = %v (retornou antes do evento?)", elapsed, delay)
			}
			if elapsed > 5*time.Second {
				t.Errorf("elapsed = %v, delay = %v (não acordou no evento?)", elapsed, delay)
			}
			t.Logf("delay=%v: attempts=%d elapsed=%v", delay, attempts, elapsed)
		})
	}
}

// TestWaitReady_EventWarmNoWatch: the socket already exists, so the wait
// succeeds with a single attempt and no watcher at all, with no change in
// the number of descriptors around the call.
func TestWaitReady_EventWarmNoWatch(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, readinessSocketName)
	l := listenUnixHeld(t, sockPath)
	defer l.Close()
	var dials int32
	withCountingDial(t, &dials, nil)
	checker := &SocketReadinessChecker{Addresser: &fakeAddresser{path: sockPath}}
	before := countOpenFDs(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempts, err := checker.WaitReady(ctx, "inst-warm")
	if err != nil {
		t.Fatalf("WaitReady warm falhou: %v (attempts=%d)", err, attempts)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, esperado 1 (warm retorna no dial imediato)", attempts)
	}
	if got := atomic.LoadInt32(&dials); got != 1 {
		t.Errorf("dials no stub = %d, esperado 1 (nenhum watch/confirm disparado)", got)
	}
	assertNoFdLeak(t, before)
}

// TestWaitReady_EventIgnoresOtherName: a file whose name matches none of
// the clauses of the filter is ignored, with no confirmation dial for it,
// and the real socket that appears afterwards resolves the wait with three
// attempts.
//
// The hybrid filter, which accepts the expected socket name or the base
// name of the socket path, is deliberate: in production both clauses
// coincide, and the second one keeps the per-instance test doubles
// working. Here the socket path uses a base name different from the
// constant, so both clauses are exercised, and the decoy file matches
// neither.
func TestWaitReady_EventIgnoresOtherName(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "inst-other.sock")
	var dials int32
	withCountingDial(t, &dials, nil)
	checker := &SocketReadinessChecker{Addresser: &fakeAddresser{path: sockPath}}
	go func() {
		time.Sleep(50 * time.Millisecond)
		if err := os.WriteFile(filepath.Join(dir, "noise.tmp"), []byte{}, 0o644); err != nil {
			t.Errorf("criação do decoy falhou: %v", err)
		}
	}()
	go func() {
		time.Sleep(200 * time.Millisecond)
		l, err := net.Listen("unix", sockPath)
		if err != nil {
			t.Errorf("listen unix falhou: %v", err)
			return
		}
		defer l.Close()
		time.Sleep(4 * time.Second)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	start := time.Now()
	attempts, err := checker.WaitReady(ctx, "inst-other")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("WaitReady com decoy falhou: %v (attempts=%d, elapsed=%v)", err, attempts, elapsed)
	}
	// The immediate attempt, the check-watch-check pair and the
	// confirmation of the real socket: the decoy consumed no redial
	// of the budget, since otherwise the count would reach four and
	// the wait would fail.
	if attempts != 3 {
		t.Errorf("attempts = %d, esperado 3 (decoy ignorado, sem confirm extra)", attempts)
	}
	if got := atomic.LoadInt32(&dials); got != 3 {
		t.Errorf("dials no stub = %d, esperado 3", got)
	}
	if elapsed+30*time.Millisecond < 200*time.Millisecond {
		t.Errorf("elapsed = %v, esperado ≈200ms (voltou no decoy?)", elapsed)
	}
}

// TestWaitReady_EventRemoveRecreate: a cycle of removal and re-listen
// succeeds with a single attempt, since the stale file is removed and the
// dial reaches the fresh listener.
func TestWaitReady_EventRemoveRecreate(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, readinessSocketName)
	stale, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen inicial falhou: %v", err)
	}
	_ = stale.Close()
	_ = os.Remove(sockPath)
	listenUnixHeld(t, sockPath)
	checker := &SocketReadinessChecker{Addresser: &fakeAddresser{path: sockPath}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempts, err := checker.WaitReady(ctx, "inst-recreate")
	if err != nil {
		t.Fatalf("WaitReady pós Remove+recriação falhou: %v (attempts=%d)", err, attempts)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, esperado 1 (socket recriado diala de imediato)", attempts)
	}
}

// TestWaitReady_EventBurstSurvives: a moderate burst of about two hundred
// files with names that do not match the filter, followed by the real
// socket, survives entirely and succeeds with at most four attempts and
// no hang. It does not touch the limits of the host, which would make it
// fragile; the real invalidation is covered by the fallback tests, which
// remove the watched directory.
func TestWaitReady_EventBurstSurvives(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, readinessSocketName)
	checker := &SocketReadinessChecker{Addresser: &fakeAddresser{path: sockPath}}
	go func() {
		time.Sleep(30 * time.Millisecond)
		for i := 0; i < 200; i++ {
			name := filepath.Join(dir, "burst-"+string(rune('a'+i%26))+".tmp")
			_ = os.WriteFile(name, []byte{}, 0o644)
		}
		time.Sleep(50 * time.Millisecond)
		l, err := net.Listen("unix", sockPath)
		if err != nil {
			t.Errorf("listen unix falhou: %v", err)
			return
		}
		defer l.Close()
		time.Sleep(4 * time.Second)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	start := time.Now()
	attempts, err := checker.WaitReady(ctx, "inst-burst")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("WaitReady sob rajada falhou: %v (attempts=%d, elapsed=%v)", err, attempts, elapsed)
	}
	if attempts < 1 || attempts > maxReadinessDials {
		t.Errorf("attempts = %d, esperado 1..%d", attempts, maxReadinessDials)
	}
	if elapsed > 5*time.Second {
		t.Errorf("elapsed = %v (rajada travou o laço?)", elapsed)
	}
	t.Logf("burst: attempts=%d elapsed=%v", attempts, elapsed)
}

// TestWaitReady_EventStress32: thirty-two concurrent watchers, which
// cover the ceiling of the queue and the burst of descriptors, run under
// the race detector. All of them succeed and none hangs.
func TestWaitReady_EventStress32(t *testing.T) {
	const n = 32
	dirs := make([]string, n)
	for i := range dirs {
		dirs[i] = t.TempDir()
	}
	var wg sync.WaitGroup
	errs := make([]error, n)
	atts := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sockPath := filepath.Join(dirs[i], readinessSocketName)
			c := &SocketReadinessChecker{Addresser: &fakeAddresser{path: sockPath}}
			type res struct {
				a   int
				err error
			}
			rc := make(chan res, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				a, err := c.WaitReady(ctx, "inst-stress")
				rc <- res{a, err}
			}()
			time.Sleep(time.Duration(i%8) * 25 * time.Millisecond)
			l, err := net.Listen("unix", sockPath)
			if err != nil {
				errs[i] = err
				r := <-rc // ctx limita; nunca hang eterno
				atts[i] = r.a
				if r.err == nil {
					errs[i] = nil
				}
				return
			}
			defer l.Close()
			r := <-rc
			atts[i] = r.a
			errs[i] = r.err
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Errorf("watcher %d falhou: %v (attempts=%d)", i, errs[i], atts[i])
			continue
		}
		if atts[i] < 1 || atts[i] > maxReadinessDials {
			t.Errorf("watcher %d: attempts = %d, esperado 1..%d", i, atts[i], maxReadinessDials)
		}
	}
}

// TestWaitReady_EventSocketNameLiteral pins the contract of the duplicated
// constant: the name of the socket in the node API is the literal
// "handle.sock", without importing the container runtime package. The
// duplication is documented in the constant itself.
func TestWaitReady_EventSocketNameLiteral(t *testing.T) {
	if readinessSocketName != "handle.sock" {
		t.Fatalf("readinessSocketName = %q, esperado \"handle.sock\" (must match containerruntime.socketFileName)", readinessSocketName)
	}
}

// bogusDirAddresser separates the dialable socket path from a bogus
// watch directory: adding the watch to a nonexistent directory fails, so
// the constructor takes the failure path.
type bogusDirAddresser struct {
	path string
	dir  string
}

func (b *bogusDirAddresser) SocketPath(string) string { return b.path }

func (b *bogusDirAddresser) SocketWatchDir(string) (string, error) { return b.dir, nil }

var _ InstanceAddresser = (*bogusDirAddresser)(nil)

// TestWaitReady_FallbackOrDeadlineRemoveAll: removing the watched
// directory in the middle of the wait gives a bounded return, through the
// fallback or the error of the context, with no hang and the watcher
// closed and no change in the number of descriptors.
func TestWaitReady_FallbackOrDeadlineRemoveAll(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, readinessSocketName)
	checker := &SocketReadinessChecker{Addresser: &fakeAddresser{path: sockPath}}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = os.RemoveAll(dir)
	}()
	before := countOpenFDs(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	attempts, err := checker.WaitReady(ctx, "inst-rmall")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("WaitReady com watchDir removido deveria falhar, retornou nil (attempts=%d)", attempts)
	}
	if attempts < 1 || attempts > maxReadinessDials {
		t.Errorf("attempts = %d, esperado 1..%d (fallback limitado)", attempts, maxReadinessDials)
	}
	if elapsed > 2500*time.Millisecond {
		t.Errorf("elapsed = %v, esperado limitado ≪ ctx 3s (hang?)", elapsed)
	}
	assertNoFdLeak(t, before)
	t.Logf("removeall: attempts=%d err=%v elapsed=%v", attempts, err, elapsed)
}

// TestWaitReady_FallbackOrDeadlineInvalidated: the invalidation of the
// watch, produced by a real removal of the watched directory, gives a
// single fallback with no hang and with the contractual log line.
func TestWaitReady_FallbackOrDeadlineInvalidated(t *testing.T) {
	logBuf := captureNodeLog(t)
	dir := t.TempDir()
	sockPath := filepath.Join(dir, readinessSocketName)
	checker := &SocketReadinessChecker{Addresser: &fakeAddresser{path: sockPath}}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = os.RemoveAll(dir)
	}()
	before := countOpenFDs(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempts, err := checker.WaitReady(ctx, "inst-inval")
	if err == nil {
		t.Fatalf("WaitReady com watch invalidado deveria falhar, retornou nil (attempts=%d)", attempts)
	}
	// The immediate attempt, the check and two attempts of the
	// fallback up to the ceiling. The fallback completes in about
	// 150 milliseconds, far below the five-second context, so the
	// result is deterministic.
	if attempts != maxReadinessDials {
		t.Errorf("attempts = %d, esperado %d (fallback até o teto)", attempts, maxReadinessDials)
	}
	if got := logBuf.String(); !strings.Contains(got, "nodeapi: inotify unavailable (") {
		t.Errorf("log sem a linha contratada `nodeapi: inotify unavailable (`; obtido %q", got)
	}
	assertNoFdLeak(t, before)
	t.Logf("invalidated: attempts=%d err=%v", attempts, err)
}

// TestWaitReady_FallbackOrDeadlineBadWatchDir: a failed constructor, with
// a nonexistent watch directory given by a false addresser, falls back to
// the bounded wait, which resolves when the socket appears, with the
// contractual log line.
func TestWaitReady_FallbackOrDeadlineBadWatchDir(t *testing.T) {
	logBuf := captureNodeLog(t)
	dir := t.TempDir()
	sockPath := filepath.Join(dir, readinessSocketName)
	addr := &bogusDirAddresser{path: sockPath, dir: filepath.Join(dir, "no-such-dir")}
	checker := &SocketReadinessChecker{Addresser: addr}
	go func() {
		time.Sleep(120 * time.Millisecond)
		l, err := net.Listen("unix", sockPath)
		if err != nil {
			t.Errorf("listen unix falhou: %v", err)
			return
		}
		defer l.Close()
		time.Sleep(4 * time.Second)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	attempts, err := checker.WaitReady(ctx, "inst-baddir")
	if err != nil {
		t.Fatalf("WaitReady via fallback deveria resolver: %v (attempts=%d)", err, attempts)
	}
	if attempts < 2 || attempts > maxReadinessDials {
		t.Errorf("attempts = %d, esperado 2..%d (1 imediato + dials do fallback)", attempts, maxReadinessDials)
	}
	if got := logBuf.String(); !strings.Contains(got, "nodeapi: inotify unavailable (") {
		t.Errorf("log sem a linha contratada `nodeapi: inotify unavailable (`; obtido %q", got)
	}
	t.Logf("baddir-fallback: attempts=%d", attempts)
}

// TestWaitReady_FallbackOrDeadlineTraversalGate: a watch directory that
// escapes the base or is empty is refused by the real adapter with a
// fail-closed error, and the wait returns before any watcher is created,
// with no change in the number of descriptors.
func TestWaitReady_FallbackOrDeadlineTraversalGate(t *testing.T) {
	root := t.TempDir()
	pa := ctr.NewPoolAdapterWithSocketRoot(nil, root)
	for _, id := range []string{"../../x", ""} {
		if _, err := pa.SocketWatchDir(id); err == nil {
			t.Errorf("SocketWatchDir(%q) deveria falhar (fail-closed), retornou nil", id)
		}
	}
	before := countOpenFDs(t)
	checker := &SocketReadinessChecker{Addresser: pa}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	attempts, err := checker.WaitReady(ctx, "../../x")
	if err == nil {
		t.Fatalf("WaitReady com traversal deveria falhar, retornou nil (attempts=%d)", attempts)
	}
	assertNoFdLeak(t, before)
	t.Logf("traversal: attempts=%d err=%v", attempts, err)
}

// TestWaitReady_FallbackOrDeadlineCancelMidRead: a context cancelled in
// the middle of the read gives the error of the context and releases the
// descriptors, with no change in their count within the settling window.
func TestWaitReady_FallbackOrDeadlineCancelMidRead(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, readinessSocketName)
	checker := &SocketReadinessChecker{Addresser: &fakeAddresser{path: sockPath}}
	before := countOpenFDs(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	attempts, err := checker.WaitReady(ctx, "inst-cancel")
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("esperado context.Canceled, obtido %v (attempts=%d, elapsed=%v)", err, attempts, elapsed)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, esperado 2 (imediato + check, sem dial pós-cancel)", attempts)
	}
	if elapsed > 2*time.Second {
		t.Errorf("elapsed = %v, esperado ≈100ms (poll não acordou no cancel?)", elapsed)
	}
	assertNoFdLeak(t, before)
}
