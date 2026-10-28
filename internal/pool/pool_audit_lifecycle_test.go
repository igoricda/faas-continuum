package pool

// Regression tests for the instance lifecycle: partial failure, queue
// wakeup on removal, index consistency and lost wakeups.
//
// Each test exercises real concurrency with goroutines and barriers, and
// runs under the race detector.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

// failTerminateProvisioner provisions everything and fails the termination
// of one configured identifier only, which exposes the window of a partial
// failure during removal.
type failTerminateProvisioner struct {
	mu              sync.Mutex
	provisioned     []string
	failTerminateOn string
}

func (f *failTerminateProvisioner) Provision(_ context.Context, instanceID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.provisioned = append(f.provisioned, instanceID)
	return nil
}

func (f *failTerminateProvisioner) Terminate(_ context.Context, instanceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if instanceID == f.failTerminateOn {
		return errors.New("Terminate simulado")
	}
	for i, id := range f.provisioned {
		if id == instanceID {
			f.provisioned = append(f.provisioned[:i], f.provisioned[i+1:]...)
			return nil
		}
	}
	return errors.New("instância não encontrada")
}

// indexedLocked returns a copy of the global index of instance identifiers.
func indexedLocked(m *Manager) map[string]struct{} {
	m.globalMu.RLock()
	defer m.globalMu.RUnlock()
	out := make(map[string]struct{}, len(m.instanceByID))
	for id := range m.instanceByID {
		out[id] = struct{}{}
	}
	return out
}

// esperaFila waits until the queue of the function holds n waiters, a
// deterministic barrier that avoids an arbitrary sleep before the teardown
// or the cancellation.
func esperaFila(t *testing.T, m *Manager, fn string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for m.QueueLen(fn) != n {
		if time.Now().After(deadline) {
			t.Fatalf("fila de %q = %d, esperado %d (timeout)", fn, m.QueueLen(fn), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// A partial failure must not leave phantom slots behind
// ---------------------------------------------------------------------------

// The second provisioning fails, so the instances after it would stay marked
// busy forever: the count would be inflated, the reaper could not collect
// them, the next index would skip ahead and the minimum would never be
// reached without a restart.
func TestAUD04_EnsureMinInstancesRollsBackAllUnprovisioned(t *testing.T) {
	fp := &fakeProvisioner{failOn: "fn-1"}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "fn", "img", 5); err == nil {
		t.Fatal("esperava erro de Provision em fn-1")
	}
	// Only the instance that was already provisioned stays, and it is free.
	if got := m.Count("fn"); got != 1 {
		t.Fatalf("Count() = %d, esperado 1 (só fn-0 provisionada)", got)
	}
	if ids := m.Instances("fn"); len(ids) != 1 || ids[0] != "fn-0" {
		t.Fatalf("Instances() = %v, esperado [fn-0]", ids)
	}
	if idx := indexedLocked(m); len(idx) != 1 {
		t.Fatalf("índice global com %d IDs, esperado 1 (fn-0)", len(idx))
	} else if _, ok := idx["fn-0"]; !ok {
		t.Fatalf("índice global = %v, esperado conter fn-0", idx)
	}
	// The instance is free and can be acquired without a new provisioning.
	provAntes := len(fp.provisioned)
	id, err := m.AcquireWait(ctx, "fn", "img", 0)
	if err != nil {
		t.Fatalf("AcquireWait() após rollback: %v", err)
	}
	if id != "fn-0" {
		t.Fatalf("AcquireWait() = %q, esperado fn-0 reaproveitada", id)
	}
	if len(fp.provisioned) != provAntes {
		t.Fatal("AcquireWait() reprovisionou em vez de reusar fn-0")
	}
	m.Release("fn", id)

	// The minimum is reached on the next attempt, with no restart and no
	// skipped identifier.
	fp.failOn = ""
	if err := m.EnsureMinInstances(ctx, "fn", "img", 5); err != nil {
		t.Fatalf("retry do piso: %v", err)
	}
	if got := m.Count("fn"); got != 5 {
		t.Fatalf("Count() após retry = %d, esperado 5", got)
	}
	ids := m.Instances("fn")
	vistos := map[string]bool{}
	for _, got := range ids {
		if vistos[got] {
			t.Fatalf("ID duplicado %q após retry: %v", got, ids)
		}
		vistos[got] = true
	}
	for i := 0; i < 5; i++ {
		want := "fn-" + string(rune('0'+i))
		if !vistos[want] {
			t.Fatalf("IDs após retry = %v, faltando %q (nextIndex pulou fantasma)", ids, want)
		}
	}
}

// A failure on the first instance must leave nothing behind, neither in the
// shard nor in the index.
func TestAUD04_EnsureMinInstancesFailFirstLeavesNothing(t *testing.T) {
	fp := &fakeProvisioner{failOn: "fn-0"}
	m := NewManagerWithQueueCap(fp, nil)

	if err := m.EnsureMinInstances(context.Background(), "fn", "img", 3); err == nil {
		t.Fatal("esperava erro de Provision em fn-0")
	}
	if got := m.Count("fn"); got != 0 {
		t.Fatalf("Count() = %d, esperado 0", got)
	}
	if idx := indexedLocked(m); len(idx) != 0 {
		t.Fatalf("índice global = %v, esperado vazio", idx)
	}
}

// The rollback runs while other goroutines read the pool state, so the race
// detector covers the readers of the count, the identifiers and the queue
// length during a partial failure.
func TestAUD04_EnsureMinInstancesRollbackUnderReaders(t *testing.T) {
	fp := &fakeProvisioner{failOn: "fn-2"}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	para := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-para:
					return
				default:
					_ = m.Count("fn")
					_ = m.Instances("fn")
					_ = m.QueueLen("fn")
				}
			}
		}()
	}
	_ = m.EnsureMinInstances(ctx, "fn", "img", 5) // falha esperada em fn-2
	close(para)
	wg.Wait()

	// Only the instances provisioned before the failure survive.
	if got := m.Count("fn"); got != 2 {
		t.Fatalf("Count() = %d, esperado 2 (fn-0, fn-1)", got)
	}
}

// ---------------------------------------------------------------------------
// Removing a function wakes the queue, so no waiter hangs
// ---------------------------------------------------------------------------

// The function is removed while the queue is full: the waiters must receive
// the teardown error immediately instead of waiting for a timeout.
func TestAUD09_RemoveFunctionWakesWaitersWithError(t *testing.T) {
	bp := &blockingCountingProvisioner{block: make(chan struct{})}
	m := NewManagerWithQueueCap(bp, nil)
	ctx := context.Background()

	// The provisioning blocks, so the waiters queue up behind it.
	pRes := make(chan error, 1)
	go func() {
		_, err := m.AcquireWait(ctx, "fn", "img", 0)
		pRes <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt32(&bp.calls) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("P nunca entrou em Provision")
		}
		time.Sleep(time.Millisecond)
	}

	const waiters = 8
	resCh := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			_, err := m.AcquireWait(ctx, "fn", "img", 0)
			resCh <- err
		}()
	}
	esperaFila(t, m, "fn", waiters)

	rmErr := make(chan error, 1)
	go func() { rmErr <- m.RemoveFunction(ctx, "fn") }()

	// Every waiter fails fast with the teardown error, and none of them
	// stays pending until the gate opens.
	timeout := time.After(5 * time.Second)
	for i := 0; i < waiters; i++ {
		select {
		case err := <-resCh:
			if err == nil {
				t.Fatal("waiter servido após RemoveFunction (esperava erro de teardown)")
			}
			if !strings.HasPrefix(err.Error(), "pool:") {
				t.Fatalf("erro sem prefixo de pacote (PROJECT §5): %v", err)
			}
			if !strings.Contains(err.Error(), "removida") {
				t.Fatalf("erro não é de teardown: %v", err)
			}
		case <-timeout:
			t.Fatalf("waiter %d/%d pendurado após RemoveFunction (AUD-09)", i+1, waiters)
		}
	}
	select {
	case err := <-rmErr:
		if err != nil {
			t.Fatalf("RemoveFunction() com Terminate ok: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RemoveFunction() travou")
	}
	if got := m.QueueLen("fn"); got != 0 {
		t.Fatalf("QueueLen() = %d após teardown, esperado 0", got)
	}

	// The provisioning is drained and returns an obsolete identifier, since
	// the container left the accounting when the teardown terminated it.
	close(bp.block)
	select {
	case <-pRes:
	case <-time.After(5 * time.Second):
		t.Fatal("P não retornou após liberar o gate")
	}
}

// ---------------------------------------------------------------------------
// The global index keeps an identifier until its termination succeeded
// ---------------------------------------------------------------------------

// The termination of the first instance fails: the identifier must stay in
// the index, otherwise the next acquisition would reuse it and create a
// duplicate.
func TestAUD10_RemoveFunctionKeepsIDReservationOnTerminateFailure(t *testing.T) {
	fp := &failTerminateProvisioner{failTerminateOn: "fn-0"}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "fn", "img", 2); err != nil {
		t.Fatalf("piso inicial: %v", err)
	}
	if err := m.RemoveFunction(ctx, "fn"); err == nil {
		t.Fatal("esperava erro agregado de Terminate em fn-0")
	}
	// The shard is empty, since the instances being torn down cannot serve,
	if got := m.Count("fn"); got != 0 {
		t.Fatalf("Count() = %d após RemoveFunction, esperado 0", got)
	}
	// but the leftover instance keeps its identifier in the global index.
	idx := indexedLocked(m)
	if _, ok := idx["fn-0"]; !ok {
		t.Fatalf("fn-0 saiu do índice global após Terminate falho (colisão futura): %v", idx)
	}
	if _, ok := idx["fn-1"]; ok {
		t.Fatalf("fn-1 (Terminate ok) segue no índice global: %v", idx)
	}

	// The next acquisition does not reuse that identifier.
	if err := m.EnsureMinInstances(ctx, "fn", "img", 2); err != nil {
		t.Fatalf("repiso: %v", err)
	}
	for _, id := range m.Instances("fn") {
		if id == "fn-0" {
			t.Fatalf("fn-0 reutilizada sobre o zombie: %v", m.Instances("fn"))
		}
	}
	if got := m.Count("fn"); got != 2 {
		t.Fatalf("Count() após repiso = %d, esperado 2", got)
	}
}

// ---------------------------------------------------------------------------
// A cancelled waiter forwards its signal to the next one
// ---------------------------------------------------------------------------

type aud12res struct {
	id  string
	err error
}

// tryAUD12Iteration sets up a blocked provisioning, a cancellable waiter and
// a second waiter in the queue, releases a slot and cancels the first waiter
// while it holds the signal. It returns whether the first waiter took the
// cancellation branch with a buffered signal, which is the interleaving that
// lost the wakeup, and whether the second waiter was served by the
// forwarded signal without a new provisioning.
func tryAUD12Iteration(t *testing.T) (observado bool, ok bool) {
	p := &pendingBlockingProvisioner{blockOn: "fn-1", started: make(chan struct{}), block: make(chan struct{})}
	m := NewManagerWithQueueCap(p, nil)
	ctx := context.Background()

	a, err := m.AcquireWait(ctx, "fn", "img", 0) // fn-0, livre
	if err != nil {
		t.Fatalf("setup Acquire: %v", err)
	}
	pRes := make(chan aud12res, 1)
	go func() {
		id, err := m.AcquireWait(ctx, "fn", "img", 0) // trava em fn-1
		pRes <- aud12res{id, err}
	}()
	select {
	case <-p.started:
	case <-time.After(5 * time.Second):
		t.Fatal("P nunca entrou em Provision")
	}

	w1ctx, cancel := context.WithCancel(ctx)
	w1res := make(chan aud12res, 1)
	go func() {
		id, err := m.AcquireWait(w1ctx, "fn", "img", 0)
		if err == nil {
			m.Release("fn", id)
		}
		w1res <- aud12res{id, err}
	}()
	esperaFila(t, m, "fn", 1)
	w2res := make(chan aud12res, 1)
	go func() {
		id, err := m.AcquireWait(ctx, "fn", "img", 0)
		w2res <- aud12res{id, err}
	}()
	esperaFila(t, m, "fn", 2)

	// The order matters: the cancellation happens before the release and
	// without yielding, so the first waiter is still parked in the select
	// when the signal arrives. The select sees both the channel and the
	// context ready and picks one at random, so a release first would wake
	// that waiter, which would then consume the signal and never exercise
	// the cancellation branch.
	cancel()
	m.Release("fn", a) // libera fn-0 + sinal bufferizado p/ W1 (pop FIFO)

	select {
	case r := <-w1res:
		if r.err == nil {
			// The waiter consumed the signal, which is the regular success
			// path, so the iteration is retried: this interleaving does not
			// exercise the lost wakeup.
			close(p.block)
			select {
			case <-w2res:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup: W2 não resolveu")
			}
			select {
			case <-pRes:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup: P não retornou")
			}
			return false, true
		}
	case <-time.After(5 * time.Second):
		t.Fatal("W1 não retornou após cancel")
	}

	// The waiter took the cancellation branch with a buffered signal: the
	// second waiter takes the free slot without a new provisioning, while
	// without the forwarding it would hang until the timeout.
	select {
	case r := <-w2res:
		if r.err != nil {
			t.Fatalf("W2 errou após repasse: %v", r.err)
		}
		if r.id != "fn-0" {
			t.Fatalf("W2 = %q, esperado fn-0 via repasse (sem novo Provision)", r.id)
		}
		m.Release("fn", r.id)
	case <-time.After(3 * time.Second):
		t.Fatal("W2 pendurado com slot livre (lost wakeup, AUD-12)")
	}
	close(p.block)
	select {
	case <-pRes:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup: P não retornou")
	}
	return true, true
}

func TestAUD12_CancelledWaiterForwardsSignalToNext(t *testing.T) {
	const maxIter = 50
	for i := 0; i < maxIter; i++ {
		observado, _ := tryAUD12Iteration(t)
		if observado {
			return // interleaving exercitado e repasse confirmado
		}
	}
	t.Fatalf("interleaving sinal-vs-cancel nunca ocorreu em %d iterações", maxIter)
}
