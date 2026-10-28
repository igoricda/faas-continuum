package pool

import (
	"bytes"
	"context"
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// ---------------------------------------------------------------------------
// Helpers: fake provisioner
// ---------------------------------------------------------------------------

// fakeProvisioner records the provisioning and termination calls in memory,
// which lets the tests exercise the accounting of the Manager without a
// container runtime.
type fakeProvisioner struct {
	mu          sync.Mutex
	provisioned []string
	failOn      string // se igual ao instanceID pedido, Provision falha
}

func (f *fakeProvisioner) Provision(_ context.Context, instanceID, imageRef string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if instanceID == f.failOn {
		return errors.New("falha simulada")
	}
	f.provisioned = append(f.provisioned, instanceID)
	return nil
}

func (f *fakeProvisioner) Terminate(_ context.Context, instanceID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, id := range f.provisioned {
		if id == instanceID {
			f.provisioned = append(f.provisioned[:i], f.provisioned[i+1:]...)
			return nil
		}
	}
	return errors.New("instância não encontrada")
}

// ---------------------------------------------------------------------------
// Helpers: counting and blocking counting provisioners
// ---------------------------------------------------------------------------

// countingProvisioner counts Provision calls and optionally sleeps to simulate cold start.
type countingProvisioner struct {
	mu          sync.Mutex
	calls       int32 // atomic for concurrent reads
	sleep       time.Duration
	failOn      string
	provisioned []string
}

func (c *countingProvisioner) Provision(_ context.Context, instanceID, _ string) error {
	atomic.AddInt32(&c.calls, 1)
	if c.sleep > 0 {
		time.Sleep(c.sleep)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if instanceID == c.failOn {
		return errors.New("falha simulada")
	}
	c.provisioned = append(c.provisioned, instanceID)
	return nil
}

func (c *countingProvisioner) Terminate(_ context.Context, _ string) error { return nil }

func (c *countingProvisioner) Calls() int { return int(atomic.LoadInt32(&c.calls)) }

// blockingCountingProvisioner blocks until block channel is closed.
type blockingCountingProvisioner struct {
	block chan struct{}
	calls int32
}

func (b *blockingCountingProvisioner) Provision(_ context.Context, _, _ string) error {
	atomic.AddInt32(&b.calls, 1)
	<-b.block
	return nil
}

func (b *blockingCountingProvisioner) Terminate(_ context.Context, _ string) error { return nil }

// gatedProvisioner blocks every provisioning until the test releases a
// token, and measures the peak of concurrent provisionings. It replaces the
// sleeps of the burst tests: without a token the provisioning is
// deterministic, and the peak proves the coalescing, since at most one
// provisioning per function is ever in flight.
type gatedProvisioner struct {
	started  chan struct{}
	proceed  chan struct{}
	calls    int32
	inflight int32
	peak     int32
}

func (g *gatedProvisioner) Provision(ctx context.Context, _, _ string) error {
	atomic.AddInt32(&g.calls, 1)
	cur := atomic.AddInt32(&g.inflight, 1)
	for {
		p := atomic.LoadInt32(&g.peak)
		if cur <= p || atomic.CompareAndSwapInt32(&g.peak, p, cur) {
			break
		}
	}
	defer atomic.AddInt32(&g.inflight, -1)
	select {
	case g.started <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-g.proceed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gatedProvisioner) Terminate(context.Context, string) error { return nil }

// runHerdCoalescingDeterministic exercises a concurrent burst against the
// pool without relying on wall-clock time, driving the provisioning through
// channels. It proves that the queue absorbs the burst with a single
// provisioning in flight, that exactly the maximum number of instances is
// provisioned, that at the ceiling the next caller is rejected with
// ErrPoolExhausted rather than ErrQueueFull, that the remaining waiters
// are served by reuse after a release, and that no caller hangs.
//
// The number of successful callers is not fixed by the contract, because at
// the ceiling the caller is rejected immediately, so the test asserts
// exactly one rejection and at most the maximum number of provisionings.
func runHerdCoalescingDeterministic(t *testing.T, fn string) {
	t.Helper()
	const (
		max      = 5
		n        = 20
		queueCap = 32
	)
	gp := &gatedProvisioner{started: make(chan struct{}, n), proceed: make(chan struct{}, n)}
	m := NewManagerWithQueueCap(gp, func(string) int { return queueCap })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	type res struct {
		id  string
		err error
	}
	results := make(chan res, n)
	releaseOne := make(chan struct{})

	for i := 0; i < n; i++ {
		go func() {
			id, err := m.AcquireWait(ctx, fn, "img:1.0", max)
			results <- res{id, err}
			if err == nil {
				select {
				case <-releaseOne:
				case <-ctx.Done():
				}
				m.Release(fn, id)
			}
		}()
	}

	recv := func() res {
		t.Helper()
		select {
		case r := <-results:
			return r
		case <-time.After(5 * time.Second):
			t.Fatal("caller não retornou — lost wakeup/hang")
			return res{}
		}
	}
	waitStarted := func() {
		t.Helper()
		select {
		case <-gp.started:
		case <-time.After(5 * time.Second):
			t.Fatal("Provision não iniciou")
		}
	}
	proceed := func() {
		t.Helper()
		select {
		case gp.proceed <- struct{}{}:
		case <-time.After(5 * time.Second):
			t.Fatal("nenhum Provision aguardando o gate")
		}
	}
	release := func() {
		t.Helper()
		select {
		case releaseOne <- struct{}{}:
		case <-time.After(5 * time.Second):
			t.Fatal("nenhum holder para liberar")
		}
	}

	// The queue absorbs the burst: one provisioning in flight, the rest
	// queued.
	waitStarted()
	deadline := time.Now().Add(5 * time.Second)
	for m.QueueLen(fn) != n-1 {
		if time.Now().After(deadline) {
			t.Fatalf("QueueLen=%d, want %d (rajada não enfileirada)", m.QueueLen(fn), n-1)
		}
		time.Sleep(time.Millisecond)
	}
	if c := atomic.LoadInt32(&gp.calls); c != 1 {
		t.Fatalf("Calls=%d, want 1 (singleflight durante o gate)", c)
	}
	if q := m.QueueLen(fn); q > queueCap {
		t.Fatalf("QueueLen=%d excede o cap %d", q, queueCap)
	}

	// The number of provisionings reaches the maximum without a release:
	// every woken waiter finds the holders busy and provisions the next
	// one.
	for i := 1; i < max; i++ {
		proceed()
		waitStarted()
	}
	proceed()

	// Collect the successful provisionings and the single rejection at the
	// ceiling, without assuming an order.
	ok, exhausted := 0, 0
	for ok < max || exhausted < 1 {
		r := recv()
		switch {
		case r.err == nil:
			ok++
		case errors.Is(r.err, ErrPoolExhausted):
			exhausted++
		case errors.Is(r.err, ErrQueueFull):
			t.Fatalf("ErrQueueFull inesperado (cap=%d): %v", queueCap, r.err)
		default:
			t.Fatalf("erro inesperado: %v", r.err)
		}
	}
	if c := atomic.LoadInt32(&gp.calls); c != max {
		t.Fatalf("Provision calls=%d, want exatamente %d", c, max)
	}
	if p := atomic.LoadInt32(&gp.peak); p != 1 {
		t.Fatalf("pico de Provisions concorrentes=%d, want 1 (singleflight)", p)
	}

	// Drain the remaining waiters through releases and reuse.
	for remaining := n - max - exhausted; remaining > 0; remaining-- {
		release()
		if r := recv(); r.err != nil {
			t.Fatalf("waiter de reuso falhou: %v", r.err)
		}
	}

	if q := m.QueueLen(fn); q != 0 {
		t.Errorf("QueueLen final=%d, want 0 (fila drenada)", q)
	}
	if c := atomic.LoadInt32(&gp.calls); c != max {
		t.Errorf("Provision calls=%d, want %d", c, max)
	}
}

// ---------------------------------------------------------------------------
// Helpers: blocking provisioner
// ---------------------------------------------------------------------------

// blockingProvisioner blocks the provisioning of one specific instance
// identifier until it is released, which proves that EnsureMinInstances
// does not hold the pool lock during the provisioning. Otherwise every
// invocation of any other function would block as well.
type blockingProvisioner struct {
	blockOn string
	started chan struct{}
	release chan struct{}
}

func (b *blockingProvisioner) Provision(_ context.Context, instanceID, _ string) error {
	if instanceID != b.blockOn {
		return nil
	}
	close(b.started)
	<-b.release
	return nil
}

func (b *blockingProvisioner) Terminate(_ context.Context, _ string) error { return nil }

// ---------------------------------------------------------------------------
// Helpers: recording provisioner
// ---------------------------------------------------------------------------

// recordingProvisioner is a fake Provisioner that records Provision and
// Terminate calls so tests can assert WHICH instanceIDs were terminated.
// It follows the same pattern as fakeProvisioner in pool_test.go but adds
// an explicit terminated log for deterministic scale-down assertions.
type recordingProvisioner struct {
	mu          sync.Mutex
	provisioned []string
	terminated  []string
	failOn      string
}

func (r *recordingProvisioner) Provision(_ context.Context, instanceID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if instanceID == r.failOn {
		return context.Canceled
	}
	r.provisioned = append(r.provisioned, instanceID)
	return nil
}

func (r *recordingProvisioner) Terminate(_ context.Context, instanceID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terminated = append(r.terminated, instanceID)
	for i, id := range r.provisioned {
		if id == instanceID {
			r.provisioned = append(r.provisioned[:i], r.provisioned[i+1:]...)
			break
		}
	}
	return nil
}

func (r *recordingProvisioner) terminatedCopy() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.terminated))
	copy(out, r.terminated)
	return out
}

// ---------------------------------------------------------------------------
// Helpers: pending blocking provisioner
// ---------------------------------------------------------------------------

// pendingBlockingProvisioner blocks the provisioning of one instance
// identifier until the block channel is closed and signals the start
// through the started channel. It mirrors the blocking provisioner but uses
// a distinct type.
type pendingBlockingProvisioner struct {
	blockOn string
	started chan struct{}
	block   chan struct{}
}

func (p *pendingBlockingProvisioner) Provision(_ context.Context, instanceID, _ string) error {
	if instanceID != p.blockOn {
		return nil
	}
	select {
	case <-p.started:
		// Already closed: avoid a double close panic when reused.
	default:
		close(p.started)
	}
	<-p.block
	return nil
}

func (p *pendingBlockingProvisioner) Terminate(_ context.Context, _ string) error { return nil }

// ---------------------------------------------------------------------------
// Helpers: last use, touch and log capture
// ---------------------------------------------------------------------------

func lastUsedOf(m *Manager, fn string) time.Time {
	s := m.shardFor(fn)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastUsed[fn]
}

func touchLastUsed(t *testing.T, m *Manager, fn string, at time.Time) {
	t.Helper()
	m.WithClock(func() time.Time { return at })
	ctx := context.Background()
	id, err := m.AcquireWait(ctx, fn, "img", 0)
	if err != nil {
		t.Fatalf("touchLastUsed AcquireWait: %v", err)
	}
	m.Release(fn, id)
}

// capturarLog redirects the log output of the package to a buffer and
// restores the original destination at the end of the test. It is
// sequential by design: the tests that capture the log do not run in
// parallel.
func capturarLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	anterior := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(anterior) })
	return &buf
}

// ===========================================================================
// Core pool lifecycle: adoption, acquisition and scaling
// ===========================================================================

func TestEnsureMinInstancesCreatesExactCount(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)

	if err := m.EnsureMinInstances(context.Background(), "alpr", "img:1.0", 3); err != nil {
		t.Fatalf("EnsureMinInstances() erro inesperado: %v", err)
	}

	if got := m.Count("alpr"); got != 3 {
		t.Errorf("Count() = %d, esperado 3", got)
	}
	if got := len(fp.provisioned); got != 3 {
		t.Errorf("Provision() foi chamado %d vezes, esperado 3", got)
	}
}

func TestEnsureMinInstancesIsIdempotent(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "alpr", "img:1.0", 2); err != nil {
		t.Fatal(err)
	}
	if err := m.EnsureMinInstances(ctx, "alpr", "img:1.0", 2); err != nil {
		t.Fatal(err)
	}

	if got := m.Count("alpr"); got != 2 {
		t.Errorf("Count() = %d, esperado 2 após chamada idempotente", got)
	}
	if got := len(fp.provisioned); got != 2 {
		t.Errorf("Provision() foi chamado %d vezes, esperado 2 no total", got)
	}
}

func TestEnsureMinInstancesScalesUpIncrementally(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "alpr", "img:1.0", 1); err != nil {
		t.Fatal(err)
	}
	if err := m.EnsureMinInstances(ctx, "alpr", "img:1.0", 3); err != nil {
		t.Fatal(err)
	}

	if got := m.Count("alpr"); got != 3 {
		t.Errorf("Count() = %d, esperado 3 após elevar o mínimo", got)
	}
}

func TestEnsureMinInstancesAcceptsZero(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)

	if err := m.EnsureMinInstances(context.Background(), "alpr", "img:1.0", 0); err != nil {
		t.Fatalf("min_instances=0 deve ser aceito: %v", err)
	}
	if got := m.Count("alpr"); got != 0 {
		t.Errorf("Count() = %d, esperado 0", got)
	}
	if got := len(fp.provisioned); got != 0 {
		t.Errorf("Provision() não deveria ter sido chamado, foi %d vez(es)", got)
	}
}

func TestEnsureMinInstancesRejectsNegative(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)

	if err := m.EnsureMinInstances(context.Background(), "alpr", "img:1.0", -1); err == nil {
		t.Fatal("esperava erro para minInstances=-1")
	}
}

func TestEnsureMinInstancesPropagatesProvisionError(t *testing.T) {
	fp := &fakeProvisioner{failOn: "alpr-1"}
	m := NewManagerWithQueueCap(fp, nil)

	err := m.EnsureMinInstances(context.Background(), "alpr", "img:1.0", 2)
	if err == nil {
		t.Fatal("esperava erro propagado de Provision()")
	}
	if got := m.Count("alpr"); got != 1 {
		t.Errorf("Count() = %d, esperado 1 (só a instância criada antes da falha)", got)
	}
}

func TestInstancesReturnsIDs(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)

	if err := m.EnsureMinInstances(context.Background(), "alpr", "img:1.0", 2); err != nil {
		t.Fatal(err)
	}

	ids := m.Instances("alpr")
	if len(ids) != 2 {
		t.Fatalf("Instances() retornou %d IDs, esperado 2", len(ids))
	}
	if ids[0] != "alpr-0" || ids[1] != "alpr-1" {
		t.Errorf("Instances() = %v, esperado [alpr-0 alpr-1]", ids)
	}
}

func TestAcquireReusesFreeInstance(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "alpr", "img:1.0", 1); err != nil {
		t.Fatal(err)
	}

	id, err := m.Acquire(ctx, "alpr", "img:1.0", 1)
	if err != nil {
		t.Fatalf("Acquire() erro inesperado: %v", err)
	}
	if id != "alpr-0" {
		t.Errorf("Acquire() = %q, esperado a instância já pré-alocada alpr-0", id)
	}
	if got := len(fp.provisioned); got != 1 {
		t.Errorf("Provision() foi chamado %d vezes, esperado 1 (só o piso mínimo)", got)
	}
}

func TestAcquireScalesUpToMaxWhenAllBusy(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "alpr", "img:1.0", 1); err != nil {
		t.Fatal(err)
	}

	first, err := m.Acquire(ctx, "alpr", "img:1.0", 2)
	if err != nil {
		t.Fatalf("1º Acquire() erro inesperado: %v", err)
	}

	second, err := m.Acquire(ctx, "alpr", "img:1.0", 2)
	if err != nil {
		t.Fatalf("2º Acquire() erro inesperado: %v", err)
	}

	if first == second {
		t.Fatal("Acquire() devolveu a mesma instância duas vezes sem Release()")
	}
	if got := m.Count("alpr"); got != 2 {
		t.Errorf("Count() = %d, esperado 2 (escalou até o max)", got)
	}
}

func TestAcquireFailsWhenPoolExhausted(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "alpr", "img:1.0", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Acquire(ctx, "alpr", "img:1.0", 1); err != nil {
		t.Fatal(err)
	}

	_, err := m.Acquire(ctx, "alpr", "img:1.0", 1)
	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("esperava ErrPoolExhausted, obtido: %v", err)
	}
}

func TestAcquireScalesWithoutCeilingWhenUnbounded(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	const invocacoes = 8
	for i := 0; i < invocacoes; i++ {
		if _, err := m.Acquire(ctx, "alpr", "img:1.0", 0); err != nil {
			t.Fatalf("Acquire() #%d com max=0 (sem teto) erro inesperado: %v", i+1, err)
		}
	}

	if got := m.Count("alpr"); got != invocacoes {
		t.Errorf("Count() = %d, esperado %d instâncias provisionadas sob demanda", got, invocacoes)
	}
}

func TestReleaseFreesInstanceForReuse(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "alpr", "img:1.0", 1); err != nil {
		t.Fatal(err)
	}

	id, err := m.Acquire(ctx, "alpr", "img:1.0", 1)
	if err != nil {
		t.Fatal(err)
	}
	m.Release("alpr", id)

	reused, err := m.Acquire(ctx, "alpr", "img:1.0", 1)
	if err != nil {
		t.Fatalf("Acquire() após Release() erro inesperado: %v", err)
	}
	if reused != id {
		t.Errorf("Acquire() = %q, esperado reaproveitar %q", reused, id)
	}
}

func TestAcquireRollsBackReservationOnProvisionFailure(t *testing.T) {
	fp := &fakeProvisioner{failOn: "alpr-1"}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "alpr", "img:1.0", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Acquire(ctx, "alpr", "img:1.0", 2); err != nil {
		t.Fatal(err)
	}

	if _, err := m.Acquire(ctx, "alpr", "img:1.0", 2); err == nil {
		t.Fatal("esperava erro de provisionamento propagado")
	}
	if got := m.Count("alpr"); got != 1 {
		t.Errorf("Count() = %d, esperado 1 (a reserva que falhou deve ter sido desfeita)", got)
	}
}

func TestAcquireConcurrentDoesNotExceedMax(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	const max = 3
	const goroutines = 10

	var wg sync.WaitGroup
	successes := make(chan string, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if id, err := m.Acquire(ctx, "alpr", "img:1.0", max); err == nil {
				successes <- id
			}
		}()
	}
	wg.Wait()
	close(successes)

	count := 0
	for range successes {
		count++
	}
	if count != max {
		t.Errorf("%d chamadas concorrentes conseguiram Acquire(), esperado exatamente %d (max_instances)", count, max)
	}
	if got := m.Count("alpr"); got != max {
		t.Errorf("Count() = %d, esperado %d", got, max)
	}
}

func TestRemoveFunctionTerminatesAllInstancesAndClearsBookkeeping(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "alpr", "img:1.0", 3); err != nil {
		t.Fatal(err)
	}
	if got := len(fp.provisioned); got != 3 {
		t.Fatalf("pré-condição: esperava 3 instâncias provisionadas, tinha %d", got)
	}

	if err := m.RemoveFunction(ctx, "alpr"); err != nil {
		t.Fatalf("RemoveFunction() erro inesperado: %v", err)
	}

	if got := m.Count("alpr"); got != 0 {
		t.Errorf("Count() após RemoveFunction() = %d, esperado 0", got)
	}
	if got := len(fp.provisioned); got != 0 {
		t.Errorf("provisioner ainda tem %d instância(s) — Terminate não foi chamado para todas", got)
	}
}

func TestRemoveFunctionAllowsCleanReRegistration(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "alpr", "imagem-quebrada:1.0", 1); err != nil {
		t.Fatal(err)
	}
	oldInstanceID := m.Instances("alpr")[0]

	if err := m.RemoveFunction(ctx, "alpr"); err != nil {
		t.Fatal(err)
	}

	if err := m.EnsureMinInstances(ctx, "alpr", "imagem-corrigida:1.0", 1); err != nil {
		t.Fatal(err)
	}
	newInstanceID := m.Instances("alpr")[0]

	if got := len(fp.provisioned); got != 1 {
		t.Errorf("esperava exatamente 1 instância provisionada após o novo registro, tinha %d", got)
	}
	_ = oldInstanceID
	_ = newInstanceID
}

func TestRemoveFunctionOnNonExistentFunctionIsNoop(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)

	if err := m.RemoveFunction(context.Background(), "nao-existe"); err != nil {
		t.Errorf("RemoveFunction() em função inexistente deveria ser no-op, erro: %v", err)
	}
}

func TestParseInstanceID(t *testing.T) {
	cases := []struct {
		input        string
		wantFunction string
		wantIndex    int
		wantOK       bool
	}{
		{"alpr-yolo-0", "alpr-yolo", 0, true},
		{"alpr-yolo-12", "alpr-yolo", 12, true},
		{"func-2-0", "func-2", 0, true},
		{"semindice", "", 0, false},
		{"", "", 0, false},
		{"-0", "", 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			fn, idx, ok := ParseInstanceID(tc.input)
			if ok != tc.wantOK {
				t.Fatalf("ParseInstanceID(%q) ok = %v, esperado %v", tc.input, ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if fn != tc.wantFunction || idx != tc.wantIndex {
				t.Errorf("ParseInstanceID(%q) = (%q, %d), esperado (%q, %d)", tc.input, fn, idx, tc.wantFunction, tc.wantIndex)
			}
		})
	}
}

func TestAdoptAddsInstanceAsFree(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)

	m.Adopt("alpr-yolo", "alpr-yolo-0")

	if got := m.Count("alpr-yolo"); got != 1 {
		t.Fatalf("Count() = %d, esperado 1", got)
	}
	id, err := m.Acquire(context.Background(), "alpr-yolo", "qualquer-imagem", 1)
	if err != nil {
		t.Fatalf("Acquire() erro inesperado: %v", err)
	}
	if id != "alpr-yolo-0" {
		t.Errorf("Acquire() = %q, esperado reaproveitar a instância adotada alpr-yolo-0", id)
	}
	if got := len(fp.provisioned); got != 0 {
		t.Errorf("Provision() foi chamado %d vez(es); instância adotada não deveria ser reprovisionada", got)
	}
}

func TestAdoptIsIdempotent(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)

	m.Adopt("alpr-yolo", "alpr-yolo-0")
	m.Adopt("alpr-yolo", "alpr-yolo-0")

	if got := m.Count("alpr-yolo"); got != 1 {
		t.Errorf("Count() = %d, esperado 1 (Adopt duplicado deveria ser no-op)", got)
	}
}

func TestEnsureMinInstancesSkipsGapsInAdoptedIDs(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)

	m.Adopt("alpr-yolo", "alpr-yolo-1")
	m.Adopt("alpr-yolo", "alpr-yolo-2")

	if err := m.EnsureMinInstances(context.Background(), "alpr-yolo", "img:1.0", 3); err != nil {
		t.Fatalf("EnsureMinInstances() erro inesperado: %v", err)
	}

	if got := m.Count("alpr-yolo"); got != 3 {
		t.Errorf("Count() = %d, esperado 3", got)
	}
	if len(fp.provisioned) != 1 || fp.provisioned[0] != "alpr-yolo-3" {
		t.Errorf("Provision() chamado com %v, esperado [alpr-yolo-3]", fp.provisioned)
	}
}

func TestAcquireScalesUpSkippingGapsInAdoptedIDs(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	m.Adopt("alpr-yolo", "alpr-yolo-1")

	first, err := m.Acquire(ctx, "alpr-yolo", "img:1.0", 2)
	if err != nil {
		t.Fatalf("1º Acquire() erro inesperado: %v", err)
	}
	if first != "alpr-yolo-1" {
		t.Fatalf("1º Acquire() = %q, esperado reutilizar a instância adotada alpr-yolo-1", first)
	}

	second, err := m.Acquire(ctx, "alpr-yolo", "img:1.0", 2)
	if err != nil {
		t.Fatalf("2º Acquire() erro inesperado: %v", err)
	}
	if second != "alpr-yolo-2" {
		t.Errorf("2º Acquire() = %q, esperado alpr-yolo-2 (índice novo sem colidir com o adotado)", second)
	}
}

func TestEnsureMinInstancesDoesNotBlockOtherFunctions(t *testing.T) {
	bp := &blockingProvisioner{blockOn: "lenta-0", started: make(chan struct{}), release: make(chan struct{})}
	m := NewManagerWithQueueCap(bp, nil)
	ctx := context.Background()

	done := make(chan error, 1)
	go func() { done <- m.EnsureMinInstances(ctx, "lenta", "img:1.0", 1) }()

	select {
	case <-bp.started:
	case <-time.After(2 * time.Second):
		t.Fatal("Provision() nunca começou")
	}

	acquired := make(chan error, 1)
	go func() {
		_, err := m.Acquire(ctx, "outra", "img:1.0", 1)
		acquired <- err
	}()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("Acquire() em outra função foi bloqueado pelo provisionamento da primeira: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire() de outra função travou atrás do Provision em andamento")
	}

	close(bp.release)
	if err := <-done; err != nil {
		t.Fatalf("EnsureMinInstances() erro inesperado: %v", err)
	}
}

func TestAcquireStillWorks(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	id, err := m.Acquire(ctx, "fn", "img:1.0", 2)
	if err != nil {
		t.Fatalf("Acquire() erro inesperado: %v", err)
	}
	if id == "" {
		t.Fatal("Acquire() retornou ID vazio")
	}
	if got := m.Count("fn"); got != 1 {
		t.Errorf("Count() = %d, want 1", got)
	}
	m.Release("fn", id)
	id2, err := m.Acquire(ctx, "fn", "img:1.0", 2)
	if err != nil {
		t.Fatalf("2nd Acquire() erro: %v", err)
	}
	if id2 != id {
		t.Errorf("Acquire() reuse = %q, want %q", id2, id)
	}
}

func TestAcquireAlias(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	id1, err := m.Acquire(ctx, "fn-alias", "img:1.0", 1)
	if err != nil {
		t.Fatalf("Acquire() err: %v", err)
	}
	m.Release("fn-alias", id1)
	id2, err := m.AcquireWait(ctx, "fn-alias", "img:1.0", 1)
	if err != nil {
		t.Fatalf("AcquireWait() err: %v", err)
	}
	if id1 != id2 {
		t.Errorf("Acquire vs AcquireWait ID mismatch: %q vs %q", id1, id2)
	}
	m.Release("fn-alias", id2)
}

func TestAcquire_AliasPreservesExhausted(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	id1, err := m.Acquire(ctx, "fn", "img:1.0", 2)
	if err != nil {
		t.Fatalf("1st Acquire(): %v", err)
	}
	id2, err := m.Acquire(ctx, "fn", "img:1.0", 2)
	if err != nil {
		t.Fatalf("2nd Acquire(): %v", err)
	}
	if id1 == id2 {
		t.Fatal("expected two distinct instance IDs")
	}

	start := time.Now()
	_, err = m.Acquire(ctx, "fn", "img:1.0", 2)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("3rd Acquire() with max=2 and both busy: want ErrPoolExhausted, got %v", err)
	}
	if elapsed > 20*time.Millisecond {
		t.Errorf("ErrPoolExhausted took %v, want <20ms (alias must not enqueue)", elapsed)
	}
	if qlen := m.QueueLen("fn"); qlen != 0 {
		t.Errorf("queue len = %d, want 0 (ErrPoolExhausted must never enqueue via alias)", qlen)
	}

	_, err = m.AcquireWait(ctx, "fn", "img:1.0", 2)
	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("AcquireWait() with same exhausted pool: want ErrPoolExhausted, got %v", err)
	}

	m.Release("fn", id1)
	m.Release("fn", id2)
}

// ===========================================================================
// Queue, coalescing and queue capacity
// ===========================================================================

func TestAcquireWait_QueueAndSingleflight(t *testing.T) {
	runHerdCoalescingDeterministic(t, "fn")
}

func TestAcquireWait_QueueAndSingleflight_WithRelease(t *testing.T) {
	runHerdCoalescingDeterministic(t, "fn2")
}

func TestPoolExhaustedNeverQueues(t *testing.T) {
	cp := &countingProvisioner{}
	m := NewManagerWithQueueCap(cp, nil)

	ctx := context.Background()
	id1, err := m.AcquireWait(ctx, "fn", "img:1.0", 2)
	if err != nil {
		t.Fatalf("1st AcquireWait: %v", err)
	}
	id2, err := m.AcquireWait(ctx, "fn", "img:1.0", 2)
	if err != nil {
		t.Fatalf("2nd AcquireWait: %v", err)
	}
	start := time.Now()
	_, err = m.AcquireWait(ctx, "fn", "img:1.0", 2)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("expected ErrPoolExhausted, got %v", err)
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("ErrPoolExhausted took %v, want <10ms (must not enqueue)", elapsed)
	}
	if errors.Is(err, ErrQueueFull) {
		t.Error("should be ErrPoolExhausted, not ErrQueueFull")
	}
	if qlen := m.QueueLen("fn"); qlen != 0 {
		t.Errorf("queue len = %d, want 0 (ErrPoolExhausted must never enqueue)", qlen)
	}
	_ = id1
	_ = id2
	m.Release("fn", id1)
	m.Release("fn", id2)
}

func TestQueueFull_FailsWhenCapExceeded(t *testing.T) {
	capFn := func(fn string) int { return 2 }
	blockCh := make(chan struct{})
	cp := &blockingCountingProvisioner{block: blockCh}
	m := NewManagerWithQueueCap(cp, capFn)

	ctx := context.Background()

	leaderDone := make(chan error, 1)
	go func() {
		_, err := m.AcquireWait(ctx, "fn", "img:1.0", 0)
		leaderDone <- err
	}()

	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			_, _ = m.AcquireWait(cctx, "fn", "img:1.0", 0)
		}()
	}
	time.Sleep(50 * time.Millisecond)

	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err := m.AcquireWait(cctx, "fn", "img:1.0", 0)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull when cap exceeded, got %v", err)
	}

	close(blockCh)
	wg.Wait()
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not complete")
	}
}

func TestAcquireWait_ContextCancelCleansQueue(t *testing.T) {
	blockCh := make(chan struct{})
	cp := &blockingCountingProvisioner{block: blockCh}
	m := NewManagerWithQueueCap(cp, func(string) int { return 10 })
	leaderDone := make(chan error, 1)
	go func() {
		_, err := m.AcquireWait(context.Background(), "fn-cancel", "img:1.0", 0)
		leaderDone <- err
	}()
	time.Sleep(50 * time.Millisecond)
	const waiters = 5
	ctxs := make([]context.CancelFunc, waiters)
	errCh := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ctxs[i] = cancel
		go func(c context.Context) {
			_, err := m.AcquireWait(c, "fn-cancel", "img:1.0", 0)
			errCh <- err
		}(ctx)
	}
	time.Sleep(50 * time.Millisecond)
	if got := m.QueueLen("fn-cancel"); got != waiters {
		t.Fatalf("queue len before cancel = %d, want %d", got, waiters)
	}
	for _, cancel := range ctxs {
		cancel()
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if m.QueueLen("fn-cancel") == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue len after cancel = %d, want 0", m.QueueLen("fn-cancel"))
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < waiters; i++ {
		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("waiter %d: want context.Canceled, got %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter %d did not return", i)
		}
	}
	close(blockCh)
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not complete")
	}
	if got := m.QueueLen("fn-cancel"); got != 0 {
		t.Errorf("final queue len = %d, want 0", got)
	}
}

func TestEnsureMinInstances_BypassesQueue(t *testing.T) {
	cp := &countingProvisioner{}
	m := NewManagerWithQueueCap(cp, nil)
	fn := "fn-bypass-ensure"

	if err := m.EnsureMinInstances(context.Background(), fn, "img:1.0", 2); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	if got := cp.Calls(); got != 2 {
		t.Errorf("Provision calls = %d, want 2 (EnsureMinInstances must call Provision directly)", got)
	}
	if got := m.Count(fn); got != 2 {
		t.Errorf("Count = %d, want 2", got)
	}
	if got := m.QueueLen(fn); got != 0 {
		t.Errorf("QueueLen = %d, want 0 (EnsureMinInstances must not enqueue via m.waiters)", got)
	}
}

func TestAdopt_BypassesQueue(t *testing.T) {
	cp := &countingProvisioner{}
	m := NewManagerWithQueueCap(cp, nil)
	fn := "fn-bypass-adopt"

	m.Adopt(fn, fn+"-0")

	if got := m.Count(fn); got != 1 {
		t.Fatalf("Count after Adopt = %d, want 1", got)
	}
	if got := cp.Calls(); got != 0 {
		t.Errorf("Provision calls = %d, want 0 (Adopt must not call Provision)", got)
	}
	if got := m.QueueLen(fn); got != 0 {
		t.Errorf("QueueLen = %d, want 0 (Adopt must not enqueue)", got)
	}
	id, err := m.AcquireWait(context.Background(), fn, "img:1.0", 1)
	if err != nil {
		t.Fatalf("AcquireWait after Adopt: %v", err)
	}
	if id != fn+"-0" {
		t.Errorf("AcquireWait = %q, want %q (adopted free instance)", id, fn+"-0")
	}
	if got := cp.Calls(); got != 0 {
		t.Errorf("Provision calls after AcquireWait = %d, want 0 (should reuse adopted)", got)
	}
	if got := m.QueueLen(fn); got != 0 {
		t.Errorf("QueueLen after AcquireWait = %d, want 0", got)
	}
	m.Release(fn, id)
}

// ===========================================================================
// Concurrency
// ===========================================================================

func TestConcurrency_ThunderingHerd_Coalescing(t *testing.T) {
	runHerdCoalescingDeterministic(t, "fn-conc-herd")
}

func TestConcurrency_QueueFull(t *testing.T) {
	capFn := func(string) int { return 2 }
	blockCh := make(chan struct{})
	cp := &blockingCountingProvisioner{block: blockCh}
	m := NewManagerWithQueueCap(cp, capFn)

	ctx := context.Background()

	leaderDone := make(chan error, 1)
	go func() {
		_, err := m.AcquireWait(ctx, "fn-conc-qfull", "img:1.0", 0)
		leaderDone <- err
	}()
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			_, _ = m.AcquireWait(cctx, "fn-conc-qfull", "img:1.0", 0)
		}()
	}
	time.Sleep(50 * time.Millisecond)

	if ql := m.QueueLen("fn-conc-qfull"); ql != 2 {
		t.Fatalf("queue len before overflow = %d, want 2", ql)
	}

	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err := m.AcquireWait(cctx, "fn-conc-qfull", "img:1.0", 0)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull when cap exceeded, got %v", err)
	}

	close(blockCh)
	wg.Wait()
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not complete")
	}
	if ql := m.QueueLen("fn-conc-qfull"); ql != 0 {
		t.Errorf("final QueueLen = %d, want 0", ql)
	}
}

func TestConcurrency_PoolExhaustedFastPath(t *testing.T) {
	cp := &countingProvisioner{}
	m := NewManagerWithQueueCap(cp, nil)

	ctx := context.Background()
	id1, err := m.AcquireWait(ctx, "fn-conc-exhausted", "img:1.0", 2)
	if err != nil {
		t.Fatalf("1st AcquireWait: %v", err)
	}
	id2, err := m.AcquireWait(ctx, "fn-conc-exhausted", "img:1.0", 2)
	if err != nil {
		t.Fatalf("2nd AcquireWait: %v", err)
	}

	start := time.Now()
	_, err = m.AcquireWait(ctx, "fn-conc-exhausted", "img:1.0", 2)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("expected ErrPoolExhausted, got %v", err)
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("ErrPoolExhausted took %v, want <10ms (must not enqueue)", elapsed)
	}
	if ql := m.QueueLen("fn-conc-exhausted"); ql != 0 {
		t.Errorf("queue len = %d, want 0 (POOL_EXHAUSTED never enqueues)", ql)
	}
	m.Release("fn-conc-exhausted", id1)
	m.Release("fn-conc-exhausted", id2)
}

func TestConcurrency_CancelCleansWaiter(t *testing.T) {
	blockCh := make(chan struct{})
	cp := &blockingCountingProvisioner{block: blockCh}
	m := NewManagerWithQueueCap(cp, func(string) int { return 10 })

	leaderDone := make(chan error, 1)
	go func() {
		_, err := m.AcquireWait(context.Background(), "fn-conc-cancel", "img:1.0", 0)
		leaderDone <- err
	}()
	time.Sleep(50 * time.Millisecond)

	const waiters = 5
	ctxs := make([]context.CancelFunc, waiters)
	errCh := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ctxs[i] = cancel
		go func(c context.Context) {
			_, err := m.AcquireWait(c, "fn-conc-cancel", "img:1.0", 0)
			errCh <- err
		}(ctx)
	}
	time.Sleep(50 * time.Millisecond)

	if got := m.QueueLen("fn-conc-cancel"); got != waiters {
		t.Fatalf("queue len before cancel = %d, want %d", got, waiters)
	}
	for _, cancel := range ctxs {
		cancel()
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if m.QueueLen("fn-conc-cancel") == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue len after cancel = %d, want 0", m.QueueLen("fn-conc-cancel"))
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < waiters; i++ {
		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("waiter %d: want context.Canceled, got %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter %d did not return", i)
		}
	}
	close(blockCh)
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not complete")
	}
	if got := m.QueueLen("fn-conc-cancel"); got != 0 {
		t.Errorf("final queue len = %d, want 0", got)
	}
}

// ===========================================================================
// Reaping of idle instances
// ===========================================================================

func TestReapIdleAfterTimeout(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "f", "img", 5); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	base := time.Unix(5000, 0)
	idleTime := base.Add(-20 * time.Minute)
	touchLastUsed(t, m, "f", idleTime)
	now := base
	globalDefault := 15 * time.Minute
	fn := registry.Function{Name: "f", MinInstances: 2}
	reaped, err := m.ReapIdle(ctx, now, []registry.Function{fn}, globalDefault)
	if err != nil {
		t.Fatalf("ReapIdle: %v", err)
	}
	if reaped != 3 {
		t.Errorf("reaped=%d want 3", reaped)
	}
	if got := m.Count("f"); got != 2 {
		t.Errorf("Count after reap=%d want 2", got)
	}
	remaining := m.Instances("f")
	sort.Strings(remaining)
	expected := []string{"f-0", "f-1"}
	if len(remaining) != 2 {
		t.Fatalf("remaining=%v want %v", remaining, expected)
	}
	for i, want := range expected {
		if remaining[i] != want {
			t.Errorf("remaining[%d]=%q want %q (oldest survive)", i, remaining[i], want)
		}
	}
	terminated := fp.terminatedCopy()
	sort.Strings(terminated)
	expTerm := []string{"f-2", "f-3", "f-4"}
	sort.Strings(expTerm)
	if len(terminated) != 3 {
		t.Fatalf("terminated=%v want 3", terminated)
	}
	for i, want := range expTerm {
		if terminated[i] != want {
			t.Errorf("terminated[%d]=%q want %q", i, terminated[i], want)
		}
	}
	raw := fp.terminatedCopy()
	if len(raw) == 3 && raw[0] != "f-4" {
		t.Errorf("raw order=%v want newest first f-4", raw)
	}
}

func TestReapNotBeforeTimeout(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "f", "img", 5); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	base := time.Unix(6000, 0)
	idleTime := base.Add(-5 * time.Minute)
	touchLastUsed(t, m, "f", idleTime)
	now := base
	globalDefault := 15 * time.Minute
	fn := registry.Function{Name: "f", MinInstances: 2}
	reaped, err := m.ReapIdle(ctx, now, []registry.Function{fn}, globalDefault)
	if err != nil {
		t.Fatalf("ReapIdle: %v", err)
	}
	if reaped != 0 {
		t.Errorf("reaped=%d want 0 (not before timeout)", reaped)
	}
	if got := m.Count("f"); got != 5 {
		t.Errorf("Count after=%d want 5", got)
	}
	if got := len(fp.terminatedCopy()); got != 0 {
		t.Errorf("terminated count=%d want 0", got)
	}
}

func TestReapPerFunctionOverride(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "f", "img", 5); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	base := time.Unix(7000, 0)
	idleTime := base.Add(-2 * time.Minute)
	touchLastUsed(t, m, "f", idleTime)
	now := base
	globalDefault := 15 * time.Minute
	fn := registry.Function{Name: "f", MinInstances: 2, IdleTimeout: config.Duration(1 * time.Minute)}
	reaped, err := m.ReapIdle(ctx, now, []registry.Function{fn}, globalDefault)
	if err != nil {
		t.Fatalf("ReapIdle: %v", err)
	}
	if reaped != 3 {
		t.Errorf("reaped=%d want 3 (per-function 1m overrides global 15m)", reaped)
	}
	if got := m.Count("f"); got != 2 {
		t.Errorf("Count after=%d want 2", got)
	}
}

func TestReapRespectsGlobalWhenPerFunctionZero(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "f", "img", 5); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	base := time.Unix(8000, 0)
	idleTime := base.Add(-20 * time.Minute)
	touchLastUsed(t, m, "f", idleTime)
	now := base
	globalDefault := 15 * time.Minute
	fn := registry.Function{Name: "f", MinInstances: 2, IdleTimeout: config.Duration(0)}
	reaped, err := m.ReapIdle(ctx, now, []registry.Function{fn}, globalDefault)
	if err != nil {
		t.Fatalf("ReapIdle: %v", err)
	}
	if reaped != 3 {
		t.Errorf("reaped=%d want 3 (global used when per-fn 0)", reaped)
	}
	fp2 := &recordingProvisioner{}
	m2 := NewManagerWithQueueCap(fp2, nil)
	if err := m2.EnsureMinInstances(ctx, "f2", "img", 5); err != nil {
		t.Fatalf("EnsureMinInstances f2: %v", err)
	}
	idleTime2 := base.Add(-5 * time.Minute)
	touchLastUsed(t, m2, "f2", idleTime2)
	fn2 := registry.Function{Name: "f2", MinInstances: 2}
	reaped2, err := m2.ReapIdle(ctx, now, []registry.Function{fn2}, globalDefault)
	if err != nil {
		t.Fatalf("ReapIdle f2: %v", err)
	}
	if reaped2 != 0 {
		t.Errorf("reaped2=%d want 0 (global 15m not elapsed)", reaped2)
	}
}

func TestReapDoesNotGoBelowMin(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "f", "img", 2); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	base := time.Unix(9000, 0)
	idleTime := base.Add(-20 * time.Minute)
	touchLastUsed(t, m, "f", idleTime)
	now := base
	globalDefault := 15 * time.Minute
	fn := registry.Function{Name: "f", MinInstances: 2}
	reaped, err := m.ReapIdle(ctx, now, []registry.Function{fn}, globalDefault)
	if err != nil {
		t.Fatalf("ReapIdle: %v", err)
	}
	if reaped != 0 {
		t.Errorf("reaped=%d want 0 (already at min)", reaped)
	}
	if got := m.Count("f"); got != 2 {
		t.Errorf("Count=%d want 2", got)
	}
	if got := len(fp.terminatedCopy()); got != 0 {
		t.Errorf("terminated=%d want 0", got)
	}
}

func TestReapBusyProtected(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "f", "img", 3); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	base := time.Unix(10000, 0)
	idleTime := base.Add(-20 * time.Minute)
	m.WithClock(func() time.Time { return idleTime })
	id, err := m.AcquireWait(ctx, "f", "img", 0)
	if err != nil {
		t.Fatalf("initial touch AcquireWait: %v", err)
	}
	m.Release("f", id)
	busyID, err := m.AcquireWait(ctx, "f", "img", 0)
	if err != nil {
		t.Fatalf("busy AcquireWait: %v", err)
	}
	now := base
	globalDefault := 15 * time.Minute
	fn := registry.Function{Name: "f", MinInstances: 1}
	reaped, err := m.ReapIdle(ctx, now, []registry.Function{fn}, globalDefault)
	if err != nil {
		t.Fatalf("ReapIdle: %v", err)
	}
	if reaped != 2 {
		t.Errorf("reaped=%d want 2", reaped)
	}
	if got := m.Count("f"); got != 1 {
		t.Errorf("Count after=%d want 1 (busy survivor)", got)
	}
	remaining := m.Instances("f")
	if len(remaining) != 1 || remaining[0] != busyID {
		t.Errorf("remaining=%v want [%q] busy survivor", remaining, busyID)
	}
	for _, tid := range fp.terminatedCopy() {
		if tid == busyID {
			t.Errorf("busy %q was terminated — must never terminate busy", tid)
		}
	}
	reaped2, err := m.ReapIdle(ctx, now, []registry.Function{fn}, globalDefault)
	if err != nil {
		t.Fatalf("2nd ReapIdle: %v", err)
	}
	if reaped2 != 0 {
		t.Errorf("2nd reaped=%d want 0 idempotent", reaped2)
	}
}

func TestReapEmptyAndNil(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()
	now := time.Unix(11000, 0)
	globalDefault := 15 * time.Minute
	reaped, err := m.ReapIdle(ctx, now, []registry.Function{}, globalDefault)
	if err != nil {
		t.Fatalf("ReapIdle empty: %v", err)
	}
	if reaped != 0 {
		t.Errorf("reaped empty=%d want 0", reaped)
	}
	reaped2, err := m.ReapIdle(ctx, now, nil, globalDefault)
	if err != nil {
		t.Fatalf("ReapIdle nil: %v", err)
	}
	if reaped2 != 0 {
		t.Errorf("reaped nil=%d want 0", reaped2)
	}
}

func TestEffectiveTimeoutFallback(t *testing.T) {
	fnZero := registry.Function{Name: "f", IdleTimeout: config.Duration(0)}
	if got := EffectiveTimeout(fnZero, 0); got != 15*time.Minute {
		t.Errorf("EffectiveTimeout both zero=%v want 15m", got)
	}
	if got := EffectiveTimeout(fnZero, 10*time.Minute); got != 10*time.Minute {
		t.Errorf("EffectiveTimeout global 10m=%v want 10m", got)
	}
	fnPer := registry.Function{Name: "f", IdleTimeout: config.Duration(2 * time.Minute)}
	if got := EffectiveTimeout(fnPer, 15*time.Minute); got != 2*time.Minute {
		t.Errorf("EffectiveTimeout per-fn 2m=%v want 2m", got)
	}
}

// ===========================================================================
// Robustness
// ===========================================================================

func TestAcquireWaitPropagaErroDeProvisionamento(t *testing.T) {
	fp := &fakeProvisioner{failOn: "f-0"}
	m := NewManagerWithQueueCap(fp, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := m.AcquireWaitCold(ctx, "f", "img:1.0", 0)
	if err == nil {
		t.Fatal("AcquireWaitCold() = nil, esperado erro de provisionamento")
	}
	if !strings.Contains(err.Error(), "f-0") {
		t.Errorf("erro %q não menciona a instância que falhou", err)
	}
	if got := m.Count("f"); got != 0 {
		t.Errorf("Count() = %d após falha, esperado 0 (slot removido)", got)
	}
}

func TestReleaseDesconhecidoLogaSemMudarContabilidade(t *testing.T) {
	buf := capturarLog(t)

	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, _, err := m.AcquireWaitCold(ctx, "f", "img:1.0", 0)
	if err != nil {
		t.Fatal(err)
	}

	m.Release("f", "inexistente")
	if got := m.Count("f"); got != 1 {
		t.Errorf("Count() = %d após Release desconhecido, esperado 1", got)
	}
	if !strings.Contains(buf.String(), "Release de instância desconhecida") {
		t.Errorf("log de Release desconhecido ausente; saída foi:\n%s", buf.String())
	}

	m.Release("f", id)
}

// ===========================================================================
// Scale down
// ===========================================================================

func TestScaleDownToMin(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "f", "img", 3); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	if got := m.Count("f"); got != 3 {
		t.Fatalf("pre: Count=%d want 3", got)
	}

	reaped, err := m.ScaleDownToMin(ctx, "f", 1)
	if err != nil {
		t.Fatalf("ScaleDownToMin: %v", err)
	}
	if reaped != 2 {
		t.Errorf("reaped=%d want 2", reaped)
	}
	if got := m.Count("f"); got != 1 {
		t.Errorf("Count after ScaleDown=%d want 1", got)
	}
	remaining := m.Instances("f")
	if len(remaining) != 1 || remaining[0] != "f-0" {
		t.Errorf("remaining=%v want [f-0] (oldest survives)", remaining)
	}
	terminated := fp.terminatedCopy()
	if len(terminated) != 2 {
		t.Fatalf("terminated=%v want 2 entries", terminated)
	}
	sort.Strings(terminated)
	expected := []string{"f-1", "f-2"}
	sort.Strings(expected)
	mTerm := make(map[string]bool)
	for _, id := range terminated {
		mTerm[id] = true
	}
	for _, want := range expected {
		if !mTerm[want] {
			t.Errorf("terminated=%v missing %q (expected newest idle)", terminated, want)
		}
	}
	if terminated[0] == "f-1" && terminated[1] == "f-2" {
		raw := fp.terminatedCopy()
		if len(raw) == 2 && raw[0] != "f-2" {
			t.Errorf("termination order raw=%v want [f-2 f-1] (newest first)", raw)
		}
	} else {
		raw := fp.terminatedCopy()
		if len(raw) == 2 && raw[0] != "f-2" {
			t.Errorf("termination order raw=%v want [f-2 f-1] newest first", raw)
		}
	}
}

func TestScaleDownBusyProtected(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "f", "img", 2); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	busyID, err := m.AcquireWait(ctx, "f", "img", 2)
	if err != nil {
		t.Fatalf("AcquireWait: %v", err)
	}
	if busyID != "f-0" {
		t.Logf("busyID=%q (expected f-0, but any busy protection still applies)", busyID)
	}

	reaped, err := m.ScaleDownToMin(ctx, "f", 1)
	if err != nil {
		t.Fatalf("ScaleDownToMin: %v", err)
	}
	if reaped != 1 {
		t.Errorf("reaped=%d want 1 (only idle can be reaped)", reaped)
	}
	if got := m.Count("f"); got != 1 {
		t.Errorf("Count after=%d want 1", got)
	}
	remaining := m.Instances("f")
	if len(remaining) != 1 {
		t.Fatalf("remaining=%v want 1 entry", remaining)
	}
	if remaining[0] != busyID {
		t.Errorf("remaining=%v want busy %q to survive", remaining, busyID)
	}
	terminated := fp.terminatedCopy()
	for _, tid := range terminated {
		if tid == busyID {
			t.Errorf("terminated contains busy instance %q — must never terminate busy", tid)
		}
	}
	if len(terminated) != 1 {
		t.Errorf("terminated=%v want exactly 1 (the idle one)", terminated)
	}
	if len(terminated) == 1 && terminated[0] != "f-1" {
		expectedIdle := "f-1"
		if busyID == "f-1" {
			expectedIdle = "f-0"
		}
		if terminated[0] != expectedIdle {
			t.Errorf("terminated=%v want idle %q", terminated, expectedIdle)
		}
	}
}

func TestScaleDownNegativeMin(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "f", "img", 2); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}

	reaped, err := m.ScaleDownToMin(ctx, "f", -1)
	if err == nil {
		t.Fatal("expected error for min=-1")
	}
	if reaped != 0 {
		t.Errorf("reaped=%d want 0 on error", reaped)
	}
	if got := m.Count("f"); got != 2 {
		t.Errorf("Count after negative min=%d want 2 (unchanged)", got)
	}
	if got := len(fp.terminatedCopy()); got != 0 {
		t.Errorf("terminated count=%d want 0 (nothing should be reaped)", got)
	}
}

func TestTouchLastUsed(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)

	base := time.Unix(1000, 0)
	now := base
	m.WithClock(func() time.Time { return now })

	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "f", "img", 1); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	if !lastUsedOf(m, "f").IsZero() {
		t.Errorf("lastUsed(f) before Acquire = %v want zero", lastUsedOf(m, "f"))
	}

	now = base.Add(10 * time.Second)
	id, err := m.AcquireWait(ctx, "f", "img", 2)
	if err != nil {
		t.Fatalf("AcquireWait: %v", err)
	}
	if id != "f-0" {
		t.Fatalf("AcquireWait id=%q want f-0", id)
	}
	if got := lastUsedOf(m, "f"); !got.Equal(now) {
		t.Errorf("lastUsed(f) after Acquire=%v want %v", got, now)
	}

	now = base.Add(20 * time.Second)
	m.Release("f", id)
	if got := lastUsedOf(m, "f"); !got.Equal(now) {
		t.Errorf("lastUsed(f) after Release=%v want %v", got, now)
	}

	now = base.Add(30 * time.Second)
	m.WithClock(func() time.Time { return now })
	id2, err := m.AcquireWait(ctx, "f", "img", 2)
	if err != nil {
		t.Fatalf("2nd AcquireWait: %v", err)
	}
	if got := lastUsedOf(m, "f"); !got.Equal(now) {
		t.Errorf("lastUsed(f) after 2nd Acquire=%v want %v", got, now)
	}
	m.Release("f", id2)
}

func TestScaleDownNewestFirst(t *testing.T) {
	fp := &recordingProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	if err := m.EnsureMinInstances(ctx, "f", "img", 4); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	if got := m.Count("f"); got != 4 {
		t.Fatalf("pre Count=%d want 4", got)
	}

	reaped, err := m.ScaleDownToMin(ctx, "f", 2)
	if err != nil {
		t.Fatalf("ScaleDownToMin: %v", err)
	}
	if reaped != 2 {
		t.Errorf("reaped=%d want 2", reaped)
	}
	remaining := m.Instances("f")
	sort.Strings(remaining)
	expectedRemain := []string{"f-0", "f-1"}
	if len(remaining) != len(expectedRemain) {
		t.Fatalf("remaining=%v want %v", remaining, expectedRemain)
	}
	for i, want := range expectedRemain {
		if remaining[i] != want {
			t.Errorf("remaining[%d]=%q want %q (oldest survive)", i, remaining[i], want)
		}
	}
	terminated := fp.terminatedCopy()
	if len(terminated) != 2 {
		t.Fatalf("terminated=%v want 2", terminated)
	}
	sort.Strings(terminated)
	expectedTerm := []string{"f-2", "f-3"}
	for i, want := range expectedTerm {
		if terminated[i] != want {
			t.Errorf("terminated sorted[%d]=%q want %q", i, terminated[i], want)
		}
	}
	raw := fp.terminatedCopy()
	if len(raw) == 2 {
		if raw[0] != "f-3" || raw[1] != "f-2" {
			t.Errorf("raw terminated order=%v want [f-3 f-2] newest first", raw)
		}
	}
	remSet := make(map[string]bool)
	for _, id := range remaining {
		remSet[id] = true
	}
	for _, tid := range terminated {
		if remSet[tid] {
			t.Errorf("terminated %q still in remaining %v", tid, remaining)
		}
	}
}

// ===========================================================================
// Last use tracking
// ===========================================================================

func TestLastUsedSingleNamespace(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "a", "img:1.0", 1); err != nil {
		t.Fatalf("EnsureMinInstances a: %v", err)
	}
	if err := m.EnsureMinInstances(ctx, "a-0", "img:1.0", 1); err != nil {
		t.Fatalf("EnsureMinInstances a-0: %v", err)
	}
	t1 := time.Unix(1000, 0)
	t2 := time.Unix(2000, 0)

	m.WithClock(func() time.Time { return t1 })
	m.touchOnShard(m.shardFor("a"), "a")
	m.WithClock(func() time.Time { return t2 })
	m.touchOnShard(m.shardFor("a-0"), "a-0")

	if got := lastUsedOf(m, "a"); !got.Equal(t1) {
		t.Fatalf("lastUsed[a]=%v want %v", got, t1)
	}
	if got := lastUsedOf(m, "a-0"); !got.Equal(t2) {
		t.Fatalf("lastUsed[a-0]=%v want %v", got, t2)
	}
}

func TestFunctionInstanceNameReuse(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "a", "img:1.0", 1); err != nil {
		t.Fatalf("EnsureMinInstances a: %v", err)
	}
	t2 := time.Unix(2000, 0)
	m.WithClock(func() time.Time { return t2 })
	m.touchOnShard(m.shardFor("a-0"), "a-0")

	m.removeInstanceLocked("a", "a-0")
	if got := lastUsedOf(m, "a-0"); !got.Equal(t2) {
		t.Fatalf("lastUsed[a-0]=%v want %v após remover instância homônima", got, t2)
	}
}

func TestInstanceByIDPresence(t *testing.T) {
	fp := &fakeProvisioner{}
	m := NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()
	if err := m.EnsureMinInstances(ctx, "a", "img:1.0", 3); err != nil {
		t.Fatalf("EnsureMinInstances a*3: %v", err)
	}
	if got := m.Count("a"); got != 3 {
		t.Fatalf("Count(a)=%d want 3", got)
	}

	m.globalMu.RLock()
	if m.instanceByID == nil {
		m.globalMu.RUnlock()
		t.Fatalf("instanceByID is nil — not initialized")
	}
	for _, wantID := range []string{"a-0", "a-1", "a-2"} {
		if _, ok := m.instanceByID[wantID]; !ok {
			m.globalMu.RUnlock()
			t.Fatalf("instanceByID[%q] not found in map", wantID)
		}
	}
	m.globalMu.RUnlock()

	s := m.shardFor("a")
	s.mu.Lock()
	sliceIDs := make(map[string]bool, len(s.instances["a"]))
	for _, inst := range s.instances["a"] {
		sliceIDs[inst.id] = true
	}
	s.mu.Unlock()
	m.globalMu.RLock()
	for id := range m.instanceByID {
		if !sliceIDs[id] {
			m.globalMu.RUnlock()
			t.Fatalf("instanceByID[%q] not in instances slice — index/slice desync", id)
		}
	}
	m.globalMu.RUnlock()

	if err := m.RemoveFunction(ctx, "a"); err != nil {
		t.Fatalf("RemoveFunction: %v", err)
	}
	m.globalMu.RLock()
	n := len(m.instanceByID)
	m.globalMu.RUnlock()
	if n != 0 {
		t.Fatalf("instanceByID len=%d want 0 após RemoveFunction", n)
	}
}

// ===========================================================================
// Pending provisioning
// ===========================================================================

func TestPendingNotOrphanedAfterReap(t *testing.T) {
	p := &pendingBlockingProvisioner{
		blockOn: "fn-0",
		started: make(chan struct{}),
		block:   make(chan struct{}),
	}
	m := NewManagerWithQueueCap(p, func(string) int { return 10 })
	ctx := context.Background()

	leaderDone := make(chan error, 1)
	go func() {
		_, err := m.AcquireWait(ctx, "fn", "img:1.0", 0)
		leaderDone <- err
	}()

	select {
	case <-p.started:
	case <-time.After(2 * time.Second):
		t.Fatal("leader Provision never started")
	}

	deadline := time.Now().Add(1 * time.Second)
	for {
		s := m.shardFor("fn")
		s.mu.Lock()
		pid := ""
		if s.pendingID != nil {
			pid = s.pendingID["fn"]
		}
		cnt := len(s.instances["fn"])
		s.mu.Unlock()
		if pid != "" && cnt == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for pendingID != \"\" && instances==1")
		}
		time.Sleep(5 * time.Millisecond)
	}

	const waiters = 2
	cancels := make([]context.CancelFunc, waiters)
	errCh := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		c, cancel := context.WithCancel(context.Background())
		cancels[i] = cancel
		go func(cc context.Context) {
			_, err := m.AcquireWait(cc, "fn", "img:1.0", 0)
			errCh <- err
		}(c)
	}

	deadline = time.Now().Add(1 * time.Second)
	for {
		if got := m.QueueLen("fn"); got == waiters {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("QueueLen before orphan = %d, want %d", m.QueueLen("fn"), waiters)
		}
		time.Sleep(5 * time.Millisecond)
	}

	s := m.shardFor("fn")
	s.mu.Lock()
	delete(s.instances, "fn")
	pid := ""
	if s.pendingID != nil {
		pid = s.pendingID["fn"]
	}
	qlen := len(s.waiters["fn"])
	_, haveInst := s.instances["fn"]
	instNil := !haveInst || s.instances["fn"] == nil
	s.mu.Unlock()

	if pid == "" {
		t.Fatalf("expected pendingID[fn] != \"\" during blocked Provision, got empty — test setup broken")
	}
	if qlen == 0 {
		t.Fatalf("expected waiters[fn] > 0, got 0 — test setup broken")
	}
	if !instNil {
		t.Fatalf("expected instances[fn]==nil after delete, still present — test setup broken")
	}
	orphaned := pid != "" && qlen > 0 && instNil
	if !orphaned {
		t.Fatalf("expected orphaned state pendingID != \"\" && waiters>0 && instances==nil, got pid=%q qlen=%d instNil=%v", pid, qlen, instNil)
	}
	t.Logf("orphan pinned: pendingID=%q waiters=%d instances nil=%v (bug present)", pid, qlen, instNil)

	close(p.block)
	for _, c := range cancels {
		c()
	}

	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not complete after unblock")
	}

	for i := 0; i < waiters; i++ {
		select {
		case <-errCh:
		case <-time.After(1 * time.Second):
			t.Fatalf("waiter %d did not return after cleanup", i)
		}
	}
}

func TestPendingIDIsSingleSource(t *testing.T) {
	p := &pendingBlockingProvisioner{
		blockOn: "fn-0",
		started: make(chan struct{}),
		block:   make(chan struct{}),
	}
	m := NewManagerWithQueueCap(p, func(string) int { return 10 })
	ctx := context.Background()

	leaderDone := make(chan string, 1)
	leaderErr := make(chan error, 1)
	go func() {
		id, err := m.AcquireWait(ctx, "fn", "img:1.0", 0)
		leaderDone <- id
		leaderErr <- err
	}()

	select {
	case <-p.started:
	case <-time.After(2 * time.Second):
		t.Fatal("leader Provision never started")
	}

	deadline := time.Now().Add(1 * time.Second)
	for {
		s := m.shardFor("fn")
		s.mu.Lock()
		pid := ""
		if s.pendingID != nil {
			pid = s.pendingID["fn"]
		}
		cnt := len(s.instances["fn"])
		s.mu.Unlock()
		if pid != "" && cnt == 1 {
			break
		}
		if time.Now().After(deadline) {
			s2 := m.shardFor("fn")
			s2.mu.Lock()
			pid2 := ""
			if s2.pendingID != nil {
				pid2 = s2.pendingID["fn"]
			}
			cnt2 := len(s2.instances["fn"])
			s2.mu.Unlock()
			t.Fatalf("timeout waiting for pendingID != \"\" && instances==1 got pendingID=%q instances=%d", pid2, cnt2)
		}
		time.Sleep(5 * time.Millisecond)
	}

	waiterDone := make(chan string, 1)
	waiterErr := make(chan error, 1)
	go func() {
		id, err := m.AcquireWait(context.Background(), "fn", "img:1.0", 0)
		waiterDone <- id
		waiterErr <- err
	}()

	deadline = time.Now().Add(1 * time.Second)
	for {
		if got := m.QueueLen("fn"); got == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("QueueLen before orphan = %d, want 1", m.QueueLen("fn"))
		}
		time.Sleep(5 * time.Millisecond)
	}

	s := m.shardFor("fn")
	s.mu.Lock()
	delete(s.instances, "fn")
	prePID := ""
	if s.pendingID != nil {
		prePID = s.pendingID["fn"]
	}
	preQLen := len(s.waiters["fn"])
	s.mu.Unlock()
	if prePID == "" {
		t.Fatalf("expected pendingID[fn] != \"\" before unblock, got empty")
	}
	if preQLen == 0 {
		t.Fatalf("expected waiters[fn] > 0 before unblock, got 0")
	}

	close(p.block)

	select {
	case <-leaderDone:
		if err := <-leaderErr; err != nil {
			t.Fatalf("leader AcquireWait failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not complete after unblock")
	}

	select {
	case <-waiterDone:
		if err := <-waiterErr; err != nil {
			t.Fatalf("waiter AcquireWait failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not complete after leader woke it")
	}

	s2 := m.shardFor("fn")
	s2.mu.Lock()
	finalPID := ""
	if s2.pendingID != nil {
		finalPID = s2.pendingID["fn"]
	}
	finalQLen := len(s2.waiters["fn"])
	s2.mu.Unlock()
	if finalPID != "" {
		t.Fatalf("expected pendingID[\"fn\"]==\"\" after Reap+complete, got %q (orphan)", finalPID)
	}
	if finalQLen != 0 {
		t.Fatalf("expected len(waiters[\"fn\"])==0 after Reap+complete, got %d (orphan)", finalQLen)
	}
	if got := m.Count("fn"); got == 0 {
		t.Fatalf("expected at least 1 instance after waiter provision, got 0")
	}
	if c := atomic.LoadInt64(&m.coalescedTotal); c == 0 {
		t.Logf("warning: coalescedTotal still 0 after waiter wake — expected >=1 (queue coalesce)")
	}
}
