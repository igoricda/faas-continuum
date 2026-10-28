// Consolidated test suite of the node API.
package nodeapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/auth"
	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/ntime"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// ---------------------------------------------------------------------------
// Helpers: fake backend, socket, collector and server
// ---------------------------------------------------------------------------

type fakeInstanceBackend struct {
	t             *testing.T
	socketDir     string
	handler       http.HandlerFunc
	mu            sync.Mutex
	listeners     map[string]net.Listener
	failProvision bool
}

func newFakeInstanceBackend(t *testing.T, handler http.HandlerFunc) *fakeInstanceBackend {
	t.Helper()
	return &fakeInstanceBackend{
		t:         t,
		socketDir: t.TempDir(),
		handler:   handler,
		listeners: make(map[string]net.Listener),
	}
}

func (f *fakeInstanceBackend) SocketPath(instanceID string) string {
	return filepath.Join(f.socketDir, instanceID+".sock")
}

func (f *fakeInstanceBackend) SocketWatchDir(instanceID string) (string, error) {
	if instanceID == "" {
		return "", errors.New("nodeapi: SocketWatchDir com instanceID vazio (fail-closed)")
	}
	return f.socketDir, nil
}

var _ InstanceAddresser = (*fakeInstanceBackend)(nil)

func (f *fakeInstanceBackend) Provision(_ context.Context, instanceID, _ string) error {
	f.mu.Lock()
	fail := f.failProvision
	f.mu.Unlock()
	if fail {
		return errors.New("falha simulada de provisionamento (ex.: imagem inválida)")
	}
	path := f.SocketPath(instanceID)
	os.Remove(path)
	l, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.listeners[instanceID] = l
	f.mu.Unlock()
	go http.Serve(l, f.handler)
	return nil
}

func (f *fakeInstanceBackend) Terminate(_ context.Context, instanceID string) error {
	f.mu.Lock()
	l, ok := f.listeners[instanceID]
	delete(f.listeners, instanceID)
	f.mu.Unlock()
	if ok {
		return l.Close()
	}
	return nil
}

func echoHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

func slowHandler(w http.ResponseWriter, r *http.Request) {
	time.Sleep(500 * time.Millisecond)
	w.Write([]byte(`{"status":"nunca deveria chegar aqui a tempo"}`))
}

func textHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte("Hello, world!"))
}

func emptyHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func errorHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, `{"erro":"boom interno simulado"}`, http.StatusInternalServerError)
}

func panicHandler(w http.ResponseWriter, r *http.Request) {
	panic("falha deliberada da função para o teste de vazamento")
}

type panicAddresser struct{}

func (panicAddresser) SocketPath(string) string {
	panic("falha deliberada do endereçador para o teste de vazamento")
}

func (panicAddresser) SocketWatchDir(string) (string, error) {
	panic("falha deliberada do endereçador para o teste de vazamento")
}

var _ InstanceAddresser = panicAddresser{}

func newTestCollector(t *testing.T) *telemetry.Collector {
	t.Helper()
	dir := t.TempDir()
	stat := filepath.Join(dir, "stat")
	meminfo := filepath.Join(dir, "meminfo")
	cgroup := filepath.Join(dir, "cgroup")
	if err := os.MkdirAll(cgroup, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stat, []byte("cpu  0 0 0 0 0 0 0 0 0 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(meminfo, []byte("MemTotal:        1024 kB\nMemAvailable:     512 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return telemetry.NewCollector("xp4-borda-01", true, cgroup, stat, meminfo, "", "")
}

func newTestServer(t *testing.T, backend *fakeInstanceBackend, samplers ...*fakeSampler) (*httptest.Server, string, *pool.Manager) {
	t.Helper()
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("falha ao gerar credencial de teste: %v", err)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(backend, nil)
	sampler := newFakeSampler()
	if len(samplers) > 0 && samplers[0] != nil {
		sampler = samplers[0]
	}
	srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, newTestCollector(t), sampler, 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend})
	return httptest.NewServer(srv.Handler()), g.PlainToken, pl
}

func newTestServerWithAllowlist(t *testing.T, allowedRegistries []string, backend *fakeInstanceBackend, samplers ...*fakeSampler) (*httptest.Server, string, *pool.Manager) {
	t.Helper()
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("falha ao gerar credencial de teste: %v", err)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"), allowedRegistries...)
	pl := pool.NewManagerWithQueueCap(backend, nil)
	sampler := newFakeSampler()
	if len(samplers) > 0 && samplers[0] != nil {
		sampler = samplers[0]
	}
	srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, newTestCollector(t), sampler, 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend})
	return httptest.NewServer(srv.Handler()), g.PlainToken, pl
}

func doRegister(t *testing.T, ts *httptest.Server, plainToken, name string, body registerRequest) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/function/"+name, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if plainToken != "" {
		req.Header.Set(auth.DefaultHeaderName, plainToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func doInvoke(t *testing.T, ts *httptest.Server, name, invokeToken string, body invokeRequest) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/function/"+name+"/invoke", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if invokeToken != "" {
		req.Header.Set(auth.DefaultHeaderName, invokeToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func doGet(t *testing.T, ts *httptest.Server, name, invokeToken string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/function/"+name, nil)
	if err != nil {
		t.Fatal(err)
	}
	if invokeToken != "" {
		req.Header.Set(auth.DefaultHeaderName, invokeToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func mustRegister(t *testing.T, resp *http.Response) (registry.Function, string) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("registro: status=%d want 201 body=%s", resp.StatusCode, body)
	}
	var env registerResponse
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if env.InvokeToken == "" {
		t.Fatal("resposta de registro sem invoke_token show-once")
	}
	return env.Function, env.InvokeToken
}

func doDelete(t *testing.T, ts *httptest.Server, plainToken, name string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/function/"+name, nil)
	if err != nil {
		t.Fatal(err)
	}
	if plainToken != "" {
		req.Header.Set(auth.DefaultHeaderName, plainToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func doUpdate(t *testing.T, ts *httptest.Server, plainToken, name string, body registerRequest) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/function/"+name, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if plainToken != "" {
		req.Header.Set(auth.DefaultHeaderName, plainToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func doList(t *testing.T, ts *httptest.Server, plainToken string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/function", nil)
	if err != nil {
		t.Fatal(err)
	}
	if plainToken != "" {
		req.Header.Set(auth.DefaultHeaderName, plainToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func doWithMap(t *testing.T, tsURL, token, method, path string, body map[string]any) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(method, tsURL+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set(auth.DefaultHeaderName, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

type fakeSampler struct {
	mu    sync.Mutex
	seq   []telemetry.InvocationSample
	idx   int
	err   error
	calls int
}

func newFakeSampler(seq ...telemetry.InvocationSample) *fakeSampler {
	if len(seq) == 0 {
		seq = []telemetry.InvocationSample{
			{CPUUsageUSec: 1_000_000, MemoryBytes: 4096},
			{CPUUsageUSec: 3_000_000, MemoryBytes: 8192},
		}
	}
	return &fakeSampler{seq: seq}
}

func (f *fakeSampler) SampleMetrics(_ context.Context, _ string) (telemetry.InvocationSample, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return telemetry.InvocationSample{}, f.err
	}
	s := f.seq[min(f.idx, len(f.seq)-1)]
	f.idx++
	return s, nil
}

var leakTestClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

func goroutineBaseline(t *testing.T) int {
	t.Helper()
	prev := runtime.NumGoroutine()
	for stable := 0; stable < 3; {
		time.Sleep(20 * time.Millisecond)
		if n := runtime.NumGoroutine(); n == prev {
			stable++
		} else {
			stable, prev = 0, n
		}
	}
	return prev
}

func waitGoroutinesSettle(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if n := runtime.NumGoroutine(); n <= before+1 {
			return
		} else if time.Now().After(deadline) {
			buf := make([]byte, 64<<10)
			buf = buf[:runtime.Stack(buf, true)]
			t.Errorf("goroutines após a requisição = %d, antes = %d (tolerância ±1): possível goroutine do sampler pendurada\nstacks:\n%s", runtime.NumGoroutine(), before, buf)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// Helpers of the readiness
// ---------------------------------------------------------------------------

type fakeAddresser struct {
	path string
}

func (f *fakeAddresser) SocketPath(instanceID string) string {
	return f.path
}

func (f *fakeAddresser) SocketWatchDir(instanceID string) (string, error) {
	if instanceID == "" {
		return "", errors.New("nodeapi: SocketWatchDir com instanceID vazio (fail-closed)")
	}
	return filepath.Dir(f.path), nil
}

var _ InstanceAddresser = (*fakeAddresser)(nil)

type fakeReadinessChecker struct {
	calls  int32
	lastID string
	err    error
}

func (f *fakeReadinessChecker) WaitReady(ctx context.Context, instanceID string) (int, error) {
	calls := atomic.AddInt32(&f.calls, 1)
	f.lastID = instanceID
	return int(calls), f.err
}

// ---------------------------------------------------------------------------
// Helpers of the invocation
// ---------------------------------------------------------------------------

type delayedBackend struct {
	t              *testing.T
	socketDir      string
	handler        http.HandlerFunc
	provisionDelay time.Duration
	socketDelay    time.Duration
	mu             sync.Mutex
	listeners      map[string]net.Listener
}

func newDelayedBackend(t *testing.T, handler http.HandlerFunc, provisionDelay, socketDelay time.Duration) *delayedBackend {
	t.Helper()
	return &delayedBackend{
		t:              t,
		socketDir:      t.TempDir(),
		handler:        handler,
		provisionDelay: provisionDelay,
		socketDelay:    socketDelay,
		listeners:      make(map[string]net.Listener),
	}
}

func (d *delayedBackend) SocketPath(instanceID string) string {
	return filepath.Join(d.socketDir, instanceID+".sock")
}

func (d *delayedBackend) SocketWatchDir(instanceID string) (string, error) {
	if instanceID == "" {
		return "", errors.New("nodeapi: SocketWatchDir com instanceID vazio (fail-closed)")
	}
	return d.socketDir, nil
}

func (d *delayedBackend) Provision(ctx context.Context, instanceID, _ string) error {
	select {
	case <-time.After(d.provisionDelay):
	case <-ctx.Done():
		return ctx.Err()
	}
	go func() {
		time.Sleep(d.socketDelay)
		path := d.SocketPath(instanceID)
		_ = os.Remove(path)
		l, err := net.Listen("unix", path)
		if err != nil {
			return
		}
		d.mu.Lock()
		d.listeners[instanceID] = l
		d.mu.Unlock()
		go http.Serve(l, d.handler)
	}()
	return nil
}

func (d *delayedBackend) Terminate(_ context.Context, instanceID string) error {
	d.mu.Lock()
	l, ok := d.listeners[instanceID]
	delete(d.listeners, instanceID)
	d.mu.Unlock()
	if ok {
		return l.Close()
	}
	return nil
}

func newDelayedTestServer(t *testing.T, backend *delayedBackend) (*httptest.Server, string, *pool.Manager) {
	t.Helper()
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("falha ao gerar credencial de teste: %v", err)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(backend, nil)
	sampler := newFakeSampler()
	srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, newTestCollector(t), sampler, 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend})
	return httptest.NewServer(srv.Handler()), g.PlainToken, pl
}

func readAll(resp *http.Response) ([]byte, error) {
	b, err := io.ReadAll(resp.Body)
	return b, err
}

var (
	_ pool.Provisioner  = (*delayedBackend)(nil)
	_ InstanceAddresser = (*delayedBackend)(nil)
)

type blockingAddrProvisioner struct {
	dir       string
	block     chan struct{}
	mu        sync.Mutex
	listeners map[string]net.Listener
}

func (b *blockingAddrProvisioner) SocketPath(instanceID string) string {
	return filepath.Join(b.dir, instanceID+".sock")
}

func (b *blockingAddrProvisioner) SocketWatchDir(instanceID string) (string, error) {
	if instanceID == "" {
		return "", errors.New("nodeapi: SocketWatchDir com instanceID vazio (fail-closed)")
	}
	return b.dir, nil
}

var _ InstanceAddresser = (*blockingAddrProvisioner)(nil)

func (b *blockingAddrProvisioner) Provision(ctx context.Context, instanceID, _ string) error {
	select {
	case <-b.block:
		path := b.SocketPath(instanceID)
		_ = os.Remove(path)
		l, err := net.Listen("unix", path)
		if err != nil {
			return nil
		}
		b.mu.Lock()
		b.listeners[instanceID] = l
		b.mu.Unlock()
		go http.Serve(l, http.HandlerFunc(echoHandler))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (b *blockingAddrProvisioner) Terminate(_ context.Context, instanceID string) error {
	b.mu.Lock()
	l, ok := b.listeners[instanceID]
	delete(b.listeners, instanceID)
	b.mu.Unlock()
	if ok {
		return l.Close()
	}
	return nil
}

type ctxBlockingProvisioner struct {
	delay   time.Duration
	mu      sync.Mutex
	lastErr error
}

func (c *ctxBlockingProvisioner) SocketPath(instanceID string) string {
	return filepath.Join(os.TempDir(), instanceID+".sock")
}

func (c *ctxBlockingProvisioner) SocketWatchDir(instanceID string) (string, error) {
	if instanceID == "" {
		return "", errors.New("nodeapi: SocketWatchDir com instanceID vazio (fail-closed)")
	}
	return os.TempDir(), nil
}

var _ InstanceAddresser = (*ctxBlockingProvisioner)(nil)

func (c *ctxBlockingProvisioner) Provision(ctx context.Context, _, _ string) error {
	select {
	case <-time.After(c.delay):
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		c.lastErr = ctx.Err()
		c.mu.Unlock()
		return ctx.Err()
	}
}
func (c *ctxBlockingProvisioner) Terminate(_ context.Context, _ string) error { return nil }

type hangReadiness struct{ delay time.Duration }

func (h *hangReadiness) WaitReady(ctx context.Context, instanceID string) (int, error) {
	select {
	case <-time.After(h.delay):
		return 0, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// ---------------------------------------------------------------------------
// Helpers of the profiler
// ---------------------------------------------------------------------------

func writeTestFixtures(stat, meminfo, cgroup string) error {
	if err := os.MkdirAll(cgroup, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(stat, []byte("cpu  0 0 0 0 0 0 0 0 0 0\n"), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(meminfo, []byte("MemTotal:        1024 kB\nMemAvailable:     512 kB\n"), 0o644); err != nil {
		return err
	}
	return nil
}

type ctxSensitiveSampler struct {
	mu       sync.Mutex
	callLog  []string
	t0Sample telemetry.InvocationSample
}

func newCtxSensitiveSampler() *ctxSensitiveSampler {
	return &ctxSensitiveSampler{
		t0Sample: telemetry.InvocationSample{CPUUsageUSec: 1_000_000, MemoryBytes: 4096},
	}
}

func (s *ctxSensitiveSampler) SampleMetrics(ctx context.Context, instanceID string) (telemetry.InvocationSample, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		s.callLog = append(s.callLog, "FAIL:ctx-canceled")
		return telemetry.InvocationSample{}, ctx.Err()
	default:
		s.callLog = append(s.callLog, "OK")
		if len(s.callLog) == 1 {
			return s.t0Sample, nil
		}
		return telemetry.InvocationSample{CPUUsageUSec: 5_000_000, MemoryBytes: 16384}, nil
	}
}

type deadlineRequiredSampler struct{}

func (d *deadlineRequiredSampler) SampleMetrics(ctx context.Context, _ string) (telemetry.InvocationSample, error) {
	_, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		return telemetry.InvocationSample{}, context.DeadlineExceeded
	}
	return telemetry.InvocationSample{CPUUsageUSec: 2_000_000, MemoryBytes: 8192}, nil
}

// ---------------------------------------------------------------------------
// Tests of the buffer pool
// ---------------------------------------------------------------------------

func TestBufferPool_ResetPreventsLeak(t *testing.T) {
	buf := bufferPool.Get().(*bytes.Buffer)
	large := `"` + strings.Repeat("a", 7000) + `"`
	largeJSON := `{"data":` + large + `}`
	if !json.Valid([]byte(largeJSON)) {
		t.Fatalf("large fixture not valid JSON")
	}
	buf.WriteString(largeJSON)
	if buf.Len() == 0 {
		t.Fatal("buf should contain large payload")
	}
	buf.Reset()
	if buf.Len() != 0 {
		t.Fatalf("after Reset Len=%d want 0", buf.Len())
	}
	if buf.Cap() < 4096 {
		t.Errorf("cap after Reset = %d, want >=4096 (pre-allocated)", buf.Cap())
	}
	bufferPool.Put(buf)
	buf2 := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		buf2.Reset()
		bufferPool.Put(buf2)
	}()
	if buf2.Len() != 0 {
		t.Fatalf("reused buffer Len=%d want 0 — leak of previous large body", buf2.Len())
	}
	small := `{"a":1}`
	buf2.WriteString(small)
	if buf2.String() != small {
		t.Fatalf("small write leaked: got %q want %q (len %d vs %d) content %q", buf2.String(), small, buf2.Len(), len(small), buf2.String()[:min(100, buf2.Len())])
	}
	if !json.Valid(buf2.Bytes()) {
		t.Fatalf("small payload should be valid JSON")
	}
	if strings.Contains(buf2.String(), strings.Repeat("a", 100)) {
		t.Fatalf("large body leaked into small buffer")
	}
	copied := make([]byte, buf2.Len())
	copy(copied, buf2.Bytes())
	buf2.Reset()
	if string(copied) != small {
		t.Fatalf("copied bytes corrupted after Reset: got %q want %q", string(copied), small)
	}
}

func TestBufferPool_LimitEnforcedBeforeReset(t *testing.T) {
	const limit = 100
	payload := strings.Repeat("x", limit+10)
	r := io.LimitReader(strings.NewReader(payload), int64(limit)+1)
	buf := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufferPool.Put(buf)
	}()
	n, err := io.Copy(buf, r)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if int(n) != limit+1 {
		t.Fatalf("Copy n=%d want %d (limit+1)", n, limit+1)
	}
	if buf.Len() <= limit {
		t.Fatalf("should detect overflow: Len=%d limit=%d", buf.Len(), limit)
	}
	buf.Reset()
	if buf.Len() != 0 {
		t.Fatalf("after Reset Len=%d want 0", buf.Len())
	}
}

func TestCallInstance_BufferPoolNoLeak(t *testing.T) {
	largePayload := `{"data":"` + strings.Repeat("b", 5000) + `"}`
	smallPayload := `{"a":1}`
	var callCount int
	handler := func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		if callCount == 1 {
			_, _ = w.Write([]byte(largePayload))
		} else {
			_, _ = w.Write([]byte(smallPayload))
		}
	}
	backend := newFakeInstanceBackend(t, handler)
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("cred: %v", err)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(backend, nil)
	sampler := newFakeSampler()
	collector := newTestCollector(t)
	srv := NewServer("test:8080", reg, pl, backend, g.Cred, 5*time.Second, collector, sampler, 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend})
	ctx := context.Background()
	instanceID := "test-leak-0"
	if err := backend.Provision(ctx, instanceID, "img:1.0"); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	defer backend.Terminate(ctx, instanceID)
	time.Sleep(20 * time.Millisecond)
	res1, err := srv.callInstance(ctx, instanceID, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("first callInstance: %v", err)
	}
	if string(res1) != largePayload {
		t.Fatalf("first result mismatch: got len %d want %d", len(res1), len(largePayload))
	}
	res2, err := srv.callInstance(ctx, instanceID, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("second callInstance: %v", err)
	}
	if string(res2) != smallPayload {
		t.Fatalf("second result leaked: got %q (len %d) want %q (len %d)", string(res2), len(res2), smallPayload, len(smallPayload))
	}
	if strings.Contains(string(res2), strings.Repeat("b", 100)) {
		t.Fatalf("large payload leaked into second response")
	}
	if !json.Valid(res2) {
		t.Fatalf("second result not valid JSON")
	}
	buf := bufferPool.Get().(*bytes.Buffer)
	if buf.Cap() < 4096 {
		t.Errorf("pool buffer cap=%d want >=4096", buf.Cap())
	}
	buf.Reset()
	bufferPool.Put(buf)
}

func TestBufferPool_InitialCapacity(t *testing.T) {
	for i := 0; i < 3; i++ {
		b := bufferPool.Get().(*bytes.Buffer)
		if b.Cap() < 4096 {
			t.Errorf("buffer cap=%d want >=4096", b.Cap())
		}
		b.Reset()
		bufferPool.Put(b)
	}
	fresh := bytes.NewBuffer(make([]byte, 0, 4096))
	if fresh.Cap() != 4096 {
		t.Errorf("fresh buffer cap=%d want 4096", fresh.Cap())
	}
}

func TestCallInstance_LimitExceededWithPool(t *testing.T) {
	limit := 1024
	oversized := `"` + strings.Repeat("z", limit+5) + `"`
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(oversized))
	}
	backend := newFakeInstanceBackend(t, handler)
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("cred: %v", err)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(backend, nil)
	srv := NewServer("test:8080", reg, pl, backend, g.Cred, 5*time.Second, newTestCollector(t), newFakeSampler(), 64<<10, limit, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend})
	instanceID := "test-limit-0"
	if err := backend.Provision(context.Background(), instanceID, "img:1.0"); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	defer backend.Terminate(context.Background(), instanceID)
	time.Sleep(20 * time.Millisecond)
	_, err = srv.callInstance(context.Background(), instanceID, json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected limit exceeded error")
	}
	if !strings.Contains(err.Error(), "excede o limite") {
		t.Fatalf("error %q should contain limit message", err.Error())
	}
	small := `{"ok":true}`
	backend2 := newFakeInstanceBackend(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(small))
	})
	srv2 := NewServer("test:8080", reg, pl, backend2, g.Cred, 5*time.Second, newTestCollector(t), newFakeSampler(), 64<<10, limit, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend2})
	if err := backend2.Provision(context.Background(), "test-limit-1", "img:1.0"); err != nil {
		t.Fatalf("Provision2: %v", err)
	}
	defer backend2.Terminate(context.Background(), "test-limit-1")
	time.Sleep(20 * time.Millisecond)
	res, err := srv2.callInstance(context.Background(), "test-limit-1", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("small after limit error: %v", err)
	}
	if string(res) != small {
		t.Fatalf("got %q want %q", string(res), small)
	}
}

// ---------------------------------------------------------------------------
// Tests of the readiness
// ---------------------------------------------------------------------------

func TestWaitReady_SucceedsAfterDelay(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")
	addr := &fakeAddresser{path: sockPath}
	origDial := unixDial
	var attempts int32
	unixDial = func(network, address string, timeout time.Duration) (net.Conn, error) {
		atomic.AddInt32(&attempts, 1)
		return origDial(network, address, timeout)
	}
	defer func() { unixDial = origDial }()
	checker := &SocketReadinessChecker{Addresser: addr}
	srv := &Server{
		addresser: addr,
		readiness: checker,
	}
	go func() {
		time.Sleep(150 * time.Millisecond)
		l, err := net.Listen("unix", sockPath)
		if err != nil {
			t.Errorf("listen failed: %v", err)
			return
		}
		defer l.Close()
		time.Sleep(800 * time.Millisecond)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := srv.waitReady(ctx, "inst-1")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("waitReady should succeed after socket appears, got %v (attempts=%d, elapsed=%v)", err, atomic.LoadInt32(&attempts), elapsed)
	}
	if elapsed < 100*time.Millisecond {
		t.Errorf("waitReady returned too quickly: %v (expected ~150ms, attempts=%d)", elapsed, atomic.LoadInt32(&attempts))
	}
	at := atomic.LoadInt32(&attempts)
	if at < 1 || at > 3 {
		t.Errorf("attempts = %d, expected 1-3 (fixo 200ms+jitter)", at)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(nil, nil)
	collector := newTestCollector(t)
	g, _ := auth.Generate()
	srv2 := NewServer("test:8080", reg, pl, addr, g.Cred, 5*time.Second, collector, newFakeSampler(), 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, checker)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	dir2 := t.TempDir()
	sockPath2 := filepath.Join(dir2, "test2.sock")
	addr2 := &fakeAddresser{path: sockPath2}
	checker2 := &SocketReadinessChecker{Addresser: addr2}
	srv2.addresser = addr2
	srv2.readiness = checker2
	go func() {
		time.Sleep(150 * time.Millisecond)
		l, err := net.Listen("unix", sockPath2)
		if err != nil {
			t.Errorf("listen2 failed: %v", err)
			return
		}
		defer l.Close()
		time.Sleep(800 * time.Millisecond)
	}()
	start2 := time.Now()
	if _, err := srv2.waitReady(ctx2, "inst-2"); err != nil {
		t.Fatalf("srv2.waitReady failed: %v", err)
	}
	if time.Since(start2) < 100*time.Millisecond {
		t.Errorf("srv2 waitReady too fast")
	}
}

func TestWaitReady_Timeout(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "never.sock")
	addr := &fakeAddresser{path: sockPath}
	checker := &SocketReadinessChecker{Addresser: addr}
	srv := &Server{
		addresser: addr,
		readiness: checker,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := srv.waitReady(ctx, "inst-never")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("waitReady should have timed out, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("waitReady took %v, expected ~100ms (deadline + small slack)", elapsed)
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("waitReady returned too early: %v (expected ~100ms)", elapsed)
	}
}

func TestReadinessCheckerInjection(t *testing.T) {
	fake := &fakeReadinessChecker{err: nil}
	srv := &Server{
		readiness: fake,
	}
	ctx := context.Background()
	if _, err := srv.waitReady(ctx, "my-instance-42"); err != nil {
		t.Fatalf("waitReady with fake checker should return nil, got %v", err)
	}
	if atomic.LoadInt32(&fake.calls) != 1 {
		t.Errorf("fake WaitReady calls = %d, want 1", fake.calls)
	}
	if fake.lastID != "my-instance-42" {
		t.Errorf("fake lastID = %q, want %q", fake.lastID, "my-instance-42")
	}
	dir := t.TempDir()
	addr := &fakeAddresser{path: filepath.Join(dir, "unused.sock")}
	fake2 := &fakeReadinessChecker{err: nil}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(nil, nil)
	collector := newTestCollector(t)
	g, _ := auth.Generate()
	srv2 := NewServer("test:8080", reg, pl, addr, g.Cred, 5*time.Second, collector, newFakeSampler(), 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, fake2)
	if _, err := srv2.waitReady(ctx, "injected-id"); err != nil {
		t.Fatalf("srv2 waitReady: %v", err)
	}
	if atomic.LoadInt32(&fake2.calls) != 1 || fake2.lastID != "injected-id" {
		t.Errorf("injected fake not used: calls=%d lastID=%q", fake2.calls, fake2.lastID)
	}
	fakeErr := &fakeReadinessChecker{err: context.Canceled}
	srv3 := &Server{readiness: fakeErr}
	if _, err := srv3.waitReady(ctx, "id-err"); err != context.Canceled {
		t.Errorf("expected context.Canceled from fake, got %v", err)
	}
}

func TestWaitReady_RespectsDeadline(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "never-deadline.sock")
	addr := &fakeAddresser{path: sockPath}
	checker := &SocketReadinessChecker{Addresser: addr}
	srv := &Server{
		addresser: addr,
		readiness: checker,
	}
	deadline := 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	start := time.Now()
	_, err := srv.waitReady(ctx, "inst-never-deadline")
	elapsed := time.Since(start)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v (elapsed=%v)", err, elapsed)
	}
	if elapsed > deadline+50*time.Millisecond {
		t.Errorf("waitReady took %v, expected <= %v (deadline + 50ms); intervalo fixo overshot deadline", elapsed, deadline+50*time.Millisecond)
	}
}

func TestWaitReady_Concurrent(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "concurrent.sock")
	addr := &fakeAddresser{path: sockPath}
	checker := &SocketReadinessChecker{Addresser: addr}
	srv := &Server{
		addresser: addr,
		readiness: checker,
	}
	listenErr := make(chan error, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		l, err := net.Listen("unix", sockPath)
		if err != nil {
			listenErr <- err
			return
		}
		defer l.Close()
		conn, err := l.Accept()
		if err != nil {
			listenErr <- err
			return
		}
		_ = conn.Close()
		listenErr <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	numGoroutines := 10
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := srv.waitReady(ctx, "inst-concurrent")
			if err != nil {
				t.Logf("goroutine %d: waitReady returned %v", id, err)
			}
		}(i)
	}
	wg.Wait()
	if err := <-listenErr; err != nil {
		t.Fatalf("listener falhou: %v", err)
	}
}

func TestWaitReady_CountsAttempts(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("wait event-driven é linux-only; o fallback sem inotify tem teto de dials e não alcança socket tardio (todo 2)")
	}
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "count.sock")
	addr := &fakeAddresser{path: sockPath}
	checker := &SocketReadinessChecker{Addresser: addr}
	srv := &Server{
		addresser: addr,
		readiness: checker,
	}
	go func() {
		// 450 ms: long enough to prove that the number of attempts does not
		// grow with the delay, since the wait is event-driven: the
		// immediate attempt, the check and the confirmation.
		time.Sleep(450 * time.Millisecond)
		l, err := net.Listen("unix", sockPath)
		if err != nil {
			t.Errorf("listen failed: %v", err)
			return
		}
		defer l.Close()
		time.Sleep(2 * time.Second)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempts, err := srv.waitReady(ctx, "inst-count")
	if err != nil {
		t.Fatalf("waitReady: %v", err)
	}
	if attempts < 2 {
		t.Errorf("attempts = %d, want >= 2 (socket tardio exige passar pelo evento)", attempts)
	}
	if attempts > maxReadinessDials {
		t.Errorf("attempts = %d, want <= %d (teto total)", attempts, maxReadinessDials)
	}
	hot, err := checker.WaitReady(ctx, "inst-count")
	if err != nil {
		t.Fatalf("direct WaitReady on hot socket: %v", err)
	}
	if hot < 1 {
		t.Errorf("direct attempts = %d, want >= 1", hot)
	}
}

func TestInvoke_ReadinessTimeoutMaps504(t *testing.T) {
	backend := newDelayedBackend(t, echoHandler, 0, 30*time.Second)
	ts, token, _ := newDelayedTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "func-readiness-timeout", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 5, TimeoutMS: 150})
	_, invTok := mustRegister(t, resp)
	invResp := doInvoke(t, ts, "func-readiness-timeout", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusGatewayTimeout {
		b, _ := readAll(invResp)
		t.Fatalf("status = %d, want 504; body: %s", invResp.StatusCode, string(b))
	}
	// The instance was not ready in time: the container came up and never
	// became ready, so the function did not run and this is not a
	// service timeout.
	body, _ := readAll(invResp)
	if !strings.Contains(string(body), "INSTANCE_NOT_READY_TIMEOUT") {
		t.Errorf("body %s should contain INSTANCE_NOT_READY_TIMEOUT", string(body))
	}
}

// TestInvoke_InstanceTimeoutMaps504 closes the pair of codes of the
// deadline.
//
// Another test covers the stage in which the container comes up and never
// becomes ready. This one covers the next stage: the container came up,
// became ready, the call entered the process and the function did not
// answer in time. Both are gateway timeouts, so they have to be
// distinguishable:
//
//   - an expired readiness gives the instance-not-ready timeout, in which
//     the function did not run, and the arbiter treats it as a
//     saturation;
//   - an expired call gives the invocation timeout, in which the function
//     ran and did not answer, and the arbiter treats it as a service
//     failure.
//
// The second is the only service timeout of the system and therefore the
// only one that can veto a node. The first two stages received a code of
// their own, but no test required that the third one still emitted its
// own, so changing it back by mistake would pass the whole suite. This
// test is the counterweight on the side that emits the code.
func TestInvoke_InstanceTimeoutMaps504(t *testing.T) {
	// An instantaneous socket so that the readiness passes, and a slow
	// handler that overflows the budget of 150 ms already inside the
	// call.
	backend := newDelayedBackend(t, slowHandler, 0, 0)
	ts, token, _ := newDelayedTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "func-inv-timeout", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 5, TimeoutMS: 150})
	_, invTok := mustRegister(t, resp)
	invResp := doInvoke(t, ts, "func-inv-timeout", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusGatewayTimeout {
		b, _ := readAll(invResp)
		t.Fatalf("status = %d, want 504; body: %s", invResp.StatusCode, string(b))
	}
	body, _ := readAll(invResp)
	if !strings.Contains(string(body), "INVOCATION_TIMEOUT") {
		t.Errorf("body %s should contain INVOCATION_TIMEOUT", string(body))
	}
	// The stage of the call must not emit an admission or a readiness
	// code: that would punish the node for a delay in which the
	// function was not even involved. Both are gateway timeouts with a
	// distinct body, and the arbiter decides by it.
	for _, errado := range []string{"ACQUIRE_TIMEOUT", "INSTANCE_NOT_READY_TIMEOUT"} {
		if strings.Contains(string(body), errado) {
			t.Errorf("body %s: estágio da chamada emitiu %s (código de admissão/readiness)", string(body), errado)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests of the invocation: delay, error mapping, queue and readiness
// ---------------------------------------------------------------------------

func TestHandleInvoke_ScaleUpSucceeds(t *testing.T) {
	backend := newDelayedBackend(t, echoHandler, 200*time.Millisecond, 20*time.Millisecond)
	ts, token, _ := newDelayedTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "func-scaleup", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 5, TimeoutMS: 2000})
	_, invTok := mustRegister(t, resp)
	input := json.RawMessage(`{"hello":"world"}`)
	invResp := doInvoke(t, ts, "func-scaleup", invTok, invokeRequest{Input: input})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusOK {
		b, _ := readAll(invResp)
		t.Fatalf("invoke status = %d, esperado %d; corpo: %s", invResp.StatusCode, http.StatusOK, string(b))
	}
	var result invokeResponse
	if err := json.NewDecoder(invResp.Body).Decode(&result); err != nil {
		t.Fatalf("decode invoke response: %v", err)
	}
	if string(result.Result) != string(input) {
		t.Errorf("result = %s, esperado %s (função deveria ter ecoado o input)", result.Result, input)
	}
}

func TestHandleInvoke_FirstRequestNotFailed(t *testing.T) {
	backend := newDelayedBackend(t, echoHandler, 200*time.Millisecond, 20*time.Millisecond)
	ts, token, pl := newDelayedTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "func-first", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 3, TimeoutMS: 2000})
	_, invTok := mustRegister(t, resp)
	if got := pl.Count("func-first"); got != 0 {
		t.Fatalf("pool.Count antes da invocação = %d, esperado 0 (scale-to-zero)", got)
	}
	input := json.RawMessage(`{"first":"request"}`)
	invResp := doInvoke(t, ts, "func-first", invTok, invokeRequest{Input: input})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusOK {
		b, _ := readAll(invResp)
		t.Fatalf("primeira invocação (scale-up) status = %d, esperado %d; corpo: %s (a requisição que dispara scale-up não deve falhar)", invResp.StatusCode, http.StatusOK, string(b))
	}
	var result invokeResponse
	if err := json.NewDecoder(invResp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(result.Result) != string(input) {
		t.Errorf("result = %s, esperado %s", result.Result, input)
	}
	if got := pl.Count("func-first"); got != 1 {
		t.Errorf("pool.Count após primeira invocação = %d, esperado 1", got)
	}
}

func TestHandleInvoke_ErrorMapping(t *testing.T) {
	t.Run("queue_full", func(t *testing.T) {
		blockCh := make(chan struct{})
		dir := t.TempDir()
		backend := &blockingAddrProvisioner{dir: dir, block: blockCh, listeners: make(map[string]net.Listener)}
		pl := pool.NewManagerWithQueueCap(backend, func(string) int { return 1 })
		g, err := auth.Generate()
		if err != nil {
			t.Fatalf("cred: %v", err)
		}
		reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
		srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, newTestCollector(t), newFakeSampler(), 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend})
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()
		resp := doRegister(t, ts, g.PlainToken, "func-qfull", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 0})
		_, invTok := mustRegister(t, resp)
		leaderDone := make(chan error, 1)
		go func() {
			_, err := pl.AcquireWait(context.Background(), "func-qfull", "img:1.0", 0)
			leaderDone <- err
		}()
		time.Sleep(50 * time.Millisecond)
		waiterDone := make(chan error, 1)
		go func() {
			_, err := pl.AcquireWait(context.Background(), "func-qfull", "img:1.0", 0)
			waiterDone <- err
		}()
		time.Sleep(50 * time.Millisecond)
		invResp := doInvoke(t, ts, "func-qfull", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
		defer invResp.Body.Close()
		if invResp.StatusCode != http.StatusServiceUnavailable {
			b, _ := io.ReadAll(invResp.Body)
			t.Fatalf("queue_full: status %d want 503 body %s", invResp.StatusCode, string(b))
		}
		body, _ := io.ReadAll(invResp.Body)
		if !strings.Contains(string(body), "QUEUE_FULL") {
			t.Errorf("queue_full: body %s should contain QUEUE_FULL", string(body))
		}
		close(blockCh)
		select {
		case <-leaderDone:
		case <-time.After(2 * time.Second):
			t.Fatal("leader not done")
		}
		select {
		case <-waiterDone:
		case <-time.After(2 * time.Second):
			t.Fatal("waiter not done")
		}
	})
	t.Run("pool_exhausted", func(t *testing.T) {
		backend := newFakeInstanceBackend(t, echoHandler)
		ts, token, pl := newTestServer(t, backend)
		defer ts.Close()
		resp := doRegister(t, ts, token, "func-exhaust", registerRequest{ImageRef: "img:1.0", MinInstances: 1, MaxInstances: 1})
		_, invTok := mustRegister(t, resp)
		if _, err := pl.Acquire(context.Background(), "func-exhaust", "img:1.0", 1); err != nil {
			t.Fatalf("pre-acquire: %v", err)
		}
		invResp := doInvoke(t, ts, "func-exhaust", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
		defer invResp.Body.Close()
		if invResp.StatusCode != http.StatusServiceUnavailable {
			b, _ := io.ReadAll(invResp.Body)
			t.Fatalf("pool_exhausted: status %d want 503 body %s", invResp.StatusCode, string(b))
		}
		body, _ := io.ReadAll(invResp.Body)
		if !strings.Contains(string(body), "POOL_EXHAUSTED") {
			t.Errorf("pool_exhausted: body %s should contain POOL_EXHAUSTED", string(body))
		}
	})
	t.Run("deadline_exceeded", func(t *testing.T) {
		backend := &ctxBlockingProvisioner{delay: 500 * time.Millisecond}
		dir := t.TempDir()
		addr := &fakeAddresser{path: filepath.Join(dir, "unused.sock")}
		pl := pool.NewManagerWithQueueCap(backend, nil)
		g, err := auth.Generate()
		if err != nil {
			t.Fatalf("cred: %v", err)
		}
		reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
		srv := NewServer("xp4-borda-01:8080", reg, pl, addr, g.Cred, 5*time.Second, newTestCollector(t), newFakeSampler(), 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: addr})
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()
		resp := doRegister(t, ts, g.PlainToken, "func-timeout", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 0, TimeoutMS: 80})
		_, invTok := mustRegister(t, resp)
		invResp := doInvoke(t, ts, "func-timeout", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
		defer invResp.Body.Close()
		if invResp.StatusCode != http.StatusGatewayTimeout {
			b, _ := io.ReadAll(invResp.Body)
			t.Fatalf("deadline: status %d want 504 body %s", invResp.StatusCode, string(b))
		}
		// The acquisition timeout: the deadline expired while waiting
		// for the provisioning, with the function still not executed.
		body, _ := io.ReadAll(invResp.Body)
		if !strings.Contains(string(body), "ACQUIRE_TIMEOUT") {
			t.Errorf("deadline: body %s should contain ACQUIRE_TIMEOUT", string(body))
		}
	})
}

func TestHandleInvoke_CancelReleases(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ready := &hangReadiness{delay: 500 * time.Millisecond}
	pl := pool.NewManagerWithQueueCap(backend, nil)
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("cred: %v", err)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, newTestCollector(t), newFakeSampler(), 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, ready)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp := doRegister(t, ts, g.PlainToken, "func-cancel", registerRequest{ImageRef: "img:1.0", MinInstances: 1, MaxInstances: 1, TimeoutMS: 100})
	_, invTok := mustRegister(t, resp)
	invResp := doInvoke(t, ts, "func-cancel", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusGatewayTimeout {
		b, _ := io.ReadAll(invResp.Body)
		t.Fatalf("cancelRelease: status %d want 504 body %s", invResp.StatusCode, string(b))
	}
	// The instance was not ready in time: the cancellation happened
	// during the wait for the readiness. What this test assures is the
	// release of the slot of the pool, with no leak, and the code of
	// the body is checked to pin the stage.
	body, _ := io.ReadAll(invResp.Body)
	if !strings.Contains(string(body), "INSTANCE_NOT_READY_TIMEOUT") {
		t.Errorf("cancelRelease: body %s want INSTANCE_NOT_READY_TIMEOUT", string(body))
	}
	time.Sleep(50 * time.Millisecond)
	id, err := pl.Acquire(context.Background(), "func-cancel", "img:1.0", 1)
	if err != nil {
		t.Fatalf("after cancel, Acquire should succeed (Release was called), got %v", err)
	}
	if id == "" {
		t.Fatal("acquired empty id")
	}
	pl.Release("func-cancel", id)
}

func TestInvokeResponseTimeIncludesReadiness(t *testing.T) {
	backend := newDelayedBackend(t, echoHandler, 200*time.Millisecond, 20*time.Millisecond)
	ts, token, _ := newDelayedTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "func-readiness", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 5, TimeoutMS: 2000})
	_, invTok := mustRegister(t, resp)
	invResp := doInvoke(t, ts, "func-readiness", invTok, invokeRequest{Input: json.RawMessage(`{"x":1}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusOK {
		b, _ := readAll(invResp)
		t.Fatalf("invoke status = %d, esperado %d; corpo: %s", invResp.StatusCode, http.StatusOK, string(b))
	}
	var result invokeResponse
	if err := json.NewDecoder(invResp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	m := result.ExecutionMetrics
	if m.Phases.ReadinessMS <= 0 {
		t.Errorf("phases.readiness_ms = %d, esperado > 0 (cold start com socket tardio)", m.Phases.ReadinessMS)
	}
	if m.ResponseTimeMS < m.Phases.FunctionMS+m.Phases.ReadinessMS {
		t.Errorf("response_time_ms (%d) < phases.function_ms (%d) + phases.readiness_ms (%d): readiness deveria contribuir para o total", m.ResponseTimeMS, m.Phases.FunctionMS, m.Phases.ReadinessMS)
	}
	if m.ResponseTimeMS != m.Phases.AcquireWaitMS+m.Phases.ReadinessMS+m.Phases.FunctionMS+m.Phases.OverheadMS {
		t.Errorf("invariante phases quebrado: %d != %d+%d+%d+%d", m.ResponseTimeMS, m.Phases.AcquireWaitMS, m.Phases.ReadinessMS, m.Phases.FunctionMS, m.Phases.OverheadMS)
	}
	if !m.ColdStart {
		t.Errorf("cold_start deveria ser true para provision + readiness tardia")
	}
	if m.InstanceID == "" {
		t.Error("instance_id deveria estar preenchido")
	}
}

func TestInvokeResponseTimeIncludesQueue(t *testing.T) {
	blockCh := make(chan struct{})
	dir := t.TempDir()
	backend := &blockingAddrProvisioner{dir: dir, block: blockCh, listeners: make(map[string]net.Listener)}
	pl := pool.NewManagerWithQueueCap(backend, func(string) int { return 4 })
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("cred: %v", err)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, newTestCollector(t), newFakeSampler(), 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp := doRegister(t, ts, g.PlainToken, "func-queue", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 0})
	_, invTok := mustRegister(t, resp)
	leaderDone := make(chan error, 1)
	go func() {
		_, err := pl.AcquireWait(context.Background(), "func-queue", "img:1.0", 0)
		leaderDone <- err
	}()
	time.Sleep(50 * time.Millisecond)
	type invokeResult struct {
		resp *http.Response
		err  error
	}
	invCh := make(chan invokeResult, 1)
	go func() {
		data, _ := json.Marshal(invokeRequest{Input: json.RawMessage(`{"q":1}`)})
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-queue/invoke", bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(auth.DefaultHeaderName, invTok)
		resp, err := http.DefaultClient.Do(req)
		invCh <- invokeResult{resp, err}
	}()
	time.Sleep(50 * time.Millisecond)
	close(blockCh)
	ir := <-invCh
	if ir.err != nil {
		t.Fatalf("invoke: %v", ir.err)
	}
	defer ir.resp.Body.Close()
	if ir.resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(ir.resp.Body)
		t.Fatalf("invoke status = %d, esperado %d; corpo: %s", ir.resp.StatusCode, http.StatusOK, string(b))
	}
	var result invokeResponse
	if err := json.NewDecoder(ir.resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	m := result.ExecutionMetrics
	if m.Phases.AcquireWaitMS <= 0 {
		t.Errorf("phases.acquire_wait_ms = %d, esperado > 0 (esperou na fila atrás do líder)", m.Phases.AcquireWaitMS)
	}
	if m.ResponseTimeMS < m.Phases.FunctionMS+m.Phases.AcquireWaitMS {
		t.Errorf("response_time_ms (%d) < phases.function_ms (%d) + phases.acquire_wait_ms (%d): fila deveria contribuir para o total", m.ResponseTimeMS, m.Phases.FunctionMS, m.Phases.AcquireWaitMS)
	}
	if m.ResponseTimeMS != m.Phases.AcquireWaitMS+m.Phases.ReadinessMS+m.Phases.FunctionMS+m.Phases.OverheadMS {
		t.Errorf("invariante phases quebrado: %d != %d+%d+%d+%d", m.ResponseTimeMS, m.Phases.AcquireWaitMS, m.Phases.ReadinessMS, m.Phases.FunctionMS, m.Phases.OverheadMS)
	}
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader not done")
	}
}

// ---------------------------------------------------------------------------
// Tests — profiler_interval
// ---------------------------------------------------------------------------

func TestStartProfilerNonPositiveInterval(t *testing.T) {
	tests := []struct {
		name             string
		interval         time.Duration
		wantProfile      bool
		wantSamplerCalls int
	}{
		{name: "interval_zero", interval: 0, wantProfile: false, wantSamplerCalls: 0},
		{name: "interval_negative", interval: -100 * time.Millisecond, wantProfile: false, wantSamplerCalls: 0},
		{name: "interval_valid_200ms", interval: 200 * time.Millisecond, wantProfile: true, wantSamplerCalls: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stat := dir + "/stat"
			meminfo := dir + "/meminfo"
			cgroup := dir + "/cgroup"
			if err := writeTestFixtures(stat, meminfo, cgroup); err != nil {
				t.Fatal(err)
			}
			collector := telemetry.NewCollector("xp4-borda-01", true, cgroup, stat, meminfo, "", "")
			if !collector.Enabled() {
				t.Fatal("collector should be enabled")
			}
			g, err := auth.Generate()
			if err != nil {
				t.Fatalf("auth generate: %v", err)
			}
			backend := newFakeInstanceBackend(t, echoHandler)
			sampler := newFakeSampler(
				telemetry.InvocationSample{CPUUsageUSec: 1_000_000, MemoryBytes: 100},
				telemetry.InvocationSample{CPUUsageUSec: 3_000_000, MemoryBytes: 500},
			)
			reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
			pl := pool.NewManagerWithQueueCap(backend, nil)
			srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, collector, sampler, 64<<10, 8<<20, 10*time.Second, 120*time.Second, tc.interval, &SocketReadinessChecker{Addresser: backend})
			ts := httptest.NewServer(srv.Handler())
			defer ts.Close()
			body, _ := json.Marshal(registerRequest{ImageRef: "img:1.0", MinInstances: 1})
			req, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-interval", bytes.NewReader(body))
			req.Header.Set(auth.DefaultHeaderName, g.PlainToken)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			var regEnv registerResponse
			if err := json.NewDecoder(resp.Body).Decode(&regEnv); err != nil {
				resp.Body.Close()
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("register status %d want 201", resp.StatusCode)
			}
			invBody, _ := json.Marshal(invokeRequest{Input: json.RawMessage(`{"x":1}`)})
			invReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-interval/invoke", bytes.NewReader(invBody))
			invReq.Header.Set("Content-Type", "application/json")
			invReq.Header.Set(auth.DefaultHeaderName, regEnv.InvokeToken)
			invResp, err := http.DefaultClient.Do(invReq)
			if err != nil {
				t.Fatal(err)
			}
			defer invResp.Body.Close()
			if invResp.StatusCode != http.StatusOK {
				b, _ := io.ReadAll(invResp.Body)
				t.Fatalf("invoke status %d want 200 body %s", invResp.StatusCode, b)
			}
			data, err := io.ReadAll(invResp.Body)
			if err != nil {
				t.Fatal(err)
			}
			bodyStr := string(data)
			if tc.wantProfile {
				if !strings.Contains(bodyStr, "resource_profile") {
					t.Errorf("JSON should contain resource_profile for valid interval: %s", bodyStr)
				}
				var result invokeResponse
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatal(err)
				}
				if result.ExecutionMetrics.ResourceProfile == nil {
					t.Errorf("ResourceProfile should be present for valid interval")
				} else {
					if result.ExecutionMetrics.ResourceProfile.MemPeakBytes != 500 {
						t.Errorf("mem_peak_bytes = %d, want 500", result.ExecutionMetrics.ResourceProfile.MemPeakBytes)
					}
				}
			} else {
				if strings.Contains(bodyStr, "resource_profile") {
					t.Errorf("JSON should NOT contain resource_profile for non-positive interval: %s", bodyStr)
				}
				var result invokeResponse
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatal(err)
				}
				if result.ExecutionMetrics.ResourceProfile != nil {
					t.Errorf("ResourceProfile should be nil for non-positive interval, got %+v", result.ExecutionMetrics.ResourceProfile)
				}
			}
			sampler.mu.Lock()
			calls := sampler.calls
			sampler.mu.Unlock()
			if tc.wantSamplerCalls > 0 && calls < tc.wantSamplerCalls {
				t.Errorf("sampler calls = %d, want at least %d for valid interval", calls, tc.wantSamplerCalls)
			}
			if tc.wantSamplerCalls == 0 && calls != 0 {
				t.Errorf("sampler calls = %d, want 0 for non-positive interval", calls)
			}
			var result invokeResponse
			if err := json.Unmarshal(data, &result); err != nil {
				t.Fatal(err)
			}
			if string(result.Result) != `{"x":1}` {
				t.Errorf("result = %s want echoed input", result.Result)
			}
		})
	}
}

func TestStartProfilerNonPositiveIntervalNoGoroutineLeak(t *testing.T) {
	dir := t.TempDir()
	stat := dir + "/stat"
	meminfo := dir + "/meminfo"
	cgroup := dir + "/cgroup"
	if err := writeTestFixtures(stat, meminfo, cgroup); err != nil {
		t.Fatal(err)
	}
	collector := telemetry.NewCollector("xp4-borda-01", true, cgroup, stat, meminfo, "", "")
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("auth generate: %v", err)
	}
	backend := newFakeInstanceBackend(t, echoHandler)
	sampler := newFakeSampler()
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(backend, nil)
	srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, collector, sampler, 64<<10, 8<<20, 10*time.Second, 120*time.Second, 0, &SocketReadinessChecker{Addresser: backend})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	body, _ := json.Marshal(registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-leak", bytes.NewReader(body))
	req.Header.Set(auth.DefaultHeaderName, g.PlainToken)
	resp, _ := http.DefaultClient.Do(req)
	var regEnv registerResponse
	json.NewDecoder(resp.Body).Decode(&regEnv)
	resp.Body.Close()
	before := runtime.NumGoroutine()
	invBody, _ := json.Marshal(invokeRequest{Input: json.RawMessage(`{"x":1}`)})
	invReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-leak/invoke", bytes.NewReader(invBody))
	invReq.Header.Set("Content-Type", "application/json")
	invReq.Header.Set(auth.DefaultHeaderName, regEnv.InvokeToken)
	invResp, _ := http.DefaultClient.Do(invReq)
	invResp.Body.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if n := runtime.NumGoroutine(); n <= before+1 {
			break
		} else if time.Now().After(deadline) {
			buf := make([]byte, 64<<10)
			buf = buf[:runtime.Stack(buf, true)]
			t.Errorf("goroutines after request = %d, before = %d (tolerance ±1): possible sampler goroutine leak\nstacks:\n%s", runtime.NumGoroutine(), before, buf)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	sampler.mu.Lock()
	calls := sampler.calls
	sampler.mu.Unlock()
	if calls != 0 {
		t.Errorf("sampler calls = %d, want 0 when interval=0", calls)
	}
}

// ---------------------------------------------------------------------------
// Tests — disabled (telemetry disabled)
// ---------------------------------------------------------------------------

func TestInvocationWithoutProfileWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	collector := telemetry.NewCollector("n", false, "", "", "", "", "")
	if collector.Enabled() {
		t.Fatal("collector should be disabled")
	}
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("auth generate: %v", err)
	}
	backend := newFakeInstanceBackend(t, echoHandler)
	sampler := newFakeSampler()
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(backend, nil)
	srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5_000_000_000, collector, sampler, 64<<10, 8<<20, 10_000_000_000, 120_000_000_000, 200_000_000, &SocketReadinessChecker{Addresser: backend})
	if strings.Contains(dir, "") {
		t.Log(dir)
	}
	if got := srv.telemetryActive(); got != false {
		t.Fatalf("telemetryActive() = %v, want false", got)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	body, _ := json.Marshal(registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-disabled", bytes.NewReader(body))
	req.Header.Set(auth.DefaultHeaderName, g.PlainToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var regEnv registerResponse
	if err := json.NewDecoder(resp.Body).Decode(&regEnv); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("register status %d want 201", resp.StatusCode)
	}
	invBody, _ := json.Marshal(invokeRequest{Input: json.RawMessage(`{"x":1}`)})
	invReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-disabled/invoke", bytes.NewReader(invBody))
	invReq.Header.Set("Content-Type", "application/json")
	invReq.Header.Set(auth.DefaultHeaderName, regEnv.InvokeToken)
	invResp, err := http.DefaultClient.Do(invReq)
	if err != nil {
		t.Fatal(err)
	}
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(invResp.Body)
		t.Fatalf("invoke status %d want 200 body %s", invResp.StatusCode, b)
	}
	data, err := io.ReadAll(invResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "resource_profile") {
		t.Errorf("JSON should not contain resource_profile when disabled: %s", data)
	}
	if strings.Contains(string(data), `"telemetry"`) {
		t.Errorf("JSON should not contain telemetry when disabled: %s", data)
	}
	var result invokeResponse
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.ExecutionMetrics.ResourceProfile != nil {
		t.Errorf("ResourceProfile should be nil when disabled, got %+v", result.ExecutionMetrics.ResourceProfile)
	}
	if result.ExecutionMetrics.Telemetry != nil {
		t.Errorf("Telemetry should be nil when disabled, got %+v", result.ExecutionMetrics.Telemetry)
	}
	if string(result.Result) != `{"x":1}` {
		t.Errorf("result = %s want echoed input", result.Result)
	}
	sampler.mu.Lock()
	calls := sampler.calls
	sampler.mu.Unlock()
	if calls != 0 {
		t.Errorf("sampler calls = %d, want 0 when telemetry disabled", calls)
	}
}

// ---------------------------------------------------------------------------
// Tests — profiler_stop
// ---------------------------------------------------------------------------

func TestProfilerStopFinalSampleWithCanceledParent(t *testing.T) {
	sampler := newCtxSensitiveSampler()
	parentCtx, parentCancel := context.WithCancel(context.Background())
	sampleCtx, sampleCancel := context.WithCancel(context.WithoutCancel(parentCtx))
	defer sampleCancel()
	tracker := telemetry.NewProfileTracker(1)
	t0, err := sampler.SampleMetrics(sampleCtx, "test-instance")
	if err != nil {
		t.Fatalf("t0 sample falhou: %v", err)
	}
	tracker.StartMono(t0, ntime.Now())
	parentCancel()
	finalSampleCtx := context.WithoutCancel(sampleCtx)
	if finalSampleCtx.Err() != nil {
		t.Fatalf("context.WithoutCancel(sampleCtx) deveria estar ativo, mas está: %v", finalSampleCtx.Err())
	}
	final, err := sampler.SampleMetrics(finalSampleCtx, "test-instance")
	if err != nil {
		t.Fatalf("amostra final falhou (regressão!): %v", err)
	}
	tracker.ObserveMono(final, ntime.Now())
	tracker.FinishMono(ntime.Now())
	profile, ok := tracker.Profile()
	if !ok {
		t.Fatal("tracker.Profile() retornou false")
	}
	if profile.SampleCount != 2 {
		t.Errorf("SampleCount = %d, esperado 2 (t0 + final)", profile.SampleCount)
	}
	if len(sampler.callLog) != 2 {
		t.Errorf("callLog length = %d, esperado 2: %v", len(sampler.callLog), sampler.callLog)
	}
	if sampler.callLog[0] != "OK" {
		t.Errorf("primeira chamada deveria ser OK: %v", sampler.callLog)
	}
	if sampler.callLog[1] != "OK" {
		t.Errorf("segunda chamada (final) deveria ser OK com WithoutCancel: %v", sampler.callLog)
	}
}

func TestProfilerStopFinalSampleWithCanceledParent_ChildContextFallback(t *testing.T) {
	deadlineSampler := &deadlineRequiredSampler{}
	parentCtx, parentCancel := context.WithCancel(context.Background())
	t0Ctx, t0Cancel := context.WithTimeout(context.WithoutCancel(parentCtx), 100*time.Millisecond)
	defer t0Cancel()
	sampleCtx, sampleCancel := context.WithCancel(context.WithoutCancel(parentCtx))
	defer sampleCancel()
	tracker := telemetry.NewProfileTracker(1)
	t0, err := deadlineSampler.SampleMetrics(t0Ctx, "test-instance")
	if err != nil {
		t.Fatalf("t0 sample falhou: %v", err)
	}
	tracker.StartMono(t0, ntime.Now())
	parentCancel()
	childCtx, childCancel := context.WithTimeout(context.WithoutCancel(sampleCtx), 100*time.Millisecond)
	defer childCancel()
	final, err := deadlineSampler.SampleMetrics(childCtx, "test-instance")
	if err != nil {
		t.Fatalf("amostra final com child context falhou: %v", err)
	}
	tracker.ObserveMono(final, ntime.Now())
	tracker.FinishMono(ntime.Now())
	profile, ok := tracker.Profile()
	if !ok {
		t.Fatal("tracker.Profile() retornou false")
	}
	if profile.SampleCount != 2 {
		t.Errorf("SampleCount = %d, esperado 2", profile.SampleCount)
	}
}

func TestProfilerStopRealStopWithCanceledParent(t *testing.T) {
	sampler := newCtxSensitiveSampler()
	parentCtx, parentCancel := context.WithCancel(context.Background())
	sampleCtx, cancel := context.WithCancel(context.WithoutCancel(parentCtx))
	tracker := telemetry.NewProfileTracker(1)
	t0, err := sampler.SampleMetrics(sampleCtx, "test-instance")
	if err != nil {
		t.Fatalf("t0 sample falhou: %v", err)
	}
	tracker.StartMono(t0, ntime.Now())
	parentCancel()
	prof := &invokeProfiler{
		tracker:    tracker,
		done:       make(chan struct{}),
		sampleCtx:  sampleCtx,
		cancel:     cancel,
		sampler:    sampler,
		interval:   200 * time.Millisecond,
		instanceID: "test-instance",
	}
	prof.stop()
	profile, ok := tracker.Profile()
	if !ok {
		t.Fatal("tracker.Profile() retornou false após stop()")
	}
	if profile.SampleCount != 2 {
		t.Errorf("SampleCount = %d, esperado 2 (t0 + final via stop real)", profile.SampleCount)
	}
	if len(sampler.callLog) != 2 || sampler.callLog[0] != "OK" || sampler.callLog[1] != "OK" {
		t.Errorf("callLog = %v, esperado [OK OK]", sampler.callLog)
	}
	prof.stop()
	profile2, ok := tracker.Profile()
	if !ok {
		t.Fatal("tracker.Profile() retornou false após segundo stop()")
	}
	if profile2.SampleCount != 2 {
		t.Errorf("SampleCount após segundo stop = %d, esperado 2 (idempotente)", profile2.SampleCount)
	}
	if len(sampler.callLog) != 2 {
		t.Errorf("callLog após segundo stop = %v, esperado [OK OK]", sampler.callLog)
	}
}

// ---------------------------------------------------------------------------
// Tests of the idle timeout of the server
// ---------------------------------------------------------------------------

func TestRegisterIdleTimeout_Success(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	body := map[string]any{"image_ref": "img:1.0", "min_instances": 1, "idle_timeout": "5m"}
	resp := doWithMap(t, ts.URL, token, http.MethodPost, "/function/func-idle-ok", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST with idle_timeout 5m: status=%d want 201 body=%s", resp.StatusCode, b)
	}
	var regEnv registerResponse
	if err := json.NewDecoder(resp.Body).Decode(&regEnv); err != nil {
		t.Fatal(err)
	}
	fn := regEnv.Function
	if fn.IdleTimeout != config.Duration(5*time.Minute) {
		t.Errorf("IdleTimeout = %v want %v", time.Duration(fn.IdleTimeout), 5*time.Minute)
	}
	getResp := doGet(t, ts, "func-idle-ok", regEnv.InvokeToken)
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status=%d want 200", getResp.StatusCode)
	}
	raw, _ := io.ReadAll(getResp.Body)
	if !strings.Contains(string(raw), `"idle_timeout":"5m0s"`) {
		t.Errorf("GET body should contain idle_timeout 5m0s, got %s", raw)
	}
	var fn2 registry.Function
	if err := json.Unmarshal(raw, &fn2); err != nil {
		t.Fatal(err)
	}
	if fn2.IdleTimeout != config.Duration(5*time.Minute) {
		t.Errorf("GET IdleTimeout = %v want 5m", time.Duration(fn2.IdleTimeout))
	}
}

func TestRegisterIdleTimeout_BadFormat(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	body := map[string]any{"image_ref": "img:1.0", "min_instances": 1, "idle_timeout": "bad"}
	resp := doWithMap(t, ts.URL, token, http.MethodPost, "/function/func-bad-format", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("bad idle_timeout: status=%d want 400 body=%s", resp.StatusCode, b)
	}
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "INVALID_IDLE_TIMEOUT") {
		t.Errorf("body should contain INVALID_IDLE_TIMEOUT, got %s", raw)
	}
}

func TestRegisterIdleTimeout_Negative(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	body := map[string]any{"image_ref": "img:1.0", "min_instances": 1, "idle_timeout": "-1s"}
	resp := doWithMap(t, ts.URL, token, http.MethodPost, "/function/func-bad-neg", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("negative idle_timeout: status=%d want 400 body=%s", resp.StatusCode, b)
	}
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "INVALID_IDLE_TIMEOUT") {
		t.Errorf("body should contain INVALID_IDLE_TIMEOUT, got %s", raw)
	}
}

func TestRegisterIdleTimeout_Over24h(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	body := map[string]any{"image_ref": "img:1.0", "min_instances": 1, "idle_timeout": "25h"}
	resp := doWithMap(t, ts.URL, token, http.MethodPost, "/function/func-bad-over", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("over 24h idle_timeout: status=%d want 400 body=%s", resp.StatusCode, b)
	}
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "INVALID_IDLE_TIMEOUT") {
		t.Errorf("body should contain INVALID_IDLE_TIMEOUT, got %s", raw)
	}
}

func TestRegisterIdleTimeout_DefaultZero(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	body := map[string]any{"image_ref": "img:1.0", "min_instances": 1}
	resp := doWithMap(t, ts.URL, token, http.MethodPost, "/function/func-default-zero", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST without idle_timeout: status=%d want 201 body=%s", resp.StatusCode, b)
	}
	var regEnv0 registerResponse
	if err := json.NewDecoder(resp.Body).Decode(&regEnv0); err != nil {
		t.Fatal(err)
	}
	fn := regEnv0.Function
	if fn.IdleTimeout != 0 {
		t.Errorf("IdleTimeout = %v want 0 (default)", time.Duration(fn.IdleTimeout))
	}
	getResp := doGet(t, ts, "func-default-zero", regEnv0.InvokeToken)
	defer getResp.Body.Close()
	raw, _ := io.ReadAll(getResp.Body)
	var f registry.Function
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if f.IdleTimeout != 0 {
		t.Errorf("GET IdleTimeout = %v want 0, body=%s", time.Duration(f.IdleTimeout), raw)
	}
}

func TestUpdateIdleTimeout_Success(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	body := map[string]any{"image_ref": "img:1.0", "min_instances": 1, "idle_timeout": "5m"}
	resp := doWithMap(t, ts.URL, token, http.MethodPost, "/function/func-update-idle", body)
	var regEnvU registerResponse
	if err := json.NewDecoder(resp.Body).Decode(&regEnvU); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("initial POST status=%d want 201", resp.StatusCode)
	}
	body2 := map[string]any{"image_ref": "img:1.0", "min_instances": 1, "idle_timeout": "10m"}
	resp2 := doWithMap(t, ts.URL, token, http.MethodPut, "/function/func-update-idle", body2)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("PUT status=%d want 200 body=%s", resp2.StatusCode, b)
	}
	var updated registry.Function
	if err := json.NewDecoder(resp2.Body).Decode(&updated); err != nil {
		t.Fatal(err)
	}
	if time.Duration(updated.IdleTimeout) != 10*time.Minute {
		t.Errorf("PUT IdleTimeout = %v want 10m", time.Duration(updated.IdleTimeout))
	}
	getResp := doGet(t, ts, "func-update-idle", regEnvU.InvokeToken)
	defer getResp.Body.Close()
	raw, _ := io.ReadAll(getResp.Body)
	if !strings.Contains(string(raw), `"idle_timeout":"10m0s"`) {
		t.Errorf("GET after PUT should contain 10m0s, got %s", raw)
	}
}

func TestUpdateIdleTimeout_Invalid(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	body := map[string]any{"image_ref": "img:1.0", "min_instances": 1}
	resp := doWithMap(t, ts.URL, token, http.MethodPost, "/function/func-upd-bad", body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("initial POST status=%d want 201", resp.StatusCode)
	}
	body2 := map[string]any{"image_ref": "img:1.0", "min_instances": 1, "idle_timeout": "bad"}
	resp2 := doWithMap(t, ts.URL, token, http.MethodPut, "/function/func-upd-bad", body2)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("PUT bad idle_timeout: status=%d want 400 body=%s", resp2.StatusCode, b)
	}
	raw, _ := io.ReadAll(resp2.Body)
	if !strings.Contains(string(raw), "INVALID_IDLE_TIMEOUT") {
		t.Errorf("body should contain INVALID_IDLE_TIMEOUT, got %s", raw)
	}
}

func TestUpdateIdleTimeout_NegativeAndOver24h(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"negative", "-1s"},
		{"over24h", "25h"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := newFakeInstanceBackend(t, echoHandler)
			ts, token, _ := newTestServer(t, backend)
			defer ts.Close()
			body := map[string]any{"image_ref": "img:1.0", "min_instances": 1}
			resp := doWithMap(t, ts.URL, token, http.MethodPost, "/function/func-upd-"+tc.name, body)
			resp.Body.Close()
			body2 := map[string]any{"image_ref": "img:1.0", "min_instances": 1, "idle_timeout": tc.value}
			resp2 := doWithMap(t, ts.URL, token, http.MethodPut, "/function/func-upd-"+tc.name, body2)
			defer resp2.Body.Close()
			if resp2.StatusCode != http.StatusBadRequest {
				b, _ := io.ReadAll(resp2.Body)
				t.Fatalf("value %q: status=%d want 400 body=%s", tc.value, resp2.StatusCode, b)
			}
			raw, _ := io.ReadAll(resp2.Body)
			if !strings.Contains(string(raw), "INVALID_IDLE_TIMEOUT") {
				t.Errorf("value %q: body should contain INVALID_IDLE_TIMEOUT, got %s", tc.value, raw)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests of the concurrency of the server
// ---------------------------------------------------------------------------

func TestConcurrency_PoolExhausted_HTTP503(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, pl := newTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "func-conc-503", registerRequest{ImageRef: "img:1.0", MinInstances: 1, MaxInstances: 1})
	_, invTok := mustRegister(t, resp)
	busyID, err := pl.Acquire(context.Background(), "func-conc-503", "img:1.0", 1)
	if err != nil {
		t.Fatalf("pre-acquire: %v", err)
	}
	defer pl.Release("func-conc-503", busyID)
	if ql := pl.QueueLen("func-conc-503"); ql != 0 {
		t.Fatalf("precondition: QueueLen before invoke = %d, want 0", ql)
	}
	start := time.Now()
	invResp := doInvoke(t, ts, "func-conc-503", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
	elapsed := time.Since(start)
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusServiceUnavailable {
		b, _ := io.ReadAll(invResp.Body)
		t.Fatalf("want 503, got %d body %s", invResp.StatusCode, string(b))
	}
	body, _ := io.ReadAll(invResp.Body)
	if !strings.Contains(string(body), "POOL_EXHAUSTED") {
		t.Errorf("body %s should contain POOL_EXHAUSTED", string(body))
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("POOL_EXHAUSTED HTTP took %v, want <10ms (must not enqueue)", elapsed)
	}
	if ql := pl.QueueLen("func-conc-503"); ql != 0 {
		t.Errorf("QueueLen after POOL_EXHAUSTED = %d, want 0 (never enqueues)", ql)
	}
}

func TestConcurrency_CancelCleansWaiter_HTTP(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, pl := newTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "func-conc-cancel-http", registerRequest{ImageRef: "img:1.0", MinInstances: 1, MaxInstances: 1})
	_, invTok := mustRegister(t, resp)
	busyID, err := pl.Acquire(context.Background(), "func-conc-cancel-http", "img:1.0", 1)
	if err != nil {
		t.Fatalf("pre-acquire: %v", err)
	}
	start := time.Now()
	invResp := doInvoke(t, ts, "func-conc-cancel-http", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
	elapsed := time.Since(start)
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusServiceUnavailable {
		b, _ := io.ReadAll(invResp.Body)
		t.Fatalf("want 503, got %d body %s", invResp.StatusCode, string(b))
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("second invoke took %v, want <10ms", elapsed)
	}
	pl.Release("func-conc-cancel-http", busyID)
	time.Sleep(10 * time.Millisecond)
	okResp := doInvoke(t, ts, "func-conc-cancel-http", invTok, invokeRequest{Input: json.RawMessage(`{"ok":1}`)})
	defer okResp.Body.Close()
	if okResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(okResp.Body)
		t.Fatalf("after release, invoke should succeed: status %d body %s", okResp.StatusCode, string(b))
	}
}

// ---------------------------------------------------------------------------
// Tests of the server: standalone end to end, invocation, registry and
// integration of the profiler
// ---------------------------------------------------------------------------

func TestStandaloneEndToEnd(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, pl := newTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "alpr-deteccao-placa", registerRequest{ImageRef: "registry.local/alpr:1.0", MinInstances: 2})
	fn, invTok := mustRegister(t, resp)
	wantURL := "xp4-borda-01:8080/function/alpr-deteccao-placa"
	if fn.ResourceURL != wantURL {
		t.Errorf("ResourceURL = %q, esperado %q", fn.ResourceURL, wantURL)
	}
	if got := pl.Count("alpr-deteccao-placa"); got != 2 {
		t.Errorf("pool.Count() = %d, esperado 2 (min_instances)", got)
	}
	getResp := doGet(t, ts, "alpr-deteccao-placa", invTok)
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("GET status = %d, esperado %d", getResp.StatusCode, http.StatusOK)
	}
	invResp := doInvoke(t, ts, "alpr-deteccao-placa", invTok, invokeRequest{Input: json.RawMessage(`{"placa":"ABC1D23"}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(invResp.Body)
		t.Fatalf("invoke status = %d, esperado %d; corpo: %s", invResp.StatusCode, http.StatusOK, body)
	}
	var result invokeResponse
	if err := json.NewDecoder(invResp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if string(result.Result) != `{"placa":"ABC1D23"}` {
		t.Errorf("result = %s, esperado o input ecoado de volta", result.Result)
	}
}

func TestInvokeResponseCarriesPiggybackTelemetry(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	_, invTok := mustRegister(t, doRegister(t, ts, token, "func-observada", registerRequest{ImageRef: "img:1.0", MinInstances: 1}))
	invResp := doInvoke(t, ts, "func-observada", invTok, invokeRequest{Input: json.RawMessage(`{"x":1}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(invResp.Body)
		t.Fatalf("invoke status = %d, esperado %d; corpo: %s", invResp.StatusCode, http.StatusOK, body)
	}
	body, err := io.ReadAll(invResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var result invokeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	metrics := result.ExecutionMetrics
	if metrics.ResponseTimeMS < 0 || metrics.Phases.FunctionMS < 0 {
		t.Errorf("tempos negativos (response=%d function=%d)", metrics.ResponseTimeMS, metrics.Phases.FunctionMS)
	}
	if metrics.Phases.FunctionMS > metrics.ResponseTimeMS {
		t.Errorf("function_time_ms (%d) maior que response_time_ms (%d)", metrics.Phases.FunctionMS, metrics.ResponseTimeMS)
	}
	if metrics.Phases.ReadinessMS > 0 && metrics.ResponseTimeMS < metrics.Phases.FunctionMS+metrics.Phases.ReadinessMS {
		t.Errorf("response_time_ms (%d) < function_time_ms (%d) + readiness_ms (%d): readiness deveria contribuir para o total", metrics.ResponseTimeMS, metrics.Phases.FunctionMS, metrics.Phases.ReadinessMS)
	}
	if metrics.StartedAt.IsZero() || metrics.EndedAt.IsZero() {
		t.Errorf("started_at/ended_at deveriam estar preenchidos")
	}
	if metrics.InstanceID == "" {
		t.Error("instance_id deveria estar preenchido")
	}
	if metrics.Telemetry == nil {
		t.Fatalf("telemetria piggyback ausente na resposta: %s", body)
	}
	if metrics.Telemetry.Source != "" {
		t.Errorf("telemetry.source = %q, esperado omitido (empty) na forma embedded piggyback", metrics.Telemetry.Source)
	}
	if metrics.Telemetry.NodeID != "xp4-borda-01" {
		t.Errorf("telemetry.node_id = %q, esperado xp4-borda-01", metrics.Telemetry.NodeID)
	}
	if metrics.Telemetry.Timestamp.IsZero() {
		t.Error("telemetry.timestamp deveria estar preenchido")
	}
	if metrics.ResponseTimeMS != metrics.Phases.AcquireWaitMS+metrics.Phases.ReadinessMS+metrics.Phases.FunctionMS+metrics.Phases.OverheadMS {
		t.Errorf("invariante de fases quebrado: response_time_ms %d != sum phases %d", metrics.ResponseTimeMS, metrics.Phases.AcquireWaitMS+metrics.Phases.ReadinessMS+metrics.Phases.FunctionMS+metrics.Phases.OverheadMS)
	}
	for _, chave := range []string{"cpu_pressure", "memory_pressure"} {
		if strings.Contains(string(body), chave) {
			t.Errorf("JSON %s não deveria conter a chave %q (PSI indisponível no fixture)", body, chave)
		}
	}
}

func TestStandaloneRejectsUnauthenticatedRegister(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, _, _ := newTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, "", "func-x", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, esperado %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestStandaloneRejectsWrongCredential(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, _, _ := newTestServer(t, backend)
	defer ts.Close()
	tokenErrado := strings.Repeat("ab", 32)
	resp := doRegister(t, ts, tokenErrado, "func-y", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, esperado %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestStandaloneBlocksDuplicateFunctionName(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	body := registerRequest{ImageRef: "img:1.0", MinInstances: 1}
	first := doRegister(t, ts, token, "func-dup", body)
	first.Body.Close()
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("1º registro: status = %d, esperado %d", first.StatusCode, http.StatusCreated)
	}
	second := doRegister(t, ts, token, "func-dup", body)
	defer second.Body.Close()
	if second.StatusCode != http.StatusConflict {
		t.Errorf("2º registro (duplicado): status = %d, esperado %d", second.StatusCode, http.StatusConflict)
	}
}

func TestStandaloneGetMissingFunctionReturns401(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/function/nao-existe")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, esperado %d", resp.StatusCode, http.StatusUnauthorized)
	}
	_, invTok := mustRegister(t, doRegister(t, ts, token, "outra", registerRequest{ImageRef: "img:1.0", MinInstances: 0}))
	getResp := doGet(t, ts, "nao-existe", invTok)
	defer getResp.Body.Close()
	// Without the function there is no way to verify the binding of the
	// token, so a nonexistent function is denied with 401 and never
	// with 404.
	if getResp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, esperado %d (auth antes do lookup)", getResp.StatusCode, http.StatusUnauthorized)
	}
}

func TestStandaloneGetRejectsCrossFunctionToken(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	_, _ = mustRegister(t, doRegister(t, ts, token, "func-a", registerRequest{ImageRef: "img:1.0", MinInstances: 0}))
	_, tokB := mustRegister(t, doRegister(t, ts, token, "func-b", registerRequest{ImageRef: "img:1.0", MinInstances: 0}))
	getResp := doGet(t, ts, "func-a", tokB)
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, esperado %d (token cruzado)", getResp.StatusCode, http.StatusUnauthorized)
	}
}

func TestInvokeMissingFunctionReturns401(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	resp := doInvoke(t, ts, "nao-existe", "", invokeRequest{Input: json.RawMessage(`{}`)})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, esperado %d", resp.StatusCode, http.StatusUnauthorized)
	}
	_, invTok := mustRegister(t, doRegister(t, ts, token, "outra", registerRequest{ImageRef: "img:1.0", MinInstances: 0}))
	resp2 := doInvoke(t, ts, "nao-existe", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
	defer resp2.Body.Close()
	// The authentication comes before the lookup: 401, never 404.
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, esperado %d (auth antes do lookup)", resp2.StatusCode, http.StatusUnauthorized)
	}
}

func TestInvokeReturns503WhenPoolExhausted(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, pl := newTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "func-cheia", registerRequest{ImageRef: "img:1.0", MinInstances: 1, MaxInstances: 1})
	_, invTok := mustRegister(t, resp)
	if _, err := pl.Acquire(context.Background(), "func-cheia", "img:1.0", 1); err != nil {
		t.Fatal(err)
	}
	invResp := doInvoke(t, ts, "func-cheia", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, esperado %d", invResp.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestNoSLOTimeout(t *testing.T) {
	backend := newFakeInstanceBackend(t, slowHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "func-lenta", registerRequest{ImageRef: "img:1.0", MinInstances: 1, TimeoutMS: 100})
	_, invTok := mustRegister(t, resp)
	invResp := doInvoke(t, ts, "func-lenta", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status = %d, esperado %d", invResp.StatusCode, http.StatusGatewayTimeout)
	}
}

func TestInvokeRejectsNonJSONFunctionOutput(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"texto cru", textHandler},
		{"corpo vazio", emptyHandler},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := newFakeInstanceBackend(t, tc.handler)
			ts, token, _ := newTestServer(t, backend)
			defer ts.Close()
			resp := doRegister(t, ts, token, "func-texto", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
			_, invTok := mustRegister(t, resp)
			invResp := doInvoke(t, ts, "func-texto", invTok, invokeRequest{Input: json.RawMessage(`{"chave":"valor"}`)})
			defer invResp.Body.Close()
			if invResp.StatusCode != http.StatusBadGateway {
				body, _ := io.ReadAll(invResp.Body)
				t.Fatalf("status = %d, esperado %d; corpo: %s", invResp.StatusCode, http.StatusBadGateway, body)
			}
			if ct := invResp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, esperado application/json", ct)
			}
			var errResp errorResponse
			if err := json.NewDecoder(invResp.Body).Decode(&errResp); err != nil {
				t.Fatalf("corpo da resposta não é o JSON de erro esperado: %v", err)
			}
			if errResp.Code != "INVALID_FUNCTION_OUTPUT" {
				t.Errorf("code = %q, esperado %q", errResp.Code, "INVALID_FUNCTION_OUTPUT")
			}
		})
	}
}

func TestRegisterRollsBackOnPoolProvisionFailure(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	backend.failProvision = true
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	first := doRegister(t, ts, token, "func-quebrada", registerRequest{ImageRef: "imagem-invalida:1.0", MinInstances: 1})
	defer first.Body.Close()
	if first.StatusCode != http.StatusInternalServerError {
		t.Fatalf("1ª tentativa: status = %d, esperado %d", first.StatusCode, http.StatusInternalServerError)
	}
	_, witnessTok := mustRegister(t, doRegister(t, ts, token, "func-testemunha", registerRequest{ImageRef: "img:1.0", MinInstances: 0}))
	getResp := doGet(t, ts, "func-quebrada", witnessTok)
	defer getResp.Body.Close()
	// A nonexistent function is denied with 401. The proof that the
	// rollback happened is the registration below with the free name.
	if getResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET após falha de provisionamento: status = %d, esperado %d (auth antes do lookup)", getResp.StatusCode, http.StatusUnauthorized)
	}
	backend.mu.Lock()
	backend.failProvision = false
	backend.mu.Unlock()
	second := doRegister(t, ts, token, "func-quebrada", registerRequest{ImageRef: "imagem-corrigida:1.0", MinInstances: 1})
	defer second.Body.Close()
	if second.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(second.Body)
		t.Fatalf("2ª tentativa (após corrigir): status = %d, esperado %d; corpo: %s", second.StatusCode, http.StatusCreated, body)
	}
}

func TestDeleteRemovesFunctionAndFreesName(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, pl := newTestServer(t, backend)
	defer ts.Close()
	reg := doRegister(t, ts, token, "func-removivel", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	_, invTok := mustRegister(t, reg)
	delResp := doDelete(t, ts, token, "func-removivel")
	defer delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: status = %d, esperado %d", delResp.StatusCode, http.StatusNoContent)
	}
	if got := pl.Count("func-removivel"); got != 0 {
		t.Errorf("pool.Count() após DELETE = %d, esperado 0", got)
	}
	getResp := doGet(t, ts, "func-removivel", invTok)
	defer getResp.Body.Close()
	// After the deletion the name is gone from the registry, so the
	// read is denied with 401. The removal is proved by the
	// registration below with the free name.
	if getResp.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET após DELETE: status = %d, esperado %d (auth antes do lookup)", getResp.StatusCode, http.StatusUnauthorized)
	}
	reReg := doRegister(t, ts, token, "func-removivel", registerRequest{ImageRef: "img:2.0", MinInstances: 1})
	defer reReg.Body.Close()
	if reReg.StatusCode != http.StatusCreated {
		t.Errorf("re-registro após DELETE: status = %d, esperado %d", reReg.StatusCode, http.StatusCreated)
	}
}

func TestDeleteRequiresAuthentication(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	reg := doRegister(t, ts, token, "func-protegida", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	reg.Body.Close()
	resp := doDelete(t, ts, "", "func-protegida")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, esperado %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestDeleteMissingFunctionReturns404(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	resp := doDelete(t, ts, token, "nao-existe")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, esperado %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestUpdateReplacesConfigAndInstances(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, pl := newTestServer(t, backend)
	defer ts.Close()
	reg := doRegister(t, ts, token, "alpr-yolo", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	reg.Body.Close()
	upd := doUpdate(t, ts, token, "alpr-yolo", registerRequest{ImageRef: "img:2.0", MinInstances: 2})
	defer upd.Body.Close()
	if upd.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(upd.Body)
		t.Fatalf("PUT status = %d, esperado %d; corpo: %s", upd.StatusCode, http.StatusOK, body)
	}
	var fn registry.Function
	if err := json.NewDecoder(upd.Body).Decode(&fn); err != nil {
		t.Fatal(err)
	}
	if fn.ImageRef != "docker.io/library/img:2.0" || fn.MinInstances != 2 {
		t.Errorf("função atualizada = %+v, esperado ImageRef=docker.io/library/img:2.0 MinInstances=2", fn)
	}
	if got := pl.Count("alpr-yolo"); got != 2 {
		t.Errorf("pool.Count() após update = %d, esperado 2 (pool reprovisionado com a config nova)", got)
	}
}

func TestUpdateMissingFunctionReturns404(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	resp := doUpdate(t, ts, token, "nao-existe", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, esperado %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestUpdateRequiresAuthentication(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	reg := doRegister(t, ts, token, "alpr-yolo", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	reg.Body.Close()
	resp := doUpdate(t, ts, "", "alpr-yolo", registerRequest{ImageRef: "img:2.0", MinInstances: 1})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, esperado %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestUpdateMarksDegradedInsteadOfUnregisteringOnFailure(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, pl := newTestServer(t, backend)
	defer ts.Close()
	reg := doRegister(t, ts, token, "alpr-yolo", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	_, invTok := mustRegister(t, reg)
	backend.mu.Lock()
	backend.failProvision = true
	backend.mu.Unlock()
	upd := doUpdate(t, ts, token, "alpr-yolo", registerRequest{ImageRef: "img-quebrada:2.0", MinInstances: 1})
	defer upd.Body.Close()
	if upd.StatusCode != http.StatusInternalServerError {
		t.Fatalf("PUT status = %d, esperado %d", upd.StatusCode, http.StatusInternalServerError)
	}
	getResp := doGet(t, ts, "alpr-yolo", invTok)
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET após PUT malsucedido: status = %d, esperado %d (função não deveria ter sido desregistrada)", getResp.StatusCode, http.StatusOK)
	}
	var fn registry.Function
	if err := json.NewDecoder(getResp.Body).Decode(&fn); err != nil {
		t.Fatal(err)
	}
	if !fn.Degraded() {
		t.Error("função deveria estar marcada como Degraded")
	}
	if fn.LastError == "" {
		t.Error("LastError deveria estar preenchido")
	}
	if got := pl.Count("alpr-yolo"); got != 0 {
		t.Errorf("pool.Count() = %d, esperado 0 (reprovisionamento falhou completamente)", got)
	}
	backend.mu.Lock()
	backend.failProvision = false
	backend.mu.Unlock()
	retry := doUpdate(t, ts, token, "alpr-yolo", registerRequest{ImageRef: "img-corrigida:3.0", MinInstances: 1})
	defer retry.Body.Close()
	if retry.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(retry.Body)
		t.Fatalf("PUT (retry) status = %d, esperado %d; corpo: %s", retry.StatusCode, http.StatusOK, body)
	}
	var retryFn registry.Function
	if err := json.NewDecoder(retry.Body).Decode(&retryFn); err != nil {
		t.Fatal(err)
	}
	if retryFn.Degraded() {
		t.Error("Degraded deveria ter sido limpo após o PUT bem-sucedido")
	}
}

func TestStandaloneRateLimitReturns429(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	const nomeFunc = "func-marteleada"
	corpo := registerRequest{ImageRef: "img:1.0", MinInstances: 1}
	_, witnessTok := mustRegister(t, doRegister(t, ts, token, "func-testemunha", corpo))
	for i := 0; i < 299; i++ {
		resp := doRegister(t, ts, "", nomeFunc, corpo)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("requisição %d (dentro do limite): status = %d, esperado %d", i+1, resp.StatusCode, http.StatusUnauthorized)
		}
	}
	excedida := doRegister(t, ts, "", nomeFunc, corpo)
	defer excedida.Body.Close()
	if excedida.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("além do limite: status = %d, esperado %d", excedida.StatusCode, http.StatusTooManyRequests)
	}
	body, err := io.ReadAll(excedida.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, trecho := range []string{`"code":"TOO_MANY_REQUESTS"`, `"retryable":true`} {
		if !strings.Contains(string(body), trecho) {
			t.Errorf("corpo do 429 = %s, esperado conter %s", body, trecho)
		}
	}
	getResp := doGet(t, ts, nomeFunc, "")
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET sem credencial durante throttle: status = %d, esperado %d (rota fechada, não limitada)", getResp.StatusCode, http.StatusUnauthorized)
	}
	getResp2 := doGet(t, ts, nomeFunc, witnessTok)
	defer getResp2.Body.Close()
	// A nonexistent function is denied with 401 even with a valid token
	// of another function, since the authentication comes before the
	// lookup and the lookup is not restricted to the owner.
	if getResp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET com token durante throttle: status = %d, esperado %d (auth antes do lookup)", getResp2.StatusCode, http.StatusUnauthorized)
	}
	var errResp errorResponse
	if err := json.NewDecoder(getResp2.Body).Decode(&errResp); err != nil {
		t.Fatalf("corpo da resposta não é o JSON de erro esperado: %v", err)
	}
	if errResp.Code != "UNAUTHORIZED" {
		t.Errorf("code = %q, esperado %q", errResp.Code, "UNAUTHORIZED")
	}
}

func TestInvokeResourceProfileHappy(t *testing.T) {
	sampler := newFakeSampler(
		telemetry.InvocationSample{CPUUsageUSec: 1_000_000, MemoryBytes: 100},
		telemetry.InvocationSample{CPUUsageUSec: 3_000_000, MemoryBytes: 500},
	)
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend, sampler)
	defer ts.Close()
	reg := doRegister(t, ts, token, "func-perfilada", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	_, invTok := mustRegister(t, reg)
	invResp := doInvoke(t, ts, "func-perfilada", invTok, invokeRequest{Input: json.RawMessage(`{"x":1}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(invResp.Body)
		t.Fatalf("invoke status = %d, esperado %d; corpo: %s", invResp.StatusCode, http.StatusOK, body)
	}
	body, err := io.ReadAll(invResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var result invokeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if string(result.Result) != `{"x":1}` {
		t.Errorf("result = %s, esperado o input ecoado de volta", result.Result)
	}
	var generic struct {
		ExecutionMetrics struct {
			ResourceProfile map[string]any `json:"resource_profile"`
		} `json:"execution_metrics"`
	}
	if err := json.Unmarshal(body, &generic); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(generic.ExecutionMetrics.ResourceProfile))
	for k := range generic.ExecutionMetrics.ResourceProfile {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	wantKeys := []string{"cpu_pct_avg", "cpu_pct_peak", "mem_peak_bytes", "sample_count"}
	if !slices.Equal(keys, wantKeys) {
		t.Errorf("chaves de resource_profile = %v, esperado exatamente %v", keys, wantKeys)
	}
	profile := result.ExecutionMetrics.ResourceProfile
	if profile == nil {
		t.Fatalf("resource_profile ausente na resposta 200: %s", body)
	}
	if profile.MemPeakBytes != 500 {
		t.Errorf("mem_peak_bytes = %d, esperado 500 (pico da sequência, t0 incluso)", profile.MemPeakBytes)
	}
	if profile.CPUPctPeak < profile.CPUPctAvg {
		t.Errorf("cpu_pct_peak (%f) menor que cpu_pct_avg (%f)", profile.CPUPctPeak, profile.CPUPctAvg)
	}
	if profile.CPUPctAvg <= 0 || profile.CPUPctPeak > 100 {
		t.Errorf("percentuais fora do esperado: avg=%f peak=%f (janela carregada única ⇒ avg>0, clamp ≤100)", profile.CPUPctAvg, profile.CPUPctPeak)
	}
	if profile.SampleCount <= 0 {
		t.Errorf("sample_count = %d, esperado >0", profile.SampleCount)
	}
}

func TestInvokeResourceProfileOmittedWhenSamplerFails(t *testing.T) {
	sampler := newFakeSampler()
	sampler.err = errors.New("containerd metrics indisponível (simulado)")
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend, sampler)
	defer ts.Close()
	reg := doRegister(t, ts, token, "func-sem-perfil", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	_, invTok := mustRegister(t, reg)
	invResp := doInvoke(t, ts, "func-sem-perfil", invTok, invokeRequest{Input: json.RawMessage(`{"x":1}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(invResp.Body)
		t.Fatalf("invoke status = %d, esperado %d; corpo: %s", invResp.StatusCode, http.StatusOK, body)
	}
	body, err := io.ReadAll(invResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "resource_profile") {
		t.Errorf("JSON não deveria conter resource_profile com sampler em falha: %s", body)
	}
	var result invokeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if string(result.Result) != `{"x":1}` {
		t.Errorf("result = %s, esperado o input ecoado de volta (falha de perfil não pode alterá-lo)", result.Result)
	}
	metrics := result.ExecutionMetrics
	if metrics.Telemetry == nil {
		t.Fatalf("telemetria piggyback deveria continuar presente: %s", body)
	}
	if metrics.Telemetry.Source != "" || metrics.Telemetry.NodeID != "xp4-borda-01" {
		t.Errorf("piggyback alterado pela falha do sampler: source=%q (esperado omitido) node_id=%q", metrics.Telemetry.Source, metrics.Telemetry.NodeID)
	}
}

func TestInvokeResourceProfileErrorPathKeepsShape(t *testing.T) {
	backend := newFakeInstanceBackend(t, errorHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	reg := doRegister(t, ts, token, "func-com-erro", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	_, invTok := mustRegister(t, reg)
	before := goroutineBaseline(t)
	env, err := json.Marshal(invokeRequest{Input: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/function/func-com-erro/invoke", bytes.NewReader(env))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(auth.DefaultHeaderName, invTok)
	invResp, err := leakTestClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(invResp.Body)
		t.Fatalf("status = %d, esperado %d; corpo: %s", invResp.StatusCode, http.StatusBadGateway, body)
	}
	body, err := io.ReadAll(invResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "execution_metrics") {
		t.Errorf("resposta de erro não deveria conter execution_metrics: %s", body)
	}
	var errResp errorResponse
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&errResp); err != nil {
		t.Fatalf("corpo da resposta não é o JSON de erro esperado: %v", err)
	}
	if errResp.Code != "INVOCATION_FAILED" {
		t.Errorf("code = %q, esperado %q", errResp.Code, "INVOCATION_FAILED")
	}
	waitGoroutinesSettle(t, before)
}

func TestInvokeResourceProfileConcurrentInvokes(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	reg := doRegister(t, ts, token, "func-paralela", registerRequest{ImageRef: "img:1.0", MinInstances: 2})
	_, invTok := mustRegister(t, reg)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			input := fmt.Sprintf(`{"i":%d}`, i)
			env, err := json.Marshal(invokeRequest{Input: json.RawMessage(input)})
			if err != nil {
				t.Errorf("invoke %d: envelope: %v", i, err)
				return
			}
			req, err := http.NewRequest(http.MethodPost, ts.URL+"/function/func-paralela/invoke", bytes.NewReader(env))
			if err != nil {
				t.Errorf("invoke %d: %v", i, err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(auth.DefaultHeaderName, invTok)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("invoke %d: %v", i, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("invoke %d: status = %d, esperado %d; corpo: %s", i, resp.StatusCode, http.StatusOK, body)
				return
			}
			var result invokeResponse
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				t.Errorf("invoke %d: decode: %v", i, err)
				return
			}
			if string(result.Result) != input {
				t.Errorf("invoke %d: result = %s, esperado %s (sem cruzamento entre invocações)", i, result.Result, input)
			}
			if result.ExecutionMetrics.ResourceProfile == nil {
				t.Errorf("invoke %d: resource_profile ausente", i)
			} else if result.ExecutionMetrics.ResourceProfile.MemPeakBytes == 0 {
				t.Errorf("invoke %d: mem_peak_bytes = 0, sequência padrão tem memória positiva", i)
			}
		}(i)
	}
	wg.Wait()
}

func TestInvokeResourceProfileNoLeakOnBackendPanic(t *testing.T) {
	t.Run("panico", func(t *testing.T) {
		backend := newFakeInstanceBackend(t, panicHandler)
		ts, token, _ := newTestServer(t, backend)
		defer ts.Close()
		reg := doRegister(t, ts, token, "func-panico", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
		_, invTok := mustRegister(t, reg)
		before := goroutineBaseline(t)
		env, err := json.Marshal(invokeRequest{Input: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/function/func-panico/invoke", bytes.NewReader(env))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(auth.DefaultHeaderName, invTok)
		invResp, err := leakTestClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer invResp.Body.Close()
		if invResp.StatusCode != http.StatusBadGateway {
			body, _ := io.ReadAll(invResp.Body)
			t.Fatalf("status = %d, esperado %d; corpo: %s", invResp.StatusCode, http.StatusBadGateway, body)
		}
		var errResp errorResponse
		if err := json.NewDecoder(invResp.Body).Decode(&errResp); err != nil {
			t.Fatalf("corpo da resposta não é o JSON de erro esperado: %v", err)
		}
		if errResp.Code != "INVOCATION_FAILED" {
			t.Errorf("code = %q, esperado %q (pânico do backend vira erro de transporte, nunca propaga)", errResp.Code, "INVOCATION_FAILED")
		}
		waitGoroutinesSettle(t, before)
	})
}

func TestInvokeResourceProfilePanicBeforeClose(t *testing.T) {
	t.Run("addr", func(t *testing.T) {
		g, err := auth.Generate()
		if err != nil {
			t.Fatalf("falha ao gerar credencial de teste: %v", err)
		}
		backend := newFakeInstanceBackend(t, echoHandler)
		sampler := newFakeSampler()
		reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
		pl := pool.NewManagerWithQueueCap(backend, nil)
		srv := NewServer("xp4-borda-01:8080", reg, pl, panicAddresser{}, g.Cred, 5*time.Second, newTestCollector(t), sampler, 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, nil)
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()
		regResp := doRegister(t, ts, g.PlainToken, "func-f2", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
		_, invTok := mustRegister(t, regResp)
		before := goroutineBaseline(t)
		env, err := json.Marshal(invokeRequest{Input: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/function/func-f2/invoke", bytes.NewReader(env))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(auth.DefaultHeaderName, invTok)
		if resp, ierr := leakTestClient.Do(req); ierr == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("invoke deveria falhar no cliente (conexão derrubada ao recuperar o pânico); status = %d, corpo: %s", resp.StatusCode, body)
		}
		sampler.mu.Lock()
		t0Calls := sampler.calls
		sampler.mu.Unlock()
		if t0Calls == 0 {
			t.Fatal("premissa violada: SampleMetrics não rodou nem para t0 — a goroutine do perfil nunca nasceu e o braço sampleCtx.Done() não foi exercitado")
		}
		waitGoroutinesSettle(t, before)
	})
}

func TestInvokeProfilerDoubleStopIdentical(t *testing.T) {
	sampler := newFakeSampler(
		telemetry.InvocationSample{CPUUsageUSec: 1_000_000, MemoryBytes: 4096},
		telemetry.InvocationSample{CPUUsageUSec: 3_000_000, MemoryBytes: 8192},
	)
	prof := &invokeProfiler{sampler: sampler, interval: time.Millisecond, instanceID: "f-0"}
	ctx, cancel := context.WithCancel(context.WithoutCancel(context.Background()))
	prof.sampleCtx, prof.cancel = ctx, cancel
	prof.tracker = telemetry.NewProfileTracker(1)
	prof.tracker.StartMono(telemetry.InvocationSample{CPUUsageUSec: 1_000_000, MemoryBytes: 4096}, ntime.Now())
	prof.done = make(chan struct{})
	go prof.loop()
	prof.stop()
	first, ok := prof.tracker.Profile()
	if !ok {
		t.Fatalf("Profile() após stop = false, esperado true")
	}
	prof.stop()
	second, ok := prof.tracker.Profile()
	if !ok {
		t.Fatalf("Profile() após 2o stop = false, esperado true")
	}
	if first != second {
		t.Fatalf("stop duplo divergiu: %+v vs %+v (idempotência exige idêntico)", first, second)
	}
	cancel()
}

func TestInvokeBodyLimit(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	_, invTok := mustRegister(t, doRegister(t, ts, token, "func-limite", registerRequest{ImageRef: "img:1.0", MinInstances: 1}))
	body2MiB := strings.Repeat("x", 2<<20)
	resp200 := doInvoke(t, ts, "func-limite", invTok, invokeRequest{Input: json.RawMessage(`{"data":"` + body2MiB + `"}`)})
	defer resp200.Body.Close()
	if resp200.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp200.Body)
		t.Fatalf("2 MiB: status = %d, esperado %d; corpo: %s", resp200.StatusCode, http.StatusOK, body)
	}
	body9MiB := strings.Repeat("y", 9<<20)
	resp413 := doInvoke(t, ts, "func-limite", invTok, invokeRequest{Input: json.RawMessage(`{"data":"` + body9MiB + `"}`)})
	defer resp413.Body.Close()
	if resp413.StatusCode != http.StatusRequestEntityTooLarge {
		body, _ := io.ReadAll(resp413.Body)
		t.Fatalf("9 MiB: status = %d, esperado %d; corpo: %s", resp413.StatusCode, http.StatusRequestEntityTooLarge, body)
	}
	var errResp errorResponse
	if err := json.NewDecoder(resp413.Body).Decode(&errResp); err != nil {
		t.Fatalf("corpo do 413 não é JSON de erro esperado: %v", err)
	}
	if errResp.Code != "BODY_TOO_LARGE" {
		t.Errorf("code = %q, esperado %q", errResp.Code, "BODY_TOO_LARGE")
	}
}

func TestRegisterInvalidImageRef(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	_, witnessTok := mustRegister(t, doRegister(t, ts, token, "witness", registerRequest{ImageRef: "img:1.0", MinInstances: 0}))
	testCases := []struct {
		name     string
		imageRef string
		funcName string
	}{
		{"empty", "", "func-empty"},
		{"whitespace", "   ", "func-whitespace"},
		{"invalid", "::::invalida", "func-invalid"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doRegister(t, ts, token, tc.funcName, registerRequest{ImageRef: tc.imageRef, MinInstances: 1})
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("Register(%q): status = %d, esperado %d; corpo: %s", tc.imageRef, resp.StatusCode, http.StatusBadRequest, body)
			}
			var errResp errorResponse
			if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
				t.Fatalf("corpo não é JSON de erro: %v", err)
			}
			if errResp.Code != "INVALID_IMAGE_REF" {
				t.Errorf("code = %q, esperado %q", errResp.Code, "INVALID_IMAGE_REF")
			}
			getResp := doGet(t, ts, tc.funcName, witnessTok)
			defer getResp.Body.Close()
			// A failed registration is not persisted, so the read is
			// denied with 401, without telling a nonexistent function
			// apart from one without access.
			if getResp.StatusCode != http.StatusUnauthorized {
				t.Errorf("GET após Register falho: status = %d, esperado %d (não deveria ter persistido / auth antes do lookup)", getResp.StatusCode, http.StatusUnauthorized)
			}
		})
	}
}

func TestServer_ListFunctions(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()
	listNames := func(t *testing.T, resp *http.Response) []string {
		t.Helper()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("list: status=%d want 200 body=%s", resp.StatusCode, body)
		}
		var fns []registry.Function
		if err := json.NewDecoder(resp.Body).Decode(&fns); err != nil {
			t.Fatal(err)
		}
		names := make([]string, 0, len(fns))
		for _, fn := range fns {
			names = append(names, fn.Name)
		}
		return names
	}
	resp := doList(t, ts, "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("list sem credencial: status=%d want 401", resp.StatusCode)
	}
	rawResp := doList(t, ts, token)
	raw, _ := io.ReadAll(rawResp.Body)
	rawResp.Body.Close()
	if strings.TrimSpace(string(raw)) != "[]" {
		t.Fatalf("list vazio: corpo=%q want []", raw)
	}
	mustRegister(t, doRegister(t, ts, token, "f1", registerRequest{ImageRef: "img:1.0", MinInstances: 0}))
	_, witnessTok := mustRegister(t, doRegister(t, ts, token, "f2", registerRequest{ImageRef: "img:2.0", MinInstances: 0}))
	names := listNames(t, doList(t, ts, token))
	if len(names) != 2 || !slices.Contains(names, "f1") || !slices.Contains(names, "f2") {
		t.Fatalf("list: nomes=%v want [f1 f2]", names)
	}
	resp2 := doList(t, ts, witnessTok)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("list com invoke token: status=%d want 401", resp2.StatusCode)
	}
}

func TestRegisterAllowlist(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServerWithAllowlist(t, []string{"example.com"}, backend)
	defer ts.Close()
	respOK := doRegister(t, ts, token, "func-ok", registerRequest{ImageRef: "example.com/alpr:1.0", MinInstances: 1})
	defer respOK.Body.Close()
	if respOK.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(respOK.Body)
		t.Fatalf("Register(example.com/alpr:1.0) com allowlist [example.com]: status = %d, esperado %d; corpo: %s", respOK.StatusCode, http.StatusCreated, body)
	}
	_, invTok := mustRegister(t, respOK)
	respBlocked := doRegister(t, ts, token, "func-blocked", registerRequest{ImageRef: "evil.io/x:latest", MinInstances: 1})
	defer respBlocked.Body.Close()
	if respBlocked.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(respBlocked.Body)
		t.Fatalf("Register(evil.io/x:latest) com allowlist [example.com]: status = %d, esperado %d; corpo: %s", respBlocked.StatusCode, http.StatusBadRequest, body)
	}
	var errResp errorResponse
	if err := json.NewDecoder(respBlocked.Body).Decode(&errResp); err != nil {
		t.Fatalf("corpo não é JSON de erro: %v", err)
	}
	if errResp.Code != "INVALID_IMAGE_REF" {
		t.Errorf("code = %q, esperado %q", errResp.Code, "INVALID_IMAGE_REF")
	}
	getResp := doGet(t, ts, "func-blocked", invTok)
	defer getResp.Body.Close()
	// A blocked registration is not persisted, so the read is denied
	// with 401.
	if getResp.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET após Register bloqueado: status = %d, esperado %d (não deveria ter persistido / auth antes do lookup)", getResp.StatusCode, http.StatusUnauthorized)
	}
	ts2, token2, _ := newTestServer(t, backend)
	defer ts2.Close()
	respAny := doRegister(t, ts2, token2, "func-any", registerRequest{ImageRef: "qualquer.io/imagem:tag", MinInstances: 1})
	defer respAny.Body.Close()
	if respAny.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(respAny.Body)
		t.Fatalf("Register com allowlist vazia: status = %d, esperado %d; corpo: %s", respAny.StatusCode, http.StatusCreated, body)
	}
}

func TestUpdateInvalidImageRef(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServerWithAllowlist(t, []string{"example.com"}, backend)
	defer ts.Close()
	_, invTok := mustRegister(t, doRegister(t, ts, token, "func-upd", registerRequest{ImageRef: "example.com/original:1.0", MinInstances: 1}))
	resp := doUpdate(t, ts, token, "func-upd", registerRequest{ImageRef: "::::invalida", MinInstances: 1})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Update(::::invalida): status = %d, esperado %d; corpo: %s", resp.StatusCode, http.StatusBadRequest, body)
	}
	var errResp errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("corpo não é JSON de erro: %v", err)
	}
	if errResp.Code != "INVALID_IMAGE_REF" {
		t.Errorf("code = %q, esperado %q", errResp.Code, "INVALID_IMAGE_REF")
	}
	getResp := doGet(t, ts, "func-upd", invTok)
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET após Update falho: status = %d, esperado %d", getResp.StatusCode, http.StatusOK)
	}
	var fn registry.Function
	if err := json.NewDecoder(getResp.Body).Decode(&fn); err != nil {
		t.Fatal(err)
	}
	if fn.ImageRef != "example.com/original:1.0" {
		t.Errorf("ImageRef após Update falho = %q, esperado preservado %q", fn.ImageRef, "example.com/original:1.0")
	}
}

func TestUpdateAllowlist(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServerWithAllowlist(t, []string{"example.com"}, backend)
	defer ts.Close()
	_, invTok := mustRegister(t, doRegister(t, ts, token, "func-upd", registerRequest{ImageRef: "example.com/original:1.0", MinInstances: 1}))
	resp := doUpdate(t, ts, token, "func-upd", registerRequest{ImageRef: "evil.io/bad:2.0", MinInstances: 1})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Update(evil.io/bad:2.0) com allowlist [example.com]: status = %d, esperado %d; corpo: %s", resp.StatusCode, http.StatusBadRequest, body)
	}
	var errResp errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("corpo não é JSON de erro: %v", err)
	}
	if errResp.Code != "INVALID_IMAGE_REF" {
		t.Errorf("code = %q, esperado %q", errResp.Code, "INVALID_IMAGE_REF")
	}
	getResp := doGet(t, ts, "func-upd", invTok)
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET após Update bloqueado: status = %d, esperado %d", getResp.StatusCode, http.StatusOK)
	}
	var fn registry.Function
	if err := json.NewDecoder(getResp.Body).Decode(&fn); err != nil {
		t.Fatal(err)
	}
	if fn.ImageRef != "example.com/original:1.0" {
		t.Errorf("ImageRef após Update bloqueado = %q, esperado preservado %q", fn.ImageRef, "example.com/original:1.0")
	}
}
