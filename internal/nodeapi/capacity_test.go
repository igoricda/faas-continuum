package nodeapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/auth"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// newCapacityTestServer builds a server like the generic test helper, but
// it returns the collector so that the test can collect the periodic
// snapshot directly.
func newCapacityTestServer(t *testing.T, backend *fakeInstanceBackend, collector *telemetry.Collector) (*httptest.Server, string, *pool.Manager) {
	t.Helper()
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("falha ao gerar credencial de teste: %v", err)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(backend, nil)
	srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, collector, newFakeSampler(), 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend})
	return httptest.NewServer(srv.Handler()), g.PlainToken, pl
}

// The piggyback carries only the invoked function, with its three fields,
// even when the pool is at its ceiling with one live instance occupied
// during the invocation.
func TestInvokePiggybackCarriesSingleFunctionCapacity(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	collector := newTestCollector(t)
	ts, token, _ := newCapacityTestServer(t, backend, collector)
	defer ts.Close()

	_, invTok := mustRegister(t, doRegister(t, ts, token, "sleep", registerRequest{ImageRef: "img:1.0", MinInstances: 1, MaxInstances: 1}))
	// A second registered function that was not invoked must not appear in
	// the piggyback.
	second := doRegister(t, ts, token, "outra", registerRequest{ImageRef: "img:1.0", MinInstances: 0})
	second.Body.Close()

	invResp := doInvoke(t, ts, "sleep", invTok, invokeRequest{Input: json.RawMessage(`{"sleep_ms":1}`)})
	body, err := io.ReadAll(invResp.Body)
	invResp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if invResp.StatusCode != http.StatusOK {
		t.Fatalf("invoke status = %d, esperado 200; corpo: %s", invResp.StatusCode, body)
	}
	var result invokeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if result.ExecutionMetrics.Telemetry == nil {
		t.Fatalf("telemetria piggyback ausente: %s", body)
	}
	cap := result.ExecutionMetrics.Telemetry.Capacity
	if len(cap) != 1 {
		t.Fatalf("capacity piggyback tem %d entradas, esperado 1 (só sleep): %v", len(cap), cap)
	}
	sl, ok := cap["sleep"]
	if !ok {
		t.Fatalf("capacity sem a função invocada sleep: %v", cap)
	}
	if sl.MaxInstances != 1 {
		t.Errorf("max_instances = %d, esperado 1", sl.MaxInstances)
	}
	if sl.InstancesAvailable != 1 {
		t.Errorf("instances_available = %d, esperado 1 (viva, ocupada pelo próprio invoke)", sl.InstancesAvailable)
	}
	if sl.QueueDepth != 0 {
		t.Errorf("queue_depth = %d, esperado 0 (sem contenção)", sl.QueueDepth)
	}
	rawTel, _ := json.Marshal(result.ExecutionMetrics.Telemetry)
	t.Logf("piggyback telemetry JSON: %s", rawTel)
}

// A collection that fails never fails the invocation: the block is
// omitted and the answer is a success with the result.
func TestInvokeSucceedsWhenCollectFails(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	broken := telemetry.NewCollector("xp4-borda-01", true,
		filepath.Join(t.TempDir(), "sem-cgroup"),
		filepath.Join(t.TempDir(), "sem-stat"),
		filepath.Join(t.TempDir(), "sem-meminfo"), "", "")
	ts, token, _ := newCapacityTestServer(t, backend, broken)
	defer ts.Close()

	_, invTok := mustRegister(t, doRegister(t, ts, token, "sleep", registerRequest{ImageRef: "img:1.0", MinInstances: 1, MaxInstances: 1}))
	if _, err := broken.Collect(telemetry.SourcePiggyback); err == nil {
		t.Fatal("precondição: Collect do fixture quebrado deveria falhar")
	}

	invResp := doInvoke(t, ts, "sleep", invTok, invokeRequest{Input: json.RawMessage(`{"x":1}`)})
	body, err := io.ReadAll(invResp.Body)
	invResp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if invResp.StatusCode != http.StatusOK {
		t.Fatalf("invoke com coleta falha: status = %d, esperado 200; corpo: %s", invResp.StatusCode, body)
	}
	var result invokeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if string(result.Result) != `{"x":1}` {
		t.Errorf("result = %s, esperado o eco intacto", result.Result)
	}
	if result.ExecutionMetrics.Telemetry != nil {
		t.Errorf("telemetria deveria ser omitida sob falha de coleta, obteve %+v", result.ExecutionMetrics.Telemetry)
	}
}

// The periodic report, through the hook installed by the constructor,
// carries the complete map.
func TestPeriodicSnapshotFillsCompleteMap(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	collector := newTestCollector(t)
	ts, token, _ := newCapacityTestServer(t, backend, collector)
	defer ts.Close()

	mustRegister(t, doRegister(t, ts, token, "sleep", registerRequest{ImageRef: "img:1.0", MinInstances: 1, MaxInstances: 1}))
	other := doRegister(t, ts, token, "alpr", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 4})
	other.Body.Close()

	pkg, err := collector.Collect(telemetry.SourcePeriodic)
	if err != nil {
		t.Fatalf("Collect(periodic): %v", err)
	}
	if len(pkg.Capacity) != 2 {
		t.Fatalf("capacity periódica tem %d entradas, esperado 2: %v", len(pkg.Capacity), pkg.Capacity)
	}
	if pkg.Capacity["sleep"].MaxInstances != 1 || pkg.Capacity["alpr"].MaxInstances != 4 {
		t.Errorf("tetos errados: %v", pkg.Capacity)
	}
	if pkg.Capacity["sleep"].InstancesAvailable != 1 {
		t.Errorf("sleep instances_available = %d, esperado 1 (min_instances)", pkg.Capacity["sleep"].InstancesAvailable)
	}
	raw, _ := json.Marshal(pkg.Capacity)
	t.Logf("periodic capacity JSON (%dB): %s", len(raw), raw)
}
