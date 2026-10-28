package server

// Periodic ingestion of the capacity over HTTP: the telemetry handler merges the
// capacity of the package with the reception instant of the arbiter. The test
// of the correct telemetry proves the latest reading, but no test reads the
// capacity, so here the merge, the absence of a capacity and the timestamp of
// the node are covered, since the freshness comes only from the reception.

import (
	"net/http"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func registerCapacityNode(t *testing.T, h http.Handler, token, id string) {
	t.Helper()
	rr := doReq(t, h, "POST", "/nodes", map[string]any{"node_id": id, "layer": "borda", "host": "10.0.0.1:8080"}, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("register %s: %d body=%s", id, rr.Code, rr.Body.String())
	}
}

func TestTelemetryCapacityPeriodicMerge(t *testing.T) {
	srv, token, _, mon := newTestServerEnv(t)
	h := srv.Handler()
	registerCapacityNode(t, h, token, "n1")

	pkg := telemetry.Package{
		NodeID:         "n1",
		Timestamp:      time.Now().UTC(),
		CPUUsagePct:    10,
		MemoryUsagePct: 20,
		Source:         "periodic",
		Capacity: map[string]telemetry.FunctionCapacity{
			"sleep": {MaxInstances: 1, InstancesAvailable: 0, QueueDepth: 3},
			"alpr":  {MaxInstances: 4, InstancesAvailable: 4},
		},
	}
	rr := doReq(t, h, "POST", "/nodes/n1/telemetry", pkg, token)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("telemetry status=%d want 204 body=%s", rr.Code, rr.Body.String())
	}
	now := time.Now()
	sleep, ok := mon.GetCapacity("n1", "sleep", now)
	if !ok {
		t.Fatal("GetCapacity sleep = false, esperado true após periódico com capacity")
	}
	if sleep.MaxInstances != 1 || sleep.InstancesAvailable != 0 || sleep.QueueDepth != 3 {
		t.Fatalf("sleep = %+v, esperado {1 0 3}", sleep)
	}
	alpr, ok := mon.GetCapacity("n1", "alpr", now)
	if !ok || alpr.MaxInstances != 4 || alpr.InstancesAvailable != 4 {
		t.Fatalf("alpr = (%+v,%v), esperado (max=4,avail=4,true)", alpr, ok)
	}
	// The classic latest reading stays intact next to the ingestion of the capacity.
	if latest, ok := mon.Latest("n1"); !ok || latest.CPUUsagePct != 10 {
		t.Fatalf("Latest = (%+v,%v), esperado cpu=10", latest, ok)
	}
}

func TestTelemetryCapacityAbsentNoOp(t *testing.T) {
	srv, token, _, mon := newTestServerEnv(t)
	h := srv.Handler()
	registerCapacityNode(t, h, token, "n1")

	pkg := telemetry.Package{
		NodeID:         "n1",
		Timestamp:      time.Now().UTC(),
		CPUUsagePct:    10,
		MemoryUsagePct: 20,
		Source:         "periodic",
	}
	rr := doReq(t, h, "POST", "/nodes/n1/telemetry", pkg, token)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("telemetry status=%d want 204 body=%s", rr.Code, rr.Body.String())
	}
	if _, ok := mon.GetCapacity("n1", "sleep", time.Now()); ok {
		t.Fatal("GetCapacity sleep = true sem capacity no pacote, esperado false (nada a fundir)")
	}
	if latest, ok := mon.Latest("n1"); !ok || latest.CPUUsagePct != 10 {
		t.Fatalf("Latest = (%+v,%v), esperado cpu=10 (telemetria sem capacity segue gravada)", latest, ok)
	}
}

func TestTelemetryCapacityTimestampIgnored(t *testing.T) {
	srv, token, _, mon := newTestServerEnv(t)
	h := srv.Handler()
	registerCapacityNode(t, h, token, "n1")

	// The timestamp of the node is forty-eight hours in the future and is
	// irrelevant, since the freshness measures the reception instant of the
	// arbiter and the entry starts fresh.
	pkg := telemetry.Package{
		NodeID:         "n1",
		Timestamp:      time.Now().UTC().Add(48 * time.Hour),
		CPUUsagePct:    10,
		MemoryUsagePct: 20,
		Source:         "periodic",
		Capacity: map[string]telemetry.FunctionCapacity{
			"sleep": {MaxInstances: 1, InstancesAvailable: 1},
		},
	}
	rr := doReq(t, h, "POST", "/nodes/n1/telemetry", pkg, token)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("telemetry status=%d want 204 body=%s", rr.Code, rr.Body.String())
	}
	got, ok := mon.GetCapacity("n1", "sleep", time.Now())
	if !ok || got.InstancesAvailable != 1 {
		t.Fatalf("GetCapacity com Timestamp futuro = (%+v,%v), esperado (avail=1,true)", got, ok)
	}
}
