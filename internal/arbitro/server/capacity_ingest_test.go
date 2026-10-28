package server

// Ingestão periódica de capacidade via HTTP (capacity-history-arbitro,
// todo 7): o handleTelemetry funde pkg.Capacity com recvNow do Árbitro.
// TestServer_TelemetriaCorreta204EGetMostra prova o Latest, mas nenhum
// teste lê GetCapacity — aqui o merge, o no-op sem capacity e o
// Timestamp do nó ignorado (frescura só por recepção).

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
	// latest clássico segue intacto junto da ingestão de capacidade.
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

	// Timestamp do nó 48h no futuro: irrelevante — frescura mede o
	// recvNow do Árbitro, então a entrada nasce fresca.
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
