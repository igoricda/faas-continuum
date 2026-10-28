package server

// Regressions of the validated ingestion in the periodic channel and of the
// re-registration: an unknown function is discarded, an availability above the
// registered ceiling is limited, and the upsert clears the capacity without
// touching the telemetry.

import (
	"net/http"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func registerAuditedNode(t *testing.T, h http.Handler, token string) {
	t.Helper()
	rr := doReq(t, h, "POST", "/nodes", map[string]any{
		"node_id":       "n1",
		"layer":         "borda",
		"host":          "10.0.0.1:8080",
		"functions":     []string{"sleep"},
		"max_instances": map[string]int{"sleep": 2},
	}, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("register n1: %d body=%s", rr.Code, rr.Body.String())
	}
}

func postAuditedTelemetry(t *testing.T, h http.Handler, token string, caps map[string]telemetry.FunctionCapacity) {
	t.Helper()
	pkg := telemetry.Package{
		NodeID:         "n1",
		Timestamp:      time.Now().UTC(),
		CPUUsagePct:    10,
		MemoryUsagePct: 20,
		Source:         "periodic",
		Capacity:       caps,
	}
	rr := doReq(t, h, "POST", "/nodes/n1/telemetry", pkg, token)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("telemetry status=%d want 204 body=%s", rr.Code, rr.Body.String())
	}
}

func TestTelemetryCapacityUnknownFunctionDiscarded(t *testing.T) {
	srv, token, _, mon := newTestServerEnv(t)
	h := srv.Handler()
	registerAuditedNode(t, h, token)

	// The occupancy of a function that the node does not host is discarded, since
	// before it was merged and caused an undue failover for the time to live.
	postAuditedTelemetry(t, h, token, map[string]telemetry.FunctionCapacity{
		"alpr": {MaxInstances: 1, InstancesAvailable: 0},
	})
	if _, ok := mon.GetCapacity("n1", "alpr", time.Now()); ok {
		t.Fatal("GetCapacity(fn desconhecida) = true, esperado false (descartada na ingestão)")
	}
}

func TestTelemetryCapacityClampedToRegisteredCeiling(t *testing.T) {
	srv, token, _, mon := newTestServerEnv(t)
	h := srv.Handler()
	registerAuditedNode(t, h, token)

	// An availability of nine, above the registered ceiling of two, is limited to the ceiling.
	postAuditedTelemetry(t, h, token, map[string]telemetry.FunctionCapacity{
		"sleep": {MaxInstances: 2, InstancesAvailable: 9},
	})
	got, ok := mon.GetCapacity("n1", "sleep", time.Now())
	if !ok {
		t.Fatal("GetCapacity(sleep) = false, esperado true")
	}
	if got.InstancesAvailable != 2 {
		t.Fatalf("avail = %d, esperado clamp ao teto registrado 2", got.InstancesAvailable)
	}
}

func TestReregisterClearsStaleExhausted(t *testing.T) {
	srv, token, _, mon := newTestServerEnv(t)
	h := srv.Handler()
	registerAuditedNode(t, h, token)

	// A node marked as exhausted by an optimistic rejection and registered again
	// does not inherit the mark, since it received no load.
	mon.MarkExhausted("n1", "sleep", time.Now())
	if _, ok := mon.GetCapacity("n1", "sleep", time.Now()); !ok {
		t.Fatal("pré-condição: MarkExhausted deveria criar entrada fresca")
	}
	rr := doReq(t, h, "POST", "/nodes", map[string]any{
		"node_id":       "n1",
		"layer":         "borda",
		"host":          "10.0.0.1:8080",
		"functions":     []string{"sleep"},
		"max_instances": map[string]int{"sleep": 2},
	}, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("re-register n1: %d body=%s", rr.Code, rr.Body.String())
	}
	if _, ok := mon.GetCapacity("n1", "sleep", time.Now()); ok {
		t.Fatal("GetCapacity após re-registro = true, esperado false (ocupação herdada limpa)")
	}
	// The telemetry stays valid after the re-registration, since only the capacity is cleared.
	postAuditedTelemetry(t, h, token, map[string]telemetry.FunctionCapacity{
		"sleep": {MaxInstances: 2, InstancesAvailable: 2},
	})
	if latest, ok := mon.Latest("n1"); !ok || latest.CPUUsagePct != 10 {
		t.Fatalf("Latest após re-registro = (%+v,%v), esperado cpu=10", latest, ok)
	}
}
