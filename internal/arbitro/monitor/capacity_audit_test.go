package monitor

// Regression tests for the validated ingestion of the capacity and for its
// cleanup: negative occupancy is clamped to zero, an unknown function is
// discarded and its availability is capped at the registered ceiling,
// expired entries are removed and a re-registration clears the occupancy
// without touching the telemetry.

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func TestClampCapacityNegatives(t *testing.T) {
	got := ClampCapacity(telemetry.FunctionCapacity{MaxInstances: -2, InstancesAvailable: -1, QueueDepth: -5})
	if got.MaxInstances != 0 || got.InstancesAvailable != 0 || got.QueueDepth != 0 {
		t.Fatalf("ClampCapacity = %+v, esperado tudo 0", got)
	}
	got = ClampCapacity(telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 3, QueueDepth: 1})
	if got.MaxInstances != 4 || got.InstancesAvailable != 3 || got.QueueDepth != 1 {
		t.Fatalf("ClampCapacity alterou valores válidos: %+v", got)
	}
}

func TestSanitizeForNodeUnknownFunction(t *testing.T) {
	// The node only serves one function, so the occupancy of another is
	// discarded: an availability of zero would otherwise cause an
	// unwarranted failover for the whole lifetime of the sample.
	if _, ok := SanitizeForNode([]string{"A"}, nil, "B", telemetry.FunctionCapacity{InstancesAvailable: 0}); ok {
		t.Fatal("SanitizeForNode(fn desconhecida) = true, esperado false")
	}
	// A node eligible for every function accepts any key.
	if _, ok := SanitizeForNode(nil, nil, "B", telemetry.FunctionCapacity{InstancesAvailable: 1}); !ok {
		t.Fatal("SanitizeForNode(hosted vazio) = false, esperado true")
	}
}

func TestSanitizeForNodeCeilingClamp(t *testing.T) {
	// An availability above the registered ceiling is capped.
	got, ok := SanitizeForNode([]string{"sleep"}, map[string]int{"sleep": 2}, "sleep", telemetry.FunctionCapacity{MaxInstances: 2, InstancesAvailable: 9})
	if !ok {
		t.Fatal("SanitizeForNode(fn conhecida) = false, esperado true")
	}
	if got.InstancesAvailable != 2 {
		t.Fatalf("avail = %d, esperado clamp ao teto 2", got.InstancesAvailable)
	}
	// A ceiling of zero means no ceiling, so nothing is clamped.
	got, ok = SanitizeForNode([]string{"sleep"}, map[string]int{"sleep": 0}, "sleep", telemetry.FunctionCapacity{InstancesAvailable: 9})
	if !ok || got.InstancesAvailable != 9 {
		t.Fatalf("teto 0 deveria preservar avail=9: (%+v,%v)", got, ok)
	}
	// A negative value is zeroed before the clamping.
	got, ok = SanitizeForNode([]string{"sleep"}, nil, "sleep", telemetry.FunctionCapacity{InstancesAvailable: -1})
	if !ok || got.InstancesAvailable != 0 {
		t.Fatalf("avail negativo deveria virar 0: (%+v,%v)", got, ok)
	}
}

func TestSweepExpiredGC(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	m := NewMonitor()
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{InstancesAvailable: 1}, now)
	m.UpdateCapacity("n1", "alpr", telemetry.FunctionCapacity{InstancesAvailable: 1}, now.Add(-CapacityTTL-time.Second))
	m.SweepExpired(now)
	if _, ok := m.GetCapacity("n1", "sleep", now); !ok {
		t.Fatal("SweepExpired removeu entrada fresca, esperado manter")
	}
	if _, ok := m.GetCapacity("n1", "alpr", now); ok {
		t.Fatal("SweepExpired manteve entrada expirada, esperado remover")
	}
	// A node whose only function expired disappears from the map, leaving
	// no phantom key behind.
	m.SweepExpired(now.Add(CapacityTTL + time.Second))
	if _, ok := m.GetCapacity("n1", "sleep", now.Add(CapacityTTL+time.Second)); ok {
		t.Fatal("SweepExpired deveria remover a última entrada expirada do nó")
	}
}

func TestDeleteCapacitiesPreservesTelemetry(t *testing.T) {
	m := NewMonitor()
	now := time.Now()
	m.Update(telemetry.Package{NodeID: "n1", CPUUsagePct: 10})
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{InstancesAvailable: 0}, now)
	// A re-registration clears the inherited occupancy without dropping
	// the telemetry.
	m.DeleteCapacities("n1")
	if _, ok := m.GetCapacity("n1", "sleep", now); ok {
		t.Fatal("DeleteCapacities manteve ocupação, esperado remover")
	}
	if latest, ok := m.Latest("n1"); !ok || latest.CPUUsagePct != 10 {
		t.Fatalf("DeleteCapacities derrubou telemetria: (%+v,%v)", latest, ok)
	}
}
