package monitor

// Edge cases of the capacity store that the other tests do not cover:
// marking as exhausted without a previous sample, preserving the queue
// depth, an update with the same timestamp, a negative age and the
// isolation between functions.

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func TestCapacityMarkExhaustedUnknownNode(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	m := NewMonitor()

	// Without a previous sample, the exhausted entry is created.
	m.MarkExhausted("n-novo", "sleep", now)
	got, ok := m.GetCapacity("n-novo", "sleep", now)
	if !ok {
		t.Fatal("GetCapacity após MarkExhausted sem baseline = false, esperado true")
	}
	if got.InstancesAvailable != 0 {
		t.Fatalf("avail = %d, esperado 0", got.InstancesAvailable)
	}
	if got.MaxInstances != 0 || got.QueueDepth != 0 {
		t.Fatalf("sem baseline = %+v, esperado zeros (nada a preservar)", got)
	}
	// Another function of the same node stays unknown: the mark belongs
	// to one function.
	if _, ok := m.GetCapacity("n-novo", "alpr", now); ok {
		t.Fatal("alpr após MarkExhausted(sleep) = true, esperado false")
	}
}

func TestCapacityMarkExhaustedPreservesDepth(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	m := NewMonitor()

	base := telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 4, QueueDepth: 7}
	m.UpdateCapacity("n1", "sleep", base, now)
	m.MarkExhausted("n1", "sleep", now.Add(time.Second))
	got, ok := m.GetCapacity("n1", "sleep", now.Add(time.Second))
	if !ok {
		t.Fatal("GetCapacity após MarkExhausted = false, esperado true")
	}
	if got.InstancesAvailable != 0 {
		t.Fatalf("avail = %d, esperado 0", got.InstancesAvailable)
	}
	if got.MaxInstances != 4 {
		t.Fatalf("MaxInstances = %d, esperado 4 (preservado)", got.MaxInstances)
	}
	if got.QueueDepth != 7 {
		t.Fatalf("QueueDepth = %d, esperado 7 (preservado)", got.QueueDepth)
	}
}

func TestCapacityUpdateEqualTimestampOverwrites(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	m := NewMonitor()

	// The guard compares the update time with the receive time, so an
	// equal timestamp overwrites the entry.
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 1}, now)
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 0, QueueDepth: 2}, now)
	got, ok := m.GetCapacity("n1", "sleep", now)
	if !ok {
		t.Fatal("GetCapacity = false, esperado true")
	}
	if got.InstancesAvailable != 0 || got.QueueDepth != 2 {
		t.Fatalf("após overwrite mesmo-ts = %+v, esperado (avail=0,queue=2)", got)
	}
}

func TestCapacityGetNegativeAgeFresh(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	m := NewMonitor()

	// An update time in the future, which means the sample was received
	// early: the negative age never exceeds the lifetime, so the entry
	// stays fresh.
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 1}, now.Add(time.Minute))
	got, ok := m.GetCapacity("n1", "sleep", now)
	if !ok || got.InstancesAvailable != 1 {
		t.Fatalf("idade negativa = (%+v,%v), esperado (avail=1,true)", got, ok)
	}
	// And it stays fresh exactly until the update time plus the lifetime,
	// not until the current time plus the lifetime.
	if _, ok := m.GetCapacity("n1", "sleep", now.Add(time.Minute).Add(CapacityTTL)); !ok {
		t.Fatal("no limite a partir do updatedAt futuro = false, esperado true")
	}
	if _, ok := m.GetCapacity("n1", "sleep", now.Add(time.Minute).Add(CapacityTTL+time.Nanosecond)); ok {
		t.Fatal("além do TTL a partir do updatedAt futuro = true, esperado false")
	}
}

func TestCapacityExhaustedPerFunctionIsolation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	m := NewMonitor()

	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 1}, now)
	m.UpdateCapacity("n1", "alpr", telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 4}, now)
	m.MarkExhausted("n1", "sleep", now.Add(time.Second))

	sleep, ok := m.GetCapacity("n1", "sleep", now.Add(time.Second))
	if !ok || sleep.InstancesAvailable != 0 {
		t.Fatalf("sleep = (%+v,%v), esperado (avail=0,true)", sleep, ok)
	}
	alpr, ok := m.GetCapacity("n1", "alpr", now.Add(time.Second))
	if !ok || alpr.InstancesAvailable != 4 {
		t.Fatalf("alpr = (%+v,%v), esperado (avail=4,true) — esgotamento é por função", alpr, ok)
	}
}
