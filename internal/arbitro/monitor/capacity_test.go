package monitor

// The capacity store keeps the freshness of each sample with a lifetime
// measured on the clock of the gateway, merges the samples without ever
// regressing and marks a node as exhausted after a parsed rejection.

import (
	"sync"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func TestCapacityTTL(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	m := NewMonitor()
	if m == nil {
		t.Fatalf("NewMonitor retornou nil")
	}

	cap := telemetry.FunctionCapacity{MaxInstances: 2, InstancesAvailable: 2}
	m.UpdateCapacity("n1", "sleep", cap, now)

	// Fresh within the lifetime.
	if got, ok := m.GetCapacity("n1", "sleep", now.Add(29*time.Second)); !ok || got.InstancesAvailable != 2 {
		t.Fatalf("GetCapacity fresco = (%+v,%v), esperado (avail=2,true)", got, ok)
	}
	// Exactly at the lifetime the sample is still fresh, since only a
	// sample older than the lifetime expires.
	if _, ok := m.GetCapacity("n1", "sleep", now.Add(CapacityTTL)); !ok {
		t.Fatalf("GetCapacity no limite do TTL = false, esperado true (só > TTL expira)")
	}
	// Older than the lifetime the sample is unknown, which never causes
	// gating.
	if _, ok := m.GetCapacity("n1", "sleep", now.Add(CapacityTTL+time.Nanosecond)); ok {
		t.Fatalf("GetCapacity expirado = true, esperado false")
	}
	// An absent key is unknown and never causes gating.
	if _, ok := m.GetCapacity("n1", "inexistente", now); ok {
		t.Fatalf("GetCapacity função ausente = true, esperado false")
	}
	if _, ok := m.GetCapacity("no-inexistente", "sleep", now); ok {
		t.Fatalf("GetCapacity nó ausente = true, esperado false")
	}

	// A refresh renews the lifetime by moving the update time forward.
	m.UpdateCapacity("n1", "sleep", cap, now.Add(20*time.Second))
	if _, ok := m.GetCapacity("n1", "sleep", now.Add(45*time.Second)); !ok {
		t.Fatalf("GetCapacity após refresh = false, esperado true (updatedAt=now+20s, 25s < TTL)")
	}
}

func TestCapacityMergePiggybackPeriodic(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	m := NewMonitor()

	// The piggyback of a successful invocation carries only the invoked
	// function.
	piggy := telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 0, QueueDepth: 3}
	m.UpdateCapacity("n1", "sleep", piggy, now)
	if got, ok := m.GetCapacity("n1", "sleep", now); !ok || got.QueueDepth != 3 {
		t.Fatalf("piggyback = (%+v,%v), esperado (queue=3,true)", got, ok)
	}
	// A function not touched by the piggyback stays unknown.
	if _, ok := m.GetCapacity("n1", "alpr", now); ok {
		t.Fatalf("função fora do piggyback = true, esperado false")
	}

	// A later periodic report carries the complete map and is merged on
	// top, keeping the newest update time per function.
	periodic := map[string]telemetry.FunctionCapacity{
		"sleep": {MaxInstances: 1, InstancesAvailable: 1},
		"alpr":  {MaxInstances: 4, InstancesAvailable: 4},
	}
	later := now.Add(10 * time.Second)
	for fn, c := range periodic {
		m.UpdateCapacity("n1", fn, c, later)
	}
	if got, ok := m.GetCapacity("n1", "sleep", later); !ok || got.InstancesAvailable != 1 {
		t.Fatalf("sleep após periódico = (%+v,%v), esperado (avail=1,true)", got, ok)
	}
	if got, ok := m.GetCapacity("n1", "alpr", later); !ok || got.MaxInstances != 4 {
		t.Fatalf("alpr após periódico = (%+v,%v), esperado (max=4,true)", got, ok)
	}

	// An out-of-order delivery never regresses the entry, which covers a
	// duplicated piggyback and a delayed periodic report.
	stale := telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 0, QueueDepth: 9}
	m.UpdateCapacity("n1", "sleep", stale, now)
	if got, ok := m.GetCapacity("n1", "sleep", later); !ok || got.InstancesAvailable != 1 || got.QueueDepth != 0 {
		t.Fatalf("merge retrocedeu = (%+v,%v), esperado (avail=1,queue=0,true)", got, ok)
	}

	// The optimistic mark after a parsed rejection zeroes the availability
	// of the requested function even without a fresh piggyback, and
	// expires with the same lifetime.
	m.MarkExhausted("n1", "sleep", later.Add(time.Second))
	if got, ok := m.GetCapacity("n1", "sleep", later.Add(time.Second)); !ok || got.InstancesAvailable != 0 {
		t.Fatalf("após MarkExhausted = (%+v,%v), esperado (avail=0,true)", got, ok)
	}
	// The ceiling of the current registration is preserved.
	if got, _ := m.GetCapacity("n1", "sleep", later.Add(time.Second)); got.MaxInstances != 1 {
		t.Fatalf("MarkExhausted perdeu MaxInstances = %+v, esperado max=1", got)
	}
	if _, ok := m.GetCapacity("n1", "sleep", later.Add(time.Second).Add(CapacityTTL+time.Nanosecond)); ok {
		t.Fatalf("MarkExhausted não expirou após TTL, esperado false")
	}

	// The optimistic mark with an older receive time does not regress.
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 1}, later.Add(5*time.Second))
	m.MarkExhausted("n1", "sleep", later)
	if got, ok := m.GetCapacity("n1", "sleep", later.Add(5*time.Second)); !ok || got.InstancesAvailable != 1 {
		t.Fatalf("MarkExhausted retrocedeu = (%+v,%v), esperado (avail=1,true)", got, ok)
	}
}

func TestCapacityTimestampIgnored(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	m := NewMonitor()

	// The timestamp of the node package, whether one hour in the future, absent
	// or any other value, never enters the ordering: the store only knows the
	// receive time on the gateway, which removes the clock skew between
	// devices.
	fresh := telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 1}
	m.UpdateCapacity("n1", "sleep", fresh, now)

	stale := telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 0}
	m.UpdateCapacity("n1", "sleep", stale, now.Add(-time.Second))
	if got, ok := m.GetCapacity("n1", "sleep", now); !ok || got.InstancesAvailable != 1 {
		t.Fatalf("recvNow antigo retrocedeu = (%+v,%v), esperado (avail=1,true)", got, ok)
	}

	// An update time in the future on the clock of the gateway does not
	// panic and counts as fresh, since the lifetime measures the age of
	// the sample and a negative age is fresh by construction.
	m.UpdateCapacity("n2", "sleep", fresh, now.Add(time.Hour))
	if _, ok := m.GetCapacity("n2", "sleep", now); !ok {
		t.Fatalf("recvNow +1h = false, esperado true (fresco, sem panic)")
	}
}

func TestCapacityDeleteClearsNode(t *testing.T) {
	now := time.Now().UTC()
	m := NewMonitor()
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1}, now)
	m.Delete("n1")
	if _, ok := m.GetCapacity("n1", "sleep", now); ok {
		t.Fatalf("GetCapacity após Delete = true, esperado false")
	}
}

func TestCapacityConcurrency(t *testing.T) {
	now := time.Now().UTC()
	m := NewMonitor()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			m.UpdateCapacity("n-conc", "sleep", telemetry.FunctionCapacity{InstancesAvailable: i}, now)
		}(i)
		go func() { defer wg.Done(); _, _ = m.GetCapacity("n-conc", "sleep", now) }()
		go func() { defer wg.Done(); m.MarkExhausted("n-conc", "sleep", now) }()
	}
	wg.Wait()
	if _, ok := m.GetCapacity("n-conc", "sleep", now); !ok {
		t.Fatalf("GetCapacity após concorrência = false, esperado true")
	}
}
