package monitor

// The capacity lookup also reports the optimistic exhaustion mark, which
// distinguishes a node that is warm but at its ceiling from a node without
// a live instance. The plain lookup ignores the mark.

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func TestGetCapacityExNilReceiver(t *testing.T) {
	var m *Monitor
	cap, mark, ok := m.GetCapacityEx("n1", "sleep", time.Now())
	if ok || mark {
		t.Fatalf("nil receiver = (%+v, mark=%v, ok=%v), esperado (zero,false,false)", cap, mark, ok)
	}
	// The plain lookup delegates and inherits the nil safety.
	if _, ok := m.GetCapacity("n1", "sleep", time.Now()); ok {
		t.Fatalf("GetCapacity nil receiver = true, esperado false")
	}
}

func TestGetCapacityExUnknownAndTTL(t *testing.T) {
	now := time.Now().UTC()
	m := NewMonitor()
	if _, mark, ok := m.GetCapacityEx("n1", "sleep", now); ok || mark {
		t.Fatalf("nó ausente = (mark=%v, ok=%v), esperado (false,false)", mark, ok)
	}
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 1}, now)
	if _, mark, ok := m.GetCapacityEx("n1", "alpr", now); ok || mark {
		t.Fatalf("função ausente = (mark=%v, ok=%v), esperado (false,false)", mark, ok)
	}
	if _, mark, ok := m.GetCapacityEx("n1", "sleep", now.Add(CapacityTTL+time.Nanosecond)); ok || mark {
		t.Fatalf("TTL expirado = (mark=%v, ok=%v), esperado (false,false)", mark, ok)
	}
	// Exactly at the limit, and still fresh.
	if _, _, ok := m.GetCapacityEx("n1", "sleep", now.Add(CapacityTTL)); !ok {
		t.Fatalf("TTL == ainda fresco, esperado ok=true")
	}
}

func TestGetCapacityExExhaustedMarkLifecycle(t *testing.T) {
	now := time.Now().UTC()
	m := NewMonitor()
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 0}, now)

	// An availability of zero is not exhaustion: the node is genuinely
	// cold and carries no mark.
	cap, mark, ok := m.GetCapacityEx("n1", "sleep", now)
	if !ok || mark {
		t.Fatalf("UpdateCapacity avail=0 = (mark=%v, ok=%v), esperado genuine-cold sem marca", mark, ok)
	}
	if cap.InstancesAvailable != 0 {
		t.Fatalf("avail=%d esperado 0", cap.InstancesAvailable)
	}

	// Marking as exhausted sets the mark and zeroes the availability.
	m.MarkExhausted("n1", "sleep", now.Add(time.Second))
	if _, mark, ok := m.GetCapacityEx("n1", "sleep", now.Add(time.Second)); !ok || !mark {
		t.Fatalf("após MarkExhausted = (mark=%v, ok=%v), esperado (true,true)", mark, ok)
	}
	// The plain lookup ignores the mark.
	if cap, ok := m.GetCapacity("n1", "sleep", now.Add(time.Second)); !ok || cap.InstancesAvailable != 0 {
		t.Fatalf("GetCapacity após MarkExhausted = (%+v, ok=%v), esperado (avail=0,true)", cap, ok)
	}

	// A newer occupancy update clears the mark.
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 1}, now.Add(2*time.Second))
	if _, mark, ok := m.GetCapacityEx("n1", "sleep", now.Add(2*time.Second)); !ok || mark {
		t.Fatalf("UpdateCapacity novo deveria limpar a marca = (mark=%v, ok=%v)", mark, ok)
	}

	// The mark is set again, and an older occupancy update does not undo
	// it, since the merge never regresses.
	m.MarkExhausted("n1", "sleep", now.Add(3*time.Second))
	m.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 9}, now.Add(2*time.Second))
	if _, mark, ok := m.GetCapacityEx("n1", "sleep", now.Add(3*time.Second)); !ok || !mark {
		t.Fatalf("UpdateCapacity antigo não deveria limpar a marca = (mark=%v, ok=%v)", mark, ok)
	}

	// Deleting the node clears everything.
	m.Delete("n1")
	if _, mark, ok := m.GetCapacityEx("n1", "sleep", now.Add(3*time.Second)); ok || mark {
		t.Fatalf("após Delete = (mark=%v, ok=%v), esperado (false,false)", mark, ok)
	}
}
