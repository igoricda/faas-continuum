package monitor

// The optimistic mark is cleared per key: an occupancy update clears the
// mark only in the same node and function where the mark was set, so an
// update of one variant never clears the mark of another.

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func TestCapacityExhaustedSameKeyUpdateClearsDifferentKeyDoesNot(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	free := telemetry.FunctionCapacity{MaxInstances: 2, InstancesAvailable: 2}

	// The same key: a fresh update clears the mark.
	same := NewMonitor()
	same.MarkExhausted("n1", "cat-v1", now)
	if _, mark, ok := same.GetCapacityEx("n1", "cat-v1", now); !ok || !mark {
		t.Fatalf("pré-condição: GetCapacityEx(cat-v1) = (mark=%v,ok=%v), esperado (true,true)", mark, ok)
	}
	same.UpdateCapacity("n1", "cat-v1", free, now)
	if _, mark, ok := same.GetCapacityEx("n1", "cat-v1", now); !ok || mark {
		t.Fatalf("update same-key deveria limpar a marca: (mark=%v,ok=%v)", mark, ok)
	}

	// A different key: the update of one variant does not clear the mark
	// of another.
	diff := NewMonitor()
	diff.MarkExhausted("n1", "cat-v1", now)
	diff.UpdateCapacity("n1", "cat-v2", free, now)
	if _, mark, ok := diff.GetCapacityEx("n1", "cat-v1", now); !ok || !mark {
		t.Fatalf("update different-key NÃO pode limpar cat-v1: (mark=%v,ok=%v)", mark, ok)
	}
	if _, mark, ok := diff.GetCapacityEx("n1", "cat-v2", now); !ok || mark {
		t.Fatalf("cat-v2 após update fresco = (mark=%v,ok=%v), esperado (false,true)", mark, ok)
	}
}
