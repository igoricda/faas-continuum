package registry

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

// The invocation timeout is validated like the idle timeout, and the queue
// size is accepted by both Register and Update with its combinations
// validated.
func TestAuditRegisterValidatesTimeoutAndQueue(t *testing.T) {
	reg, err := NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	if err != nil {
		t.Fatal(err)
	}

	for _, timeout := range []int{-1, -60000, MaxTimeoutMS + 1, 1<<31 - 1} {
		if _, _, err := reg.Register("host", "f", "img:1.0", RegisterOptions{TimeoutMS: timeout}); !errors.Is(err, ErrInvalidTimeoutMS) {
			t.Errorf("Register(TimeoutMS=%d) err = %v, esperado %v", timeout, err, ErrInvalidTimeoutMS)
		}
	}
	for i, timeout := range []int{0, 1, 5000, MaxTimeoutMS} {
		name := fmt.Sprintf("ok-%d", i)
		fn, _, err := reg.Register("host", name, "img:1.0", RegisterOptions{TimeoutMS: timeout})
		if err != nil {
			t.Errorf("Register(TimeoutMS=%d) err inesperado: %v", timeout, err)
			continue
		}
		if fn.TimeoutMS != timeout {
			t.Errorf("TimeoutMS = %d, esperado %d", fn.TimeoutMS, timeout)
		}
	}

	if _, _, err := reg.Register("host", "q1", "img:1.0", RegisterOptions{QueueMax: 4}); err != nil {
		t.Errorf("Register(QueueMax=4, sem teto) err inesperado: %v", err)
	} else if fn, _ := reg.Get("q1"); fn.QueueMax != 4 {
		t.Errorf("QueueMax = %d, esperado 4", fn.QueueMax)
	}
	if _, _, err := reg.Register("host", "q2", "img:1.0", RegisterOptions{MaxInstances: 3, QueueMax: 4}); !errors.Is(err, ErrInvalidScalingParams) {
		t.Errorf("Register(teto+fila) err = %v, esperado %v", err, ErrInvalidScalingParams)
	}
	if _, _, err := reg.Register("host", "q3", "img:1.0", RegisterOptions{QueueMax: 999}); !errors.Is(err, ErrInvalidScalingParams) {
		t.Errorf("Register(QueueMax=999) err = %v, esperado %v", err, ErrInvalidScalingParams)
	}
	if _, _, err := reg.Update("host", "q1", "img:2.0", RegisterOptions{TimeoutMS: -10}, false); !errors.Is(err, ErrInvalidTimeoutMS) {
		t.Errorf("Update(TimeoutMS=-10) err = %v, esperado %v", err, ErrInvalidTimeoutMS)
	}
}
