package pool

import (
	"context"
	"testing"
)

// benchPool returns a Manager backed by a fake provisioner, without a
// container runtime, pre-provisioned with n warm instances of the function.
func benchPool(b *testing.B, warm int) *Manager {
	b.Helper()
	p := &fakeProvisioner{}
	m := NewManagerWithQueueCap(p, nil)
	for i := 0; i < warm; i++ {
		if _, _, err := m.AcquireWaitCold(context.Background(), "bench-fn", "img:1.0", 0); err != nil {
			b.Fatalf("warm provision %d: %v", i, err)
		}
	}
	return m
}

// BenchmarkAcquireWaitWarm measures the steady state of the warm path: the
// pre-provisioned instance is reserved and returned on every iteration,
// never touching the provisioner and never using the queue. It is the case
// of a pool under real load with its minimum already provisioned.
func BenchmarkAcquireWaitWarm(b *testing.B) {
	m := benchPool(b, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, err := m.AcquireWait(context.Background(), "bench-fn", "img:1.0", 0)
		if err != nil {
			b.Fatalf("AcquireWait: %v", err)
		}
		m.Release("bench-fn", id)
	}
}

// BenchmarkAcquireWaitWarmSharded measures contention between shards: the
// same warm path with distinct functions spread over the shards by the hash.
// Each iteration alternates between eight functions, all with one warm
// instance, which exercises the hash and the locks of different shards.
func BenchmarkAcquireWaitWarmSharded(b *testing.B) {
	m := benchPool(b, 1)
	for i := 1; i < 8; i++ {
		fn := "bench-fn-" + string(rune('a'+i))
		if _, _, err := m.AcquireWaitCold(context.Background(), fn, "img:1.0", 0); err != nil {
			b.Fatalf("warm provision %s: %v", fn, err)
		}
	}
	fns := []string{"bench-fn", "bench-fn-b", "bench-fn-c", "bench-fn-d", "bench-fn-e", "bench-fn-f", "bench-fn-g", "bench-fn-h"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn := fns[i%len(fns)]
		id, err := m.AcquireWait(context.Background(), fn, "img:1.0", 0)
		if err != nil {
			b.Fatalf("AcquireWait(%s): %v", fn, err)
		}
		m.Release(fn, id)
	}
}
