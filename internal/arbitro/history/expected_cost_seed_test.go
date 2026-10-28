package history

// Coverage of the expected cost with the pressure gate and the layer seed.
//
// Convention of the seed, in the helper below: eligible samples with one
// request in flight, a positive processor usage, a warm instance, a success and
// a positive pressure on both axes, with the layer filled, which is what
// produces the aggregated fan-out consumed by the seeding branch. The thin node
// never has an eligible solo sample of its own, and the donor feeds the prior of
// the layer.

import (
	"testing"
	"time"
)

// seedCostEntry records an eligible solo sample, except for the overrides, for a
// given function, node, layer and bucket.
func seedCostEntry(t *testing.T, h *History, fn, node, layer, bucket string, cpu float64, now time.Time) {
	t.Helper()
	e := Entry{
		FunctionName:      fn,
		NodeID:            node,
		Layer:             layer,
		Timestamp:         now,
		ScenarioContext:   ScenarioContext{LoadBucket: bucket},
		ObservedLatencyMS: 100,
		Outcome:           OutcomeSucesso,
		Inflight:          1,
		CPUPctAvg:         cpu,
		PSICpuSomeAvg10:   5,
		PSIMemFullAvg10:   3,
	}
	if err := h.AppendWithGroup(e, fn); err != nil {
		t.Fatalf("AppendWithGroup(%s/%s): %v", node, layer, err)
	}
}

func TestExpectedCostSeed(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Minute
	const fn = "grp"

	t.Run("par-sem-solo-recebe-seed-da-camada", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		// The donor at the edge: two eligible solo samples plus pollution that must
		// never enter the seed, which are a contended sample, a cold one and one
		// without pressure, all with a layer so that they fall into the fan-out.
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 20, now)
		seedCostEntry(t, h, fn, "donor", "borda", Bucket50_75, 40, now)
		for _, e := range []Entry{
			{FunctionName: fn, NodeID: "donor", Layer: "borda", Timestamp: now, ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100, Outcome: OutcomeSucesso, Inflight: 2, CPUPctAvg: 9999, PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3},
			{FunctionName: fn, NodeID: "donor", Layer: "borda", Timestamp: now, ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100, Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 9999, ColdStart: true, PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3},
			{FunctionName: fn, NodeID: "donor", Layer: "borda", Timestamp: now, ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100, Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 9999},
		} {
			if err := h.AppendWithGroup(e, fn); err != nil {
				t.Fatalf("AppendWithGroup poluição: %v", err)
			}
		}
		// The thin node has only contended samples with a layer, which probes the layer and proves the exclusion of the local branch.
		if err := h.AppendWithGroup(Entry{
			FunctionName: fn, NodeID: "thin", Layer: "borda", Timestamp: now,
			ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100,
			Outcome: OutcomeSucesso, Inflight: 2, CPUPctAvg: 777,
			PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3,
		}, fn); err != nil {
			t.Fatalf("AppendWithGroup thin: %v", err)
		}

		cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now)
		if !ok || !seeded || n != 2 || cost != 30 {
			t.Fatalf("seed = (%v,%d,%v,seeded=%v), esperado (30,2,true,true)", cost, n, ok, seeded)
		}
	})

	t.Run("par-com-solo-usa-media-local", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 20, now)
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 40, now)
		// The thin node with a solo sample of its own: the local sample wins and the seed is ignored.
		seedCostEntry(t, h, fn, "thin", "borda", Bucket0_10, 100, now)

		cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now)
		if !ok || seeded || n != 1 || cost != 100 {
			t.Fatalf("local = (%v,%d,%v,seeded=%v), esperado (100,1,true,false)", cost, n, ok, seeded)
		}
	})

	t.Run("psi-zero-excluido-nas-duas-pernas", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 20, now)
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 40, now)
		// The thin node with a solo sample of a high processor usage but without pressure stays out of the local branch, since it neither counts nor blocks the seed, and falls back to the prior of the layer.
		if err := h.AppendWithGroup(Entry{
			FunctionName: fn, NodeID: "thin", Layer: "borda", Timestamp: now,
			ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100,
			Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 9999,
		}, fn); err != nil {
			t.Fatalf("AppendWithGroup thin PSI==0: %v", err)
		}

		cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now)
		if !ok || !seeded || n != 2 || cost != 30 {
			t.Fatalf("seed-sobre-PSI==0 = (%v,%d,%v,seeded=%v), esperado (30,2,true,true)", cost, n, ok, seeded)
		}
	})

	t.Run("seed-escopado-na-camada-do-par", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		seedCostEntry(t, h, fn, "donor-b", "borda", Bucket0_10, 30, now)
		seedCostEntry(t, h, fn, "donor-n", "nevoa", Bucket0_10, 1000, now)
		// The thin node probes the edge with a contended sample that has a layer, so the seed comes only from the edge.
		if err := h.AppendWithGroup(Entry{
			FunctionName: fn, NodeID: "thin", Layer: "borda", Timestamp: now,
			ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100,
			Outcome: OutcomeSucesso, Inflight: 2, CPUPctAvg: 777,
			PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3,
		}, fn); err != nil {
			t.Fatalf("AppendWithGroup thin: %v", err)
		}

		cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now)
		if !ok || !seeded || n != 1 || cost != 30 {
			t.Fatalf("seed-borda = (%v,%d,%v,seeded=%v), esperado (30,1,true,true) sem a névoa", cost, n, ok, seeded)
		}
	})

	t.Run("sem-sonda-de-camada-seed-grupo-wide", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		seedCostEntry(t, h, fn, "donor-b", "borda", Bucket0_10, 20, now)
		seedCostEntry(t, h, fn, "donor-b", "borda", Bucket0_10, 40, now)
		seedCostEntry(t, h, fn, "donor-n", "nevoa", Bucket0_10, 1000, now)
		// Thin, with no entry at all: without a probe it pools every layer.

		cost, n, ok, seeded := h.ExpectedCost(fn, "ghost", window, now)
		want := (20.0 + 40.0 + 1000.0) / 3.0
		if !ok || !seeded || n != 3 || cost != want {
			t.Fatalf("seed-wide = (%v,%d,%v,seeded=%v), esperado (%v,3,true,true)", cost, n, ok, seeded, want)
		}
	})

	t.Run("sem-dado-de-camada-preserva-zero", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		// Without a layer, the contended samples reach neither the local branch nor the fan-out, which gives zero, zero and false.
		if err := h.AppendWithGroup(Entry{
			FunctionName: fn, NodeID: "thin", Timestamp: now,
			ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100,
			Outcome: OutcomeSucesso, Inflight: 2, CPUPctAvg: 80,
			PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3,
		}, fn); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
		if cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now); ok || n != 0 || cost != 0 || seeded {
			t.Fatalf("sem-camada = (%v,%d,%v,seeded=%v), esperado (0,0,false,false)", cost, n, ok, seeded)
		}
		// An empty history gives the same result.
		h2, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		if cost, n, ok, seeded := h2.ExpectedCost(fn, "thin", window, now); ok || n != 0 || cost != 0 || seeded {
			t.Fatalf("vazio = (%v,%d,%v,seeded=%v), esperado (0,0,false,false)", cost, n, ok, seeded)
		}
	})

	t.Run("cache-memoriza-seed-e-append-invalida", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 20, now)
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 40, now)

		cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now)
		if !ok || !seeded || n != 2 || cost != 30 {
			t.Fatalf("seed = (%v,%d,%v,seeded=%v), esperado (30,2,true,true)", cost, n, ok, seeded)
		}
		// A memorized hit with the seeded flag under the same generation of the pair.
		h.mu.RLock()
		entry, hit := h.costCache[fn+"\x00thin"]
		h.mu.RUnlock()
		if !hit {
			t.Fatal("seed não memorizado no costCache")
		}
		if !entry.seeded || !entry.ok || entry.n != 2 || entry.mean != 30 {
			t.Fatalf("cache = %+v, esperado {ok seeded n=2 mean=30}", entry)
		}
		// A second query at the same instant serves the hit, without a recomputation.
		if cost2, n2, ok2, seeded2 := h.ExpectedCost(fn, "thin", window, now); cost2 != 30 || n2 != 2 || !ok2 || !seeded2 {
			t.Fatalf("hit = (%v,%d,%v,seeded=%v), esperado (30,2,true,true)", cost2, n2, ok2, seeded2)
		}
		// An append of a solo sample in the pair bumps the generation and the seed
		// evaporates, becoming a measured local average.
		seedCostEntry(t, h, fn, "thin", "borda", Bucket0_10, 100, now)
		cost3, n3, ok3, seeded3 := h.ExpectedCost(fn, "thin", window, now)
		if !ok3 || seeded3 || n3 != 1 || cost3 != 100 {
			t.Fatalf("pós-append = (%v,%d,%v,seeded=%v), esperado (100,1,true,false)", cost3, n3, ok3, seeded3)
		}
	})
}
