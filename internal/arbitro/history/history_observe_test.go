// Observation hook and replay at the level of the entries.
package history

import (
	"sync"
	"testing"
	"time"
)

func observeTestEntry(node string, outcome string, latency int64) Entry {
	return Entry{
		Timestamp:         time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC),
		FunctionName:      "fn",
		NodeID:            node,
		Layer:             "borda",
		ScenarioContext:   ScenarioContext{LoadBucket: BucketUnknown},
		ObservedLatencyMS: latency,
		Outcome:           outcome,
	}
}

// TestSetObserverFiresOnce: one call per grouped append, with the group and the
// entry, and the aggregated fan-out does not fire again.
func TestSetObserverFiresOnce(t *testing.T) {
	h, err := NewHistory("", 10)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	calls := 0
	var gotGroup, gotNode, gotOutcome string
	h.SetObserver(func(group string, e Entry) {
		calls++
		gotGroup, gotNode, gotOutcome = group, e.NodeID, e.Outcome
	})
	if err := h.AppendWithGroup(observeTestEntry("n1", OutcomeSucesso, 100), "g"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if calls != 1 {
		t.Fatalf("chamadas=%d want 1 (fan-out agregado não re-dispara)", calls)
	}
	if gotGroup != "g" || gotNode != "n1" || gotOutcome != OutcomeSucesso {
		t.Fatalf("got (%q,%q,%q) want (g,n1,sucesso)", gotGroup, gotNode, gotOutcome)
	}
	h.SetObserver(nil)
	if err := h.AppendWithGroup(observeTestEntry("n1", OutcomeSucesso, 100), "g"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if calls != 1 {
		t.Fatalf("chamadas=%d após nil want 1", calls)
	}
}

// TestForEachEntryChronological: the order is by key and chronological within a
// key, including the aggregated key, since the API is generic and the replay of
// the selector filters.
func TestForEachEntryChronological(t *testing.T) {
	h, err := NewHistory("", 10)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	base := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	mk := func(lat int64, at time.Time) Entry {
		e := observeTestEntry("n1", OutcomeSucesso, lat)
		e.Timestamp = at
		return e
	}
	// An append out of chronological order: the storage keeps the order of the
	// appends.
	if err := h.AppendWithGroup(mk(300, base.Add(3*time.Second)), "g2"); err != nil {
		t.Fatal(err)
	}
	if err := h.AppendWithGroup(mk(100, base.Add(time.Second)), "g1"); err != nil {
		t.Fatal(err)
	}
	if err := h.AppendWithGroup(mk(200, base.Add(2*time.Second)), "g1"); err != nil {
		t.Fatal(err)
	}
	var keys []string
	var lats []int64
	h.ForEachEntry(func(key string, e Entry) {
		keys = append(keys, key)
		lats = append(lats, e.ObservedLatencyMS)
	})
	// The ordering between the primary and the aggregated keys is not asserted,
	// since only the presence of each key with its entries in the order of the
	// appends matters.
	byKey := map[string][]int64{}
	for i, k := range keys {
		byKey[k] = append(byKey[k], lats[i])
	}
	prim1 := SegmentKey("g1", "n1", ScenarioContext{LoadBucket: BucketUnknown})
	if got := byKey[prim1]; len(got) != 2 || got[0] != 100 || got[1] != 200 {
		t.Fatalf("g1 = %v want [100 200] (ordem de append)", got)
	}
	prim2 := SegmentKey("g2", "n1", ScenarioContext{LoadBucket: BucketUnknown})
	if got := byKey[prim2]; len(got) != 1 || got[0] != 300 {
		t.Fatalf("g2 = %v want [300]", got)
	}
	agg1 := AggLayerKey("g1", "borda", ScenarioContext{LoadBucket: BucketUnknown})
	if got := byKey[agg1]; len(got) != 2 {
		t.Fatalf("agg g1 = %v want 2 (fan-out incluído na API genérica)", got)
	}
}

// TestForEachEntrySnapshotIsolation: the callback sees the copy taken under the
// read lock, and the concurrent appends neither corrupt it nor change the
// snapshot.
func TestForEachEntrySnapshotIsolation(t *testing.T) {
	h, err := NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := h.AppendWithGroup(observeTestEntry("n1", OutcomeSucesso, int64(i)), "g"); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = h.AppendWithGroup(observeTestEntry("n1", OutcomeSucesso, int64(100+i)), "g")
		}
	}()
	seen := 0
	h.ForEachEntry(func(_ string, _ Entry) { seen++ })
	wg.Wait()
	if seen < 10 {
		t.Fatalf("vistos=%d want >= 10", seen)
	}
}
