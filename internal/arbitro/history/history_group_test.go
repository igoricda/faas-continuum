package history

// Tests of the semantics of the group: the grouped append keys the statistics by
// the resolved group while the entry preserves the requested name, the old keys
// by name coexist without a migration, and a new category, with no samples,
// reports false, so the cold start falls back to the hard limit.

import (
	"testing"
	"time"
)

func TestAppendWithGroupSharedKey(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	mk := func(requested string) Entry {
		return Entry{FunctionName: requested, NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: 100, Outcome: OutcomeSucesso}
	}
	// Two variants of the same category are recorded under the same group.
	if err := h.AppendWithGroup(mk("alpr-yolo-tiny"), "alpr-yolo"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	if err := h.AppendWithGroup(mk("alpr-yolo-full"), "alpr-yolo"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	key := SegmentKey("alpr-yolo", "n1", sc)
	count, _, _, _, ok := h.Stats(key)
	if !ok {
		t.Fatalf("Stats(%q) ok=false, esperado true (curva compartilhada)", key)
	}
	if count != 2 {
		t.Fatalf("Stats(%q) count=%d, esperado 2 (duas variantes, uma chave)", key, count)
	}
	// The keys by variant do not exist in isolation, so the history does not fragment.
	for _, variant := range []string{"alpr-yolo-tiny", "alpr-yolo-full"} {
		if _, _, _, _, ok := h.Stats(SegmentKey(variant, "n1", sc)); ok {
			t.Fatalf("Stats(%q) ok=true, esperado false (sem chave por variante)", variant)
		}
	}
}

func TestAppendWithGroupKeepsRequestedName(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	e := Entry{FunctionName: "alpr-yolo-tiny", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: 120, Outcome: OutcomeSucesso}
	if err := h.AppendWithGroup(e, "alpr-yolo"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	keys := h.KeysForNode("n1")
	if len(keys) != 1 {
		t.Fatalf("KeysForNode = %d chaves, esperado 1 (chave de grupo única)", len(keys))
	}
	// A Entry armazenada preserva o nome pedido (client-facing).
	stored := h.m[keys[0]]
	if len(stored) != 1 || stored[0].FunctionName != "alpr-yolo-tiny" {
		t.Fatalf("Entry armazenada = %+v, esperado function_name=pedido", stored)
	}
}

func TestOldPerNameKeysStillParse(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	// The old key by name, through a grouped append whose group is the name, coexists with the key of the group.
	eOld := Entry{FunctionName: "alpr-yolo-tiny", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: 100, Outcome: OutcomeSucesso}
	if err := h.AppendWithGroup(eOld, eOld.FunctionName); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	if err := h.AppendWithGroup(Entry{FunctionName: "alpr-yolo-full", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: 100, Outcome: OutcomeSucesso}, "alpr-yolo"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	oldKey := SegmentKey("alpr-yolo-tiny", "n1", sc)
	if _, _, _, _, _, ok := h.StatsAndRecent(oldKey, time.Minute, now); !ok {
		t.Fatalf("StatsAndRecent(%q) ok=false, chave antiga deve continuar legível", oldKey)
	}
	groupKey := SegmentKey("alpr-yolo", "n1", sc)
	if _, _, _, _, _, ok := h.StatsAndRecent(groupKey, time.Minute, now); !ok {
		t.Fatalf("StatsAndRecent(%q) ok=false, chave de grupo deve ser legível", groupKey)
	}
}

func TestColdStartFreshCategory(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	// A new category has no samples, so it reports false, is not ready and falls
	// back to the hard limit until the minimum of samples, without a panic.
	fresh := SegmentKey("categoria-nova", "n1", sc)
	if _, _, _, _, _, ok := h.StatsAndRecent(fresh, time.Minute, now); ok {
		t.Fatalf("StatsAndRecent(%q) ok=true, esperado false (categoria fria)", fresh)
	}
	if _, _, _, _, ok := h.Stats(fresh); ok {
		t.Fatalf("Stats(%q) ok=true, esperado false (categoria fria)", fresh)
	}
	// A sample below the usual minimum of samples is not ready at the gate, which
	// requires the existence of the segment and at least the minimum of samples,
	// and it counts one without a panic.
	if err := h.AppendWithGroup(Entry{FunctionName: "v1", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: 100, Outcome: OutcomeSucesso}, "categoria-nova"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	count, _, _, _, ok := h.Stats(fresh)
	if !ok || count != 1 {
		t.Fatalf("Stats(%q) = (%d,%v), esperado (1,true)", fresh, count, ok)
	}
}
