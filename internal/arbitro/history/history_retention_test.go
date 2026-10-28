package history

// Coverage of the cut by age, which prevents an unbounded growth of the history:
// the pruning at the retention setting and at the load, the periodic pruning
// through the compaction, the removal of the empty segments, the invalidation of
// the derived caches and the flush only when there is something to write.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTrimSegment(t *testing.T) {
	now := time.Now().UTC()
	entrada := func(age time.Duration) Entry { return Entry{Timestamp: now.Add(-age)} }

	t.Run("retem After(cutoff) e descarta o resto", func(t *testing.T) {
		in := []Entry{entrada(3 * time.Hour), entrada(2 * time.Hour), entrada(30 * time.Minute), entrada(0)}
		out, removed := trimSegment(in, 100, time.Hour, now)
		if removed != 2 || len(out) != 2 {
			t.Fatalf("removed=%d len=%d, want 2/2", removed, len(out))
		}
		if !out[0].Timestamp.Equal(now.Add(-30*time.Minute)) || !out[1].Timestamp.Equal(now) {
			t.Fatalf("sobreviventes errados: %+v", out)
		}
	})

	t.Run("timestamp futuro e' preservado (clock skew)", func(t *testing.T) {
		futuro := Entry{Timestamp: now.Add(5 * time.Minute)}
		out, removed := trimSegment([]Entry{entrada(2 * time.Hour), futuro}, 100, time.Hour, now)
		if removed != 1 || len(out) != 1 || !out[0].Timestamp.Equal(futuro.Timestamp) {
			t.Fatalf("removed=%d out=%+v, want futuro preservado", removed, out)
		}
	})

	t.Run("retention<=0 nao corta (so cap FIFO)", func(t *testing.T) {
		in := []Entry{entrada(10 * time.Hour), entrada(9 * time.Hour), entrada(0)}
		out, removed := trimSegment(in, 2, 0, now)
		if removed != 1 || len(out) != 2 {
			t.Fatalf("removed=%d len=%d, want 1/2 (cap)", removed, len(out))
		}
		if !out[0].Timestamp.Equal(now.Add(-9*time.Hour)) || !out[1].Timestamp.Equal(now) {
			t.Fatalf("FIFO errado: %+v", out)
		}
	})

	t.Run("retention depois cap", func(t *testing.T) {
		in := []Entry{entrada(5 * time.Hour), entrada(90 * time.Minute), entrada(50 * time.Minute), entrada(10 * time.Minute)}
		out, removed := trimSegment(in, 1, time.Hour, now)
		if removed != 3 || len(out) != 1 || !out[0].Timestamp.Equal(now.Add(-10*time.Minute)) {
			t.Fatalf("removed=%d out=%+v, want 3 e a mais recente", removed, out)
		}
	})
}

func TestSetRetentionTrimsAndDropsEmptySegments(t *testing.T) {
	h, err := NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	old := Entry{NodeID: "n1", FunctionName: "f", Timestamp: time.Now().Add(-2 * time.Hour), Outcome: OutcomeSucesso, ObservedLatencyMS: 100}
	recent := Entry{NodeID: "n1", FunctionName: "f", Timestamp: time.Now().Add(-time.Minute), Outcome: OutcomeSucesso, ObservedLatencyMS: 200}
	sóAntiga := Entry{NodeID: "n2", FunctionName: "f", Timestamp: time.Now().Add(-3 * time.Hour), Outcome: OutcomeSucesso, ObservedLatencyMS: 300}
	for _, e := range []Entry{old, recent} {
		if err := h.AppendWithGroup(e, "f"); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.AppendWithGroup(sóAntiga, "f"); err != nil {
		t.Fatal(err)
	}
	if h.Retention() != 0 {
		t.Fatalf("retention inicial=%s, want 0", h.Retention())
	}

	removed, segments := h.SetRetention(time.Hour)
	if removed != 2 || segments != 1 {
		t.Fatalf("SetRetention: removed=%d segments=%d, want 2/1", removed, segments)
	}
	keys := h.Keys()
	if len(keys) != 1 {
		t.Fatalf("chaves=%v, want só o segmento com entrada recente", keys)
	}
	count, _, total, _, ok := h.Stats(keys[0])
	if !ok || count != 1 || total != 1 {
		t.Fatalf("Stats=%d/%d ok=%v, want 1/1", count, total, ok)
	}

	// Idempotent: the second pass finds nothing.
	removed2, segments2 := h.SetRetention(time.Hour)
	if removed2 != 0 || segments2 != 0 {
		t.Fatalf("segunda poda: removed=%d segments=%d, want 0/0", removed2, segments2)
	}
}

func TestRetentionZeroDisablesTrim(t *testing.T) {
	h, err := NewHistory("", 10)
	if err != nil {
		t.Fatal(err)
	}
	e := Entry{NodeID: "n1", FunctionName: "f", Timestamp: time.Now().Add(-100 * time.Hour), Outcome: OutcomeSucesso}
	if err := h.AppendWithGroup(e, "f"); err != nil {
		t.Fatal(err)
	}
	if removed, _ := h.SetRetention(0); removed != 0 {
		t.Fatalf("SetRetention(0) removeu %d, want 0 (desligado)", removed)
	}
	if len(h.Keys()) != 1 {
		t.Fatalf("chaves=%v, want entry preservada", h.Keys())
	}
}

func TestLoadAppliesRetentionAndRewritesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	h, err := NewHistory(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	old := Entry{NodeID: "n1", FunctionName: "f", Timestamp: time.Now().Add(-4 * time.Hour), Outcome: OutcomeSucesso, ObservedLatencyMS: 100}
	recent := Entry{NodeID: "n1", FunctionName: "f", Timestamp: time.Now().Add(-time.Minute), Outcome: OutcomeSucesso, ObservedLatencyMS: 200}
	for _, e := range []Entry{old, recent} {
		if err := h.AppendWithGroup(e, "f"); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}

	h2, err := NewHistoryWithRetention(path, 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	keys := h2.Keys()
	if len(keys) != 1 {
		t.Fatalf("chaves pós-load=%v, want 1", keys)
	}
	count, _, total, _, ok := h2.Stats(keys[0])
	if !ok || count != 1 || total != 1 {
		t.Fatalf("Stats pós-load=%d/%d ok=%v, want 1/1 (antiga fora)", count, total, ok)
	}
	// O load marca dirty: o primeiro flush reescreve o arquivo sem a antiga.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if data, err := os.ReadFile(path); err == nil {
			var disk map[string][]Entry
			if jsonErr := json.Unmarshal(data, &disk); jsonErr == nil {
				if len(disk[keys[0]]) == 1 {
					break
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("arquivo não foi reescrito sem a entrada antiga")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestCompactInvalidatesCostCache(t *testing.T) {
	h, err := NewHistory("", 100)
	if err != nil {
		t.Fatal(err)
	}
	e := Entry{
		NodeID: "n1", FunctionName: "f", Timestamp: time.Now().Add(-2 * time.Second),
		Outcome: OutcomeSucesso, ObservedLatencyMS: 100, CPUPctAvg: 40, Inflight: 1,
		PSICpuSomeAvg10: 5, PSIMemFullAvg10: 5,
	}
	if err := h.AppendWithGroup(e, "f"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if mean, n, ok, _ := h.ExpectedCost("f", "n1", time.Hour, now); !ok || n != 1 || mean != 40 {
		t.Fatalf("ExpectedCost inicial=(%v,%d,%v), want (40,1,true)", mean, n, ok)
	}
	time.Sleep(10 * time.Millisecond)
	if removed, _ := h.SetRetention(time.Second); removed != 1 {
		t.Fatalf("podadas=%d, want 1", removed)
	}
	if mean, n, ok, _ := h.ExpectedCost("f", "n1", time.Hour, time.Now()); ok || n != 0 || mean != 0 {
		t.Fatalf("ExpectedCost pós-poda=(%v,%d,%v), want (0,0,false) — cache não invalidado", mean, n, ok)
	}
}

func TestLoopPeriodicTrimPersists(t *testing.T) {
	oldEvery := trimEvery
	trimEvery = 20 * time.Millisecond
	defer func() { trimEvery = oldEvery }()

	path := filepath.Join(t.TempDir(), "history.json")
	h, err := NewHistoryWithRetention(path, 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	sc := ScenarioContext{LoadBucket: BucketUnknown}
	recent := Entry{NodeID: "n1", FunctionName: "f", Timestamp: time.Now().Add(-time.Minute), Outcome: OutcomeSucesso, ObservedLatencyMS: 200, ScenarioContext: sc}
	if err := h.AppendWithGroup(recent, "f"); err != nil {
		t.Fatal(err)
	}
	// An old entry inserted directly into the map, which is an old late orphan, is
	// only reached by the periodic pruning, and it has to mark the history as dirty
	// so that the flush persists it.
	key := SegmentKey("f", "n1", sc)
	h.mu.Lock()
	h.m[key] = append(h.m[key], Entry{NodeID: "n1", FunctionName: "f", Timestamp: time.Now().Add(-2 * time.Hour), Outcome: OutcomeSucesso, ObservedLatencyMS: 300})
	h.mu.Unlock()

	deadline := time.Now().Add(3 * time.Second)
	for {
		count, _, total, _, _ := h.Stats(key)
		disk, err := readHistoryFile(t, path)
		if count == 1 && total == 1 && err == nil && len(disk[key]) == 1 {
			return // pruned in memory and persisted
		}
		if time.Now().After(deadline) {
			t.Fatalf("poda periódica não estabilizou: mem=%d/%d disco=%d err=%v", count, total, len(disk[key]), err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFlushOnlyWhenDirty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	h, err := NewHistory(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// Without any change, the ticker does not create or rewrite the file.
	time.Sleep(700 * time.Millisecond)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("arquivo existe sem mudanças (err=%v) — flush incondicional?", err)
	}

	e := Entry{NodeID: "n1", FunctionName: "f", Timestamp: time.Now(), Outcome: OutcomeSucesso, ObservedLatencyMS: 100}
	if err := h.AppendWithGroup(e, "f"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(700 * time.Millisecond)
	fi1, err := os.Stat(path)
	if err != nil {
		t.Fatalf("arquivo não persistiu após append: %v", err)
	}

	// Without new appends, the file is not rewritten and its modification time is stable.
	time.Sleep(700 * time.Millisecond)
	fi2, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !fi2.ModTime().Equal(fi1.ModTime()) {
		t.Fatalf("arquivo reescrito sem mudanças: mtime %v -> %v", fi1.ModTime(), fi2.ModTime())
	}
}
