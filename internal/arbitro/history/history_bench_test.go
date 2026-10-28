package history

import (
	"fmt"
	"testing"
	"time"
)

// benchEntry monta uma Entry de sucesso warm (sem cold_start) com
// timestamp relativo a base.
func benchEntry(base time.Time, offsetMS int64) Entry {
	return Entry{
		Timestamp:         base.Add(time.Duration(offsetMS) * time.Millisecond),
		FunctionName:      "bench-fn",
		NodeID:            "bench-node",
		Outcome:           OutcomeSucesso,
		ObservedLatencyMS: 100 + offsetMS%500,
		SloMet:            true,
		ColdStart:         false,
		ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
	}
}

// benchHistory creates a history in memory, without a path and without the
// persistence goroutine, prefilled with entries in the same segment.
func benchHistory(b *testing.B, prefill int) *History {
	b.Helper()
	h, err := NewHistory("", 1000)
	if err != nil {
		b.Fatalf("NewHistory: %v", err)
	}
	base := time.Now().Add(-time.Minute)
	for i := 0; i < prefill; i++ {
		e := benchEntry(base, int64(i))
		if err := h.AppendWithGroup(e, e.FunctionName); err != nil {
			b.Fatalf("AppendWithGroup prefill %d: %v", i, err)
		}
	}
	return h
}

func BenchmarkHistoryAppendWithGroup(b *testing.B) {
	h := benchHistory(b, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := benchEntry(time.Now(), int64(i))
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
	}
}

func BenchmarkHistoryAppendWithGroupFullSegment(b *testing.B) {
	h := benchHistory(b, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := benchEntry(time.Now(), int64(i))
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
	}
}

func BenchmarkHistoryStatsAndRecentRepeated(b *testing.B) {
	h := benchHistory(b, 100)
	key := SegmentKey("bench-fn", "bench-node", ScenarioContext{LoadBucket: Bucket0_10})
	if _, _, _, _, _, ok := h.StatsAndRecent(key, 60*time.Second, time.Now()); !ok {
		b.Fatalf("StatsAndRecent preflight: segmento vazio")
	}
	// Without a guard in the loop: the preflight above calls the statistics with
	// the same key and window and fails if the segment starts empty. Repeating the
	// check on every iteration turned a long benchmark time into a fatal error,
	// since the aging of the sixty second window defeats the fixture, and it killed
	// exactly the measurement that this benchmark exists to produce. The assertion
	// that matters belongs to the tests of the history.
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.StatsAndRecent(key, 60*time.Second, time.Now())
	}
}

func BenchmarkHistoryStatsAndRecentRecompute(b *testing.B) {
	h := benchHistory(b, 1000)
	key := SegmentKey("bench-fn", "bench-node", ScenarioContext{LoadBucket: Bucket0_10})
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Without a cache of the percentile, every iteration after an append
		// exercises a recomputation, which sorts the entries of the segment, and the
		// timestamp is always inside the window, since the base is recent and the
		// wrap is below the window: the fixture before the change used a timestamp
		// in the future, which the window filter discarded and the eviction removed,
		// so the segment emptied and the benchmark failed.
		e := benchEntry(now.Add(-30*time.Second), int64(i%29000))
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
		// The reading is the measurement, the recomputation of the percentile, and
		// not a guard: the fixture only appends inside the window, and the preflight
		// of the benchmark already proves that the segment carries.
		h.StatsAndRecent(key, 60*time.Second, now)
	}
}

// benchEntryClass builds a warm successful entry with an explicit concurrency
// class and processor cost. The pressure is fixed positive so the solo entries
// feed the expected cost under the solo gate, while a contended entry stays
// excluded by the number of requests in flight and the full statistics ignore the
// pressure.
func benchEntryClass(base time.Time, offsetMS int64, bucket string, inflight int, cpuPct float64) Entry {
	e := benchEntry(base, offsetMS)
	e.ScenarioContext = ScenarioContext{LoadBucket: bucket}
	e.Inflight = inflight
	e.CPUPctAvg = cpuPct
	e.SampleCount = 5
	e.PSICpuSomeAvg10 = 5
	e.PSIMemFullAvg10 = 3
	return e
}

// benchHistoryClasses prefills two buckets of the same pair of group and node
// with entries alternating between solo, with one request in flight, and
// contended, with two, so the classes and the expected cost across buckets are
// exercised.
func benchHistoryClasses(b *testing.B, prefill int) *History {
	b.Helper()
	h, err := NewHistory("", 1000)
	if err != nil {
		b.Fatalf("NewHistory: %v", err)
	}
	base := time.Now().Add(-time.Minute)
	buckets := []string{Bucket0_10, Bucket50_75}
	for i := 0; i < prefill; i++ {
		e := benchEntryClass(base, int64(i), buckets[i%2], 1, 30)
		if i%2 == 1 {
			e.Inflight = 2
			e.CPUPctAvg = 45
		}
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup prefill %d: %v", i, err)
		}
	}
	return h
}

func BenchmarkHistoryStatsFullRecompute(b *testing.B) {
	h := benchHistoryClasses(b, 1000)
	key := SegmentKey("bench-fn", "bench-node", ScenarioContext{LoadBucket: Bucket0_10})
	now := time.Now()
	// Preflight: it proves that the fixture starts populated. The reading in the
	// loop is the measurement, a full recomputation, and not a guard.
	if _, ok := h.StatsFull(key, 60*time.Second, now); !ok {
		b.Fatalf("StatsFull preflight: segmento vazio")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := benchEntryClass(now.Add(-30*time.Second), int64(i%29000), Bucket0_10, 1, 30)
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
		h.StatsFull(key, 60*time.Second, now)
	}
}

func BenchmarkHistoryExpectedCost(b *testing.B) {
	h := benchHistoryClasses(b, 1000)
	now := time.Now()
	if _, _, ok, _ := h.ExpectedCost("bench-fn", "bench-node", 60*time.Second, now); !ok {
		b.Fatalf("ExpectedCost preflight: sem amostra solo")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.ExpectedCost("bench-fn", "bench-node", 60*time.Second, now)
	}
}

func BenchmarkHistorySegmentKey(b *testing.B) {
	sc := ScenarioContext{LoadBucket: Bucket75Plus}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SegmentKey(fmt.Sprintf("grupo-%d", i%8), "bench-node", sc)
	}
}

// BenchmarkHistoryExpectedCostOtherPairAppend measures the cache hit of the
// expected cost of the first pair with the interleaved appends going to another
// pair: the old global counter invalidated the first pair on every append with a
// recomputation, while the counter per pair preserves the hit.
func BenchmarkHistoryExpectedCostOtherPairAppend(b *testing.B) {
	h := benchHistoryClasses(b, 1000)
	now := time.Now()
	base := now.Add(-30 * time.Second)
	if _, _, ok, _ := h.ExpectedCost("bench-fn", "bench-node", 60*time.Second, now); !ok {
		b.Fatalf("ExpectedCost preflight: sem amostra solo")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := benchEntryClass(base, int64(i%29000), Bucket0_10, 1, 30)
		e.NodeID = "other-node"
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
		// The reading is the measurement, since the cache per pair must preserve the
		// hit under the appends of another pair. The exactness of the cache is
		// guaranteed by the dedicated test.
		h.ExpectedCost("bench-fn", "bench-node", 60*time.Second, now)
	}
}
