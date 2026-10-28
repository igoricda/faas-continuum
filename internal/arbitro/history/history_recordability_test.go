package history

// Regression of the decision of the percentile over the successes: the count and
// the percentile cover only the successes without a cold start, so the minimum
// of samples counts the successes, and a segment with no success never becomes
// ready, even under a burst of fast failures or long timeouts.

import (
	"testing"
	"time"
)

// Scenario of the percentile over the successes: twenty fast failures and five
// long timeouts do not make the segment ready, since the count and the percentile
// require a success. Without this the self-learning would prefer a node that
// never served, since a failure lowers the percentile and a timeout inflates it,
// with the minimum of samples reached by the raw attempts.
func TestStatsRequiresSuccessForReadiness(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	mk := func(outcome string, lat int64) Entry {
		return Entry{FunctionName: "fn", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: lat, Outcome: outcome}
	}
	for i := 0; i < 20; i++ {
		e := mk(OutcomeFalha, 5)
		if err := h.AppendWithGroup(e, e.FunctionName); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		e := mk(OutcomeTimeout, 5000)
		if err := h.AppendWithGroup(e, e.FunctionName); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	key := SegmentKey("fn", "n1", sc)
	if count, _, _, _, _, ok := h.StatsAndRecent(key, time.Minute, now); ok || count != 0 {
		t.Fatalf("StatsAndRecent após 20 falhas + 5 timeouts = (%d,%v), esperado (0,false)", count, ok)
	}
	if count, _, _, _, ok := h.Stats(key); ok || count != 0 {
		t.Fatalf("Stats após 20 falhas + 5 timeouts = (%d,%v), esperado (0,false)", count, ok)
	}
}

// Scenario of the percentile over the successes: with the successes present, a
// fast failure and a long timeout do not distort the percentile or the count,
// and only the three successes count.
func TestStatsSuccessOnlyP95(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	mk := func(outcome string, cold bool, lat int64) Entry {
		return Entry{FunctionName: "fn", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: lat, Outcome: outcome, ColdStart: cold}
	}
	for _, e := range []Entry{
		mk(OutcomeFalha, false, 5),
		mk(OutcomeSucesso, false, 100),
		mk(OutcomeSucesso, false, 120),
		mk(OutcomeTimeout, false, 5000),
		mk(OutcomeSucesso, false, 140),
	} {
		if err := h.AppendWithGroup(e, e.FunctionName); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	key := SegmentKey("fn", "n1", sc)
	count, p95, recent, _, _, ok := h.StatsAndRecent(key, time.Minute, now)
	if !ok {
		t.Fatalf("StatsAndRecent ok=false, esperado true (3 sucessos)")
	}
	if count != 3 {
		t.Fatalf("count=%d, esperado 3 (só sucessos sem cold_start)", count)
	}
	// n=3 => ceil(0.95*3)-1 = 2 => ordenado [100 120 140][2] = 140.
	if p95 != 140 {
		t.Fatalf("p95=%d, esperado 140 (falha 5ms e timeout 5000ms fora)", p95)
	}
	if recent != 3 {
		t.Fatalf("recent=%d, esperado 3", recent)
	}
}
