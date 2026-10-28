package history

// Regressão da decisão p95-sobre-sucessos (ADR-0007, AUD-01): Count e p95
// cobrem só sucessos sem cold_start, então MinSamples conta sucessos
// mínimos — segmento com zero sucessos nunca fica ready, mesmo sob rajada
// de falhas rápidas ou timeouts longos.

import (
	"testing"
	"time"
)

// Cenário AUD-01: 20 falhas rápidas (500 em 5ms) + 5 timeouts longos não
// tornam o segmento ready — Count/p95 exigem sucesso. Sem isso, o
// self-learning preferia nó que nunca serviu (falha abaixa o p95, timeout
// infla), com MinSamples atingido por tentativas cruas.
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

// Cenário AUD-01: com sucessos presentes, falha rápida e timeout longo não
// distorcem p95/Count — só os 3 sucessos (100, 120, 140ms) contam.
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
