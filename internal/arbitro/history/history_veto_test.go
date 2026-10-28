package history

// Coverage of the reliability signal, which is the smoothed success rate, and of
// the percentile with a window: the total counts only the successes, the
// failures and the function timeouts inside the window, while the exceeded
// budget, the capacity rejection and the admission timeout are saturation and
// stay out, in the saturation counter. The node saturation is the subset where
// the node is the author, which is the rejection and the admission timeout, and
// it is consumed by the floor of absolution. The percentile ignores a stale
// value, and the veto by rate bites when the count is zero.

import (
	"testing"
	"time"
)

func TestLaplaceRate(t *testing.T) {
	if got := LaplaceRate(0, 0); got != 0.5 {
		t.Fatalf("LaplaceRate(0,0)=%v, esperado 0.5 (prior neutro)", got)
	}
	if got := LaplaceRate(0, 5); got >= 0.5 {
		t.Fatalf("LaplaceRate(0,5)=%v, esperado <0.5", got)
	}
	if got := LaplaceRate(5, 5); got <= 0.5 {
		t.Fatalf("LaplaceRate(5,5)=%v, esperado >0.5", got)
	}
	// (0+1)/(5+2)=1/7; (2+1)/(5+2)=3/7.
	if got := LaplaceRate(0, 5); got != 1.0/7.0 {
		t.Fatalf("LaplaceRate(0,5)=%v, esperado 1/7", got)
	}
}

func TestVetoByRate(t *testing.T) {
	if VetoByRate(4, 0.0) {
		t.Fatalf("total=4 nunca veta (piso protege segmento frio)")
	}
	if !VetoByRate(5, 1.0/7.0) {
		t.Fatalf("total=5 rate=1/7 deveria vetar")
	}
	if VetoByRate(5, 6.0/7.0) {
		t.Fatalf("total=5 rate=6/7 não deveria vetar")
	}
	if VetoByRate(5, 0.5) {
		t.Fatalf("rate=0.5 no limite não veta (<0.5 estrito)")
	}
}

func TestStatsAndRecentWindowedP95AndRate(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	mk := func(ts time.Time, outcome string, lat int64) Entry {
		return Entry{FunctionName: "fn", NodeID: "n1", Timestamp: ts, ScenarioContext: sc, ObservedLatencyMS: lat, Outcome: outcome}
	}
	// 2 sucessos stale (fora da janela de 60s) + 1 sucesso fresco 100ms +
	// 5 falhas frescas + 1 timeout fresco + 3 orcamento_excedido frescos.
	for i := 0; i < 2; i++ {
		if err := h.AppendWithGroup(mk(now.Add(-time.Hour), OutcomeSucesso, 10), "fn"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	if err := h.AppendWithGroup(mk(now, OutcomeSucesso, 100), "fn"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := h.AppendWithGroup(mk(now, OutcomeFalha, 5), "fn"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	if err := h.AppendWithGroup(mk(now, OutcomeTimeout, 5000), "fn"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := h.AppendWithGroup(mk(now, OutcomeOrcamentoExcedido, 1), "fn"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	key := SegmentKey("fn", "n1", sc)
	count, p95, recent, total, rate, ok := h.StatsAndRecent(key, time.Minute, now)
	if !ok {
		t.Fatalf("StatsAndRecent ok=false, esperado true (1 sucesso fresco)")
	}
	if count != 1 || p95 != 100 {
		t.Fatalf("count=%d p95=%d, esperado (1,100): stale 10ms fora da janela", count, p95)
	}
	if recent != 1 {
		t.Fatalf("recent=%d, esperado 1 (só sucesso fresco)", recent)
	}
	// The total measures reliability: one success, five failures and one function
	// timeout give seven. The three exceeded budgets are saturation, since the
	// arbiter gave up, and go to the saturation counter: they entered the rate
	// before the separation, and that is what made the veto ban the saturated
	// node.
	if total != 7 {
		t.Fatalf("total=%d, esperado 7 (orcamento_excedido é saturação, não serviço)", total)
	}
	if want := LaplaceRate(1, 7); rate != want {
		t.Fatalf("rate=%v, esperado LaplaceRate(1,7)=%v", rate, want)
	}
	if !VetoByRate(total, rate) {
		t.Fatalf("total=7 rate=%v deveria vetar (2/9<0.5): falha de serviço ainda veta", rate)
	}
	full, okFull := h.StatsFull(key, time.Minute, now)
	if !okFull || full.Saturation != 3 || full.Total != 7 {
		t.Fatalf("StatsFull=(Total %d, Saturação %d, ok %v), esperado (7,3,true)", full.Total, full.Saturation, okFull)
	}
	if full.NodeSaturation != 0 {
		t.Fatalf("NodeSaturation=%d, esperado 0: orcamento_excedido é decisão do árbitro, não saturação do nó", full.NodeSaturation)
	}
}

// Regression of the separation: a segment whose only service samples are
// saturation cannot be vetoed. The fastest node of the system was vetoed in
// every bucket with ninety-seven samples and no success, all of them from
// saturation.
func TestSaturationOnlySegmentIsNotVetoed(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	add := func(outcome string) {
		e := Entry{FunctionName: "fn", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: 5, Outcome: outcome}
		if err := h.AppendWithGroup(e, e.FunctionName); err != nil {
			t.Fatalf("AppendWithGroup(%s): %v", outcome, err)
		}
	}
	// Sixty honest refusals and thirty-seven budget cuts: the node is saturated
	// and never unavailable. Only the refusals are saturation of the node, since
	// the budget cut is a decision of the arbiter.
	for i := 0; i < 60; i++ {
		add(OutcomeRejeicaoCapacidade)
	}
	for i := 0; i < 37; i++ {
		add(OutcomeOrcamentoExcedido)
	}
	key := SegmentKey("fn", "n1", sc)
	count, p95, total, rate, _ := h.Stats(key)
	if count != 0 || p95 != 0 {
		t.Fatalf("saturação contaminou p95: count=%d p95=%d", count, p95)
	}
	if total != 0 {
		t.Fatalf("total=%d, esperado 0: saturação não é falha de serviço", total)
	}
	if VetoByRate(total, rate) {
		t.Fatalf("nó só-saturado foi vetado (rate=%v): o veto puniu saturação", rate)
	}
	full, okFull := h.StatsFull(key, time.Minute, now)
	if !okFull || full.Saturation != 97 || full.NodeSaturation != 60 {
		t.Fatalf("StatsFull=(Saturação %d, NodeSaturação %d, ok %v), esperado (97,60,true)",
			full.Saturation, full.NodeSaturation, okFull)
	}
}

// The other side of the separation: a real service failure still vetoes, even
// with the saturation mixed in. Without this the repair would become a rule
// where nothing ever vetoes.
func TestServiceFailureStillVetoesWithSaturation(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	add := func(outcome string) {
		e := Entry{FunctionName: "fn", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: 5, Outcome: outcome}
		if err := h.AppendWithGroup(e, e.FunctionName); err != nil {
			t.Fatalf("AppendWithGroup(%s): %v", outcome, err)
		}
	}
	for i := 0; i < 40; i++ {
		add(OutcomeRejeicaoCapacidade)
	}
	for i := 0; i < 6; i++ {
		add(OutcomeFalha)
	}
	key := SegmentKey("fn", "n1", sc)
	count, _, total, rate, _ := h.Stats(key)
	if count != 0 || total != 6 {
		t.Fatalf("count=%d total=%d, esperado (0,6): só a falha de serviço conta", count, total)
	}
	if !VetoByRate(total, rate) {
		t.Fatalf("falha de serviço não vetou (rate=%v): o reparo do D1 não pode desligar o veto", rate)
	}
}

func TestStatsAndRecentVetoWithZeroSuccess(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	for i := 0; i < 6; i++ {
		e := Entry{FunctionName: "fn", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: 5, Outcome: OutcomeFalha}
		if err := h.AppendWithGroup(e, e.FunctionName); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	key := SegmentKey("fn", "n1", sc)
	count, _, _, total, rate, ok := h.StatsAndRecent(key, time.Minute, now)
	if ok || count != 0 {
		t.Fatalf("(%d,%v), esperado (0,false): sem sucesso, sem p95", count, ok)
	}
	if total != 6 {
		t.Fatalf("total=%d, esperado 6", total)
	}
	if !VetoByRate(total, rate) {
		t.Fatalf("amnésia negativa: Count==0 com total=6 rate=%v deveria vetar", rate)
	}
}
