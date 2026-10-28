// Mapeamento Entry -> Obs e replay no boot (Fase 2, SELETOR-V2 §6.1/§9).
package selection

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
)

func riskTestEntry(outcome string) history.Entry {
	return history.Entry{
		Timestamp:          time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC),
		FunctionName:       "fn",
		NodeID:             "n1",
		Outcome:            outcome,
		ObservedLatencyMS:  810,
		AcquireWaitMS:      10,
		Inflight:           2,
		InstancesAvailable: 4,
	}
}

// TestRiskObsFromEntry cobre cada linha da tabela 6.1.
func TestRiskObsFromEntry(t *testing.T) {
	// Sucesso warm com espera conhecida: y líquido, m = min(k, c).
	o, ok := RiskObsFromEntry(riskTestEntry(history.OutcomeSucesso))
	if !ok || o.Outcome != learn.OutcomeSuccess || !o.UseRegr {
		t.Fatalf("warm: %+v ok=%v, want success+regr", o, ok)
	}
	if o.LatencyMS != 800 || o.M != 2 {
		t.Fatalf("warm: lat=%v m=%v want 800/2", o.LatencyMS, o.M)
	}
	// Sem AcquireWait + Inflight desconhecido: Beta-only.
	e := riskTestEntry(history.OutcomeSucesso)
	e.AcquireWaitMS = 0
	e.Inflight = 0
	o, ok = RiskObsFromEntry(e)
	if !ok || o.Outcome != learn.OutcomeSuccess || o.UseRegr {
		t.Fatalf("sem-espera-desconhecida: %+v ok=%v, want success sem regr", o, ok)
	}
	// Sem AcquireWait + Inflight acima das instâncias: Beta-only.
	e = riskTestEntry(history.OutcomeSucesso)
	e.AcquireWaitMS = 0
	e.Inflight = 6
	e.InstancesAvailable = 4
	o, ok = RiskObsFromEntry(e)
	if !ok || o.UseRegr {
		t.Fatalf("acima-das-instâncias: %+v, want sem regr", o)
	}
	// Sem AcquireWait + Inflight dentro da capacidade: regr (sem espera).
	e = riskTestEntry(history.OutcomeSucesso)
	e.AcquireWaitMS = 0
	o, ok = RiskObsFromEntry(e)
	if !ok || !o.UseRegr || o.LatencyMS != 810 {
		t.Fatalf("ocioso-sem-espera: %+v, want regr lat=810", o)
	}
	// Cold: y cheio, roteado ao NIG de cold.
	e = riskTestEntry(history.OutcomeSucesso)
	e.ColdStart = true
	o, ok = RiskObsFromEntry(e)
	if !ok || !o.Cold || o.LatencyMS != 810 {
		t.Fatalf("cold: %+v, want cold lat=810", o)
	}
	// Falha/timeout: só Beta.
	for _, out := range []string{history.OutcomeFalha, history.OutcomeTimeout} {
		o, ok = RiskObsFromEntry(riskTestEntry(out))
		if !ok || o.Outcome != learn.OutcomeFailure {
			t.Fatalf("%s: %+v ok=%v, want failure", out, o, ok)
		}
	}
	// Ignorados: decisão do árbitro, saturação ou desconhecido.
	for _, out := range []string{
		history.OutcomeOrcamentoExcedido,
		history.OutcomeRejeicaoCapacidade,
		history.OutcomeTimeoutAdmissao,
		"esquisito",
	} {
		if _, ok := RiskObsFromEntry(riskTestEntry(out)); ok {
			t.Fatalf("%s: ok=true want false", out)
		}
	}
}

// TestReplayRiskHistoryRebuilds: reiniciar reconstrói o learner do
// histórico persistido — 5 sucessos (com fan-out de camada, que NÃO
// pode contar 2x) tiram o modelo de probation.
func TestReplayRiskHistoryRebuilds(t *testing.T) {
	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	ts := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		e := history.Entry{
			Timestamp: ts.Add(time.Duration(i) * time.Second), FunctionName: "fn",
			NodeID: "n1", Layer: "borda",
			ScenarioContext:    history.ScenarioContext{LoadBucket: history.BucketUnknown},
			Outcome:            history.OutcomeSucesso,
			ObservedLatencyMS:  800,
			AcquireWaitMS:      10,
			Inflight:           1,
			InstancesAvailable: 2,
		}
		if err := h.AppendWithGroup(e, "g"); err != nil {
			t.Fatal(err)
		}
	}
	l := learn.NewLearner(learn.Config{Risk: 0.05, ACIGamma: 0.01, NMin: 3})
	ReplayRiskHistory(h, l)
	p := l.Predict(ts, "g", "n1", learn.LiveState{Servers: 2, Alive: 2})
	if p.Neff != 5 {
		t.Fatalf("Neff=%v want 5 (fan-out agregado não conta 2x)", p.Neff)
	}
	if p.Probation {
		t.Fatalf("probation após replay com 5 obs")
	}
}

// TestAttachRiskObserverFeedsLearner: appends alimentam o learner ao vivo.
func TestAttachRiskObserverFeedsLearner(t *testing.T) {
	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	l := learn.NewLearner(learn.Config{Risk: 0.05, ACIGamma: 0.01, NMin: 3})
	AttachRiskObserver(h, l)
	e := history.Entry{
		Timestamp: time.Now(), FunctionName: "fn", NodeID: "n9",
		ScenarioContext:    history.ScenarioContext{LoadBucket: history.BucketUnknown},
		Outcome:            history.OutcomeSucesso,
		ObservedLatencyMS:  500,
		AcquireWaitMS:      5,
		Inflight:           1,
		InstancesAvailable: 1,
	}
	if err := h.AppendWithGroup(e, "gg"); err != nil {
		t.Fatal(err)
	}
	dump := l.Snapshot(e.Timestamp)
	if len(dump) != 1 || dump[0].Neff != 1 {
		t.Fatalf("snapshot=%+v want 1 modelo Neff=1", dump)
	}
}
