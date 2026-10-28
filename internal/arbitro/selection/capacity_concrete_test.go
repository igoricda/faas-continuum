package selection

// Leitura de capacidade pelo CONCRETO resolvido (selflearning-consistency-fixes,
// todo 6, C2/D17): os estágios de capacidade (marca otimista e cold prediction)
// consultam o Monitor pela MESMA chave concreta que o produtor (nodeapi) usa —
// a variante física resolvida por nó —, nunca o nome pedido (categoria). O
// estágio é compartilhado; estes testes o exercitam via RiskSelector, o
// seletor canônico (ADR-0012), com learner semeado para tirar os nós de
// probation e deixar a ordenação cold/warm decidir.

import (
	"slices"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func capacityConcreteFixture() []nodes.Node {
	return []nodes.Node{
		{NodeID: "A", Layer: nodes.LayerBorda, Functions: []string{"cat-v1"}, FunctionCategories: map[string]string{"cat-v1": "cat"}},
		{NodeID: "B", Layer: nodes.LayerBorda, Functions: []string{"cat-v2"}, FunctionCategories: map[string]string{"cat-v2": "cat"}},
	}
}

// capacityConcreteSelector monta o RiskSelector com learner semeado (10
// sucessos idênticos por nó, M=1): n_eff >= nmin => nós viáveis, e a ordem
// cold-primeiro-do-warm passa a valer.
func capacityConcreteSelector(mon *monitor.Monitor, now time.Time) *RiskSelector {
	cfg := DefaultRiskConfig()
	cfg.Band = 0
	learner := learn.NewLearner(cfg.LearnConfig())
	group := ResolveRequestGroup("cat", capacityConcreteFixture())
	for _, id := range []string{"A", "B"} {
		for i := 0; i < 10; i++ {
			learner.Observe(group, id, learn.Obs{At: now, Outcome: learn.OutcomeSuccess, LatencyMS: 100, UseRegr: true, M: 1})
		}
	}
	return &RiskSelector{
		Base:             QueueSelector{Mon: mon, Cfg: defaultLimits},
		ThroughputWindow: time.Minute,
		Clock:            fixedClock(now),
		Learner:          learner,
		Cfg:              cfg,
	}
}

// (a) Categoria `cat`: A reporta o concreto cat-v1 com avail==0 fresco (sem
// marca => genuine-cold); B (cat-v2) está warm. Com a chave concreta o cold
// de A é detectado: no RiskSelector o cold ordena por último entre viáveis
// e, sem modelo cold semeado, A cai em probation — nos dois casos A vem
// depois de B, mesmo com CPU menor.
func TestSelectCategoryCapacityConcreteKeyingDemotesCold(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	mon := monitor.NewMonitor()
	mon.UpdateCapacity("A", "cat-v1", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 0}, now)
	mon.UpdateCapacity("B", "cat-v2", telemetry.FunctionCapacity{MaxInstances: 2, InstancesAvailable: 2}, now)
	// CPU: A menor que B — sem cold detection, A venceria a ordenação.
	mon.UpdateAt(telemetry.Package{NodeID: "A", CPUUsagePct: 10}, now)
	mon.UpdateAt(telemetry.Package{NodeID: "B", CPUUsagePct: 50}, now)

	got := exhaustedIDs(capacityConcreteSelector(mon, now).Select(capacityConcreteFixture(), "cat", nil))
	if !slices.Equal(got, []string{"B", "A"}) {
		t.Fatalf("A genuine-cold (cat-v1 avail=0) deveria ser demovido atrás de B, got=%v", got)
	}
}

// (b) A marca de exaustão do concreto cat-v1 NÃO é limpa por um
// UpdateCapacity numa chave diferente (cat-v2): A segue filtrado. Sem a
// chave concreta o read usa `cat` e nem enxerga a marca do concreto.
func TestSelectCategoryCapacityConcreteMarkDifferentKeyStillFilters(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	mon := monitor.NewMonitor()
	mon.MarkExhausted("A", "cat-v1", now)
	mon.UpdateCapacity("A", "cat-v2", telemetry.FunctionCapacity{MaxInstances: 2, InstancesAvailable: 2}, now)
	mon.UpdateCapacity("B", "cat-v2", telemetry.FunctionCapacity{MaxInstances: 2, InstancesAvailable: 2}, now)

	got := exhaustedIDs(capacityConcreteSelector(mon, now).Select(capacityConcreteFixture(), "cat", nil))
	if !slices.Equal(got, []string{"B"}) {
		t.Fatalf("A com cat-v1 marcado deveria cair (update em cat-v2 não limpa), got=%v", got)
	}
}

// (c) UpdateCapacity fresco na MESMA chave concreta limpa a marca: A não é
// filtrado por 30s (sem filtragem stale).
func TestSelectCategoryCapacityConcreteMarkClearedByFreshUpdate(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	mon := monitor.NewMonitor()
	mon.MarkExhausted("A", "cat-v1", now)
	mon.UpdateCapacity("A", "cat-v1", telemetry.FunctionCapacity{MaxInstances: 2, InstancesAvailable: 2}, now)
	mon.UpdateCapacity("B", "cat-v2", telemetry.FunctionCapacity{MaxInstances: 2, InstancesAvailable: 2}, now)

	got := exhaustedIDs(capacityConcreteSelector(mon, now).Select(capacityConcreteFixture(), "cat", nil))
	if !slices.Contains(got, "A") {
		t.Fatalf("marca de cat-v1 limpa por update fresco na mesma chave; A deveria estar na fila, got=%v", got)
	}
}
