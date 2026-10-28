package selection

// Capacity reading by the resolved concrete variant: the capacity stages, which
// are the optimistic mark and the cold prediction, consult the monitor by the
// same concrete key that the producer uses, which is the physical variant
// resolved per node, and never by the requested name, which may be a category.
// The stage is shared, and these tests exercise it through the risk selector, the
// canonical selector, with a seeded learner that takes the nodes out of the
// probation and lets the cold and warm ordering decide.

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

// capacityConcreteSelector builds the risk selector with a seeded learner, with
// ten identical successes per node: the effective number of samples reaches the
// minimum, so the nodes are viable, and the order that puts a cold node first
// applies.
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

// (a) In the category the first node reports its concrete variant with no
// available instance and a fresh reading, which is a genuine cold start, while
// the second node is warm. With the concrete key the cold start of the first node
// is detected: in the risk selector a cold node is ordered last among the viable
// ones and, without a seeded cold model, it falls into the probation, so in both
// cases it comes after the second node even with a lower processor usage.
func TestSelectCategoryCapacityConcreteKeyingDemotesCold(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	mon := monitor.NewMonitor()
	mon.UpdateCapacity("A", "cat-v1", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 0}, now)
	mon.UpdateCapacity("B", "cat-v2", telemetry.FunctionCapacity{MaxInstances: 2, InstancesAvailable: 2}, now)
	// The processor usage of the first node is lower, so without the cold detection it would win the ordering.
	mon.UpdateAt(telemetry.Package{NodeID: "A", CPUUsagePct: 10}, now)
	mon.UpdateAt(telemetry.Package{NodeID: "B", CPUUsagePct: 50}, now)

	got := exhaustedIDs(capacityConcreteSelector(mon, now).Select(capacityConcreteFixture(), "cat", nil))
	if !slices.Equal(got, []string{"B", "A"}) {
		t.Fatalf("A genuine-cold (cat-v1 avail=0) deveria ser demovido atrás de B, got=%v", got)
	}
}

// (b) The exhaustion mark of the first concrete variant is not cleared by a
// capacity update on a different key, which is the second variant, so the first
// node stays filtered. Without the concrete key the reading uses the category and
// does not even see the mark of the concrete variant.
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

// (c) A fresh capacity update on the same concrete key clears the mark, so the
// first node is not filtered for thirty seconds, since there is no filtering of a
// stale reading.
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
