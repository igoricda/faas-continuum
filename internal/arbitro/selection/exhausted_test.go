package selection

import (
	"slices"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func exhaustedFixture() []nodes.Node {
	return []nodes.Node{
		{NodeID: "A", Layer: nodes.LayerBorda, Functions: []string{"sleep"}},
		{NodeID: "B", Layer: nodes.LayerBorda, Functions: []string{"sleep"}},
	}
}

func exhaustedIDs(ns []nodes.Node) []string {
	ids := make([]string, 0, len(ns))
	for _, n := range ns {
		ids = append(ids, n.NodeID)
	}
	return ids
}

func TestFilterExhausted(t *testing.T) {
	now := time.Now()
	free := telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 1}
	exh := telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 0}

	t.Run("nil caps noop", func(t *testing.T) {
		got := FilterExhausted(exhaustedFixture(), nil, now)
		if want := []string{"A", "B"}; !slices.Equal(exhaustedIDs(got), want) {
			t.Fatalf("got=%v want=%v", exhaustedIDs(got), want)
		}
	})

	t.Run("empty caps noop", func(t *testing.T) {
		got := FilterExhausted(exhaustedFixture(), map[string]telemetry.FunctionCapacity{}, now)
		if want := []string{"A", "B"}; !slices.Equal(exhaustedIDs(got), want) {
			t.Fatalf("got=%v want=%v", exhaustedIDs(got), want)
		}
	})

	t.Run("filters exhausted B keeps free A", func(t *testing.T) {
		caps := map[string]telemetry.FunctionCapacity{"A": free, "B": exh}
		got := FilterExhausted(exhaustedFixture(), caps, now)
		if want := []string{"A"}; !slices.Equal(exhaustedIDs(got), want) {
			t.Fatalf("got=%v want=%v", exhaustedIDs(got), want)
		}
	})

	t.Run("unknown node never filtered", func(t *testing.T) {
		caps := map[string]telemetry.FunctionCapacity{"B": exh}
		got := FilterExhausted(exhaustedFixture(), caps, now)
		if want := []string{"A"}; !slices.Equal(exhaustedIDs(got), want) {
			t.Fatalf("got=%v want=%v", exhaustedIDs(got), want)
		}
	})

	t.Run("all exhausted fail-open returns full list", func(t *testing.T) {
		caps := map[string]telemetry.FunctionCapacity{"A": exh, "B": exh}
		got := FilterExhausted(exhaustedFixture(), caps, now)
		if want := []string{"A", "B"}; !slices.Equal(exhaustedIDs(got), want) {
			t.Fatalf("got=%v want=%v", exhaustedIDs(got), want)
		}
	})

	t.Run("order preserved", func(t *testing.T) {
		in := []nodes.Node{
			{NodeID: "B", Layer: nodes.LayerBorda, Functions: []string{"sleep"}},
			{NodeID: "A", Layer: nodes.LayerBorda, Functions: []string{"sleep"}},
		}
		caps := map[string]telemetry.FunctionCapacity{"A": exh, "B": free}
		got := FilterExhausted(in, caps, now)
		if want := []string{"B"}; !slices.Equal(exhaustedIDs(got), want) {
			t.Fatalf("got=%v want=%v", exhaustedIDs(got), want)
		}
	})
}

func TestSelectSkipsExhausted(t *testing.T) {
	now := time.Now()
	free := telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 1}

	// RiskSelector (canônico, ADR-0012): o estágio de capacidade é o mesmo
	// do QueueSelector e roda antes da previsão; com learner frio os nós
	// sobreviventes saem por bootstrap (probation).
	newSelector := func(mon *monitor.Monitor, clock func() time.Time) *RiskSelector {
		return &RiskSelector{
			Base:             QueueSelector{Mon: mon, Cfg: defaultLimits},
			ThroughputWindow: time.Minute,
			Clock:            clock,
			Learner:          learn.NewLearner(DefaultRiskConfig().LearnConfig()),
			Cfg:              DefaultRiskConfig(),
		}
	}

	t.Run("B exhausted routes to A", func(t *testing.T) {
		mon := monitor.NewMonitor()
		mon.UpdateCapacity("A", "sleep", free, now)
		mon.MarkExhausted("B", "sleep", now)
		sel := newSelector(mon, func() time.Time { return now })
		got := sel.Select(exhaustedFixture(), "sleep", nil)
		if want := []string{"A"}; !slices.Equal(exhaustedIDs(got), want) {
			t.Fatalf("got=%v want=%v", exhaustedIDs(got), want)
		}
	})

	t.Run("all exhausted fail-open keeps queue non-empty", func(t *testing.T) {
		mon := monitor.NewMonitor()
		mon.MarkExhausted("A", "sleep", now)
		mon.MarkExhausted("B", "sleep", now)
		sel := newSelector(mon, func() time.Time { return now })
		got := sel.Select(exhaustedFixture(), "sleep", nil)
		if want := []string{"A", "B"}; !slices.Equal(exhaustedIDs(got), want) {
			t.Fatalf("got=%v want=%v", exhaustedIDs(got), want)
		}
	})

	t.Run("expired TTL never filtered", func(t *testing.T) {
		mon := monitor.NewMonitor()
		mon.UpdateCapacity("A", "sleep", free, now)
		mon.MarkExhausted("B", "sleep", now)
		later := now.Add(monitor.CapacityTTL + time.Second)
		sel := newSelector(mon, func() time.Time { return later })
		got := sel.Select(exhaustedFixture(), "sleep", nil)
		if want := []string{"A", "B"}; !slices.Equal(exhaustedIDs(got), want) {
			t.Fatalf("got=%v want=%v", exhaustedIDs(got), want)
		}
	})

	t.Run("nil monitor noop", func(t *testing.T) {
		sel := newSelector(nil, func() time.Time { return now })
		got := sel.Select(exhaustedFixture(), "sleep", nil)
		if want := []string{"A", "B"}; !slices.Equal(exhaustedIDs(got), want) {
			t.Fatalf("got=%v want=%v", exhaustedIDs(got), want)
		}
	})

	t.Run("skips produce no recordAttempt and consume no fanout", func(t *testing.T) {
		// Prova no nível do Select + contrato do gateway (sem teste
		// cross-package para não tocar o gateway): o gateway itera a
		// fila devolvida pelo Select (gateway_handle.go:210, uma
		// recordAttempt por nó TENTADO) e o teto maxRaceFanout fatia a
		// mesma fila (gateway_handle.go:157-159). Pulos acontecem no
		// Select, antes da fila existir — B nunca entra na fila, logo
		// nunca gera recordAttempt (nem linha no History/invLog) e
		// nunca consome slot de fanout, que conta tentativas, não
		// pulos. Fila vazia => NO_NODE_AVAILABLE (gateway_handle.go:149)
		// nunca dispara aqui: fail-open garante fila não-vazia, então
		// todos-esgotados tenta e recebe 503 NODE_OVERLOADED do nó.
		mon := monitor.NewMonitor()
		mon.UpdateCapacity("A", "sleep", free, now)
		mon.MarkExhausted("B", "sleep", now)
		sel := newSelector(mon, func() time.Time { return now })
		queue := sel.Select(exhaustedFixture(), "sleep", nil)
		if len(queue) != 1 || queue[0].NodeID != "A" {
			t.Fatalf("queue=%v want=[A]", exhaustedIDs(queue))
		}
	})
}
