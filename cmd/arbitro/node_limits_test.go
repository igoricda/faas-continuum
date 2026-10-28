package main

// The merge of the partial map of per-node limits into the complete trio that the
// selection consults. These tests protect the rule of the merge and the
// diagnosis of a phantom node identifier: the merge is the only place where the
// global values are still known, so an error here would silently enable or
// disable the ceiling of a whole node.

import (
	"slices"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/config"
)

func TestResolveNodeHardLimits(t *testing.T) {
	base := func() config.Arbitro {
		a := config.Defaults().Arbitro // MaxCPUPct 85 / MaxMemPct 90 / MaxPSIAvg10 25
		a.MaxCPUPct, a.MaxMemPct, a.MaxPSIAvg10 = 85, 90, 25
		return a
	}

	t.Run("sem mapa devolve nil", func(t *testing.T) {
		if got := resolveNodeHardLimits(base()); got != nil {
			t.Fatalf("sem node_limits = %v, want nil (PerNode fica nil)", got)
		}
	})

	t.Run("mapa vazio devolve nil", func(t *testing.T) {
		a := base()
		a.NodeLimits = map[string]config.NodeHardLimits{}
		if got := resolveNodeHardLimits(a); got != nil {
			t.Fatalf("node_limits vazio = %v, want nil", got)
		}
	})

	t.Run("parcial herda o global", func(t *testing.T) {
		a := base()
		a.NodeLimits = map[string]config.NodeHardLimits{"borda-01": {MaxCPUPct: 60}}
		got := resolveNodeHardLimits(a)
		lc, ok := got["borda-01"]
		if !ok {
			t.Fatalf("sem borda-01: %+v", got)
		}
		if lc.MaxCPUPct != 60 || lc.MaxMemPct != 90 || lc.MaxPSIAvg10 != 25 {
			t.Fatalf("borda-01 = %+v, want cpu 60 (override) e mem/psi herdados 90/25", lc)
		}
	})

	t.Run("completo sobrescreve os três", func(t *testing.T) {
		a := base()
		a.NodeLimits = map[string]config.NodeHardLimits{"nevoa-01": {MaxCPUPct: 50, MaxMemPct: 55, MaxPSIAvg10: 15}}
		got := resolveNodeHardLimits(a)
		if lc := got["nevoa-01"]; lc.MaxCPUPct != 50 || lc.MaxMemPct != 55 || lc.MaxPSIAvg10 != 15 {
			t.Fatalf("nevoa-01 = %+v, want 50/55/15", lc)
		}
	})

	t.Run("entrada toda zero vira o global", func(t *testing.T) {
		a := base()
		a.NodeLimits = map[string]config.NodeHardLimits{"n0": {}}
		lc := resolveNodeHardLimits(a)["n0"]
		if lc.MaxCPUPct != 85 || lc.MaxMemPct != 90 || lc.MaxPSIAvg10 != 25 {
			t.Fatalf("entrada zero = %+v, want o global 85/90/25", lc)
		}
	})

	t.Run("o trio resolvido já é o que For consulta", func(t *testing.T) {
		a := base()
		a.NodeLimits = map[string]config.NodeHardLimits{"borda-01": {MaxCPUPct: 60}}
		limits := selection.LimitsConfig{MaxCPUPct: a.MaxCPUPct, MaxMemPct: a.MaxMemPct, MaxPSIAvg10: a.MaxPSIAvg10}
		limits.PerNode = resolveNodeHardLimits(a)
		if got := limits.For("borda-01").MaxCPUPct; got != 60 {
			t.Fatalf("For(borda-01).MaxCPUPct = %.1f, want 60", got)
		}
		if got := limits.For("outro").MaxCPUPct; got != 85 {
			t.Fatalf("For(outro).MaxCPUPct = %.1f, want 85 (global)", got)
		}
	})
}

func TestUnknownNodeLimitIDs(t *testing.T) {
	perNode := map[string]selection.LimitsConfig{
		"borda-01": {MaxCPUPct: 60},
		"nevoa-01": {MaxCPUPct: 95},
		"sumiu-99": {MaxCPUPct: 70},
	}
	known := []nodes.Node{{NodeID: "borda-01"}, {NodeID: "nevoa-01"}}
	got := unknownNodeLimitIDs(perNode, known)
	if !slices.Equal(got, []string{"sumiu-99"}) {
		t.Fatalf("unknown = %v, want [sumiu-99]", got)
	}

	t.Run("todos conhecidos devolve nil", func(t *testing.T) {
		all := map[string]selection.LimitsConfig{"borda-01": {MaxCPUPct: 60}}
		if got := unknownNodeLimitIDs(all, known); got != nil {
			t.Fatalf("unknown = %v, want nil", got)
		}
	})

	t.Run("sem override devolve nil", func(t *testing.T) {
		if got := unknownNodeLimitIDs(nil, known); got != nil {
			t.Fatalf("unknown(nil) = %v, want nil", got)
		}
	})

	t.Run("nenhum nó registrado lista todos", func(t *testing.T) {
		got := unknownNodeLimitIDs(perNode, nil)
		if !slices.Equal(got, []string{"borda-01", "nevoa-01", "sumiu-99"}) {
			t.Fatalf("unknown = %v, want os três ids ordenados", got)
		}
	})
}
