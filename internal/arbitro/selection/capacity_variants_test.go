package selection

// The capacity gating walks every variant of the category per node. The
// scenario of the diagnosis was a fog that hosts two concrete variants of the
// same category, where the single key, which is alphabetically the first, had no
// live instance and dropped the whole node, so the hard-limit baseline went to
// the cloud without trying the live variant.
//
// Coverage: the node stays while any variant is alive; it leaves when every
// variant is exhausted; an exhausted variant is demoted at the dispatch, which
// preserves the parity between the training key and the query key; the unknown
// value and the time to live keep the fail-open; and the lifecycle regenerates
// the variant through a fresh reading.

import (
	"slices"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// nevoaNode is the fog of the diagnosis, with two concrete variants of the same
// category. The legacy key is the first variant in alphabetical order.
func nevoaNode() nodes.Node {
	return nodes.Node{
		NodeID:             "nevoa",
		Layer:              nodes.LayerNevoa,
		Functions:          []string{"alpr-cpu", "alpr-gpu"},
		FunctionCategories: map[string]string{"alpr-cpu": "lpr", "alpr-gpu": "lpr"},
	}
}

// nevoaFixture adds the edge, which hosts the concrete category, so the list
// does not become empty when the fog is demoted, since without it the fail-open
// would return the fog itself and mask the test.
func nevoaFixture() []nodes.Node {
	return []nodes.Node{
		nevoaNode(),
		{NodeID: "borda", Layer: nodes.LayerBorda, Functions: []string{"lpr"}},
	}
}

func lprCap(avail int) telemetry.FunctionCapacity {
	return telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: avail}
}

func TestCapacityVariantsForNode(t *testing.T) {
	reversed := nodes.Node{
		NodeID:             "n",
		Functions:          []string{"alpr-gpu", "alpr-cpu"},
		FunctionCategories: map[string]string{"alpr-gpu": "lpr", "alpr-cpu": "lpr"},
	}
	cases := []struct {
		name      string
		node      nodes.Node
		requested string
		want      []string
	}{
		{
			name:      "wildcard sem functions => vazio (sem gating)",
			node:      nodes.Node{NodeID: "w"},
			requested: "lpr",
			want:      nil,
		},
		{
			name:      "requested vazio => vazio",
			node:      nevoaNode(),
			requested: "",
			want:      nil,
		},
		{
			name:      "concreto hospedado => só ele",
			node:      nevoaNode(),
			requested: "alpr-cpu",
			want:      []string{"alpr-cpu"},
		},
		{
			name:      "categoria multi-variante => todas, ordem lexicográfica",
			node:      reversed,
			requested: "lpr",
			want:      []string{"alpr-cpu", "alpr-gpu"},
		},
		{
			name:      "categoria não hospedada => vazio",
			node:      nevoaNode(),
			requested: "ocr",
			want:      nil,
		},
		{
			name: "variante única de categoria => uma chave",
			node: nodes.Node{
				NodeID:             "m",
				Functions:          []string{"cat-v1"},
				FunctionCategories: map[string]string{"cat-v1": "cat"},
			},
			requested: "cat",
			want:      []string{"cat-v1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := capacityVariantsForNode(tc.node, tc.requested)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("capacityVariantsForNode(%s, %q)=%v want=%v", tc.node.NodeID, tc.requested, got, tc.want)
			}
		})
	}
}

// Parity between training and query: the variants are exactly the hosted
// concrete names, never the category and never an invented name, which is the
// same key that the producer reports and that the exhaustion mark uses.
func TestCapacityVariantsAreHostedConcreteNames(t *testing.T) {
	node := nevoaNode()
	for _, v := range capacityVariantsForNode(node, "lpr") {
		if !slices.Contains(node.Functions, v) {
			t.Fatalf("variante %q não é função hospedada", v)
		}
		if node.FunctionCategories[v] != "lpr" {
			t.Fatalf("variante %q fora da categoria lpr", v)
		}
	}
}

func TestFilterExhaustedVariantsKeepsNodeWithLiveVariant(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	cands := nevoaFixture()
	capNames := capacityNamesByNode(cands, "lpr", nil, false)
	capVars := capacityVariantsByNode(cands, "lpr")

	// Premise of the test: the legacy key of the fog is the marked variant, and
	// without the walk over the variants the single key would drop the node.
	if legacy := capacityNameForNode(cands[0], "lpr", nil, false); legacy != "alpr-cpu" {
		t.Fatalf("premissa: chave legada=%q want=alpr-cpu", legacy)
	}

	mon := monitor.NewMonitor()
	mon.MarkExhausted("nevoa", "alpr-cpu", now)
	mon.UpdateCapacity("nevoa", "alpr-gpu", lprCap(1), now)

	got, _ := filterExhaustedVariantsFromMonitor(mon, cands, capNames, capVars, now)
	if !slices.Contains(exhaustedIDs(got), "nevoa") {
		t.Fatalf("D39: alpr-gpu viva deveria manter a névoa, got=%v", exhaustedIDs(got))
	}
}

func TestFilterExhaustedVariantsDropsWhenAllExhausted(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	cands := nevoaFixture()
	capNames := capacityNamesByNode(cands, "lpr", nil, false)
	capVars := capacityVariantsByNode(cands, "lpr")

	mon := monitor.NewMonitor()
	mon.MarkExhausted("nevoa", "alpr-cpu", now)
	mon.MarkExhausted("nevoa", "alpr-gpu", now)

	got, _ := filterExhaustedVariantsFromMonitor(mon, cands, capNames, capVars, now)
	ids := exhaustedIDs(got)
	if slices.Contains(ids, "nevoa") {
		t.Fatalf("todas as variantes esgotadas => névoa fora, got=%v", ids)
	}
	if !slices.Contains(ids, "borda") {
		t.Fatalf("borda deveria permanecer, got=%v", ids)
	}
}

// An unknown reading never gates: an exhausted legacy key with another variant
// that has no fresh reading, because of the time to live or of an absent report,
// keeps the node, which is the fail-open of the previous stage.
func TestFilterExhaustedVariantsUnknownKeepsNode(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	cands := nevoaFixture()
	capNames := capacityNamesByNode(cands, "lpr", nil, false)
	capVars := capacityVariantsByNode(cands, "lpr")

	mon := monitor.NewMonitor()
	mon.MarkExhausted("nevoa", "alpr-cpu", now)

	got, _ := filterExhaustedVariantsFromMonitor(mon, cands, capNames, capVars, now)
	if !slices.Contains(exhaustedIDs(got), "nevoa") {
		t.Fatalf("variante desconhecida => mantém (fail-open), got=%v", exhaustedIDs(got))
	}
}

// Regression of the silence in the hard-limit baseline: with a live variant the
// fog stays in the queue, while before it left and the dispatch went to the
// cloud without trying the live variant.
func TestSelectHLKeepsNevoaWithLiveVariant(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	mon := monitor.NewMonitor()
	mon.MarkExhausted("nevoa", "alpr-cpu", now)
	mon.UpdateCapacity("nevoa", "alpr-gpu", lprCap(1), now)
	cands := nevoaFixture()

	q := QueueSelector{Mon: mon, Cfg: defaultLimits, Clock: fixedClock(now)}
	got := exhaustedIDs(q.Select(cands, "lpr", nil))
	if !slices.Contains(got, "nevoa") {
		t.Fatalf("HL/D39: névoa com alpr-gpu viva deveria ficar, got=%v", got)
	}

	// With every variant carrying a fresh mark the node leaves, since the edge keeps the list non-empty.
	mon.MarkExhausted("nevoa", "alpr-gpu", now)
	got = exhaustedIDs(q.Select(cands, "lpr", nil))
	if slices.Contains(got, "nevoa") {
		t.Fatalf("HL: todas marcadas => névoa fora, got=%v", got)
	}
}

// In the canonical risk selector the node only leaves when every variant carries
// a fresh optimistic mark, and a variant without a mark keeps it even when it is
// genuinely cold. The capacity stage is shared and runs before the prediction.
func TestSelectRiskKeepsUnmarkedVariant(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	mon := monitor.NewMonitor()
	mon.UpdateCapacity("nevoa", "alpr-cpu", lprCap(1), now)
	mon.MarkExhausted("nevoa", "alpr-cpu", now)
	mon.UpdateCapacity("nevoa", "alpr-gpu", lprCap(1), now)
	cands := nevoaFixture()

	sel := &RiskSelector{
		Base:             QueueSelector{Mon: mon, Cfg: defaultLimits},
		ThroughputWindow: time.Minute,
		Clock:            fixedClock(now),
		Learner:          learn.NewLearner(DefaultRiskConfig().LearnConfig()),
		Cfg:              DefaultRiskConfig(),
	}
	got := exhaustedIDs(sel.Select(cands, "lpr", nil))
	if !slices.Contains(got, "nevoa") {
		t.Fatalf("risk/D39: variante sem marca mantém o nó, got=%v", got)
	}

	// A mark on both variants drops the node.
	mon.MarkExhausted("nevoa", "alpr-gpu", now)
	got = exhaustedIDs(sel.Select(cands, "lpr", nil))
	if slices.Contains(got, "nevoa") {
		t.Fatalf("risk: todas marcadas => névoa fora, got=%v", got)
	}
}

func TestAvailableConcretePrefersLiveVariant(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	node := nevoaNode()

	// Caminho legado puro: sem Monitor ou sem leitura => ok=false.
	if got, ok := AvailableConcrete(nil, node, "lpr", now); ok {
		t.Fatalf("nil mon = %q,%v want ok=false", got, ok)
	}
	mon := monitor.NewMonitor()
	if got, ok := AvailableConcrete(mon, node, "lpr", now); ok {
		t.Fatalf("sem leitura = %q,%v want ok=false", got, ok)
	}
	// A wildcard has no concrete key, so the resolution reports false and the caller keeps the verbatim resolution.
	if got, ok := AvailableConcrete(mon, nodes.Node{NodeID: "w"}, "lpr", now); ok {
		t.Fatalf("wildcard = %q,%v want ok=false", got, ok)
	}

	// alpr-cpu no teto, alpr-gpu viva => prefere a viva.
	mon.UpdateCapacity("nevoa", "alpr-cpu", lprCap(0), now)
	mon.UpdateCapacity("nevoa", "alpr-gpu", lprCap(1), now)
	got, ok := AvailableConcrete(mon, node, "lpr", now)
	if !ok || got != "alpr-gpu" {
		t.Fatalf("preferência = %q,%v want alpr-gpu,true", got, ok)
	}

	// With both variants alive the first in deterministic order is chosen, as in the legacy resolution.
	mon.UpdateCapacity("nevoa", "alpr-cpu", lprCap(2), now)
	got, ok = AvailableConcrete(mon, node, "lpr", now)
	if !ok || got != "alpr-cpu" {
		t.Fatalf("ambas vivas = %q,%v want alpr-cpu,true", got, ok)
	}

	// Marca otimista em alpr-cpu => preterida mesmo com avail>0.
	mon.MarkExhausted("nevoa", "alpr-cpu", now)
	got, ok = AvailableConcrete(mon, node, "lpr", now)
	if !ok || got != "alpr-gpu" {
		t.Fatalf("marcada = %q,%v want alpr-gpu,true", got, ok)
	}
}

// Lifecycle through the answers: a fresh capacity reading, either piggybacked or
// periodic, clears the mark and the variant becomes preferred again, without
// waiting for the time to live.
func TestExhaustedVariantRegeneratesFromFreshCapacity(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	mon := monitor.NewMonitor()
	node := nevoaNode()

	mon.MarkExhausted("nevoa", "alpr-cpu", now)
	mon.UpdateCapacity("nevoa", "alpr-gpu", lprCap(1), now)
	if got, ok := AvailableConcrete(mon, node, "lpr", now); !ok || got != "alpr-gpu" {
		t.Fatalf("marcada = %q,%v want alpr-gpu,true", got, ok)
	}

	mon.UpdateCapacity("nevoa", "alpr-cpu", lprCap(3), now)
	if got, ok := AvailableConcrete(mon, node, "lpr", now); !ok || got != "alpr-cpu" {
		t.Fatalf("após leitura fresca = %q,%v want alpr-cpu,true", got, ok)
	}
}
