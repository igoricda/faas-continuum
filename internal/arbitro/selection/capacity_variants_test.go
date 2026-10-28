package selection

// D39 (adendo 2026-09-25 do ADR-0007): o gating de capacidade percorre TODAS
// as variantes da categoria por nó. Cenário do diagnóstico: a névoa hospeda
// [alpr-cpu, alpr-gpu] na categoria lpr; com a chave única (alfabeticamente
// primeira = alpr-cpu) um alpr-cpu sem instância viva derrubava o nó inteiro
// e o HL ia direto para a nuvem sem tentar a alpr-gpu viva.
//
// Cobertura: (a) o nó fica enquanto houver variante viva; (b) sai quando
// todas esgotam; (c) a variante esgotada é preterida no despacho
// (AvailableConcrete), preservando a paridade treino/consulta da chave;
// (d) desconhecido/TTL e fail-open intactos; (e) ciclo de vida: leitura
// fresca (piggyback/periódica) limpa a marca e a variante volta.

import (
	"slices"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// nevoaNode é a névoa do diagnóstico: DUAS variantes concretas da categoria
// lpr. A chave legada (ResolveConcrete/capacityNameForNode) é alpr-cpu.
func nevoaNode() nodes.Node {
	return nodes.Node{
		NodeID:             "nevoa",
		Layer:              nodes.LayerNevoa,
		Functions:          []string{"alpr-cpu", "alpr-gpu"},
		FunctionCategories: map[string]string{"alpr-cpu": "lpr", "alpr-gpu": "lpr"},
	}
}

// nevoaFixture acrescenta a borda (hospeda lpr concreto) para que a lista
// não fique vazia quando a névoa é preterida — sem ela o fail-open
// devolveria a própria névoa e mascararia o teste.
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

// Paridade treino/consulta: as variantes são exatamente os nomes CONCRETOS
// hospedados (nunca a categoria, nunca inventados) — a mesma chave que o
// produtor (nodeapi) reporta e que MarkExhausted usa.
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

	// Premissa do teste: a chave legada da névoa é alpr-cpu (marcada) —
	// sem o D39 o estágio de chave única derrubaria o nó.
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

// Desconhecido nunca gating: chave legada esgotada mas OUTRA variante sem
// leitura fresca (TTL/asente) => mantém (fail-open), como no estágio antigo.
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

// Regressão do silêncio HL: com uma variante viva a névoa fica na fila do
// hard-limits (antes saía e o dispatch ia à nuvem sem tentar a alpr-gpu).
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

	// Todas as variantes com marca fresca: aí sim sai (a borda segura a lista).
	mon.MarkExhausted("nevoa", "alpr-gpu", now)
	got = exhaustedIDs(q.Select(cands, "lpr", nil))
	if slices.Contains(got, "nevoa") {
		t.Fatalf("HL: todas marcadas => névoa fora, got=%v", got)
	}
}

// RiskSelector (canônico): só derruba com TODAS as variantes com marca
// otimista fresca; uma variante não marcada (mesmo genuine-cold) mantém o
// nó. O estágio de capacidade é compartilhado e roda ANTES da previsão.
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

	// Marca nas DUAS variantes => cai.
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
	// Wildcard não tem chave concreta => ok=false (o chamador mantém o
	// verbatim do ResolveConcrete).
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

	// Ambas vivas => primeira em ordem determinística (alpr-cpu), igual à
	// resolução legada.
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

// Ciclo de vida (regeneração pelas respostas): leitura FRESCA de capacidade
// (piggyback/periódica) limpa a marca e a variante volta a ser preferida —
// sem esperar o TTL.
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
