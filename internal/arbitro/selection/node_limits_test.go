package selection

// Hard-limits POR NÓ (arbitro.node_limits → LimitsConfig.PerNode).
//
// O contrato protegido aqui é o do crivo de sobrecarga: cada nó é julgado
// pelo SEU trio, com fallback silencioso para o global quando não há
// override. O escopo é deliberadamente estreito — estatística, previsão,
// ordenação e veto do self-learning não consultam PerNode, e o crivo
// reativo de sobrecarga é compartilhado pelos dois modos de seleção
// (ADR-0007, adendo 2026-09-28 b).

import (
	"slices"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// fixturePerNodeLimits: global 85/90/25 + dois overrides parciais, no
// formato que cmd/arbitro entrega (trio já resolvido).
//   - "borda-aperta": só CPU (60) — mem/PSI herdados do global.
//   - "nevoa-folga": só CPU (95) — acima do global de propósito.
func fixturePerNodeLimits() LimitsConfig {
	cfg := LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}
	cfg.PerNode = map[string]LimitsConfig{
		"borda-aperta": {MaxCPUPct: 60, MaxMemPct: 90, MaxPSIAvg10: 25},
		"nevoa-folga":  {MaxCPUPct: 95, MaxMemPct: 90, MaxPSIAvg10: 25},
	}
	return cfg
}

// TestLimitsConfigFor cobre a resolução em si: override vence, ausência cai
// no global, e o resultado é uma cópia degenerada (sem PerNode), então
// encadear For não pode reintroduzir override de terceiro nível.
//
// LimitsConfig deixou de ser comparável com `!=` quando ganhou o mapa
// PerNode, então a asserção compara os três escalares campo a campo.
func TestLimitsConfigFor(t *testing.T) {
	cfg := fixturePerNodeLimits()
	cases := []struct {
		name   string
		nodeID string
		want   LimitsConfig
	}{
		{name: "override do nó vence", nodeID: "borda-aperta", want: LimitsConfig{MaxCPUPct: 60, MaxMemPct: 90, MaxPSIAvg10: 25}},
		{name: "override folgado", nodeID: "nevoa-folga", want: LimitsConfig{MaxCPUPct: 95, MaxMemPct: 90, MaxPSIAvg10: 25}},
		{name: "nó sem override herda global", nodeID: "borda-comum", want: LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}},
		{name: "node_id vazio herda global", nodeID: "", want: LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cfg.For(tc.nodeID)
			if got.MaxCPUPct != tc.want.MaxCPUPct || got.MaxMemPct != tc.want.MaxMemPct || got.MaxPSIAvg10 != tc.want.MaxPSIAvg10 {
				t.Fatalf("For(%q) = cpu %.1f/mem %.1f/psi %.1f, want %.1f/%.1f/%.1f",
					tc.nodeID, got.MaxCPUPct, got.MaxMemPct, got.MaxPSIAvg10,
					tc.want.MaxCPUPct, tc.want.MaxMemPct, tc.want.MaxPSIAvg10)
			}
			if got.PerNode != nil {
				t.Fatalf("For(%q).PerNode = %v, want nil (cópia degenerada)", tc.nodeID, got.PerNode)
			}
		})
	}

	// Encadeamento: For de um resultado de For é inócuo (não há mapa para
	// consultar), então um override nunca "passa adiante".
	if again := cfg.For("borda-aperta").For("nevoa-folga"); again.MaxCPUPct != 60 {
		t.Fatalf("For.For deveria preservar o trio do primeiro nó, got cpu=%.1f", again.MaxCPUPct)
	}
}

// TestFilterOverloadedPerNode é a fronteira dona do recurso: um MESMO
// snapshot de CPU produz decisões diferentes por nó.
//
// O fixture discrimina nas duas direções, que é o que o torna um guarda de
// verdade: com o global 85, "borda-aperta" (CPU 70, teto 60) precisa CAIR
// (sem override sobreviveria) e "nevoa-folga" (CPU 90, teto 95) precisa
// FICAR (sem override seria derrubada). Uma fixture que só apertasse o
// teto passaria igual no código pré-recurso para o nó folgado.
func TestFilterOverloadedPerNode(t *testing.T) {
	cfg := fixturePerNodeLimits()
	candidates := []nodes.Node{
		{NodeID: "borda-aperta", Layer: nodes.LayerBorda},
		{NodeID: "nevoa-folga", Layer: nodes.LayerNevoa},
		{NodeID: "borda-comum", Layer: nodes.LayerBorda},
	}
	latest := map[string]telemetry.Package{
		"borda-aperta": {CPUUsagePct: 70}, // acima de 60, abaixo do global 85
		"nevoa-folga":  {CPUUsagePct: 90}, // acima do global 85, abaixo de 95
		"borda-comum":  {CPUUsagePct: 70}, // abaixo dos dois
	}
	got := FilterOverloaded(candidates, latest, cfg)
	if !slices.Equal(ids(got), []string{"nevoa-folga", "borda-comum"}) {
		t.Fatalf("FilterOverloaded per-node = %v, want [borda-comum nevoa-folga]", ids(got))
	}

	// Sem PerNode (cfg global puro), o MESMO snapshot derruba "nevoa-folga"
	// e mantém "borda-aperta" — a inversão prova que a diferença vem do
	// override e não da ordenação/estabilidade do filtro.
	globalOnly := LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}
	got = FilterOverloaded(candidates, latest, globalOnly)
	if !slices.Equal(ids(got), []string{"borda-aperta", "borda-comum"}) {
		t.Fatalf("FilterOverloaded global = %v, want [borda-aperta borda-comum]", ids(got))
	}
}

// TestFilterOverloadedPerNodeMemPsi garante que não é só a CPU: o override
// parcial de memória/PSI também decide, e o eixo não declarado continua
// herdando o global.
func TestFilterOverloadedPerNodeMemPsi(t *testing.T) {
	cfg := LimitsConfig{MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25}
	cfg.PerNode = map[string]LimitsConfig{
		// só memória apertada: CPU e PSI seguem o global.
		"nevoa-mem": {MaxCPUPct: 85, MaxMemPct: 50, MaxPSIAvg10: 25},
	}
	candidates := []nodes.Node{{NodeID: "nevoa-mem", Layer: nodes.LayerNevoa}}
	latest := map[string]telemetry.Package{
		"nevoa-mem": {CPUUsagePct: 80, MemoryUsagePct: 60}, // CPU ok no global, mem acima de 50
	}
	if got := FilterOverloaded(candidates, latest, cfg); len(got) != 0 {
		t.Fatalf("mem 60 > teto 50 deveria derrubar o nó, got=%v", ids(got))
	}
	// mem 45 passa no teto do nó, e CPU 80 continua ok pelo global.
	latest["nevoa-mem"] = telemetry.Package{CPUUsagePct: 80, MemoryUsagePct: 45}
	if got := FilterOverloaded(candidates, latest, cfg); len(got) != 1 {
		t.Fatalf("mem 45 < teto 50 deveria manter o nó, got=%v", ids(got))
	}
}

// TestFilterProjectedOverloadedPerNode cobre o hard guard de sobrecarga
// PROJETADA, que compara com o teto de CPU do próprio nó (o helper é
// privado porque só os seletores o chamam; o risco de regressão é o
// threshold fixo no global, que é exatamente o que o fixture exercita).
func TestFilterProjectedOverloadedPerNode(t *testing.T) {
	cfg := fixturePerNodeLimits()
	candidates := []nodes.Node{
		{NodeID: "borda-aperta", Layer: nodes.LayerBorda},
		{NodeID: "nevoa-folga", Layer: nodes.LayerNevoa},
		{NodeID: "borda-comum", Layer: nodes.LayerBorda},
	}
	projected := map[string]float64{
		"borda-aperta": 70, // > 60 (teto do nó), < 85 global
		"nevoa-folga":  90, // < 95 (teto do nó), > 85 global
		"borda-comum":  70, // abaixo dos dois
	}
	got := filterProjectedOverloaded(candidates, projected, cfg)
	if !slices.Equal(ids(got), []string{"nevoa-folga", "borda-comum"}) {
		t.Fatalf("filterProjectedOverloaded per-node = %v, want [borda-comum nevoa-folga]", ids(got))
	}
	// Projeção ausente mantém o candidato (nunca pode exceder).
	got = filterProjectedOverloaded(candidates, map[string]float64{}, cfg)
	if len(got) != len(candidates) {
		t.Fatalf("sem projeção deveria manter todos, got=%v", ids(got))
	}
}
