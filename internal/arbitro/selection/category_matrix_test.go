// Matrix de compatibilidade, wildcard e discordância de categorias
// (function-category-routing, todo 10). Este arquivo é a matriz ÚNICA que
// cobre os seis casos do plano:
//
//	(a) nil map em todo lugar  => comportamento exato (byte-idêntico ao antigo)
//	(b) Functions vazia (wildcard) => elegível para categorias
//	(c) mesma categoria, variantes cruzadas => match
//	(d) categoria cruzada => rejeição
//	(e) dois nós discordam na grafia da categoria => outcome node-relative
//	(f) valor de categoria vazio => nunca casa
//
// Sem enfraquecer nenhum teste existente — apenas adiciona. Os casos
// (a)-(f) são exercitados aqui contra AMBOS os seletores via
// assertBothSelectors, e (e)/(f) adicionalmente contra os helpers puros.
package selection

import (
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

func matrixNodes() (tiny, full, whisper, wildcard, nilMap, emptyVal nodes.Node) {
	tiny = nodes.Node{
		NodeID:    "m-tiny",
		Layer:     nodes.LayerBorda,
		Functions: []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{
			"alpr-yolo-tiny": "alpr-yolo",
		},
	}
	full = nodes.Node{
		NodeID:    "m-full",
		Layer:     nodes.LayerBorda,
		Functions: []string{"alpr-yolo-full"},
		FunctionCategories: map[string]string{
			"alpr-yolo-full": "alpr-yolo",
		},
	}
	whisper = nodes.Node{
		NodeID:    "m-whisper",
		Layer:     nodes.LayerBorda,
		Functions: []string{"whisper-small"},
		FunctionCategories: map[string]string{
			"whisper-small": "whisper",
		},
	}
	wildcard = nodes.Node{NodeID: "m-wild", Layer: nodes.LayerBorda} // Functions nil
	nilMap = nodes.Node{
		NodeID:    "m-nilmap",
		Layer:     nodes.LayerBorda,
		Functions: []string{"alpr-yolo-tiny"},
	}
	emptyVal = nodes.Node{
		NodeID:    "m-empty",
		Layer:     nodes.LayerBorda,
		Functions: []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{
			"alpr-yolo-tiny": "", // valor vazio: nunca casa
		},
	}
	return tiny, full, whisper, wildcard, nilMap, emptyVal
}

// (a) nil map em todo lugar => byte-idêntico ao filtro de hospedagem antigo:
// só o concreto exatamente hospedado é elegível; categoria nomeada vira
// string desconhecida (sem mapeamento => verbatim => exact-only).
func TestCategoryMatrix_NilMapExactOnly(t *testing.T) {
	_, _, _, _, nilMap, _ := matrixNodes()
	candidates := []nodes.Node{nilMap}
	// Concreto exato hospedado => elegível.
	assertBothSelectors(t, "a-exact", candidates, "alpr-yolo-tiny", map[string]bool{"m-nilmap": true})
	// Categoria nomeada sem mapeamento => não é concreto => nada elegível.
	assertBothSelectors(t, "a-category-name", candidates, "alpr-yolo", map[string]bool{})
	// Outra variante da "mesma família" (não mapeada) => nada.
	assertBothSelectors(t, "a-sibling", candidates, "alpr-yolo-full", map[string]bool{})
}

// (b) Functions vazia/nil (wildcard) => elegível para tudo, inclusive
// categorias.
func TestCategoryMatrix_WildcardEligibleForCategories(t *testing.T) {
	_, full, _, wildcard, _, _ := matrixNodes()
	for _, w := range []nodes.Node{
		wildcard, // nil
		{NodeID: "m-empty-slice", Layer: nodes.LayerBorda, Functions: []string{}},
	} {
		candidates := []nodes.Node{full, w}
		assertBothSelectors(t, "b-concrete", candidates, "alpr-yolo-full", map[string]bool{"m-full": true, w.NodeID: true})
		assertBothSelectors(t, "b-category", candidates, "alpr-yolo", map[string]bool{"m-full": true, w.NodeID: true})
	}
}

// (c) mesma categoria, variantes cruzadas => match em ambas as direções.
func TestCategoryMatrix_CrossVariantSameCategory(t *testing.T) {
	tiny, full, _, _, _, _ := matrixNodes()
	candidates := []nodes.Node{tiny, full}
	assertBothSelectors(t, "c-tiny-request", candidates, "alpr-yolo-tiny", map[string]bool{"m-tiny": true, "m-full": true})
	assertBothSelectors(t, "c-full-request", candidates, "alpr-yolo-full", map[string]bool{"m-tiny": true, "m-full": true})
	assertBothSelectors(t, "c-category-request", candidates, "alpr-yolo", map[string]bool{"m-tiny": true, "m-full": true})
}

// (d) categoria cruzada => rejeição: nó whisper nunca serve alpr-yolo e
// vice-versa.
func TestCategoryMatrix_CrossCategoryRejection(t *testing.T) {
	tiny, full, whisper, _, _, _ := matrixNodes()
	candidates := []nodes.Node{tiny, full, whisper}
	assertBothSelectors(t, "d-alpr", candidates, "alpr-yolo", map[string]bool{"m-tiny": true, "m-full": true})
	assertBothSelectors(t, "d-whisper", candidates, "whisper", map[string]bool{"m-whisper": true})
}

// (e) dois nós discordam na grafia => outcome node-relative (split-brain
// documentado). Cada nó resolve a própria categoria; a regra canônica de
// grupo escolhe deterministicamente o primeiro hit em ordem de NodeID.
func TestCategoryMatrix_SpellingDisagreementNodeRelative(t *testing.T) {
	a := nodes.Node{
		NodeID:             "disc-a",
		Layer:              nodes.LayerBorda,
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo"},
	}
	b := nodes.Node{
		NodeID:             "disc-b",
		Layer:              nodes.LayerBorda,
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo-legacy"},
	}
	// Helper puro: cada nó devolve a própria categoria.
	if got := EffectiveGroup("alpr-yolo-tiny", a); got != "alpr-yolo" {
		t.Fatalf("EffectiveGroup(a) = %q, want alpr-yolo", got)
	}
	if got := EffectiveGroup("alpr-yolo-tiny", b); got != "alpr-yolo-legacy" {
		t.Fatalf("EffectiveGroup(b) = %q, want alpr-yolo-legacy", got)
	}
	// Regra canônica: primeiro hit em ordem de NodeID ("disc-a" < "disc-b").
	if got := ResolveRequestGroup("alpr-yolo-tiny", []nodes.Node{b, a}); got != "alpr-yolo" {
		t.Fatalf("ResolveRequestGroup = %q, want alpr-yolo (sorted-NodeID first)", got)
	}
}

// (f) valor de categoria vazio => nunca casa, em todos os níveis.
func TestCategoryMatrix_EmptyValueNeverMatches(t *testing.T) {
	_, _, _, _, _, emptyVal := matrixNodes()
	candidates := []nodes.Node{emptyVal}
	// Concreto exato hospedado ainda é elegível por hospedagem exata
	// (independe do mapa), mas a categoria "vazia" nunca casa e a categoria
	// pedida como nome não promove nenhuma variante.
	assertBothSelectors(t, "f-exact-concrete", candidates, "alpr-yolo-tiny", map[string]bool{"m-empty": true})
	assertBothSelectors(t, "f-category-request", candidates, "alpr-yolo", map[string]bool{})

	// Helpers puros: valor vazio nunca vira categoria.
	if got := EffectiveGroup("alpr-yolo-tiny", emptyVal); got != "alpr-yolo-tiny" {
		t.Fatalf("EffectiveGroup(empty val) = %q, want verbatim", got)
	}
	if HostServesCategory(emptyVal, "alpr-yolo") {
		t.Fatal("HostServesCategory(empty val) deve ser false")
	}
	if HostServesCategory(emptyVal, "") {
		t.Fatal("HostServesCategory grupo vazio deve ser false")
	}
	if _, _, _, ok := ResolveConcrete(emptyVal, "alpr-yolo"); ok {
		t.Fatal("ResolveConcrete categoria com valor vazio deve ser miss")
	}
}
