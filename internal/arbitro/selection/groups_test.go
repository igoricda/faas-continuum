package selection

import (
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

func TestEffectiveGroup(t *testing.T) {
	mapped := nodes.Node{
		NodeID:    "n1",
		Functions: []string{"alpr-yolo-tiny", "alpr-yolo-full"},
		FunctionCategories: map[string]string{
			"alpr-yolo-tiny": "alpr-yolo",
			"alpr-yolo-full": "alpr-yolo",
		},
	}
	if got := EffectiveGroup("alpr-yolo-tiny", mapped); got != "alpr-yolo" {
		t.Fatalf("mapped concrete: got %q want %q", got, "alpr-yolo")
	}
	// Concreto hospedado sem entrada no mapa => verbatim.
	partial := nodes.Node{
		NodeID:             "n2",
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"other": "alpr-yolo"},
	}
	if got := EffectiveGroup("alpr-yolo-tiny", partial); got != "alpr-yolo-tiny" {
		t.Fatalf("unmapped hosted concrete: got %q want verbatim", got)
	}
	// Mapa nil => verbatim (compatibilidade, todo 1).
	nilMap := nodes.Node{NodeID: "n3", Functions: []string{"alpr-yolo-tiny"}}
	if got := EffectiveGroup("alpr-yolo-tiny", nilMap); got != "alpr-yolo-tiny" {
		t.Fatalf("nil map: got %q want verbatim", got)
	}
	// Nome de categoria pedido direto não é concreto hospedado => verbatim.
	if got := EffectiveGroup("alpr-yolo", mapped); got != "alpr-yolo" {
		t.Fatalf("category name: got %q want verbatim", got)
	}
	// String desconhecida => verbatim.
	if got := EffectiveGroup("ghost-fn", mapped); got != "ghost-fn" {
		t.Fatalf("unknown: got %q want verbatim", got)
	}
	// Case-sensitive: sem normalização.
	caseNode := nodes.Node{
		NodeID:             "n4",
		Functions:          []string{"ALPR-YOLO-TINY"},
		FunctionCategories: map[string]string{"ALPR-YOLO-TINY": "ALPR-YOLO"},
	}
	if got := EffectiveGroup("alpr-yolo-tiny", caseNode); got != "alpr-yolo-tiny" {
		t.Fatalf("case mismatch must stay verbatim: got %q", got)
	}
	// Valor vazio nunca casa => verbatim.
	emptyVal := nodes.Node{
		NodeID:             "n5",
		Functions:          []string{"v1"},
		FunctionCategories: map[string]string{"v1": ""},
	}
	if got := EffectiveGroup("v1", emptyVal); got != "v1" {
		t.Fatalf("empty category value: got %q want verbatim", got)
	}
}

func TestEffectiveGroupSpellingDisagreement(t *testing.T) {
	// Dois nós mapeiam a mesma variante para categorias distintas:
	// helper é node-relative (split-brain documentado).
	a := nodes.Node{
		NodeID:             "a",
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo"},
	}
	b := nodes.Node{
		NodeID:             "b",
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo-legacy"},
	}
	if got := EffectiveGroup("alpr-yolo-tiny", a); got != "alpr-yolo" {
		t.Fatalf("node a: got %q want %q", got, "alpr-yolo")
	}
	if got := EffectiveGroup("alpr-yolo-tiny", b); got != "alpr-yolo-legacy" {
		t.Fatalf("node b: got %q want %q", got, "alpr-yolo-legacy")
	}
}

func TestHostServesCategory(t *testing.T) {
	n := nodes.Node{
		NodeID:    "n1",
		Functions: []string{"alpr-yolo-full"},
		FunctionCategories: map[string]string{
			"alpr-yolo-full": "alpr-yolo",
		},
	}
	if !HostServesCategory(n, "alpr-yolo") {
		t.Fatal("want true for served category")
	}
	if HostServesCategory(n, "whisper") {
		t.Fatal("want false for unserved category")
	}
	if HostServesCategory(n, "") {
		t.Fatal("empty group must never match")
	}
	// Mapa nil => false.
	nilMap := nodes.Node{NodeID: "n2", Functions: []string{"alpr-yolo-full"}}
	if HostServesCategory(nilMap, "alpr-yolo") {
		t.Fatal("nil map must return false")
	}
	// Case-sensitive.
	if HostServesCategory(n, "ALPR-YOLO") {
		t.Fatal("category comparison must be case-sensitive")
	}
	// Valor vazio nunca casa, mesmo pedido como "" (já barrado acima) ou
	// contra categoria real.
	emptyVal := nodes.Node{
		NodeID:             "n3",
		Functions:          []string{"v1"},
		FunctionCategories: map[string]string{"v1": ""},
	}
	if HostServesCategory(emptyVal, "alpr-yolo") {
		t.Fatal("empty category value must never match")
	}
	// Entrada fantasma (chave fora de Functions) não conta como servida.
	ghost := nodes.Node{
		NodeID:             "n4",
		Functions:          []string{"v1"},
		FunctionCategories: map[string]string{"ghost": "alpr-yolo"},
	}
	if HostServesCategory(ghost, "alpr-yolo") {
		t.Fatal("non-hosted map key must not count as served")
	}
}

func TestResolveConcrete(t *testing.T) {
	n := nodes.Node{
		NodeID:    "n1",
		Functions: []string{"alpr-yolo-tiny", "alpr-yolo-full"},
		FunctionCategories: map[string]string{
			"alpr-yolo-tiny": "alpr-yolo",
			"alpr-yolo-full": "alpr-yolo",
		},
	}
	// Exact-hit vence, mesmo quando o pedido também é prefixo de categoria.
	concrete, group, wildcard, ok := ResolveConcrete(n, "alpr-yolo-tiny")
	if !ok || wildcard || concrete != "alpr-yolo-tiny" || group != "alpr-yolo" {
		t.Fatalf("exact hit: got (%q,%q,%v,%v)", concrete, group, wildcard, ok)
	}
	// Exact-hit sem mapeamento: grupo é o próprio pedido.
	plain := nodes.Node{NodeID: "n2", Functions: []string{"whisper-tiny"}}
	concrete, group, wildcard, ok = ResolveConcrete(plain, "whisper-tiny")
	if !ok || wildcard || concrete != "whisper-tiny" || group != "whisper-tiny" {
		t.Fatalf("unmapped exact hit: got (%q,%q,%v,%v)", concrete, group, wildcard, ok)
	}
	// Pedido por nome de categoria => primeira variante em ordem crescente.
	onlyFull := nodes.Node{
		NodeID:    "n3",
		Functions: []string{"alpr-yolo-full", "alpr-yolo-nano"},
		FunctionCategories: map[string]string{
			"alpr-yolo-full": "alpr-yolo",
			"alpr-yolo-nano": "alpr-yolo",
		},
	}
	concrete, group, wildcard, ok = ResolveConcrete(onlyFull, "alpr-yolo")
	if !ok || wildcard || concrete != "alpr-yolo-full" || group != "alpr-yolo" {
		t.Fatalf("category fallback: got (%q,%q,%v,%v)", concrete, group, wildcard, ok)
	}
	// String desconhecida em nó não-wildcard => miss.
	if _, _, _, ok = ResolveConcrete(n, "ghost-fn"); ok {
		t.Fatal("unknown string must miss")
	}
	// Categoria de outra família => miss.
	if _, _, _, ok = ResolveConcrete(n, "whisper"); ok {
		t.Fatal("other family category must miss")
	}
	// Valor vazio nunca casa => miss (pedido "" e categoria "").
	emptyVal := nodes.Node{
		NodeID:             "n4",
		Functions:          []string{"v1"},
		FunctionCategories: map[string]string{"v1": ""},
	}
	if _, _, _, ok = ResolveConcrete(emptyVal, ""); ok {
		t.Fatal("empty request must miss")
	}
	if _, _, _, ok = ResolveConcrete(emptyVal, "alpr-yolo"); ok {
		t.Fatal("empty category value must never match a category request")
	}
	// Wildcard: Functions vazia e nil encaminham verbatim.
	for _, w := range []nodes.Node{
		{NodeID: "w1", Functions: []string{}},
		{NodeID: "w2"},
	} {
		concrete, group, wildcard, ok = ResolveConcrete(w, "alpr-yolo")
		if !ok || !wildcard || concrete != "alpr-yolo" || group != "alpr-yolo" {
			t.Fatalf("wildcard %s: got (%q,%q,%v,%v)", w.NodeID, concrete, group, wildcard, ok)
		}
	}
	// Mapa nil com hospedagem exata => (requested, requested, false, true).
	concrete, group, wildcard, ok = ResolveConcrete(plain, "whisper-tiny")
	if !ok || wildcard || concrete != "whisper-tiny" || group != "whisper-tiny" {
		t.Fatalf("nil map exact: got (%q,%q,%v,%v)", concrete, group, wildcard, ok)
	}
	// Mapa nil sem hospedagem exata => miss (não-wildcard).
	if _, _, _, ok = ResolveConcrete(plain, "whisper"); ok {
		t.Fatal("nil map category request must miss on non-wildcard")
	}
}

func TestResolveRequestGroup(t *testing.T) {
	tiny := nodes.Node{
		NodeID:    "n-tiny",
		Functions: []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{
			"alpr-yolo-tiny": "alpr-yolo",
		},
	}
	full := nodes.Node{
		NodeID:    "n-full",
		Functions: []string{"alpr-yolo-full"},
		FunctionCategories: map[string]string{
			"alpr-yolo-full": "alpr-yolo",
		},
	}
	// Pedido de concreto mapeado => categoria (união resolve tiny→alpr-yolo).
	if got := ResolveRequestGroup("alpr-yolo-tiny", []nodes.Node{tiny, full}); got != "alpr-yolo" {
		t.Fatalf("concrete request: got %q want %q", got, "alpr-yolo")
	}
	// Pedido pelo nome da categoria conhecida => o próprio nome.
	if got := ResolveRequestGroup("alpr-yolo", []nodes.Node{tiny, full}); got != "alpr-yolo" {
		t.Fatalf("category request: got %q want %q", got, "alpr-yolo")
	}
	// Desconhecida => verbatim.
	if got := ResolveRequestGroup("ghost-fn", []nodes.Node{tiny, full}); got != "ghost-fn" {
		t.Fatalf("unknown: got %q want verbatim", got)
	}
	// Sem mapeamentos (nil) => verbatim, byte-idêntico ao filtro antigo.
	plain := []nodes.Node{{NodeID: "p1", Functions: []string{"whisper-tiny"}}}
	if got := ResolveRequestGroup("whisper-tiny", plain); got != "whisper-tiny" {
		t.Fatalf("nil maps: got %q want verbatim", got)
	}
	if got := ResolveRequestGroup("whisper", plain); got != "whisper" {
		t.Fatalf("nil maps category-like: got %q want verbatim", got)
	}
	// Vazio => verbatim.
	if got := ResolveRequestGroup("", []nodes.Node{tiny}); got != "" {
		t.Fatalf("empty request: got %q", got)
	}
	// Valor vazio nunca promove: hospedado com valor "" não cria categoria.
	emptyVal := nodes.Node{
		NodeID:             "e1",
		Functions:          []string{"v1"},
		FunctionCategories: map[string]string{"v1": ""},
	}
	if got := ResolveRequestGroup("v1", []nodes.Node{emptyVal}); got != "v1" {
		t.Fatalf("empty value concrete: got %q want verbatim", got)
	}
	// Spelling disagreement: mesma variante, categorias distintas —
	// primeiro hit em ordem de NodeID vence (determinístico).
	a := nodes.Node{
		NodeID:             "a-node",
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo"},
	}
	b := nodes.Node{
		NodeID:             "b-node",
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo-legacy"},
	}
	if got := ResolveRequestGroup("alpr-yolo-tiny", []nodes.Node{b, a}); got != "alpr-yolo" {
		t.Fatalf("disagreement: got %q want %q (sorted-NodeID first hit)", got, "alpr-yolo")
	}
	// Case-sensitive: categoria em caixa distinta é outro nome.
	if got := ResolveRequestGroup("ALPR-YOLO", []nodes.Node{tiny, full}); got != "ALPR-YOLO" {
		t.Fatalf("case: got %q want verbatim", got)
	}
}
