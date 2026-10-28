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
	// A hosted concrete variant without an entry in the map is matched verbatim.
	partial := nodes.Node{
		NodeID:             "n2",
		Functions:          []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{"other": "alpr-yolo"},
	}
	if got := EffectiveGroup("alpr-yolo-tiny", partial); got != "alpr-yolo-tiny" {
		t.Fatalf("unmapped hosted concrete: got %q want verbatim", got)
	}
	// A nil map is matched verbatim, which preserves the previous behavior.
	nilMap := nodes.Node{NodeID: "n3", Functions: []string{"alpr-yolo-tiny"}}
	if got := EffectiveGroup("alpr-yolo-tiny", nilMap); got != "alpr-yolo-tiny" {
		t.Fatalf("nil map: got %q want verbatim", got)
	}
	// A category name requested directly is not a hosted concrete variant, so it is matched verbatim.
	if got := EffectiveGroup("alpr-yolo", mapped); got != "alpr-yolo" {
		t.Fatalf("category name: got %q want verbatim", got)
	}
	// An unknown string is matched verbatim.
	if got := EffectiveGroup("ghost-fn", mapped); got != "ghost-fn" {
		t.Fatalf("unknown: got %q want verbatim", got)
	}
	// The matching is case sensitive, without normalization.
	caseNode := nodes.Node{
		NodeID:             "n4",
		Functions:          []string{"ALPR-YOLO-TINY"},
		FunctionCategories: map[string]string{"ALPR-YOLO-TINY": "ALPR-YOLO"},
	}
	if got := EffectiveGroup("alpr-yolo-tiny", caseNode); got != "alpr-yolo-tiny" {
		t.Fatalf("case mismatch must stay verbatim: got %q", got)
	}
	// An empty value never matches, so it is matched verbatim.
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
	// Two nodes map the same variant to distinct categories, so the helper is
	// node relative, which is the documented split brain.
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
	// A nil map gives false.
	nilMap := nodes.Node{NodeID: "n2", Functions: []string{"alpr-yolo-full"}}
	if HostServesCategory(nilMap, "alpr-yolo") {
		t.Fatal("nil map must return false")
	}
	// The matching is case sensitive.
	if HostServesCategory(n, "ALPR-YOLO") {
		t.Fatal("category comparison must be case-sensitive")
	}
	// An empty value never matches, either requested as an empty string, which is
	// already rejected above, or against a real category.
	emptyVal := nodes.Node{
		NodeID:             "n3",
		Functions:          []string{"v1"},
		FunctionCategories: map[string]string{"v1": ""},
	}
	if HostServesCategory(emptyVal, "alpr-yolo") {
		t.Fatal("empty category value must never match")
	}
	// A ghost entry, whose key is not in the function list, does not count as served.
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
	// The exact hit wins even when the request is also a prefix of a category.
	concrete, group, wildcard, ok := ResolveConcrete(n, "alpr-yolo-tiny")
	if !ok || wildcard || concrete != "alpr-yolo-tiny" || group != "alpr-yolo" {
		t.Fatalf("exact hit: got (%q,%q,%v,%v)", concrete, group, wildcard, ok)
	}
	// An exact hit without a mapping keeps the request itself as the group.
	plain := nodes.Node{NodeID: "n2", Functions: []string{"whisper-tiny"}}
	concrete, group, wildcard, ok = ResolveConcrete(plain, "whisper-tiny")
	if !ok || wildcard || concrete != "whisper-tiny" || group != "whisper-tiny" {
		t.Fatalf("unmapped exact hit: got (%q,%q,%v,%v)", concrete, group, wildcard, ok)
	}
	// A request by category name resolves to the first variant in ascending order.
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
	// An unknown string in a node without a wildcard is a miss.
	if _, _, _, ok = ResolveConcrete(n, "ghost-fn"); ok {
		t.Fatal("unknown string must miss")
	}
	// A category of another family is a miss.
	if _, _, _, ok = ResolveConcrete(n, "whisper"); ok {
		t.Fatal("other family category must miss")
	}
	// An empty value never matches, neither the request nor the category, so it is a miss.
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
	// The wildcard: an empty or nil function list forwards the request verbatim.
	for _, w := range []nodes.Node{
		{NodeID: "w1", Functions: []string{}},
		{NodeID: "w2"},
	} {
		concrete, group, wildcard, ok = ResolveConcrete(w, "alpr-yolo")
		if !ok || !wildcard || concrete != "alpr-yolo" || group != "alpr-yolo" {
			t.Fatalf("wildcard %s: got (%q,%q,%v,%v)", w.NodeID, concrete, group, wildcard, ok)
		}
	}
	// A nil map with an exact hosting keeps the request as the group and as the concrete variant.
	concrete, group, wildcard, ok = ResolveConcrete(plain, "whisper-tiny")
	if !ok || wildcard || concrete != "whisper-tiny" || group != "whisper-tiny" {
		t.Fatalf("nil map exact: got (%q,%q,%v,%v)", concrete, group, wildcard, ok)
	}
	// A nil map without an exact hosting is a miss for a node without a wildcard.
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
	// A request for a mapped concrete variant resolves to its category, since the union maps the tiny variant to the category.
	if got := ResolveRequestGroup("alpr-yolo-tiny", []nodes.Node{tiny, full}); got != "alpr-yolo" {
		t.Fatalf("concrete request: got %q want %q", got, "alpr-yolo")
	}
	// A request by the name of a known category keeps the name itself.
	if got := ResolveRequestGroup("alpr-yolo", []nodes.Node{tiny, full}); got != "alpr-yolo" {
		t.Fatalf("category request: got %q want %q", got, "alpr-yolo")
	}
	// An unknown name is matched verbatim.
	if got := ResolveRequestGroup("ghost-fn", []nodes.Node{tiny, full}); got != "ghost-fn" {
		t.Fatalf("unknown: got %q want verbatim", got)
	}
	// Without mappings the request is matched verbatim, byte identical to the previous filter.
	plain := []nodes.Node{{NodeID: "p1", Functions: []string{"whisper-tiny"}}}
	if got := ResolveRequestGroup("whisper-tiny", plain); got != "whisper-tiny" {
		t.Fatalf("nil maps: got %q want verbatim", got)
	}
	if got := ResolveRequestGroup("whisper", plain); got != "whisper" {
		t.Fatalf("nil maps category-like: got %q want verbatim", got)
	}
	// An empty value is matched verbatim.
	if got := ResolveRequestGroup("", []nodes.Node{tiny}); got != "" {
		t.Fatalf("empty request: got %q", got)
	}
	// An empty value never promotes, so a hosted variant with an empty value creates no category.
	emptyVal := nodes.Node{
		NodeID:             "e1",
		Functions:          []string{"v1"},
		FunctionCategories: map[string]string{"v1": ""},
	}
	if got := ResolveRequestGroup("v1", []nodes.Node{emptyVal}); got != "v1" {
		t.Fatalf("empty value concrete: got %q want verbatim", got)
	}
	// Disagreement on the spelling: the same variant with distinct categories
	// resolves to the first hit in the order of the node identifier.
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
	// The matching is case sensitive, so a category in a different case is another name.
	if got := ResolveRequestGroup("ALPR-YOLO", []nodes.Node{tiny, full}); got != "ALPR-YOLO" {
		t.Fatalf("case: got %q want verbatim", got)
	}
}
