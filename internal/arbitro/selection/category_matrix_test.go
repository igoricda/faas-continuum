// Matrix of compatibility, wildcard and disagreement of categories. This file
// is the single matrix that covers the six cases of the routing of categories:
//
//	(a) a nil map everywhere gives the exact previous behavior, byte identical
//	(b) an empty function list, which is a wildcard, is eligible for categories
//	(c) the same category with crossed variants matches
//	(d) a crossed category is rejected
//	(e) two nodes disagree on the spelling of the category, which gives a
//	    node-relative outcome
//	(f) an empty category value never matches
//
// It weakens no existing test and only adds coverage. The cases (a) to (f) are
// exercised against both selectors through a shared assertion, and the cases
// (e) and (f) additionally against the pure helpers.
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
			"alpr-yolo-tiny": "", // an empty value never matches
		},
	}
	return tiny, full, whisper, wildcard, nilMap, emptyVal
}

// (a) A nil map everywhere is byte identical to the previous hosting filter:
// only the exactly hosted concrete variant is eligible, and a named category
// becomes an unknown string, which without a mapping is matched verbatim and
// therefore exactly.
func TestCategoryMatrix_NilMapExactOnly(t *testing.T) {
	_, _, _, _, nilMap, _ := matrixNodes()
	candidates := []nodes.Node{nilMap}
	// The exactly hosted concrete variant is eligible.
	assertBothSelectors(t, "a-exact", candidates, "alpr-yolo-tiny", map[string]bool{"m-nilmap": true})
	// A named category without a mapping is not a concrete variant, so nothing is eligible.
	assertBothSelectors(t, "a-category-name", candidates, "alpr-yolo", map[string]bool{})
	// Another variant of the same family, without a mapping, matches nothing.
	assertBothSelectors(t, "a-sibling", candidates, "alpr-yolo-full", map[string]bool{})
}

// (b) An empty or nil function list, which is a wildcard, is eligible for
// everything, including categories.
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

// (c) The same category with crossed variants matches in both directions.
func TestCategoryMatrix_CrossVariantSameCategory(t *testing.T) {
	tiny, full, _, _, _, _ := matrixNodes()
	candidates := []nodes.Node{tiny, full}
	assertBothSelectors(t, "c-tiny-request", candidates, "alpr-yolo-tiny", map[string]bool{"m-tiny": true, "m-full": true})
	assertBothSelectors(t, "c-full-request", candidates, "alpr-yolo-full", map[string]bool{"m-tiny": true, "m-full": true})
	assertBothSelectors(t, "c-category-request", candidates, "alpr-yolo", map[string]bool{"m-tiny": true, "m-full": true})
}

// (d) A crossed category is rejected, so the node that hosts one variant never
// serves the other.
func TestCategoryMatrix_CrossCategoryRejection(t *testing.T) {
	tiny, full, whisper, _, _, _ := matrixNodes()
	candidates := []nodes.Node{tiny, full, whisper}
	assertBothSelectors(t, "d-alpr", candidates, "alpr-yolo", map[string]bool{"m-tiny": true, "m-full": true})
	assertBothSelectors(t, "d-whisper", candidates, "whisper", map[string]bool{"m-whisper": true})
}

// (e) Two nodes disagree on the spelling of the category, which gives a
// node-relative outcome: each node resolves its own category, and the canonical
// rule of the group deterministically chooses the first hit in the order of the
// node identifier.
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
	// The pure helper: each node returns its own category.
	if got := EffectiveGroup("alpr-yolo-tiny", a); got != "alpr-yolo" {
		t.Fatalf("EffectiveGroup(a) = %q, want alpr-yolo", got)
	}
	if got := EffectiveGroup("alpr-yolo-tiny", b); got != "alpr-yolo-legacy" {
		t.Fatalf("EffectiveGroup(b) = %q, want alpr-yolo-legacy", got)
	}
	// The canonical rule: the first hit in the order of the node identifier.
	if got := ResolveRequestGroup("alpr-yolo-tiny", []nodes.Node{b, a}); got != "alpr-yolo" {
		t.Fatalf("ResolveRequestGroup = %q, want alpr-yolo (sorted-NodeID first)", got)
	}
}

// (f) An empty category value never matches, at any level.
func TestCategoryMatrix_EmptyValueNeverMatches(t *testing.T) {
	_, _, _, _, _, emptyVal := matrixNodes()
	candidates := []nodes.Node{emptyVal}
	// The exactly hosted concrete variant is still eligible through the exact
	// hosting, which does not depend on the map, while the empty category never
	// matches and a category requested by name promotes no variant.
	assertBothSelectors(t, "f-exact-concrete", candidates, "alpr-yolo-tiny", map[string]bool{"m-empty": true})
	assertBothSelectors(t, "f-category-request", candidates, "alpr-yolo", map[string]bool{})

	// The pure helpers: an empty value never becomes a category.
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
