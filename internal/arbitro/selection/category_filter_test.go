package selection

import (
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

func categoryNodes() (tiny, full, whisper, wildcard, nilMap, exactNilMap nodes.Node) {
	tiny = nodes.Node{
		NodeID:    "n-tiny",
		Layer:     nodes.LayerBorda,
		Functions: []string{"alpr-yolo-tiny"},
		FunctionCategories: map[string]string{
			"alpr-yolo-tiny": "alpr-yolo",
		},
	}
	full = nodes.Node{
		NodeID:    "n-full",
		Layer:     nodes.LayerBorda,
		Functions: []string{"alpr-yolo-full"},
		FunctionCategories: map[string]string{
			"alpr-yolo-full": "alpr-yolo",
		},
	}
	whisper = nodes.Node{
		NodeID:    "n-whisper",
		Layer:     nodes.LayerBorda,
		Functions: []string{"whisper-small"},
		FunctionCategories: map[string]string{
			"whisper-small": "whisper",
		},
	}
	wildcard = nodes.Node{
		NodeID:    "n-wild",
		Layer:     nodes.LayerBorda,
		Functions: nil,
	}
	nilMap = nodes.Node{
		NodeID:    "n-nilmap",
		Layer:     nodes.LayerBorda,
		Functions: []string{"other-fn"},
	}
	exactNilMap = nodes.Node{
		NodeID:    "n-exact",
		Layer:     nodes.LayerBorda,
		Functions: []string{"alpr-yolo-tiny"},
	}
	return tiny, full, whisper, wildcard, nilMap, exactNilMap
}

func idSet(ns []nodes.Node) map[string]bool {
	out := map[string]bool{}
	for _, n := range ns {
		out[n.NodeID] = true
	}
	return out
}

func assertBothSelectors(t *testing.T, name string, candidates []nodes.Node, requested string, want map[string]bool) {
	t.Helper()
	q := QueueSelector{Mon: nil, Cfg: defaultLimits}
	gotQ := idSet(q.Select(candidates, requested, nil))
	for id, w := range want {
		if gotQ[id] != w {
			t.Errorf("%s [QueueSelector] request=%q node=%s want eligible=%v got=%v", name, requested, id, w, gotQ[id])
		}
	}
	gotF := idSet(filterHosting(candidates, requested))
	for id, w := range want {
		if gotF[id] != w {
			t.Errorf("%s [filterHosting] request=%q node=%s want eligible=%v got=%v", name, requested, id, w, gotF[id])
		}
	}
	if len(gotQ) != len(want) {
		t.Errorf("%s [QueueSelector] request=%q got unexpected set %v want keys %v", name, requested, gotQ, want)
	}
	if len(gotF) != len(want) {
		t.Errorf("%s [filterHosting] request=%q got unexpected set %v want keys %v", name, requested, gotF, want)
	}
	// Both selectors must agree exactly.
	for id := range gotQ {
		if !gotF[id] {
			t.Errorf("%s request=%q disagreement: QueueSelector has %s but filterHosting does not", name, requested, id)
		}
	}
	for id := range gotF {
		if !gotQ[id] {
			t.Errorf("%s request=%q disagreement: filterHosting has %s but QueueSelector does not", name, requested, id)
		}
	}
}

func TestCategoryHostingCrossVariantTinyToFull(t *testing.T) {
	tiny, full, whisper, wildcard, _, _ := categoryNodes()
	candidates := []nodes.Node{full, whisper, wildcard, tiny}
	assertBothSelectors(t, "tiny-request", candidates, "alpr-yolo-tiny", map[string]bool{
		"n-tiny": true, "n-full": true, "n-wild": true,
	})
	// whisper (different category) must be absent from both.
	q := QueueSelector{Mon: nil, Cfg: defaultLimits}
	if got := idSet(q.Select(candidates, "alpr-yolo-tiny", nil)); got["n-whisper"] {
		t.Errorf("tiny-request: whisper node must be skipped, got %v", got)
	}
	if got := idSet(filterHosting(candidates, "alpr-yolo-tiny")); got["n-whisper"] {
		t.Errorf("tiny-request filterHosting: whisper node must be skipped, got %v", got)
	}
}

func TestCategoryHostingCrossVariantFullToTiny(t *testing.T) {
	tiny, full, whisper, wildcard, _, _ := categoryNodes()
	candidates := []nodes.Node{tiny, whisper, wildcard, full}
	assertBothSelectors(t, "full-request", candidates, "alpr-yolo-full", map[string]bool{
		"n-tiny": true, "n-full": true, "n-wild": true,
	})
}

func TestCategoryHostingCategoryNameRequest(t *testing.T) {
	tiny, full, whisper, wildcard, _, _ := categoryNodes()
	candidates := []nodes.Node{tiny, full, whisper, wildcard}
	assertBothSelectors(t, "category-request", candidates, "alpr-yolo", map[string]bool{
		"n-tiny": true, "n-full": true, "n-wild": true,
	})
}

func TestCategoryHostingNilMapExactOnly(t *testing.T) {
	_, _, _, _, nilMap, exactNilMap := categoryNodes()
	candidates := []nodes.Node{nilMap, exactNilMap}
	assertBothSelectors(t, "nilmap", candidates, "alpr-yolo-tiny", map[string]bool{
		"n-exact": true,
	})
	// Unknown string keeps exact-only behavior.
	ghost := nodes.Node{NodeID: "n-ghost", Layer: nodes.LayerBorda, Functions: []string{"unrelated"}}
	candidates2 := []nodes.Node{ghost, exactNilMap}
	assertBothSelectors(t, "unknown", candidates2, "no-such-fn", map[string]bool{})
}

func TestCategoryHostingWildcardEligible(t *testing.T) {
	tiny, _, _, wildcard, _, _ := categoryNodes()
	candidates := []nodes.Node{tiny, wildcard}
	assertBothSelectors(t, "wildcard-concrete", candidates, "alpr-yolo-tiny", map[string]bool{
		"n-tiny": true, "n-wild": true,
	})
	assertBothSelectors(t, "wildcard-category", candidates, "alpr-yolo", map[string]bool{
		"n-tiny": true, "n-wild": true,
	})
	emptySlice := nodes.Node{NodeID: "n-empty", Layer: nodes.LayerBorda, Functions: []string{}}
	candidates2 := []nodes.Node{tiny, emptySlice}
	assertBothSelectors(t, "wildcard-empty-slice", candidates2, "alpr-yolo-tiny", map[string]bool{
		"n-tiny": true, "n-empty": true,
	})
}

func TestCategoryHostingCrossCategoryRejection(t *testing.T) {
	tiny, full, whisper, wildcard, _, _ := categoryNodes()
	candidates := []nodes.Node{tiny, full, whisper, wildcard}
	assertBothSelectors(t, "whisper-request", candidates, "whisper-small", map[string]bool{
		"n-whisper": true, "n-wild": true,
	})
	q := QueueSelector{Mon: nil, Cfg: defaultLimits}
	for _, id := range []string{"n-tiny", "n-full"} {
		if got := idSet(q.Select(candidates, "whisper-small", nil)); got[id] {
			t.Errorf("cross-category: %s must be skipped for whisper-small", id)
		}
	}
}
