package selection

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/registry"
)

func qualitySLO(t *testing.T, raw string) *SLO {
	t.Helper()
	return &SLO{DomainMetrics: json.RawMessage(raw)}
}

func TestParseQualityRequirements(t *testing.T) {
	cases := []struct {
		name     string
		slo      *SLO
		want     map[string]float64
		declared bool
		wantErr  bool
	}{
		{"nil SLO", nil, nil, false, false},
		{"nil DomainMetrics", &SLO{}, nil, false, false},
		{"empty DomainMetrics", qualitySLO(t, ""), nil, false, false},
		{"blank DomainMetrics", qualitySLO(t, "  \n "), nil, false, false},
		{"empty object", qualitySLO(t, "{}"), nil, false, false},
		{"null literal", qualitySLO(t, "null"), nil, false, false},
		{"single float", qualitySLO(t, `{"accuracy":0.95}`), map[string]float64{"accuracy": 0.95}, true, false},
		{"integer coerces to float64", qualitySLO(t, `{"accuracy":1}`), map[string]float64{"accuracy": 1.0}, true, false},
		{"multi metric", qualitySLO(t, `{"accuracy":0.9,"latency_ms":50}`), map[string]float64{"accuracy": 0.9, "latency_ms": 50.0}, true, false},
		{"array errors", qualitySLO(t, `[1,2]`), nil, false, true},
		{"string errors", qualitySLO(t, `"accuracy"`), nil, false, true},
		{"number errors", qualitySLO(t, `42`), nil, false, true},
		{"bool errors", qualitySLO(t, `true`), nil, false, true},
		{"non-numeric value errors", qualitySLO(t, `{"accuracy":"high"}`), nil, false, true},
		{"mixed numeric+non-numeric errors", qualitySLO(t, `{"accuracy":0.9,"label":"high"}`), nil, false, true},
		{"null value errors", qualitySLO(t, `{"accuracy":null}`), nil, false, true},
		{"bool value errors", qualitySLO(t, `{"accuracy":true}`), nil, false, true},
		{"nested object value errors", qualitySLO(t, `{"accuracy":{"value":0.9}}`), nil, false, true},
		{"malformed JSON errors", qualitySLO(t, `{"accuracy":`), nil, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, declared, err := ParseQualityRequirements(tc.slo)
			if tc.wantErr {
				if err == nil {
					t.Fatal("esperava erro, obteve nil")
				}
				if declared {
					t.Fatal("erro deve voltar com declared=false")
				}
				return
			}
			if err != nil {
				t.Fatalf("erro inesperado: %v", err)
			}
			if declared != tc.declared {
				t.Fatalf("declared=%v, queria %v", declared, tc.declared)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("reqs=%v, queria %v", got, tc.want)
			}
		})
	}
}

func TestVariantQualifies(t *testing.T) {
	hi := func(v float64) registry.ExpectedMetric {
		return registry.ExpectedMetric{Value: v, HigherIsBetter: true}
	}
	lo := func(v float64) registry.ExpectedMetric {
		return registry.ExpectedMetric{Value: v}
	}
	cases := []struct {
		name     string
		expected map[string]registry.ExpectedMetric
		reqs     map[string]float64
		want     bool
	}{
		{"empty constraining set", nil, nil, true},
		{"empty reqs", map[string]registry.ExpectedMetric{"accuracy": hi(0.9)}, map[string]float64{}, true},
		{"unknown metric ignored", nil, map[string]float64{"f1": 0.8}, true},
		{"higher_is_better meets", map[string]registry.ExpectedMetric{"accuracy": hi(0.97)}, map[string]float64{"accuracy": 0.95}, true},
		{"higher_is_better equal", map[string]registry.ExpectedMetric{"accuracy": hi(0.95)}, map[string]float64{"accuracy": 0.95}, true},
		{"higher_is_better misses", map[string]registry.ExpectedMetric{"accuracy": hi(0.90)}, map[string]float64{"accuracy": 0.95}, false},
		{"lower_is_better meets", map[string]registry.ExpectedMetric{"cost": lo(3)}, map[string]float64{"cost": 5}, true},
		{"lower_is_better equal", map[string]registry.ExpectedMetric{"cost": lo(5)}, map[string]float64{"cost": 5}, true},
		{"lower_is_better misses", map[string]registry.ExpectedMetric{"cost": lo(7)}, map[string]float64{"cost": 5}, false},
		{"AND all must pass", map[string]registry.ExpectedMetric{"accuracy": hi(0.97), "cost": lo(7)}, map[string]float64{"accuracy": 0.95, "cost": 5}, false},
		{"AND all pass", map[string]registry.ExpectedMetric{"accuracy": hi(0.97), "cost": lo(3)}, map[string]float64{"accuracy": 0.95, "cost": 5}, true},
		{"partial advertisement constrains only known", map[string]registry.ExpectedMetric{"accuracy": hi(0.97)}, map[string]float64{"accuracy": 0.95, "f1": 0.99}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := VariantQualifies(tc.expected, tc.reqs); got != tc.want {
				t.Fatalf("VariantQualifies=%v, queria %v", got, tc.want)
			}
		})
	}
}

func qualityNode(id, layer string, funcs []string, cats map[string]string, exp map[string]map[string]registry.ExpectedMetric) nodes.Node {
	return nodes.Node{
		NodeID:             id,
		Layer:              layer,
		Host:               "http://" + id,
		Functions:          funcs,
		FunctionCategories: cats,
		FunctionExpected:   exp,
	}
}

func TestBestVariant(t *testing.T) {
	hi := func(v float64) registry.ExpectedMetric {
		return registry.ExpectedMetric{Value: v, HigherIsBetter: true}
	}
	full := qualityNode("n1", nodes.LayerBorda,
		[]string{"alpr-yolo-large", "alpr-yolo-tiny"},
		map[string]string{"alpr-yolo-large": "alpr-yolo", "alpr-yolo-tiny": "alpr-yolo"},
		map[string]map[string]registry.ExpectedMetric{
			"alpr-yolo-tiny":  {"accuracy": hi(0.90)},
			"alpr-yolo-large": {"accuracy": hi(0.97)},
		})
	plain := qualityNode("n2", nodes.LayerBorda, []string{"whisper-tiny"}, nil, nil)

	t.Run("wildcard verbatim qualified", func(t *testing.T) {
		w := qualityNode("w", nodes.LayerBorda, nil, nil, nil)
		c, q := BestVariant(w, "alpr-yolo", map[string]float64{"accuracy": 0.99})
		if c != "alpr-yolo" || !q {
			t.Fatalf("wildcard=(%q,%v), queria (alpr-yolo,true)", c, q)
		}
	})
	t.Run("exact hosted qualifies", func(t *testing.T) {
		c, q := BestVariant(full, "alpr-yolo-tiny", map[string]float64{"accuracy": 0.85})
		if c != "alpr-yolo-tiny" || !q {
			t.Fatalf("=(%q,%v), queria (alpr-yolo-tiny,true)", c, q)
		}
	})
	t.Run("exact hosted unqualified", func(t *testing.T) {
		c, q := BestVariant(full, "alpr-yolo-tiny", map[string]float64{"accuracy": 0.95})
		if c != "alpr-yolo-tiny" || q {
			t.Fatalf("=(%q,%v), queria (alpr-yolo-tiny,false)", c, q)
		}
	})
	t.Run("exact hosted no advertisement qualifies", func(t *testing.T) {
		c, q := BestVariant(plain, "whisper-tiny", map[string]float64{"accuracy": 0.99})
		if c != "whisper-tiny" || !q {
			t.Fatalf("=(%q,%v), queria (whisper-tiny,true)", c, q)
		}
	})
	t.Run("category first qualified lexicographic", func(t *testing.T) {
		c, q := BestVariant(full, "alpr-yolo", map[string]float64{"accuracy": 0.95})
		if c != "alpr-yolo-large" || !q {
			t.Fatalf("=(%q,%v), queria (alpr-yolo-large,true)", c, q)
		}
	})
	t.Run("category low threshold picks lexicographic first", func(t *testing.T) {
		c, q := BestVariant(full, "alpr-yolo", map[string]float64{"accuracy": 0.85})
		if c != "alpr-yolo-large" || !q {
			t.Fatalf("=(%q,%v), queria (alpr-yolo-large,true)", c, q)
		}
	})
	t.Run("category none qualifies downgrade first", func(t *testing.T) {
		c, q := BestVariant(full, "alpr-yolo", map[string]float64{"accuracy": 0.99})
		if c != "alpr-yolo-large" || q {
			t.Fatalf("=(%q,%v), queria (alpr-yolo-large,false)", c, q)
		}
	})
	t.Run("defensive total miss", func(t *testing.T) {
		c, q := BestVariant(plain, "ghost-fn", map[string]float64{"accuracy": 0.5})
		if c != "ghost-fn" || q {
			t.Fatalf("=(%q,%v), queria (ghost-fn,false)", c, q)
		}
	})
	t.Run("downgraded mirrors qualification", func(t *testing.T) {
		if Downgraded(full, "alpr-yolo-tiny", map[string]float64{"accuracy": 0.85}) {
			t.Fatal("qualificado não deve ser downgrade")
		}
		if !Downgraded(full, "alpr-yolo-tiny", map[string]float64{"accuracy": 0.95}) {
			t.Fatal("não-qualificado deve ser downgrade")
		}
	})
}

func TestQualityPartition_UnqualifiedOnlyStillReturnsNodes(t *testing.T) {
	hi := func(v float64) registry.ExpectedMetric {
		return registry.ExpectedMetric{Value: v, HigherIsBetter: true}
	}
	mkExp := func(acc float64) map[string]map[string]registry.ExpectedMetric {
		return map[string]map[string]registry.ExpectedMetric{
			"alpr-yolo-tiny": {"accuracy": hi(acc)},
		}
	}
	candidates := []nodes.Node{
		qualityNode("n1", nodes.LayerBorda, []string{"alpr-yolo-tiny"}, nil, mkExp(0.80)),
		qualityNode("n2", nodes.LayerNevoa, []string{"alpr-yolo-tiny"}, nil, mkExp(0.85)),
	}
	slo := qualitySLO(t, `{"accuracy":0.99}`)
	q := QueueSelector{}
	if got := q.Select(candidates, "alpr-yolo-tiny", slo); len(got) == 0 {
		t.Fatal("conjunto só-não-qualificado deve retornar nós (downgrade, nunca vazio)")
	}
	// The same guarantee in the canonical risk selector, since the quality
	// partition is a shared block.
	risk := &RiskSelector{Learner: learn.NewLearner(DefaultRiskConfig().LearnConfig())}
	if got := risk.Select(candidates, "alpr-yolo-tiny", slo); len(got) == 0 {
		t.Fatal("risk: conjunto só-não-qualificado deve retornar nós")
	}
}

func TestQualityPartition_PrefersQualified(t *testing.T) {
	hi := func(v float64) registry.ExpectedMetric {
		return registry.ExpectedMetric{Value: v, HigherIsBetter: true}
	}
	candidates := []nodes.Node{
		qualityNode("n1", nodes.LayerBorda, []string{"alpr-yolo-tiny"}, nil,
			map[string]map[string]registry.ExpectedMetric{"alpr-yolo-tiny": {"accuracy": hi(0.80)}}),
		qualityNode("n2", nodes.LayerBorda, []string{"alpr-yolo-tiny"}, nil,
			map[string]map[string]registry.ExpectedMetric{"alpr-yolo-tiny": {"accuracy": hi(0.99)}}),
	}
	slo := qualitySLO(t, `{"accuracy":0.95}`)
	got := QueueSelector{}.Select(candidates, "alpr-yolo-tiny", slo)
	if len(got) != 1 || got[0].NodeID != "n2" {
		t.Fatalf("partição deve isolar o qualificado, obteve %v", got)
	}
}

func TestQualityNoReorder_AllQualifyIdenticalOrder(t *testing.T) {
	hi := func(v float64) registry.ExpectedMetric {
		return registry.ExpectedMetric{Value: v, HigherIsBetter: true}
	}
	candidates := []nodes.Node{
		qualityNode("n1", nodes.LayerNevoa, []string{"alpr-yolo-tiny"}, nil,
			map[string]map[string]registry.ExpectedMetric{"alpr-yolo-tiny": {"accuracy": hi(0.99)}}),
		qualityNode("n2", nodes.LayerBorda, []string{"alpr-yolo-tiny"}, nil,
			map[string]map[string]registry.ExpectedMetric{"alpr-yolo-tiny": {"accuracy": hi(0.99)}}),
		qualityNode("n3", nodes.LayerBorda, []string{"alpr-yolo-tiny"}, nil, nil),
	}
	q := QueueSelector{}
	verbatim := q.Select(candidates, "alpr-yolo-tiny", nil)
	declared := q.Select(candidates, "alpr-yolo-tiny", qualitySLO(t, `{"accuracy":0.90}`))
	if !reflect.DeepEqual(verbatim, declared) {
		t.Fatalf("tudo-qualificado deve preservar a ordem verbatim:\n%v\n%v", verbatim, declared)
	}
	// The same invariant in the canonical risk selector.
	risk := &RiskSelector{Learner: learn.NewLearner(DefaultRiskConfig().LearnConfig())}
	verbatimRisk := risk.Select(candidates, "alpr-yolo-tiny", nil)
	declaredRisk := risk.Select(candidates, "alpr-yolo-tiny", qualitySLO(t, `{"accuracy":0.90}`))
	if !reflect.DeepEqual(verbatimRisk, declaredRisk) {
		t.Fatalf("risk tudo-qualificado deve preservar a ordem verbatim:\n%v\n%v", verbatimRisk, declaredRisk)
	}
}

func TestQualityNoReorder_DeclaredButMalformedIsVerbatim(t *testing.T) {
	candidates := []nodes.Node{
		qualityNode("n1", nodes.LayerBorda, []string{"f"}, nil, nil),
		qualityNode("n2", nodes.LayerNevoa, []string{"f"}, nil, nil),
	}
	q := QueueSelector{}
	verbatim := q.Select(candidates, "f", nil)
	malformed := q.Select(candidates, "f", qualitySLO(t, `{"accuracy":"high"}`))
	if !reflect.DeepEqual(verbatim, malformed) {
		t.Fatalf("parse-erro deve cair no caminho verbatim:\n%v\n%v", verbatim, malformed)
	}
}
