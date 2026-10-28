// Decoupled throughput: the throughput only takes part in a decision when a
// positive minimum of requests per second is declared in the objective of that
// decision. This file pins the explicit short circuit:
//
//   - an objective with latency only never consults the recent rate, so
//     candidates that differ only in the rate keep the order of the percentile;
//   - a mixed objective orders by the percentile and breaks ties by the rate;
//   - the throughput criterion without a declared minimum reports that it is not
//     applicable, without a panic, and with a declared minimum it behaves as
//     before;
//   - the hard limit of the throughput is always satisfied.
//
// The ordering by fit is shared by the queue selector, so the decoupling also
// holds in the single canonical selector, which consumes the criteria through the
// same pair.
package selection

import (
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

func TestThroughputDecoupled(t *testing.T) {
	borda := func(id string) nodes.Node {
		return nodes.Node{NodeID: id, Layer: nodes.LayerBorda, Host: "h-" + id}
	}

	t.Run("latency-only OrderByFit ignores RPS", func(t *testing.T) {
		slo := &SLO{MaxLatencyMS: 5000}
		// The first node is fast with a negligible rate, and the second is slow with a huge rate.
		segs := map[string]SegmentStats{
			"b1": {Count: 20, P95LatencyMS: 100, RecentRPS: 0.01},
			"b2": {Count: 20, P95LatencyMS: 300, RecentRPS: 500},
		}
		out := OrderByFit([]nodes.Node{borda("b2"), borda("b1")}, nil, segs, DefaultCriteria, slo)
		if len(out) != 2 || out[0].NodeID != "b1" || out[1].NodeID != "b2" {
			t.Fatalf("SLO só-latência deveria ordenar por p95 ignorando RPS, obteve %v", nodeIDs(out))
		}
		// Inverted: the slow node with the huge rate still comes last.
		segsInv := map[string]SegmentStats{
			"b1": {Count: 20, P95LatencyMS: 300, RecentRPS: 500},
			"b2": {Count: 20, P95LatencyMS: 100, RecentRPS: 0.01},
		}
		out = OrderByFit([]nodes.Node{borda("b1"), borda("b2")}, nil, segsInv, DefaultCriteria, slo)
		if len(out) != 2 || out[0].NodeID != "b2" || out[1].NodeID != "b1" {
			t.Fatalf("RPS gigante não pode promover nó lento em SLO só-latência, obteve %v", nodeIDs(out))
		}
	})

	t.Run("latency-only identical p95 keeps input order despite RPS gap", func(t *testing.T) {
		slo := &SLO{MaxLatencyMS: 5000}
		segs := map[string]SegmentStats{
			"b1": {Count: 20, P95LatencyMS: 100, RecentRPS: 1},
			"b2": {Count: 20, P95LatencyMS: 100, RecentRPS: 999},
		}
		in := []nodes.Node{borda("b2"), borda("b1")}
		out := OrderByFit(in, nil, segs, DefaultCriteria, slo)
		if len(out) != 2 || out[0].NodeID != "b2" || out[1].NodeID != "b1" {
			t.Fatalf("RPS não pode reordenar SLO só-latência, obteve %v", nodeIDs(out))
		}
	})

	t.Run("mixed SLO orders by p95 then RPS tie-break", func(t *testing.T) {
		slo := &SLO{MaxLatencyMS: 5000, MinThroughputRPS: 0.2}
		// Empate no p95: maior RPS vence.
		tied := map[string]SegmentStats{
			"b1": {Count: 20, P95LatencyMS: 100, RecentRPS: 5},
			"b2": {Count: 20, P95LatencyMS: 100, RecentRPS: 0.5},
		}
		out := OrderByFit([]nodes.Node{borda("b2"), borda("b1")}, nil, tied, DefaultCriteria, slo)
		if len(out) != 2 || out[0].NodeID != "b1" {
			t.Fatalf("empate no p95 deveria desempatar por RPS, obteve %v", nodeIDs(out))
		}
		// The percentile dominates, so the fast node with a low rate wins over the slow node with a high rate.
		untied := map[string]SegmentStats{
			"b1": {Count: 20, P95LatencyMS: 100, RecentRPS: 0.5},
			"b2": {Count: 20, P95LatencyMS: 300, RecentRPS: 50},
		}
		out = OrderByFit([]nodes.Node{borda("b2"), borda("b1")}, nil, untied, DefaultCriteria, slo)
		if len(out) != 2 || out[0].NodeID != "b1" {
			t.Fatalf("p95 deveria dominar sobre RPS, obteve %v", nodeIDs(out))
		}
	})

	t.Run("Segment short-circuits when throughput undeclared", func(t *testing.T) {
		latOnly := &SLO{MaxLatencyMS: 5000}
		rich := SegmentStats{Count: 20, P95LatencyMS: 100, RecentRPS: 1000}
		if apt, ok := (ThroughputCriterion{}).Segment(latOnly, rich); ok || apt {
			t.Fatalf("Segment sem declarado deveria ser (false,false), obteve (%v,%v)", apt, ok)
		}
		if apt, ok := (ThroughputCriterion{}).Segment(nil, rich); ok || apt {
			t.Fatalf("Segment com slo nil deveria ser (false,false), obteve (%v,%v)", apt, ok)
		}
		// With the objective declared, the previous behavior is preserved.
		decl := &SLO{MaxLatencyMS: 5000, MinThroughputRPS: 2}
		if apt, ok := (ThroughputCriterion{}).Segment(decl, SegmentStats{Count: 0, RecentRPS: 9}); ok || apt {
			t.Fatalf("Count==0 deveria ser (false,false), obteve (%v,%v)", apt, ok)
		}
		if apt, ok := (ThroughputCriterion{}).Segment(decl, SegmentStats{Count: 20, RecentRPS: 2.5}); !ok || !apt {
			t.Fatalf("RPS acima do mínimo deveria ser (true,true), obteve (%v,%v)", apt, ok)
		}
		if apt, ok := (ThroughputCriterion{}).Segment(decl, SegmentStats{Count: 20, RecentRPS: 0.5}); !ok || apt {
			t.Fatalf("RPS abaixo do mínimo deveria ser (false,true), obteve (%v,%v)", apt, ok)
		}
	})

	t.Run("Fit unchanged with declared throughput", func(t *testing.T) {
		if _, ok := (ThroughputCriterion{}).Fit(SegmentStats{Count: 0, RecentRPS: 9}); ok {
			t.Fatalf("Fit com Count==0 deveria ser !ok")
		}
		score, ok := (ThroughputCriterion{}).Fit(SegmentStats{Count: 20, RecentRPS: 2.5})
		if !ok || score != -2.5 {
			t.Fatalf("Fit deveria ser -RPS com ok, obteve (%v,%v)", score, ok)
		}
	})

	t.Run("HardLimit stays always-true", func(t *testing.T) {
		tc := ThroughputCriterion{}
		cfg := LimitsConfig{}
		n := borda("b1")
		for _, slo := range []*SLO{nil, {MaxLatencyMS: 5000}, {MaxLatencyMS: 5000, MinThroughputRPS: 2}} {
			if !tc.HardLimit(n, slo, cfg) {
				t.Fatalf("HardLimit da vazão deve ser sempre true (slo=%+v)", slo)
			}
		}
	})
}
