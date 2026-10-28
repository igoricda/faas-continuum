package history

import (
	"testing"
	"time"
)

// seedQueueEntry records one entry in the queue curve of the pair of group and
// node, with the requested number of requests in flight and the requested
// outcome. The curve aggregates every segment of the pair, so the bucket of the
// scenario does not matter for the contract.
func seedQueueEntry(t *testing.T, h *History, group, nodeID string, ts time.Time, outcome string, k int, latMS int64, cold bool) {
	t.Helper()
	if err := h.AppendWithGroup(Entry{
		FunctionName:      "detect",
		NodeID:            nodeID,
		Timestamp:         ts,
		ScenarioContext:   ScenarioContext{LoadBucket: "0-10%"},
		ObservedLatencyMS: latMS,
		Outcome:           outcome,
		Inflight:          k,
		ColdStart:         cold,
	}, group); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
}

// TestQueueCurveBuckets fixes the contract of the delivery curve by depth of the
// queue: the bands of the number of requests in flight, the count per outcome
// with the same taxonomy as the classification of the outcomes, which keeps the
// saturation out of the delivery rate, the percentile over the warm successes
// only, since a cold start is a delivery without entering the percentile, and the
// exclusion of a zero number of requests in flight from the curve. Without this
// test a change in the mapping of the bands or in the taxonomy would pass
// silently, and the learned ceiling of the selector would be calibrated with
// wrong data.
func TestQueueCurveBuckets(t *testing.T) {
	now := time.Date(2026, 10, 2, 21, 0, 0, 0, time.UTC)
	h, err := NewHistory("", 500)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	const group, node = "detect", "n1"

	// Faixa 1: 2 sucessos warm + 1 falha.
	seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 1, 100, false)
	seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 1, 200, false)
	seedQueueEntry(t, h, group, node, now, OutcomeFalha, 1, 0, false)
	// The second band: one cold success, one warm success and one saturation, which is out of the rate.
	seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 2, 5000, true)
	seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 2, 300, false)
	seedQueueEntry(t, h, group, node, now, OutcomeOrcamentoExcedido, 2, 0, false)
	// The bands of three to five requests in flight: five successes and two failures, at the edge of the floor of evidence.
	for _, lat := range []int64{400, 500, 600, 700, 800} {
		seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 4, lat, false)
	}
	seedQueueEntry(t, h, group, node, now, OutcomeTimeout, 3, 0, false)
	seedQueueEntry(t, h, group, node, now, OutcomeTimeout, 5, 0, false)
	// The band of six to ten requests in flight: one success. The band of eleven to twenty: one failure. The band of twenty-one to forty: one saturation.
	seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 6, 900, false)
	seedQueueEntry(t, h, group, node, now, OutcomeTimeout, 20, 0, false)
	seedQueueEntry(t, h, group, node, now, OutcomeRejeicaoCapacidade, 21, 0, false)
	// Faixa 41+: 1 sucesso.
	seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 41, 1000, false)
	// Fora da curva: Inflight desconhecido.
	seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 0, 777, false)

	curve, ok := h.QueueCurve(group, node, time.Hour, now)
	if !ok {
		t.Fatalf("QueueCurve: ok=false; want true")
	}
	if len(curve.Buckets) != len(queueBucketEdges) {
		t.Fatalf("len(Buckets)=%d; want %d", len(curve.Buckets), len(queueBucketEdges))
	}
	want := []QueueBucket{
		{KMin: 1, KMax: 1, P95MS: 200, Successes: 2, Failures: 1},
		{KMin: 2, KMax: 2, P95MS: 300, Successes: 2, Failures: 0, Saturation: 1},
		{KMin: 3, KMax: 5, P95MS: 800, Successes: 5, Failures: 2},
		{KMin: 6, KMax: 10, P95MS: 900, Successes: 1},
		{KMin: 11, KMax: 20, Failures: 1},
		{KMin: 21, KMax: 40, Saturation: 1},
		{KMin: 41, KMax: queueBucketUnboundedK, P95MS: 1000, Successes: 1},
	}
	for i, w := range want {
		if got := curve.Buckets[i]; got != w {
			t.Fatalf("bucket[%d] = %+v; want %+v", i, got, w)
		}
	}
}

// TestQueueCurveWindowAndIsolation fixes the reading gates: the temporal window
// excludes an old entry, the prefix isolates the pair of group and node from
// another node and from another group, and a pair without any classifiable entry
// reports false, so the caller invents no curve. Without this the curve of one
// node could calibrate the ceiling of another, which is exactly the error that
// the self-learning cannot make.
func TestQueueCurveWindowAndIsolation(t *testing.T) {
	now := time.Date(2026, 10, 2, 21, 0, 0, 0, time.UTC)
	h, err := NewHistory("", 500)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	seedQueueEntry(t, h, "detect", "n1", now, OutcomeSucesso, 3, 400, false)
	seedQueueEntry(t, h, "detect", "n1", now.Add(-2*time.Hour), OutcomeTimeout, 3, 0, false)
	seedQueueEntry(t, h, "detect", "n2", now, OutcomeTimeout, 3, 0, false)
	seedQueueEntry(t, h, "lpr", "n1", now, OutcomeTimeout, 3, 0, false)

	curve, ok := h.QueueCurve("detect", "n1", time.Hour, now)
	if !ok {
		t.Fatalf("QueueCurve: ok=false; want true")
	}
	b := curve.Buckets[2] // 3-5
	if b.Successes != 1 || b.Failures != 0 {
		t.Fatalf("bucket 3-5 = %+v; want 1 sucesso e 0 falhas (velho/outro nó/outro grupo fora)", b)
	}

	// Par frio: nenhuma entrada de (detect, n3).
	if _, ok := h.QueueCurve("detect", "n3", time.Hour, now); ok {
		t.Fatalf("QueueCurve de par frio: ok=true; want false")
	}
}
