package history

import (
	"testing"
	"time"
)

// seedQueueEntry grava UMA entrada na curva de fila do par (group, node) com
// o Inflight (k) e o desfecho pedidos. Helper de teste: a curva agrega TODOS
// os segmentos do par, então o bucket do cenário não importa para o contrato.
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

// TestQueueCurveBuckets fixa o contrato da curva de entrega por profundidade
// de fila: faixas de k (queueBucketEdges), contagem por desfecho com a MESMA
// taxonomia de classifyOutcomeBucket (saturação fora da taxa de entrega), p95
// só sobre sucessos warm (cold conta como entrega, não no p95) e k<=0 fora da
// curva. Sem este teste, uma mudança no mapeamento de faixas ou na taxonomia
// passaria silenciosa e o teto aprendido do seletor seria calibrado por dados
// errados.
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
	// Faixa 2: 1 sucesso cold + 1 warm + 1 saturação (fora da taxa).
	seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 2, 5000, true)
	seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 2, 300, false)
	seedQueueEntry(t, h, group, node, now, OutcomeOrcamentoExcedido, 2, 0, false)
	// Faixa 3-5: 5 sucessos + 2 falhas (borda do piso de evidência).
	for _, lat := range []int64{400, 500, 600, 700, 800} {
		seedQueueEntry(t, h, group, node, now, OutcomeSucesso, 4, lat, false)
	}
	seedQueueEntry(t, h, group, node, now, OutcomeTimeout, 3, 0, false)
	seedQueueEntry(t, h, group, node, now, OutcomeTimeout, 5, 0, false)
	// Faixa 6-10: 1 sucesso. Faixa 11-20: 1 falha. Faixa 21-40: 1 saturação.
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

// TestQueueCurveWindowAndIsolation fixa os gates de leitura: a janela
// temporal exclui entrada velha, o prefixo isola o par (grupo, nó) de outro nó
// e de outro grupo, e um par sem NENHUMA entrada classificável devolve
// ok=false (o chamador não inventa curva). Sem isso, a curva de um nó poderia
// calibrar o teto de outro — exatamente o erro que o self-learning não pode
// cometer.
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
