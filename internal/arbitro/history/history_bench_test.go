package history

import (
	"fmt"
	"testing"
	"time"
)

// benchEntry monta uma Entry de sucesso warm (sem cold_start) com
// timestamp relativo a base.
func benchEntry(base time.Time, offsetMS int64) Entry {
	return Entry{
		Timestamp:         base.Add(time.Duration(offsetMS) * time.Millisecond),
		FunctionName:      "bench-fn",
		NodeID:            "bench-node",
		Outcome:           OutcomeSucesso,
		ObservedLatencyMS: 100 + offsetMS%500,
		SloMet:            true,
		ColdStart:         false,
		ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
	}
}

// benchHistory cria um History em memória (path=="", sem goroutine de
// persistência) pré-preenchido com prefill entries no mesmo segmento.
func benchHistory(b *testing.B, prefill int) *History {
	b.Helper()
	h, err := NewHistory("", 1000)
	if err != nil {
		b.Fatalf("NewHistory: %v", err)
	}
	base := time.Now().Add(-time.Minute)
	for i := 0; i < prefill; i++ {
		e := benchEntry(base, int64(i))
		if err := h.AppendWithGroup(e, e.FunctionName); err != nil {
			b.Fatalf("AppendWithGroup prefill %d: %v", i, err)
		}
	}
	return h
}

func BenchmarkHistoryAppendWithGroup(b *testing.B) {
	h := benchHistory(b, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := benchEntry(time.Now(), int64(i))
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
	}
}

func BenchmarkHistoryAppendWithGroupFullSegment(b *testing.B) {
	h := benchHistory(b, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := benchEntry(time.Now(), int64(i))
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
	}
}

func BenchmarkHistoryStatsAndRecentRepeated(b *testing.B) {
	h := benchHistory(b, 100)
	key := SegmentKey("bench-fn", "bench-node", ScenarioContext{LoadBucket: Bucket0_10})
	if _, _, _, _, _, ok := h.StatsAndRecent(key, 60*time.Second, time.Now()); !ok {
		b.Fatalf("StatsAndRecent preflight: segmento vazio")
	}
	// Sem guarda no laço: o preflight acima chama StatsAndRecent com a mesma
	// chave/janela e falha se o segmento nasce vazio. Repetir a checagem a
	// cada iteração transformava um -benchtime longo em b.Fatalf (a janela
	// de 60s aging derrota o fixture) e matava justamente a medição que este
	// benchmark existe para produzir. A asserção que vale é dos testes de
	// history (history_test.go / history_cost_cache_test.go).
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.StatsAndRecent(key, 60*time.Second, time.Now())
	}
}

func BenchmarkHistoryStatsAndRecentRecompute(b *testing.B) {
	h := benchHistory(b, 1000)
	key := SegmentKey("bench-fn", "bench-node", ScenarioContext{LoadBucket: Bucket0_10})
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Sem cache de p95 (ver history_stats.go): cada iteração após um
		// AppendWithGroup exercita recomputação (p95 sort sobre ~cap
		// entradas) a cada op. Timestamp SEMPRE dentro da janela (base
		// recente + wrap < window): a fixture pré-todo-3 usava now+i ms
		// (futuro), que o filtro de janela descartava e a FIFO evictava o
		// prefill — o segmento esvaziava e o bench falhava em N>=cap+1.
		e := benchEntry(now.Add(-30*time.Second), int64(i%29000))
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
		// A leitura é a MEDIDA (recompute de p95), não uma guarda: o
		// fixture é append-only dentro da janela e o preflight do próprio
		// benchmark já prova que o segmento carrega.
		h.StatsAndRecent(key, 60*time.Second, now)
	}
}

// benchEntryClass monta uma Entry de sucesso warm com classe de concorrência
// e custo de CPU explícitas (fixture estendida do todo 3). PSI>0 fixo para
// que as solo alimentem ExpectedCost sob o gate solo+PSI (todo 11);
// contended segue excluída pelo Inflight, StatsFull ignora PSI.
func benchEntryClass(base time.Time, offsetMS int64, bucket string, inflight int, cpuPct float64) Entry {
	e := benchEntry(base, offsetMS)
	e.ScenarioContext = ScenarioContext{LoadBucket: bucket}
	e.Inflight = inflight
	e.CPUPctAvg = cpuPct
	e.SampleCount = 5
	e.PSICpuSomeAvg10 = 5
	e.PSIMemFullAvg10 = 3
	return e
}

// benchHistoryClasses pré-preenche DOIS buckets do mesmo (grupo,nó) com
// entradas alternando solo (Inflight==1, cpu>0) e contended (Inflight==2,
// cpu>0), para exercitar classes e ExpectedCost cross-bucket.
func benchHistoryClasses(b *testing.B, prefill int) *History {
	b.Helper()
	h, err := NewHistory("", 1000)
	if err != nil {
		b.Fatalf("NewHistory: %v", err)
	}
	base := time.Now().Add(-time.Minute)
	buckets := []string{Bucket0_10, Bucket50_75}
	for i := 0; i < prefill; i++ {
		e := benchEntryClass(base, int64(i), buckets[i%2], 1, 30)
		if i%2 == 1 {
			e.Inflight = 2
			e.CPUPctAvg = 45
		}
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup prefill %d: %v", i, err)
		}
	}
	return h
}

func BenchmarkHistoryStatsFullRecompute(b *testing.B) {
	h := benchHistoryClasses(b, 1000)
	key := SegmentKey("bench-fn", "bench-node", ScenarioContext{LoadBucket: Bucket0_10})
	now := time.Now()
	// Preflight: prova que o fixture nasce populado. A leitura no laço é a
	// MEDIDA (recompute completo), não uma guarda.
	if _, ok := h.StatsFull(key, 60*time.Second, now); !ok {
		b.Fatalf("StatsFull preflight: segmento vazio")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := benchEntryClass(now.Add(-30*time.Second), int64(i%29000), Bucket0_10, 1, 30)
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
		h.StatsFull(key, 60*time.Second, now)
	}
}

func BenchmarkHistoryExpectedCost(b *testing.B) {
	h := benchHistoryClasses(b, 1000)
	now := time.Now()
	if _, _, ok, _ := h.ExpectedCost("bench-fn", "bench-node", 60*time.Second, now); !ok {
		b.Fatalf("ExpectedCost preflight: sem amostra solo")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.ExpectedCost("bench-fn", "bench-node", 60*time.Second, now)
	}
}

func BenchmarkHistorySegmentKey(b *testing.B) {
	sc := ScenarioContext{LoadBucket: Bucket75Plus}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SegmentKey(fmt.Sprintf("grupo-%d", i%8), "bench-node", sc)
	}
}

// BenchmarkHistoryExpectedCostOtherPairAppend mede o hit do cache de
// ExpectedCost(A) com appends intercalados indo para OUTRO par (B): o
// contador global antigo invalidava A a cada append (recomputação O(N));
// o contador por par preserva o hit.
func BenchmarkHistoryExpectedCostOtherPairAppend(b *testing.B) {
	h := benchHistoryClasses(b, 1000)
	now := time.Now()
	base := now.Add(-30 * time.Second)
	if _, _, ok, _ := h.ExpectedCost("bench-fn", "bench-node", 60*time.Second, now); !ok {
		b.Fatalf("ExpectedCost preflight: sem amostra solo")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := benchEntryClass(base, int64(i%29000), Bucket0_10, 1, 30)
		e.NodeID = "other-node"
		if err := h.AppendWithGroup(e, "bench-fn"); err != nil {
			b.Fatalf("AppendWithGroup: %v", err)
		}
		// A leitura é a MEDIDA (o cache por par deve preservar o hit sob
		// appends de outro par). A garantia de exatidão do cache é de
		// TestExpectedCostAppendToOtherPairKeepsCache.
		h.ExpectedCost("bench-fn", "bench-node", 60*time.Second, now)
	}
}
