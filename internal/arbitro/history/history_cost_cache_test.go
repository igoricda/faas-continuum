package history

import (
	"testing"
	"time"
)

// TestExpectedCostCacheExactWhenOldestAgesOut prova que o cache é EXATO sob
// relógio avançando sem mutação: em T1 a amostra mais antiga sai da janela e
// o resultado cacheado deve coincidir com uma computação fresca. O cache
// antigo (TTL computedAt+window) devolveria a média stale (20) porque
// T1 < computedAt+window.
func TestExpectedCostCacheExactWhenOldestAgesOut(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	const window = 10 * time.Second
	sc := ScenarioContext{LoadBucket: Bucket0_10}

	mk := func() *History {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		samples := []struct {
			age time.Duration
			cpu float64
		}{
			{9 * time.Second, 10}, // oldest em T0
			{5 * time.Second, 20},
			{1 * time.Second, 30},
		}
		for _, s := range samples {
			e := Entry{
				FunctionName:      "grp",
				NodeID:            "n1",
				Timestamp:         now.Add(-s.age),
				ScenarioContext:   sc,
				ObservedLatencyMS: 100,
				Outcome:           OutcomeSucesso,
				Inflight:          1,
				CPUPctAvg:         s.cpu,
				PSICpuSomeAvg10:   5,
				PSIMemFullAvg10:   3,
			}
			if err := h.AppendWithGroup(e, "grp"); err != nil {
				t.Fatalf("AppendWithGroup: %v", err)
			}
		}
		return h
	}

	cached := mk()
	cost0, n0, ok0, seeded0 := cached.ExpectedCost("grp", "n1", window, now)
	if !ok0 || n0 != 3 || cost0 != 20 {
		t.Fatalf("T0 = (%v,%d,%v), esperado (20,3,true)", cost0, n0, ok0)
	}
	if seeded0 {
		t.Fatalf("T0 seeded=true, esperado false (média local)")
	}

	cached.mu.RLock()
	entry, hit := cached.costCache["grp\x00n1"]
	cached.mu.RUnlock()
	if !hit {
		t.Fatal("cache não populado em T0")
	}
	if wantOldest := now.Add(-9 * time.Second); !entry.oldest.Equal(wantOldest) {
		t.Fatalf("oldest cacheado = %v, esperado %v", entry.oldest, wantOldest)
	}

	// T1 = T0+2s: a amostra de 9s passa a 11s (>window) e sai; as de 5s/1s
	// viram 7s/3s e permanecem. T1 ainda está dentro de computedAt+window
	// (T0+10s), logo o TTL antigo daria hit stale (20).
	t1 := now.Add(2 * time.Second)
	fresh := mk()
	wantCost, wantN, wantOK, _ := fresh.ExpectedCost("grp", "n1", window, t1)
	if !wantOK || wantN != 2 || wantCost != 25 {
		t.Fatalf("fresco T1 = (%v,%d,%v), esperado (25,2,true)", wantCost, wantN, wantOK)
	}

	gotCost, gotN, gotOK, _ := cached.ExpectedCost("grp", "n1", window, t1)
	if gotCost != wantCost || gotN != wantN || gotOK != wantOK {
		t.Fatalf("cache T1 = (%v,%d,%v), fresco = (%v,%d,%v): cache não exato", gotCost, gotN, gotOK, wantCost, wantN, wantOK)
	}
	if gotCost == 20 {
		t.Fatal("cache retornou média stale 20: a amostra mais antiga deveria ter expirado")
	}
}

// TestExpectedCostCacheFutureEntryEnters prova que o cache NÃO reutiliza um
// resultado que excluiu uma entry futuro-relativa ao now do cálculo quando um
// now posterior alcança o timestamp dessa entry (mesmo gen, sem mutação): a
// entry deve então entrar. O cache antigo (TTL computedAt+window) devolveria o
// negativo cacheado para sempre.
func TestExpectedCostCacheFutureEntryEnters(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	const window = 60 * time.Second
	sc := ScenarioContext{LoadBucket: Bucket0_10}

	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	// E é futuro-relativa ao primeiro `now` (ts = now+5s) mas elegível
	// (solo+PSI em ambos os eixos, gate todo 11).
	if err := h.AppendWithGroup(Entry{
		FunctionName:      "grp",
		NodeID:            "n1",
		Timestamp:         now.Add(5 * time.Second),
		ScenarioContext:   sc,
		ObservedLatencyMS: 100,
		Outcome:           OutcomeSucesso,
		Inflight:          1,
		CPUPctAvg:         40,
		PSICpuSomeAvg10:   5,
		PSIMemFullAvg10:   3,
	}, "grp"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}

	cost0, n0, ok0, _ := h.ExpectedCost("grp", "n1", window, now)
	if ok0 || n0 != 0 || cost0 != 0 {
		t.Fatalf("T0 deveria excluir E futuro-relativa: (%v,%d,%v)", cost0, n0, ok0)
	}

	h.mu.RLock()
	entry, hit := h.costCache["grp\x00n1"]
	h.mu.RUnlock()
	if !hit {
		t.Fatal("cache não populado em T0")
	}
	if want := now.Add(5 * time.Second); !entry.futureFrom.Equal(want) {
		t.Fatalf("futureFrom cacheado = %v, esperado %v", entry.futureFrom, want)
	}

	cost1, n1, ok1, _ := h.ExpectedCost("grp", "n1", window, now.Add(6*time.Second))
	if !ok1 || n1 != 1 || cost1 != 40 {
		t.Fatalf("T1 (now alcancou E) = (%v,%d,%v), esperado (40,1,true); cache reutilizou negativo stale", cost1, n1, ok1)
	}
}

// TestExpectedCostAppendToOtherPairKeepsCache prova o isolamento por par
// (grupo,nó) do contador de geração: anexar ao par B (grupo,n2) NÃO pode
// invalidar o cache do par A (grupo,n1). A observação é o computedAt da
// entry cacheada de A, que só muda numa republicação. Com o contador global
// antigo, o append em B incrementava a geração global e forçava recomputação
// (computedAt virava now2).
func TestExpectedCostAppendToOtherPairKeepsCache(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	const window = 60 * time.Second
	sc := ScenarioContext{LoadBucket: Bucket0_10}

	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	appendSolo := func(node string, cpu float64) {
		t.Helper()
		e := Entry{
			FunctionName:      "grp",
			NodeID:            node,
			Timestamp:         now.Add(-time.Second),
			ScenarioContext:   sc,
			ObservedLatencyMS: 100,
			Outcome:           OutcomeSucesso,
			Inflight:          1,
			CPUPctAvg:         cpu,
			PSICpuSomeAvg10:   5,
			PSIMemFullAvg10:   3,
		}
		if err := h.AppendWithGroup(e, "grp"); err != nil {
			t.Fatalf("AppendWithGroup(%s): %v", node, err)
		}
	}

	appendSolo("n1", 10)
	cost0, n0, ok0, _ := h.ExpectedCost("grp", "n1", window, now)
	if !ok0 || n0 != 1 || cost0 != 10 {
		t.Fatalf("T0 = (%v,%d,%v), esperado (10,1,true)", cost0, n0, ok0)
	}

	h.mu.RLock()
	entry, hit := h.costCache["grp\x00n1"]
	h.mu.RUnlock()
	if !hit {
		t.Fatal("cache não populado para o par A em T0")
	}
	if !entry.computedAt.Equal(now) {
		t.Fatalf("computedAt cacheado = %v, esperado %v", entry.computedAt, now)
	}

	// Append ao par B: não deve tocar a geração do par A.
	appendSolo("n2", 99)

	now2 := now.Add(2 * time.Second)
	cost1, n1, ok1, _ := h.ExpectedCost("grp", "n1", window, now2)
	if !ok1 || n1 != 1 || cost1 != 10 {
		t.Fatalf("T1 par A = (%v,%d,%v), esperado (10,1,true)", cost1, n1, ok1)
	}

	h.mu.RLock()
	entry2, hit2 := h.costCache["grp\x00n1"]
	h.mu.RUnlock()
	if !hit2 {
		t.Fatal("cache do par A desapareceu após append em B")
	}
	if !entry2.computedAt.Equal(now) {
		t.Fatalf("cache do par A foi republicado após append em B: computedAt=%v, esperado %v (hit não servido)", entry2.computedAt, now)
	}
	if entry2.gen != entry.gen {
		t.Fatalf("geração do par A mudou com append em B: %d -> %d", entry.gen, entry2.gen)
	}
}

// TestExpectedCostSamePairAppendInvalidates prova que anexar ao MESMO par
// ainda invalida (o contador por par cobre a autoria do próprio par): a
// amostra nova entra no resultado, sem hit stale.
func TestExpectedCostSamePairAppendInvalidates(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	const window = 60 * time.Second
	sc := ScenarioContext{LoadBucket: Bucket0_10}

	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	if err := h.AppendWithGroup(Entry{
		FunctionName: "grp", NodeID: "n1", Timestamp: now.Add(-time.Second),
		ScenarioContext: sc, Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 10,
		PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3,
	}, "grp"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	if cost, n, ok, _ := h.ExpectedCost("grp", "n1", window, now); !ok || n != 1 || cost != 10 {
		t.Fatalf("T0 = (%v,%d,%v), esperado (10,1,true)", cost, n, ok)
	}

	if err := h.AppendWithGroup(Entry{
		FunctionName: "grp", NodeID: "n1", Timestamp: now.Add(-500 * time.Millisecond),
		ScenarioContext: sc, Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 50,
		PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3,
	}, "grp"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	cost, n, ok, _ := h.ExpectedCost("grp", "n1", window, now)
	if !ok || n != 2 || cost != 30 {
		t.Fatalf("T1 = (%v,%d,%v), esperado (30,2,true): append no mesmo par não invalidou", cost, n, ok)
	}
}

// TestExpectedCostAggregateAppendBumpsOnlyPair prova que o fan-out agregado
// (AggLayerKey) não cria/incrementa contador de geração: ExpectedCost ignora
// chaves agregadas, então só o par (grupo,nó) primário conta. Um append com
// Layer preenchido grava as DUAS chaves mas incrementa UM único par.
func TestExpectedCostAggregateAppendBumpsOnlyPair(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	sc := ScenarioContext{LoadBucket: Bucket0_10}

	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	if err := h.AppendWithGroup(Entry{
		FunctionName: "grp", NodeID: "n1", Layer: "borda", Timestamp: now.Add(-time.Second),
		ScenarioContext: sc, Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 10,
	}, "grp"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.costGen) != 1 {
		t.Fatalf("esperado 1 contador de par após append agregado, obtido %d: %v", len(h.costGen), h.costGen)
	}
	if got := h.costGen["grp\x00n1"]; got != 1 {
		t.Fatalf("contador do par A = %d, esperado 1", got)
	}
}

// TestSupersedeBumpsPair prova que Supersede incrementa a geração do par
// extraído da chave primária (group\x00node\x00bucket) e que um Supersede em
// chave AGREGADA não mexe em contador algum.
func TestSupersedeBumpsPair(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	sc := ScenarioContext{LoadBucket: Bucket0_10}

	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	e := Entry{
		FunctionName: "grp", NodeID: "n1", Layer: "borda", Timestamp: now.Add(-time.Second),
		ScenarioContext: sc, Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 10,
	}
	if err := h.AppendWithGroup(e, "grp"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}

	h.mu.RLock()
	before := h.costGen["grp\x00n1"]
	h.mu.RUnlock()

	// Supersede em chave agregada: remove a entry agregada sem criar contador.
	if !h.Supersede(AggLayerKey("grp", "borda", sc), func(Entry) bool { return true }) {
		t.Fatal("Supersede agregado deveria remover a entry")
	}
	h.mu.RLock()
	afterAgg := h.costGen["grp\x00n1"]
	aggCounters := len(h.costGen)
	h.mu.RUnlock()
	if afterAgg != before || aggCounters != 1 {
		t.Fatalf("Supersede agregado alterou contadores: par=%d->%d, total=%d", before, afterAgg, aggCounters)
	}

	// Supersede em chave primária: incrementa o contador do par.
	if !h.Supersede(SegmentKey("grp", "n1", sc), func(Entry) bool { return true }) {
		t.Fatal("Supersede primário deveria remover a entry")
	}
	h.mu.RLock()
	afterPrimary := h.costGen["grp\x00n1"]
	h.mu.RUnlock()
	if afterPrimary != before+1 {
		t.Fatalf("contador do par após Supersede primário = %d, esperado %d", afterPrimary, before+1)
	}
}
