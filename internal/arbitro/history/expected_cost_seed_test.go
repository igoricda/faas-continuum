package history

// Cobertura do ExpectedCost com gate PSI + seed de camada (todo 11,
// selflearning-telemetry-seeded-history).
//
// Convenção de seed (helper abaixo): amostras solo+PSI elegíveis
// (Inflight==1, CPUPctAvg>0, warm, sucesso, PSI>0 nos dois eixos) com Layer
// preenchido — que é o que gera o fan-out AggLayerKey consumido pela perna
// de seed. O nó "thin" nunca tem solo elegível própria; o doador alimenta o
// prior da camada.

import (
	"testing"
	"time"
)

// seedCostEntry grava uma entry solo+PSI elegível (salvo overrides) para
// (fn, node, layer, bucket).
func seedCostEntry(t *testing.T, h *History, fn, node, layer, bucket string, cpu float64, now time.Time) {
	t.Helper()
	e := Entry{
		FunctionName:      fn,
		NodeID:            node,
		Layer:             layer,
		Timestamp:         now,
		ScenarioContext:   ScenarioContext{LoadBucket: bucket},
		ObservedLatencyMS: 100,
		Outcome:           OutcomeSucesso,
		Inflight:          1,
		CPUPctAvg:         cpu,
		PSICpuSomeAvg10:   5,
		PSIMemFullAvg10:   3,
	}
	if err := h.AppendWithGroup(e, fn); err != nil {
		t.Fatalf("AppendWithGroup(%s/%s): %v", node, layer, err)
	}
}

func TestExpectedCostSeed(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	const window = time.Minute
	const fn = "grp"

	t.Run("par-sem-solo-recebe-seed-da-camada", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		// Doador borda: 2 solo elegíveis (20, 40) + poluição que NUNCA pode
		// entrar no seed: contended (Inflight 2, cpu 9999), cold (cpu 9999)
		// e PSI==0 (cpu 9999) — todos com Layer para caírem no fan-out.
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 20, now)
		seedCostEntry(t, h, fn, "donor", "borda", Bucket50_75, 40, now)
		for _, e := range []Entry{
			{FunctionName: fn, NodeID: "donor", Layer: "borda", Timestamp: now, ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100, Outcome: OutcomeSucesso, Inflight: 2, CPUPctAvg: 9999, PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3},
			{FunctionName: fn, NodeID: "donor", Layer: "borda", Timestamp: now, ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100, Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 9999, ColdStart: true, PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3},
			{FunctionName: fn, NodeID: "donor", Layer: "borda", Timestamp: now, ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100, Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 9999},
		} {
			if err := h.AppendWithGroup(e, fn); err != nil {
				t.Fatalf("AppendWithGroup poluição: %v", err)
			}
		}
		// Thin tem SÓ contended com Layer (sonda a camada + prova exclusão
		// da perna local).
		if err := h.AppendWithGroup(Entry{
			FunctionName: fn, NodeID: "thin", Layer: "borda", Timestamp: now,
			ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100,
			Outcome: OutcomeSucesso, Inflight: 2, CPUPctAvg: 777,
			PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3,
		}, fn); err != nil {
			t.Fatalf("AppendWithGroup thin: %v", err)
		}

		cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now)
		if !ok || !seeded || n != 2 || cost != 30 {
			t.Fatalf("seed = (%v,%d,%v,seeded=%v), esperado (30,2,true,true)", cost, n, ok, seeded)
		}
	})

	t.Run("par-com-solo-usa-media-local", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 20, now)
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 40, now)
		// Thin COM solo própria: local vence, seed ignorado.
		seedCostEntry(t, h, fn, "thin", "borda", Bucket0_10, 100, now)

		cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now)
		if !ok || seeded || n != 1 || cost != 100 {
			t.Fatalf("local = (%v,%d,%v,seeded=%v), esperado (100,1,true,false)", cost, n, ok, seeded)
		}
	})

	t.Run("psi-zero-excluido-nas-duas-pernas", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 20, now)
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 40, now)
		// Thin com solo de CPU alta mas PSI==0: fora da perna local (não
		// conta, não bloqueia o seed) — cai no prior da camada.
		if err := h.AppendWithGroup(Entry{
			FunctionName: fn, NodeID: "thin", Layer: "borda", Timestamp: now,
			ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100,
			Outcome: OutcomeSucesso, Inflight: 1, CPUPctAvg: 9999,
		}, fn); err != nil {
			t.Fatalf("AppendWithGroup thin PSI==0: %v", err)
		}

		cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now)
		if !ok || !seeded || n != 2 || cost != 30 {
			t.Fatalf("seed-sobre-PSI==0 = (%v,%d,%v,seeded=%v), esperado (30,2,true,true)", cost, n, ok, seeded)
		}
	})

	t.Run("seed-escopado-na-camada-do-par", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		seedCostEntry(t, h, fn, "donor-b", "borda", Bucket0_10, 30, now)
		seedCostEntry(t, h, fn, "donor-n", "nevoa", Bucket0_10, 1000, now)
		// Thin sonda borda (contended com Layer): seed só da borda.
		if err := h.AppendWithGroup(Entry{
			FunctionName: fn, NodeID: "thin", Layer: "borda", Timestamp: now,
			ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100,
			Outcome: OutcomeSucesso, Inflight: 2, CPUPctAvg: 777,
			PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3,
		}, fn); err != nil {
			t.Fatalf("AppendWithGroup thin: %v", err)
		}

		cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now)
		if !ok || !seeded || n != 1 || cost != 30 {
			t.Fatalf("seed-borda = (%v,%d,%v,seeded=%v), esperado (30,1,true,true) sem a névoa", cost, n, ok, seeded)
		}
	})

	t.Run("sem-sonda-de-camada-seed-grupo-wide", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		seedCostEntry(t, h, fn, "donor-b", "borda", Bucket0_10, 20, now)
		seedCostEntry(t, h, fn, "donor-b", "borda", Bucket0_10, 40, now)
		seedCostEntry(t, h, fn, "donor-n", "nevoa", Bucket0_10, 1000, now)
		// Thin SEM nenhuma entry: sem sonda => poola todas as camadas.

		cost, n, ok, seeded := h.ExpectedCost(fn, "ghost", window, now)
		want := (20.0 + 40.0 + 1000.0) / 3.0
		if !ok || !seeded || n != 3 || cost != want {
			t.Fatalf("seed-wide = (%v,%d,%v,seeded=%v), esperado (%v,3,true,true)", cost, n, ok, seeded, want)
		}
	})

	t.Run("sem-dado-de-camada-preserva-zero", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		// Contended-only sem Layer: nem local, nem fan-out => (0,0,false).
		if err := h.AppendWithGroup(Entry{
			FunctionName: fn, NodeID: "thin", Timestamp: now,
			ScenarioContext: ScenarioContext{LoadBucket: Bucket0_10}, ObservedLatencyMS: 100,
			Outcome: OutcomeSucesso, Inflight: 2, CPUPctAvg: 80,
			PSICpuSomeAvg10: 5, PSIMemFullAvg10: 3,
		}, fn); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
		if cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now); ok || n != 0 || cost != 0 || seeded {
			t.Fatalf("sem-camada = (%v,%d,%v,seeded=%v), esperado (0,0,false,false)", cost, n, ok, seeded)
		}
		// Histórico vazio => idem.
		h2, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		if cost, n, ok, seeded := h2.ExpectedCost(fn, "thin", window, now); ok || n != 0 || cost != 0 || seeded {
			t.Fatalf("vazio = (%v,%d,%v,seeded=%v), esperado (0,0,false,false)", cost, n, ok, seeded)
		}
	})

	t.Run("cache-memoriza-seed-e-append-invalida", func(t *testing.T) {
		h, err := NewHistory("", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 20, now)
		seedCostEntry(t, h, fn, "donor", "borda", Bucket0_10, 40, now)

		cost, n, ok, seeded := h.ExpectedCost(fn, "thin", window, now)
		if !ok || !seeded || n != 2 || cost != 30 {
			t.Fatalf("seed = (%v,%d,%v,seeded=%v), esperado (30,2,true,true)", cost, n, ok, seeded)
		}
		// Hit memorizado com seeded=true sob a mesma geração do par.
		h.mu.RLock()
		entry, hit := h.costCache[fn+"\x00thin"]
		h.mu.RUnlock()
		if !hit {
			t.Fatal("seed não memorizado no costCache")
		}
		if !entry.seeded || !entry.ok || entry.n != 2 || entry.mean != 30 {
			t.Fatalf("cache = %+v, esperado {ok seeded n=2 mean=30}", entry)
		}
		// Segundo query no mesmo now serve o hit (sem recomputação).
		if cost2, n2, ok2, seeded2 := h.ExpectedCost(fn, "thin", window, now); cost2 != 30 || n2 != 2 || !ok2 || !seeded2 {
			t.Fatalf("hit = (%v,%d,%v,seeded=%v), esperado (30,2,true,true)", cost2, n2, ok2, seeded2)
		}
		// Append de solo no par bumpa a geração e o seed evapora: vira
		// média local medida.
		seedCostEntry(t, h, fn, "thin", "borda", Bucket0_10, 100, now)
		cost3, n3, ok3, seeded3 := h.ExpectedCost(fn, "thin", window, now)
		if !ok3 || seeded3 || n3 != 1 || cost3 != 100 {
			t.Fatalf("pós-append = (%v,%d,%v,seeded=%v), esperado (100,1,true,false)", cost3, n3, ok3, seeded3)
		}
	})
}
