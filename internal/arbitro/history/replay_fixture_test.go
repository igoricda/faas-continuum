package history

// Harness de replay da fixture (pin do priming 2026-09-21; convertida r4 em 2026-09-28).
//
// Convenção (base para os todos 2,3,4,15 — manter estável):
//   - fixture versionada em internal/arbitro/history/testdata/history-r4.json
//     (testdata Go padrão: ignorado pelo tooling; cópia byte-identical de
//     ~/priming-2026-09-21-r3/history.json, sha256 839402e1...af1870d).
//   - carregamento SEMPRE via NewHistory (load path de produção em
//     history_store.go:53-96) — nenhum schema JSON paralelo; Entry/History
//     são os tipos de produção.
//   - janela/now ancorados exatamente na semântica de scanStatsLocked
//     (history_stats.go:108-116): entry participa sse
//     !Timestamp.After(now) && now.Sub(Timestamp) <= window.
//   - default: window=10m (ReplayDefaultWindow), now=max Entry.Timestamp da
//     fixture (MaxReplayTimestamp); ambos override via params.
//
// ACHADO DE PIN (documentado, não bug do helper): o span da fixture é ~54min
// (21:52→22:46), então window=10m@max vê só a cauda (46 entries), NÃO as 560.
// O gate de integridade 12/560 é all-time (window<=0). Replay "quente" total
// exige window >= span (ex.: 60m@max => 560). Implicação p/ seeded-history:
// seed só aquece o experimento dentro da ThroughputWindow — ver learnings.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	// ReplayFixtureRelPath é o caminho da fixture relativo a este pacote.
	ReplayFixtureRelPath = "testdata/history-r4.json"
	// ReplayDefaultWindow é a janela default do replay (== ThroughputWindow
	// de produção; NÃO confundir com o default do wiring — aqui é só o
	// default do harness).
	ReplayDefaultWindow = 10 * time.Minute
	// Gate de integridade da fixture de replay (docs/priming-historico-lpr-ocr.md §3).
	ReplayFixtureSegments = 12
	ReplayFixtureEntries  = 560
	// ReplayFixtureCap cobre o maior segmento (108 entries) com folga; igual
	// ao -history-cap 1000 do priming para não truncar no load.
	ReplayFixtureCap = 1000
)

// LoadReplayFixture carrega path via NewHistory (load path de produção) e
// registra Close via t.Cleanup. Falha o teste em erro de load.
//
// A fixture pinada e r4 DE NASCENCA (2026-09-28). Antes ela era r3 sem
// marcador e o harness carimbava a sentinela numa copia em TempDir, porque
// NewHistory rejeita r3 fail-closed. O r3 foi aposentado: a fixture foi
// convertida (marker-only: sentinela + entries verbatim) e o passo de
// migracao saiu do harness. Os gates 12/560, fan-out 280/280 e cauda 10m=46
// foram reconferidos antes e depois da conversao e NAO se movem -- o
// gate de calibracao segue em mean_err=0.4063 (NO-GO) / qiso-offline=0.0609.
func LoadReplayFixture(t *testing.T, path string) *History {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("LoadReplayFixture(%q): %v", path, err)
	}
	var segs map[string][]Entry
	if err := json.Unmarshal(raw, &segs); err != nil {
		t.Fatalf("LoadReplayFixture(%q): %v", path, err)
	}
	if _, ok := segs[historySchemaSentinelKey]; !ok {
		segs[historySchemaSentinelKey] = nil
		stamped, err := json.Marshal(segs)
		if err != nil {
			t.Fatalf("LoadReplayFixture(%q): %v", path, err)
		}
		stamped = append(stamped, '\n')
		path = filepath.Join(t.TempDir(), "history-stamped-r4.json")
		if err := os.WriteFile(path, stamped, 0o600); err != nil {
			t.Fatalf("LoadReplayFixture(%q): %v", path, err)
		}
	}
	h, err := NewHistory(path, ReplayFixtureCap)
	if err != nil {
		t.Fatalf("LoadReplayFixture(%q): %v", path, err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Fatalf("LoadReplayFixture Close: %v", err)
		}
	})
	return h
}

// MaxReplayTimestamp devolve o maior Entry.Timestamp do histórico (default
// `now` do replay). Varre via Keys+StatsFull? Não — Timestamp não sai nas
// stats; usa o mapa sob RLock (mesmo pacote) sem copiar slices.
func MaxReplayTimestamp(h *History) time.Time {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var max time.Time
	for _, entries := range h.m {
		for i := range entries {
			if entries[i].Timestamp.After(max) {
				max = entries[i].Timestamp
			}
		}
	}
	return max
}

// CountReplayInWindow ancora window/now e conta entries in-window com a
// MESMA semântica de scanStatsLocked, mas via API pública de produção
// (Keys + StatsAndRecent por segmento — sem reimplementar o filtro).
// Retorna (segmentos com total>0 in-window, soma dos totals in-window).
// total de StatsAndRecent == todo Outcome in-window (sucesso+falha+timeout;
// orcamento_excedido excluído — ver scanStatsLocked).
func CountReplayInWindow(h *History, window time.Duration, now time.Time) (segments, total int) {
	for _, k := range h.Keys() {
		_, _, _, segTotal, _, _ := h.StatsAndRecent(k, window, now)
		if segTotal > 0 {
			segments++
			total += segTotal
		}
	}
	return segments, total
}

// CountReplayAllTime conta sem filtro temporal (window<=0 => all-time).
func CountReplayAllTime(h *History) (segments, total int) {
	keys := h.Keys()
	for _, k := range keys {
		_, _, _, segTotal, _, _ := h.StatsAndRecent(k, 0, time.Time{})
		total += segTotal
	}
	return len(keys), total
}

func TestReplayFixturePin(t *testing.T) {
	fixture := filepath.Join("testdata", "history-r4.json")
	if _, err := os.Stat(fixture); err != nil {
		t.Fatalf("fixture ausente: %v", err)
	}

	t.Run("happy/gate all-time 12 segmentos 560 entries", func(t *testing.T) {
		h := LoadReplayFixture(t, fixture)
		segs, total := CountReplayAllTime(h)
		if segs != ReplayFixtureSegments || total != ReplayFixtureEntries {
			t.Fatalf("gate all-time: got %d segmentos/%d entries, want %d/%d",
				segs, total, ReplayFixtureSegments, ReplayFixtureEntries)
		}
		// Integridade por segmento: 280 primárias + 280 agregadas (fan-out).
		var prim, agg int
		for _, k := range h.Keys() {
			_, _, _, segTotal, _, _ := h.StatsAndRecent(k, 0, time.Time{})
			if IsAggLayerKey(k) {
				agg += segTotal
			} else {
				prim += segTotal
			}
		}
		if prim != 280 || agg != 280 {
			t.Fatalf("fan-out: got prim=%d agg=%d, want 280/280", prim, agg)
		}
	})

	t.Run("happy/window 60m@max vê as 560 (replay quente total)", func(t *testing.T) {
		h := LoadReplayFixture(t, fixture)
		now := MaxReplayTimestamp(h)
		if now.IsZero() {
			t.Fatal("MaxReplayTimestamp retornou zero")
		}
		segs, total := CountReplayInWindow(h, time.Hour, now)
		if segs != ReplayFixtureSegments || total != ReplayFixtureEntries {
			t.Fatalf("60m@max: got %d/%d, want %d/%d", segs, total,
				ReplayFixtureSegments, ReplayFixtureEntries)
		}
	})

	t.Run("happy/default 10m@max vê só a cauda (span 54min > window)", func(t *testing.T) {
		h := LoadReplayFixture(t, fixture)
		now := MaxReplayTimestamp(h)
		segs, total := CountReplayInWindow(h, ReplayDefaultWindow, now)
		// Pin do valor real (derivado da fixture, ver cabeçalho): mudou =>
		// fixture mudou => investigar, não ajustar às cegas.
		if total != 46 {
			t.Fatalf("10m@max: got total=%d, want 46 (cauda da fixture de replay)", total)
		}
		if segs == 0 || segs > ReplayFixtureSegments {
			t.Fatalf("10m@max: segmentos=%d fora de (0,%d]", segs, ReplayFixtureSegments)
		}
	})

	t.Run("stale/60s@max+1h retorna 0 (stale==cold)", func(t *testing.T) {
		h := LoadReplayFixture(t, fixture)
		now := MaxReplayTimestamp(h).Add(time.Hour)
		segs, total := CountReplayInWindow(h, time.Minute, now)
		if segs != 0 || total != 0 {
			t.Fatalf("stale: got %d/%d, want 0/0", segs, total)
		}
	})

	t.Run("failure/JSON corrompido => erro limpo, nunca panic", func(t *testing.T) {
		bad := filepath.Join(t.TempDir(), "history-bad.json")
		if err := os.WriteFile(bad, []byte(`{"k": [INVALID`), 0o600); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic no load corrompido: %v", r)
			}
		}()
		h, err := NewHistory(bad, ReplayFixtureCap)
		if err == nil {
			if h != nil {
				_ = h.Close()
			}
			t.Fatal("NewHistory com JSON corrompido deveria retornar erro")
		}
	})
}
