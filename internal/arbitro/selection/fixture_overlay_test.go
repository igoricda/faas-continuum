package selection

// Helpers do harness de auditoria D32 (uso_band_audit_test.go): fixture r4
// pinada + overlay de USO sintético determinístico (Track C — TVBox sem
// CONFIG_PSI, PSI forçado unknown). Extraídos do ciclo v1 quando o
// SelfLearningSelector foi aposentado (ADR-0012): o harness auditava a
// banda preditiva do BLR no caminho usage-curve, que agora é apenas
// exercitada pelo próprio pacote history — a fixture e o overlay seguem
// aqui para a auditoria de calibração continuar reexecutável.
//
// A fixture é carregada via tipos de produção (sem schema paralelo) e só
// chaves PRIMÁRIAS são re-appendadas (o fan-out de AppendWithGroup recria
// as agg); re-appendar as agg DUPLICARIA a população.

import (
	"encoding/json"
	"hash/fnv"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
)

const (
	rgFixtureRel   = "../history/testdata/history-r4.json"
	rgTailWindow   = 10 * time.Minute
	rgFullWindow   = 60 * time.Minute
	rgFixtureTotal = 560
	rgFixtureSegs  = 12
)

// rgUsageSrc rotula o overlay do Track C em cada linha de segmento.
const rgUsageSrc = "usage-overlay(hash,fnv-key-idx,psi=0)"

// rgUsageSample é um ponto warm-solo do overlay com seu uso sintético e a
// latência REAL da fixture.
type rgUsageSample struct {
	ts  time.Time
	x   float64
	y   float64
	lat float64
}

// rgRaw carrega a fixture via tipos de produção (sem schema paralelo).
func rgRaw(t *testing.T) map[string][]history.Entry {
	t.Helper()
	raw, err := os.ReadFile(rgFixtureRel)
	if err != nil {
		t.Fatalf("fixture ausente: %v", err)
	}
	var segs map[string][]history.Entry
	if err := json.Unmarshal(raw, &segs); err != nil {
		t.Fatalf("fixture corrompida: %v", err)
	}
	return segs
}

func rgMaxTS(segs map[string][]history.Entry) time.Time {
	var max time.Time
	for _, es := range segs {
		for _, e := range es {
			if e.Timestamp.After(max) {
				max = e.Timestamp
			}
		}
	}
	return max
}

func rgSplitKey(key string) (group, node, bucket string, ok bool) {
	g, rest, found := strings.Cut(key, "\x00")
	if !found {
		return "", "", "", false
	}
	n, b, found := strings.Cut(rest, "\x00")
	if !found || strings.Contains(b, "\x00") {
		return "", "", "", false
	}
	return g, n, b, true
}

func rgMedian(xs []float64) (float64, bool) {
	if len(xs) == 0 {
		return 0, false
	}
	cp := slices.Clone(xs)
	slices.Sort(cp)
	n := len(cp)
	if n%2 == 1 {
		return cp[n/2], true
	}
	return (cp[n/2-1] + cp[n/2]) / 2, true
}

// rgSynthUsage deriva uso sintético determinístico por (chave, índice):
// base por bucket (preserva rank de carga) + jitter ±1.5 via FNV-1a.
// Sempre >0 (nunca unknown); sem RNG, sem estado, ordem-independente.
func rgSynthUsage(key string, idx int, bucket string) (float64, float64) {
	var baseX, baseY float64
	switch bucket {
	case history.Bucket0_10:
		baseX, baseY = 5, 3
	case history.Bucket10_25:
		baseX, baseY = 17, 8
	case history.Bucket25_50:
		baseX, baseY = 37, 15
	case history.Bucket50_75:
		baseX, baseY = 62, 25
	case history.Bucket75Plus:
		baseX, baseY = 85, 40
	default:
		baseX, baseY = 6, 4
	}
	jit := func(axis byte) float64 {
		h := fnv.New64a()
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte{axis})
		var b [8]byte
		v := idx
		for i := range b {
			b[i] = byte(v)
			v >>= 8
		}
		_, _ = h.Write(b[:])
		return float64(h.Sum64()%300)/100.0 - 1.5
	}
	x, y := baseX+jit('x'), baseY+jit('y')
	if x < 0.5 {
		x = 0.5
	}
	if y < 0.5 {
		y = 0.5
	}
	return x, y
}

// rgBuildUsageOverlay constrói a cópia scratch do Track C: PSI zerado
// (unknown), eixos de uso sintéticos. Só chaves PRIMÁRIAS são re-appendadas
// (o fan-out de AppendWithGroup recria as agg); re-appendar as agg
// DUPLICARIA a população. Devolve o histórico, o mapa warm-solo por chave
// e layer por nó.
func rgBuildUsageOverlay(t *testing.T, segs map[string][]history.Entry) (*history.History, map[string][]rgUsageSample, map[string]string) {
	t.Helper()
	h, err := history.NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory overlay uso: %v", err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Fatalf("Close overlay uso: %v", err)
		}
	})
	keys := make([]string, 0, len(segs))
	for k := range segs {
		if !history.IsAggLayerKey(k) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	samples := make(map[string][]rgUsageSample, len(keys))
	layerByNode := map[string]string{}
	var prim int
	for _, k := range keys {
		group, node, bucket, ok := rgSplitKey(k)
		if !ok {
			t.Fatalf("chave primária inesperada %q", k)
		}
		for i, e := range segs[k] {
			x, y := rgSynthUsage(k, i, bucket)
			e.PSICpuSomeAvg10 = 0
			e.PSIMemFullAvg10 = 0
			e.ProjectedLoadPct = x
			e.ObservedCPUPct = x
			e.ObservedMemPct = y
			if e.Layer != "" {
				layerByNode[node] = e.Layer
			}
			if err := h.AppendWithGroup(e, group); err != nil {
				t.Fatalf("AppendWithGroup overlay uso: %v", err)
			}
			prim++
			if e.Outcome == history.OutcomeSucesso && !e.ColdStart && e.Inflight == 1 {
				pk := history.SegmentKey(group, node, history.ScenarioContext{LoadBucket: bucket})
				samples[pk] = append(samples[pk], rgUsageSample{ts: e.Timestamp, x: x, y: y, lat: float64(e.ObservedLatencyMS)})
			}
		}
	}
	// Gate de integridade do overlay: mesma população da fixture.
	var total int
	for _, k := range h.Keys() {
		_, _, _, segTotal, _, _ := h.StatsAndRecent(k, 0, time.Time{})
		total += segTotal
	}
	if len(h.Keys()) != rgFixtureSegs || total != rgFixtureTotal || prim != 280 {
		t.Fatalf("overlay uso: got keys=%d total=%d prim=%d, want %d/%d/280", len(h.Keys()), total, prim, rgFixtureSegs, rgFixtureTotal)
	}
	return h, samples, layerByNode
}
