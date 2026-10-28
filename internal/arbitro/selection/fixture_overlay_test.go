package selection

// Helpers of the calibration harness: a pinned fixture plus a deterministic
// overlay of synthetic usage, which covers a device without pressure data,
// where the pressure is forced to unknown. The fixture and the overlay live
// here so that the calibration of the predictive band of the Bayesian
// regression on the usage curve stays reproducible.
//
// The fixture is loaded through the production types, without a parallel schema,
// and only the primary keys are appended again, since the fan-out of the grouped
// append recreates the aggregated keys, and appending them again would duplicate
// the population.

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

// rgUsageSrc labels the overlay on every segment line.
const rgUsageSrc = "usage-overlay(hash,fnv-key-idx,psi=0)"

// rgUsageSample is a warm solo point of the overlay, with its synthetic usage
// and the real latency of the fixture.
type rgUsageSample struct {
	ts  time.Time
	x   float64
	y   float64
	lat float64
}

// rgRaw loads the fixture through the production types, without a parallel schema.
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

// rgSynthUsage derives a deterministic synthetic usage per key and index, with a
// base per load bucket, which preserves the rank of the load, plus a jitter of
// one and a half percent from a hash. The value is always positive and never
// unknown, and there is no random generator, no state and no dependence on the
// order.
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

// rgBuildUsageOverlay builds the scratch copy of the synthetic usage track, with
// the pressure zeroed, which means unknown, and synthetic usage axes. Only the
// primary keys are appended again, since the fan-out of the grouped append
// recreates the aggregated keys, and appending them again would duplicate the
// population. It returns the history, the map of warm solo samples per key and
// the layer per node.
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
	// Integrity gate of the overlay: the same population as the fixture.
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
