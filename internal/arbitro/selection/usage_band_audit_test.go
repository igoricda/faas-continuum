package selection

// Report-only calibration audit of the parametric band and of its conformal
// correction: on the synthetic overlay, with real latencies and outcomes,
// synthetic usage axes and the pressure forced to unknown, it logs the coverage
// per bucket, the mean relative width and the mean interval score, the
// availability of the conformal correction and the counts of ready and vetoed
// nodes with and without the correction at an objective of eight seconds. It is
// harness only, with no change to the production code. The assertions are limited
// to finite, non-NaN and non-negative values, and there are no gates on the
// values, since the coverage alone is not a proper score and the triple is for
// the audit: the split per bucket stays observational and never retargets the
// band.

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
)

type bandAuditRow struct {
	bucket  string
	covered bool
	width   float64
	is      float64
	pred    float64
	upper   float64
	param   float64
	hasQ    bool
}

func TestUsageBandCalibrationAudit(t *testing.T) {
	segs := rgRaw(t)
	now := rgMaxTS(segs)
	if now.IsZero() {
		t.Fatal("max Timestamp zero")
	}
	hC, samples, layerByNode := rgBuildUsageOverlay(t, segs)
	slo := &SLO{MaxLatencyMS: 8000}

	for _, w := range []time.Duration{rgTailWindow, rgFullWindow} {
		var rows []bandAuditRow
		for _, k := range hC.Keys() {
			if history.IsAggLayerKey(k) {
				continue
			}
			group, node, bucket, ok := rgSplitKey(k)
			if !ok {
				continue
			}
			full, exists := hC.StatsFull(k, w, now)
			if !exists || full.SoloP95MS <= 0 {
				continue
			}
			var xs, ys []float64
			for _, s := range samples[k] {
				if s.ts.After(now) || now.Sub(s.ts) > w {
					continue
				}
				xs = append(xs, s.x)
				ys = append(ys, s.y)
			}
			medX, ok1 := rgMedian(xs)
			medY, ok2 := rgMedian(ys)
			if !ok1 || !ok2 {
				continue
			}
			curve, cok := hC.UsageCurveFor(group, node, layerByNode[node], w, now)
			if !cok {
				continue
			}
			pred, upper, param := curve.Predict(medX, medY), curve.Upper90(medX, medY), curve.Upper90Parametric(medX, medY)
			y := float64(full.SoloP95MS)
			if math.IsNaN(pred) || math.IsNaN(upper) || math.IsNaN(param) ||
				math.IsInf(pred, 0) || math.IsInf(upper, 0) || math.IsInf(param, 0) {
				t.Fatalf("non-finite band em %q (pred=%.3f upper=%.3f param=%.3f)", k, pred, upper, param)
			}
			width := upper - pred
			if width < 0 {
				t.Fatalf("negative width em %q (upper=%.3f pred=%.3f)", k, upper, pred)
			}
			if param < pred {
				t.Fatalf("parametric band below point em %q", k)
			}
			is := width + 20*math.Max(0, y-upper) + 20*math.Max(0, pred-y)
			if math.IsNaN(is) || math.IsInf(is, 0) || is < 0 {
				t.Fatalf("invalid Winkler IS em %q: %.6f", k, is)
			}
			_, hasQ := curve.ConformalQ()
			rows = append(rows, bandAuditRow{
				bucket: bucket, covered: y <= upper, width: width / pred,
				is: is, pred: pred, upper: upper, param: param, hasQ: hasQ,
			})
		}
		if len(rows) == 0 {
			t.Fatalf("janela %v: nenhuma linha de auditoria", w)
		}
		// Per-bucket triple + decision deltas with vs without q-hat.
		byBucket := map[string][]bandAuditRow{}
		for _, r := range rows {
			byBucket[r.bucket] = append(byBucket[r.bucket], r)
		}
		buckets := make([]string, 0, len(byBucket))
		for b := range byBucket {
			buckets = append(buckets, b)
		}
		slices.Sort(buckets)
		var covN, covD, vetoFull, vetoParam, withQ int
		var sumW, sumIS float64
		t.Logf("bucket segs cover meanWidthRel meanIS (janela %v, overlay uso, y=SoloP95)", w)
		for _, b := range buckets {
			rs := byBucket[b]
			var c, q int
			var sw, si float64
			for _, r := range rs {
				if r.covered {
					c++
				}
				if r.hasQ {
					q++
				}
				sw += r.width
				si += r.is
				if r.upper > float64(slo.MaxLatencyMS) {
					vetoFull++
				}
				if r.param > float64(slo.MaxLatencyMS) {
					vetoParam++
				}
			}
			covN += c
			covD += len(rs)
			withQ += q
			sumW += sw
			sumIS += si
			t.Logf("%-7s %4d %.3f %.4f %.1f", b, len(rs), float64(c)/float64(len(rs)), sw/float64(len(rs)), si/float64(len(rs)))
		}
		t.Logf("AUDIT janela=%v segs=%d coverage=%.3f meanWidthRel=%.4f meanIS=%.1f qhat=%d/%d veto@slo8000 full=%d param=%d delta=%+d",
			w, len(rows), float64(covN)/float64(covD), sumW/float64(len(rows)), sumIS/float64(len(rows)),
			withQ, len(rows), vetoFull, vetoParam, vetoFull-vetoParam)
		// Without a guard on the buckets: an empty bucket list is equivalent to an
		// empty row list, and an empty row list already aborts with a fatal error
		// above, so the previous assertion was unreachable.
	}
}
