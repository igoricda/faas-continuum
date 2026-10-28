// Pooled PSI pressure-curve estimator with bounded gen-guarded cache
// (selflearning-telemetry-seeded-history, todo 9).
//
// This file is the STATEFUL shell around the pure estimator in psi_curve.go
// (FitPSICurve, PosteriorPSICurve, ShrinkToPrior, FitPSICurveFromEntries,
// PSIMinClassSamples — REUSED here, never reimplemented): it fits a layer
// prior over solo-only PSI>0 samples in-window, shrinks the per-node local
// fit to that prior, and serves the posterior through a bounded cache.
//
// Cache discipline mirrors ExpectedCost EXACTLY (history_stats.go,
// costCacheEntry + costGen in history_store.go) — one pattern, not two:
//   - same mutex (h.mu), same generation map (h.costGen, key group\x00node),
//     same cap (maxExpCostCacheEntries=1024, recreate-on-overflow),
//     same invalidation trigger points (appendLocked/Supersede bump the
//     pair gen; agg fan-out appends pass pair=="" and never bump, exactly
//     as ExpectedCost ignores agg keys).
//   - cache key extends the pair with the layer suffix
//     (group\x00node\x00layer); invalidation stays per-pair via the stored
//     gen — any append to the pair bumps costGen[pair] and all layer
//     variants of that pair miss on the next query.
//   - same time-exactness guards (computedAt/newest/futureFrom/oldest +
//     window); negative (ok=false) stays exact until pair mutation.
//   - miss path: windowed scan under RLock (same shape as ExpectedCost),
//     pure fit OFF-lock, publish under Lock only if the pair gen did not
//     move — no I/O under lock anywhere.
//
// Prior staleness note: the layer prior pools entries from every node in
// (group,layer), but invalidation fires only on appends to the QUERIED
// pair — appends by sibling nodes that move the prior do NOT invalidate
// this pair's entry (same trade-off class as a pooled read under a
// per-pair gen; the next append to this pair, or window/time-guard expiry,
// refreshes). Todos 10/11/13 callers query per candidate node, and every
// candidate's own ingest path appends to its pair, so hot candidates
// refresh continuously.
//
// PSI sourcing: exclusively through psiOfEntry (the one-liner seam todo 2
// anticipated: e.PSICpuSomeAvg10 / e.PSIMemFullAvg10, 0 = unknown). The
// scan pre-filter (psiSoloEligible) matches PSIPoint.eligible() EXACTLY —
// solo-only gate plus PSI>0 on both axes — so it never pre-filters
// differently from the gate inside FitPSICurve (which re-gates
// defensively); PSI==0 never pulls the fit on either path.
package history

import (
	"strings"
	"time"
)

// psiOfEntry is the todo-2 accessor seam wired to the real Entry PSI
// fields (todo 5): cpu_some.avg10 and mem_full.avg10, 0 = unknown.
func psiOfEntry(e Entry) (cpuSome, memFull float64) {
	return e.PSICpuSomeAvg10, e.PSIMemFullAvg10
}

// psiSoloEligible mirrors PSIPoint.eligible() for Entry: the ExpectedCost
// solo-only invariant (Inflight==1 && CPUPctAvg>0 && !ColdStart &&
// Outcome==sucesso) PLUS PSI known on both axes. Any drift between this
// and eligible() changes what the scan collects versus what the fit
// accepts — keep them in lockstep.
func psiSoloEligible(e Entry) bool {
	return e.Inflight == 1 &&
		e.CPUPctAvg > 0 &&
		!e.ColdStart &&
		e.Outcome == OutcomeSucesso &&
		e.PSICpuSomeAvg10 > 0 &&
		e.PSIMemFullAvg10 > 0
}

// psiPointOfEntry maps an Entry to a PSIPoint through the psiOfEntry seam
// (same mapping body as FitPSICurveFromEntries — the fit re-applies the
// gate, so pre-filtered points pass through unchanged).
func psiPointOfEntry(e Entry) PSIPoint {
	cpu, mem := psiOfEntry(e)
	return PSIPoint{
		LatencyMS: float64(e.ObservedLatencyMS),
		CPUSome:   cpu,
		MemFull:   mem,
		CPUPctAvg: e.CPUPctAvg,
		Inflight:  e.Inflight,
		Outcome:   e.Outcome,
		ColdStart: e.ColdStart,
	}
}

// psiCacheEntry is the memoized PSICurveFor result for ONE
// (group,node,layer) key, shaped exactly like costCacheEntry: the fitted
// posterior `curve`/`ok` computed at `gen` (costGen of the group\x00node
// pair) and instant `computedAt`. `oldest` = smallest Timestamp INCLUDED
// from the union of prior+local scans; `newest` = largest Timestamp of ANY
// entry examined in either scan; `futureFrom` = smallest Timestamp among
// solo+PSI-eligible entries with ts > computedAt (future-relative).
// Reuse rules are the ExpectedCost ones verbatim (see costCacheEntry).
// aligned to eight bytes.
type psiCacheEntry struct {
	computedAt time.Time
	oldest     time.Time
	newest     time.Time
	futureFrom time.Time
	curve      PSICurve
	gen        uint64
	window     time.Duration
	ok         bool
}

// PSICurveFor fits the pooled PSI pressure curve for (group,node): the layer
// prior over solo-only PSI>0 samples in-window across ALL buckets of
// (group,layer) — read through the AggLayerKey fan-out segments — plus
// the node-local posterior over (group,node) shrunk to that prior
// (PosteriorPSICurve: thin nodes stay glued to the prior until
// SoloN>=PSIMinClassSamples).
//
// ok=false with a zero curve when NO eligible PSI data exists anywhere
// (neither prior nor local) — never silent zeros. window<=0 or nil
// receiver => (zero, false), same fail-closed shape as ExpectedCost.
//
// Served through the bounded gen-guarded psiCache (see header): stable
// data with frozen now is O(1); the miss path is one windowed scan in the
// ExpectedCost shape. Stable signature — the seam todos 10/11/13 call.
func (h *History) PSICurveFor(group, nodeID, layer string, window time.Duration, now time.Time) (PSICurve, bool) {
	if h == nil || window <= 0 {
		return PSICurve{}, false
	}
	pairKey := group + "\x00" + nodeID
	cacheKey := pairKey + "\x00" + layer

	h.mu.RLock()
	gen := h.costGen[pairKey]
	if e, hit := h.psiCache[cacheKey]; hit && e.gen == gen && e.window == window && !now.Before(e.computedAt) &&
		!e.newest.After(now) &&
		(e.futureFrom.IsZero() || now.Before(e.futureFrom)) &&
		(!e.ok || !now.After(e.oldest.Add(window))) {
		h.mu.RUnlock()
		return e.curve, e.ok
	}
	if len(h.m) == 0 {
		h.mu.RUnlock()
		return PSICurve{}, false
	}
	var priorEntries []Entry
	var localPoints []PSIPoint
	var oldest, newest, futureFrom time.Time
	haveOldest, haveNewest, haveFuture := false, false, false
	for k, entries := range h.m {
		// Route the key to at most one scan: agg keys feed the layer
		// prior (group+layer match, any bucket), primary keys feed the
		// node-local fit (group+node match, any bucket) — ExpectedCost
		// ignores agg keys; here they ARE the prior. Parse shapes mirror
		// KeysForLayer (agg) and ExpectedCost (primary).
		wantPrior := false
		if IsAggLayerKey(k) {
			rest := strings.TrimPrefix(k, aggLayerPrefix)
			g, afterGroup, found := strings.Cut(rest, "\x00")
			if !found || g != group {
				continue
			}
			thisLayer, _, found := strings.Cut(afterGroup, "\x00")
			if !found || thisLayer != layer {
				continue
			}
			wantPrior = true
		} else {
			g, rest, found := strings.Cut(k, "\x00")
			if !found || g != group {
				continue
			}
			thisNode, _, found := strings.Cut(rest, "\x00")
			if !found || thisNode != nodeID {
				continue
			}
		}
		for _, e := range entries {
			if !haveNewest || e.Timestamp.After(newest) {
				newest = e.Timestamp
				haveNewest = true
			}
			if e.Timestamp.After(now) {
				if psiSoloEligible(e) && (!haveFuture || e.Timestamp.Before(futureFrom)) {
					futureFrom = e.Timestamp
					haveFuture = true
				}
				continue
			}
			if now.Sub(e.Timestamp) > window {
				continue
			}
			if !psiSoloEligible(e) {
				continue
			}
			if !haveOldest || e.Timestamp.Before(oldest) {
				oldest = e.Timestamp
				haveOldest = true
			}
			if wantPrior {
				priorEntries = append(priorEntries, e)
			} else {
				localPoints = append(localPoints, psiPointOfEntry(e))
			}
		}
	}
	h.mu.RUnlock()

	// Pure fit off-lock (no I/O, no shared state): prior through the
	// FitPSICurveFromEntries accessor seam, posterior with shrinkage.
	prior, priorOK := FitPSICurveFromEntries(priorEntries, psiOfEntry)
	curve, ok := PosteriorPSICurve(prior, priorOK, localPoints)

	// Publish only if no mutation of the PAIR happened mid-fit (same
	// gen-guard as ExpectedCost); otherwise return unmemoized — correct,
	// just no hit next time.
	h.mu.Lock()
	if h.costGen[pairKey] == gen {
		if h.psiCache == nil {
			h.psiCache = make(map[string]psiCacheEntry)
		} else if len(h.psiCache) >= maxExpCostCacheEntries {
			h.psiCache = make(map[string]psiCacheEntry)
		}
		h.psiCache[cacheKey] = psiCacheEntry{gen: gen, computedAt: now, oldest: oldest, newest: newest, futureFrom: futureFrom, curve: curve, window: window, ok: ok}
	}
	h.mu.Unlock()

	return curve, ok
}
