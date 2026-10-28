// Pooled no-PSI usage-curve estimator with bounded gen-guarded cache
// (selflearning-telemetry-seeded-history, no-PSI usage-curve fallback).
//
// This file is the STATEFUL shell around the pure estimator in
// usage_curve.go (FitUsageCurve, PosteriorUsageCurve, ShrinkUsageToPrior,
// FitUsageCurveFromEntries, UsageMinClassSamples — REUSED here, never
// reimplemented): it fits a layer prior over solo-only usage-known samples
// in-window, shrinks the per-node local fit to that prior, and serves the
// posterior through a bounded cache.
//
// Cache discipline mirrors ExpectedCost EXACTLY (history_stats.go,
// costCacheEntry + costGen in history_store.go) — one pattern, not three:
//   - same mutex (h.mu), same generation map (h.costGen, key grupo\x00nó),
//     same cap (maxExpCostCacheEntries=1024, recreate-on-overflow),
//     same invalidation trigger points (appendLocked/Supersede bump the
//     pair gen; agg fan-out appends pass pair=="" and never bump, exactly
//     as ExpectedCost ignores agg keys).
//   - SEPARATE map (h.usageCache, key grupo\x00nó\x00camada): the usage
//     curve never shares entries with the PSI cache, so a PSI hit can never
//     serve usage data and vice versa. Invalidation stays per-pair via the
//     stored gen — any append to the pair bumps costGen[pair] and all layer
//     variants of that pair miss on the next query. The ABA reset in
//     bumpCostGenLocked recreates all three maps jointly.
//   - same time-exactness guards (computedAt/newest/futureFrom/oldest +
//     window); negative (ok=false) stays exact until pair mutation.
//   - miss path: windowed scan under RLock (same shape as ExpectedCost),
//     pure fit OFF-lock, publish under Lock only if the pair gen did not
//     move — no I/O under lock anywhere.
//
// Prior staleness note: the layer prior pools entries from every node in
// (grupo,camada), but invalidation fires only on appends to the QUERIED
// pair — appends by sibling nodes that move the prior do NOT invalidate
// this pair's entry (same trade-off class as a pooled read under a
// per-pair gen; the next append to this pair, or window/time-guard expiry,
// refreshes). Callers query per candidate node, and every candidate's own
// ingest path appends to its pair, so hot candidates refresh continuously.
//
// Usage sourcing: exclusively through usageOf/usageXOf (usage_curve.go):
// X = ProjectedLoadPct when >0 else ObservedCPUPct, Y = ObservedMemPct,
// 0 = unknown. The scan pre-filter (usageSoloEligible) matches
// UsagePoint.eligible() EXACTLY — solo-only gate plus usage known on
// either axis — so it never pre-filters differently from the gate inside
// FitUsageCurve (which re-gates defensively); X==0 && Y==0 never pulls the
// fit on either path.
package history

import (
	"strings"
	"time"
)

// usageSoloEligible mirrors UsagePoint.eligible() for Entry: the
// ExpectedCost solo-only invariant (Inflight==1 && CPUPctAvg>0 &&
// !ColdStart && Outcome==sucesso) PLUS usage known on either axis
// (usageXOf>0 || ObservedMemPct>0). Any drift between this and eligible()
// changes what the scan collects versus what the fit accepts — keep them in
// lockstep.
func usageSoloEligible(e Entry) bool {
	return e.Inflight == 1 &&
		e.CPUPctAvg > 0 &&
		!e.ColdStart &&
		e.Outcome == OutcomeSucesso &&
		(usageXOf(e) > 0 || e.ObservedMemPct > 0)
}

// usagePointOfEntry maps an Entry to a UsagePoint through the usageOf seam
// (same mapping body as FitUsageCurveFromEntries — the fit re-applies the
// gate, so pre-filtered points pass through unchanged).
func usagePointOfEntry(e Entry) UsagePoint {
	x, y := usageOf(e)
	return UsagePoint{
		LatencyMS: float64(e.ObservedLatencyMS),
		X:         x,
		Y:         y,
		CPUPctAvg: e.CPUPctAvg,
		Inflight:  e.Inflight,
		Outcome:   e.Outcome,
		ColdStart: e.ColdStart,
	}
}

// usageCacheEntry is the memoized UsageCurveFor result for ONE
// (grupo,nó,camada) key, shaped exactly like psiCacheEntry/costCacheEntry:
// the fitted posterior `curve`/`ok` computed at `gen` (costGen of the
// grupo\x00nó pair) and instant `computedAt`. `oldest` = smallest Timestamp
// INCLUDED from the union of prior+local scans; `newest` = largest
// Timestamp of ANY entry examined in either scan; `futureFrom` = smallest
// Timestamp among solo+usage-eligible entries with ts > computedAt
// (future-relative). Reuse rules are the ExpectedCost ones verbatim (see
// costCacheEntry). ordem 8B.
type usageCacheEntry struct {
	computedAt time.Time
	oldest     time.Time
	newest     time.Time
	futureFrom time.Time
	curve      UsageCurve
	gen        uint64
	window     time.Duration
	ok         bool
}

// UsageCurveFor fits the pooled no-PSI usage curve for (grupo,nó): the
// layer prior over solo-only usage-known samples in-window across ALL
// buckets of (grupo,camada) — read through the AggLayerKey fan-out
// segments — plus the node-local posterior over (grupo,nó) shrunk to that
// prior (PosteriorUsageCurve: thin nodes stay glued to the prior until
// SoloN>=UsageMinClassSamples).
//
// Training features per entry: X = ProjectedLoadPct when >0 else
// ObservedCPUPct, Y = ObservedMemPct (see usageXOf/usageOf).
//
// ok=false with a zero curve when NO eligible usage data exists anywhere
// (neither prior nor local) — never silent zeros. window<=0 or nil
// receiver => (zero, false), same fail-closed shape as ExpectedCost.
//
// Served through the bounded gen-guarded usageCache (see header): stable
// data with frozen now is O(1); the miss path is one windowed scan in the
// ExpectedCost shape. Stable signature.
func (h *History) UsageCurveFor(group, nodeID, layer string, window time.Duration, now time.Time) (UsageCurve, bool) {
	if h == nil || window <= 0 {
		return UsageCurve{}, false
	}
	pairKey := group + "\x00" + nodeID
	cacheKey := pairKey + "\x00" + layer

	h.mu.RLock()
	gen := h.costGen[pairKey]
	if e, hit := h.usageCache[cacheKey]; hit && e.gen == gen && e.window == window && !now.Before(e.computedAt) &&
		!e.newest.After(now) &&
		(e.futureFrom.IsZero() || now.Before(e.futureFrom)) &&
		(!e.ok || !now.After(e.oldest.Add(window))) {
		h.mu.RUnlock()
		return e.curve, e.ok
	}
	if len(h.m) == 0 {
		h.mu.RUnlock()
		return UsageCurve{}, false
	}
	var priorEntries []Entry
	var localPoints []UsagePoint
	var localEntries []Entry
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
				if usageSoloEligible(e) && (!haveFuture || e.Timestamp.Before(futureFrom)) {
					futureFrom = e.Timestamp
					haveFuture = true
				}
				continue
			}
			if now.Sub(e.Timestamp) > window {
				continue
			}
			if !usageSoloEligible(e) {
				continue
			}
			if !haveOldest || e.Timestamp.Before(oldest) {
				oldest = e.Timestamp
				haveOldest = true
			}
			if wantPrior {
				priorEntries = append(priorEntries, e)
			} else {
				localPoints = append(localPoints, usagePointOfEntry(e))
				localEntries = append(localEntries, e)
			}
		}
	}
	h.mu.RUnlock()

	// Pure fit off-lock (no I/O, no shared state): prior through the
	// FitUsageCurveFromEntries seam, posterior with shrinkage, Tier1
	// conformal correction pooled per (group,layer) across nodes/buckets
	// (miss path only — hot hits never pay; below the calibration minimum
	// the pure parametric Tier0 band rules).
	prior, priorOK := FitUsageCurveFromEntries(priorEntries)
	curve, ok := PosteriorUsageCurve(prior, priorOK, localPoints)
	if ok {
		if q, hasQ := blrUsageCalQ(priorEntries, localEntries, nodeID, window, now, blrCalMinSamples); hasQ {
			curve.blrQ, curve.blrHasQ = q, true
		}
	}

	// Publish only if no mutation of the PAIR happened mid-fit (same
	// gen-guard as ExpectedCost); otherwise return unmemoized — correct,
	// just no hit next time.
	h.mu.Lock()
	if h.costGen[pairKey] == gen {
		if h.usageCache == nil {
			h.usageCache = make(map[string]usageCacheEntry)
		} else if len(h.usageCache) >= maxExpCostCacheEntries {
			h.usageCache = make(map[string]usageCacheEntry)
		}
		h.usageCache[cacheKey] = usageCacheEntry{gen: gen, computedAt: now, oldest: oldest, newest: newest, futureFrom: futureFrom, curve: curve, window: window, ok: ok}
	}
	h.mu.Unlock()

	return curve, ok
}
