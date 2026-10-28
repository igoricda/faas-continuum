package history

import (
	"strings"
	"time"
)

// maxExpCostCacheEntries bounds the cache of the expected cost, since a
// collection addressable by external input needs a ceiling. The key is the
// group and the node, and above the ceiling the map is recreated, which
// discards everything and avoids a monotonic growth.
const maxExpCostCacheEntries = 1024

// costCacheEntry is the memoized result of the expected cost for a single key
// of group and node: the mean, the count and the presence computed in the
// generation of the pair, which any append or supersede of that pair
// invalidates, and at the instant of the computation. The seeded flag tells a
// layer prior from a measured local mean, and it is memoized under the same
// guards, so a seed never survives an append of the pair, since the generation
// moves and the next hit recomputes, usually turning local. The oldest field
// is the smallest included timestamp, of a local or seeded entry, the newest
// field is the largest timestamp of any examined entry of the key, including
// the aggregated segments of the layer, and the future field is the smallest
// timestamp among the eligible entries that is after the computation instant.
//
// The exactness of the cache, which is immutable since the generation freezes
// the map, requires four conditions to reuse it at a later instant: no entry
// was in the future relative to that instant, the oldest included entry is
// still inside the window, and the in-window set can only shrink as the age
// grows, while an empty set gains no entry and stays empty. Without the future
// field, an entry appended concurrently after the captured instant would enter
// and the cache would omit it. The fields are ordered for the alignment of
// eight bytes.
type costCacheEntry struct {
	computedAt time.Time
	oldest     time.Time
	newest     time.Time
	futureFrom time.Time
	mean       float64
	gen        uint64
	n          int
	window     time.Duration
	ok         bool
	seeded     bool
}

// ExpectedCost estimates the processor cost, in the percentage normalization
// used by the telemetry, of a single invocation of the group in a node: the
// mean of the observed processor usage over the eligible entries of the
// window, which are the solo entries with positive processor usage, without a
// cold start, with a success and with both pressure axes known, across every
// bucket of the pair of group and node and not only the current bucket. The
// primary key has the form of the group, the node and the bucket, and the
// aggregated keys are ignored in the local leg.
//
// The pressure gate reuses the eligibility predicate of the pressure curve
// instead of reimplementing the filter, so it derives by construction and
// never drifts. Both axes are required because the pressure estimator already
// defines a solo entry as one with pressure on both axes, so the average cost
// and the curve operate over the same solo population, without a drift
// between what feeds the fast projection and the continuous path. The cost is
// a mean of the processor usage per entry, so a gate on either axis would be
// defensible, but the consistency with the estimator prevailed.
//
// The seed of the layer applies when the pair has no eligible sample: it takes
// the arithmetic mean, and not a fit, of the processor usage over the eligible
// entries of the aggregated segments of the group, in the same reading pattern
// of the pressure curve. A mean and not a fit because the expected cost is an
// estimator of a mean, while the pressure curve predicts a latency, which is
// the wrong model for a cost. The scope of the layer is the layer of the most
// recent in-window entry of the pair with a layer filled, when it is known,
// and otherwise the whole group, since the expected cost does not receive the
// layer in the signature and the processor cost varies less by layer than the
// latency, which makes the group-wide prior a safe fallback. Without layer
// data the result is absent.
//
// The solo-only restriction is an invariant of the estimator: contended
// samples already embed the dispute and would make the cost circular, since
// the cost would feed the projection that defines the bucket itself, and a
// cold start is not a steady-state cost, so neither of them ever enters the
// mean, in the local leg or in the seed leg. The result is absent when there
// is no sample in either leg or the window is non-positive, and a nil receiver
// gives an absent result, which is safe for the selection tests that pass no
// history.
//
// The staleness of the seed is the same class of trade-off as the pressure
// curve: the prior pools entries of sibling nodes, but the invalidation is
// only by the generation of the queried pair, so appends of siblings that move
// the prior do not invalidate this pair, which refreshes on its next own
// append or on the expiration of the guards. The callers query by candidate
// node whose own ingestion appends to the pair, so the warm candidates refresh
// continuously.
//
// The cache is keyed by the pair of group and node, which is the fallback of
// performance: under a concurrent burst several selections happen before the
// next append completes, and each selection calls the expected cost per
// candidate, so without the cache each call would scan the whole map. The
// cache is invalidated by the generation of the pair and reused only while it
// is exact, which means that no entry in the future of the computation instant
// entered, that the oldest included entry is still inside the window and that
// the instant did not go back, while a negative result stays exact until a
// mutation. With stable data and a stopped instant the cost is constant.
func (h *History) ExpectedCost(group, nodeID string, window time.Duration, now time.Time) (cpuPct float64, n int, ok bool, seeded bool) {
	if h == nil || window <= 0 {
		return 0, 0, false, false
	}
	cacheKey := group + "\x00" + nodeID

	h.mu.RLock()
	gen := h.costGen[cacheKey]
	if e, hit := h.costCache[cacheKey]; hit && e.gen == gen && e.window == window && !now.Before(e.computedAt) &&
		!e.newest.After(now) &&
		(e.futureFrom.IsZero() || now.Before(e.futureFrom)) &&
		(!e.ok || !now.After(e.oldest.Add(window))) {
		h.mu.RUnlock()
		return e.mean, e.n, e.ok, e.seeded
	}
	if len(h.m) == 0 {
		h.mu.RUnlock()
		return 0, 0, false, false
	}
	sum := 0.0
	count := 0
	var oldest, newest, futureFrom time.Time
	haveOldest, haveNewest, haveFuture := false, false, false
	// The layer of the pair probes the scope of the seed leg: the layer of the
	// most recent in-window entry of the pair with a layer filled, of any class,
	// since contended and cold entries carry the layer and serve as a probe even
	// when excluded from the mean. An empty value gives a group-wide seed.
	var pairLayer string
	var pairLayerAt time.Time
	havePairLayer := false
	for k, entries := range h.m {
		if IsAggLayerKey(k) {
			continue
		}
		g, rest, found := strings.Cut(k, "\x00")
		if !found || g != group {
			continue
		}
		thisNode, _, found := strings.Cut(rest, "\x00")
		if !found || thisNode != nodeID {
			continue
		}
		for _, e := range entries {
			if !haveNewest || e.Timestamp.After(newest) {
				newest = e.Timestamp
				haveNewest = true
			}
			eligible := psiSoloEligible(e)
			if !windowFilter(e.Timestamp, now, window) {
				if e.Timestamp.After(now) && eligible && (!haveFuture || e.Timestamp.Before(futureFrom)) {
					futureFrom = e.Timestamp
					haveFuture = true
				}
				continue
			}
			if e.Layer != "" && (!havePairLayer || e.Timestamp.After(pairLayerAt)) {
				pairLayer, pairLayerAt, havePairLayer = e.Layer, e.Timestamp, true
			}
			if !eligible {
				continue
			}
			sum += e.CPUPctAvg
			count++
			if !haveOldest || e.Timestamp.Before(oldest) {
				oldest = e.Timestamp
				haveOldest = true
			}
		}
	}
	// The seed leg only runs when the pair has no eligible sample. It walks the
	// aggregated segments of the group and the layer under the same solo and
	// pressure gate and the same window, so contended, cold and unknown pressure
	// never enter.
	seedSum := 0.0
	seedCount := 0
	if count == 0 {
		for k, entries := range h.m {
			if !IsAggLayerKey(k) {
				continue
			}
			rest := strings.TrimPrefix(k, aggLayerPrefix)
			g, afterGroup, found := strings.Cut(rest, "\x00")
			if !found || g != group {
				continue
			}
			thisLayer, _, found := strings.Cut(afterGroup, "\x00")
			if !found {
				continue
			}
			if havePairLayer && thisLayer != pairLayer {
				continue
			}
			for _, e := range entries {
				if !haveNewest || e.Timestamp.After(newest) {
					newest = e.Timestamp
					haveNewest = true
				}
				if !windowFilter(e.Timestamp, now, window) {
					if e.Timestamp.After(now) && psiSoloEligible(e) && (!haveFuture || e.Timestamp.Before(futureFrom)) {
						futureFrom = e.Timestamp
						haveFuture = true
					}
					continue
				}
				if !psiSoloEligible(e) {
					continue
				}
				seedSum += e.CPUPctAvg
				seedCount++
				if !haveOldest || e.Timestamp.Before(oldest) {
					oldest = e.Timestamp
					haveOldest = true
				}
			}
		}
	}
	h.mu.RUnlock()

	// The cache is published only when no mutation of the pair happened during
	// the computation, which means the generation of the pair is still the same,
	// and otherwise the value is returned without memoization, which is correct
	// but without a hit on the next call.
	mean, outN, outOK, outSeeded := 0.0, 0, false, false
	switch {
	case count > 0:
		mean, outN, outOK = sum/float64(count), count, true
	case seedCount > 0:
		mean, outN, outOK, outSeeded = seedSum/float64(seedCount), seedCount, true, true
	}
	h.mu.Lock()
	if h.costGen[cacheKey] == gen {
		if len(h.costCache) >= maxExpCostCacheEntries {
			h.costCache = make(map[string]costCacheEntry)
		}
		h.costCache[cacheKey] = costCacheEntry{gen: gen, computedAt: now, oldest: oldest, newest: newest, futureFrom: futureFrom, mean: mean, n: outN, window: window, ok: outOK, seeded: outSeeded}
	}
	h.mu.Unlock()

	return mean, outN, outOK, outSeeded
}
