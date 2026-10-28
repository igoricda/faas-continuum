package history

import (
	"log"
	"strings"
)

// costPairFromKey extracts the group and the node from a primary key by
// substring, without allocating. Aggregated and malformed keys give an empty
// result.
func costPairFromKey(key string) string {
	if IsAggLayerKey(key) {
		return ""
	}
	i := strings.IndexByte(key, 0)
	if i < 0 {
		return ""
	}
	j := strings.IndexByte(key[i+1:], 0)
	if j < 0 {
		return ""
	}
	return key[:i+1+j]
}

// appendLocked appends the entry to the segment of the key with a FIFO
// capacity. A non-empty pair increments the generation counter of the pair
// that invalidates the cache of the expected cost, while the aggregated
// fan-out passes an empty pair, since the expected cost ignores aggregated
// keys. The caller holds the write lock.
func (h *History) appendLocked(key, pair string, e Entry) {
	if pair != "" {
		h.bumpCostGenLocked(pair)
	}
	entries := h.m[key]
	entries = append(entries, e)
	if len(entries) > h.capPerSegment {
		entries = entries[1:]
	}
	h.m[key] = entries
	h.dirtyFlag = true
}

// bumpCostGenLocked increments the generation counter of the pair and, when
// it exceeds the ceiling with a new pair, recreates the four maps of the
// counters and of the caches. The joint reset avoids the ABA problem, where
// an old entry of any cache would validate against a freshly reset counter.
// The caller holds the lock.
func (h *History) bumpCostGenLocked(pair string) {
	if _, exists := h.costGen[pair]; !exists && len(h.costGen) >= maxExpCostCacheEntries {
		h.costGen = make(map[string]uint64)
		h.costCache = make(map[string]costCacheEntry)
		h.psiCache = make(map[string]psiCacheEntry)
		h.usageCache = make(map[string]usageCacheEntry)
	}
	h.costGen[pair]++
}

// AppendWithGroup adds the entry to the primary key of the group, the node
// and the scenario and, when the layer is not empty, to the aggregated key
// of the fan-out with the same struct reused in both segments, as a shallow
// copy that does not duplicate the payload. A saved file always carries the
// version marker, and a file without it does not load. The mechanics of the
// persistence are unchanged, with a coalesced dirty signal.
func (h *History) AppendWithGroup(e Entry, group string) error {
	primary := SegmentKey(group, e.NodeID, e.ScenarioContext)
	agg := ""
	if e.Layer != "" {
		agg = AggLayerKey(group, e.Layer, e.ScenarioContext)
	}

	h.mu.Lock()
	h.appendLocked(primary, costPairFromKey(primary), e)
	if agg != "" {
		h.appendLocked(agg, "", e)
	}
	obs := h.observer
	h.mu.Unlock()

	if obs != nil {
		// The hook of the second stage is called once per append, and the
		// aggregated fan-out does not trigger it again, so there is no
		// double counting. It runs outside the lock, with no input or
		// output under the lock.
		obs(group, e)
	}

	if h.strict {
		if err := h.FlushSync(); err != nil {
			return err
		}
	} else {
		h.signalDirty()
	}
	return nil
}

// Supersede removes from the segment of the key the first entry that
// satisfies the predicate: when the late report of the same request and
// attempt arrives, the synchronous entry, with the latency truncated by the
// budget, leaves the history and the late entry, with the real latency,
// takes its place, so the count, the recent rate and the percentile do not
// count twice. It is a physical removal instead of a flag, which keeps the
// schema of the entry frozen. A key that becomes empty disappears from the
// map. The mechanics of the persistence are the same as the append, without
// input or output under the lock, only the non-blocking dirty signal, and a
// synchronous flush after the unlock in the strict mode. It returns whether
// it removed any entry.
func (h *History) Supersede(key string, pred func(Entry) bool) bool {
	if pred == nil {
		return false
	}
	h.mu.Lock()
	entries, ok := h.m[key]
	if !ok || len(entries) == 0 {
		h.mu.Unlock()
		return false
	}
	idx := -1
	for i := range entries {
		if pred(entries[i]) {
			idx = i
			break
		}
	}
	if idx < 0 {
		h.mu.Unlock()
		return false
	}
	entries = append(entries[:idx], entries[idx+1:]...)
	if pair := costPairFromKey(key); pair != "" {
		h.bumpCostGenLocked(pair)
	}
	if len(entries) == 0 {
		delete(h.m, key)
	} else {
		h.m[key] = entries
	}
	h.dirtyFlag = true
	h.mu.Unlock()

	if h.strict {
		if err := h.FlushSync(); err != nil {
			log.Printf("arbitro: history supersede flush falhou: %v", err)
		}
	} else {
		h.signalDirty()
	}
	return true
}
