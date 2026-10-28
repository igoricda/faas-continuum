package history

import "time"

// trimEvery is the interval of the periodic pruning of retention in the loop
// of persistence. Retention is a long-cycle policy, with a typical window of
// one hour, so a granularity of one minute is enough and keeps the scan
// cheap, with a single pass over the total of entries per minute that
// allocates nothing when no entry expires. It is a variable and not a
// constant only for the test of the loop with a short ticker.
var trimEvery = time.Minute

// SetRetention defines the window of retention by age, where a non-positive
// value disables it, and applies an immediate pruning, which on boot cuts a
// loaded file before the periodic pruning runs. It returns the counts of the
// immediate pass, the removed entries and the segments that became empty,
// for the log and the observability.
func (h *History) SetRetention(retention time.Duration) (removed, segments int) {
	if retention < 0 {
		retention = 0
	}
	h.mu.Lock()
	h.retention = retention
	removed, segments = h.trimLocked(time.Now())
	h.mu.Unlock()
	if removed > 0 {
		h.signalDirty()
	}
	return removed, segments
}

// Retention returns the current window, where zero means no cut.
func (h *History) Retention() time.Duration {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.retention
}

// compact applies the pruning by age to the in-memory map and, when
// something left, invalidates the derived caches of cost, pressure and usage
// and signals a pending snapshot. It is called by the loop and by the
// definition of the retention, and it returns the removed entries and the
// emptied segments.
func (h *History) compact(now time.Time) (removed, segments int) {
	h.mu.Lock()
	removed, segments = h.trimLocked(now)
	h.mu.Unlock()
	if removed > 0 {
		h.signalDirty()
	}
	return removed, segments
}

// trimLocked removes the entries older than the window from every segment,
// primary and aggregated, deletes the segments that became empty and
// invalidates the derived caches. With a non-positive retention it does
// nothing, and the map is not even scanned. The caller holds the write lock.
//
// The caches are invalidated because a memoized curve or expected cost may
// have been fitted with entries that just left, and clearing forces the next
// computation, which is cheap and rare, since the pruning runs once per
// minute and only when an entry expires.
func (h *History) trimLocked(now time.Time) (removed, segments int) {
	if h.retention <= 0 {
		return 0, 0
	}
	for key, entries := range h.m {
		podadas, dropped := trimSegment(entries, h.capPerSegment, h.retention, now)
		if dropped == 0 {
			continue
		}
		removed += dropped
		if len(podadas) == 0 {
			delete(h.m, key)
			segments++
			continue
		}
		h.m[key] = podadas
	}
	if removed > 0 {
		h.clearCachesLocked()
		h.dirtyFlag = true
	}
	return removed, segments
}

// clearCachesLocked discards the derived memoizers of cost, pressure and
// usage. The generation counter is preserved, since the future entries of
// the cache are born with the current generation and no old value survives
// in the map. The caller holds the write lock.
func (h *History) clearCachesLocked() {
	h.costCache = make(map[string]costCacheEntry)
	h.psiCache = make(map[string]psiCacheEntry)
	h.usageCache = make(map[string]usageCacheEntry)
}
