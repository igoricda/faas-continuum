// Observation hook and entry-level replay. It is the only new interface of the
// history for the risk selector.
package history

import "sort"

// EntryObserver receives the group and the entry once per append, after the
// mutex is released. The observer must not call the append or the supersede,
// since that would recurse, and only observes, since the risk selector only
// feeds the learner, without input or output under the lock.
type EntryObserver func(group string, e Entry)

// SetObserver installs or removes the observation hook. The hook is read under
// the mutex and invoked outside it, without input or output under the lock.
func (h *History) SetObserver(fn EntryObserver) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.observer = fn
}

// ForEachEntry walks every entry, primary and aggregated, where the aggregated
// one is the same entry in the fan-out and the replay skips it to avoid a
// double count, in key order and, inside each key, in chronological order of
// the append. It is read only: it copies under the read lock and invokes the
// callback outside it. The replay never supersedes, so an entry already
// superseded does not appear.
func (h *History) ForEachEntry(fn func(key string, e Entry)) {
	if fn == nil {
		return
	}
	h.mu.RLock()
	keys := make([]string, 0, len(h.m))
	for k := range h.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	snap := make([][]Entry, len(keys))
	for i, k := range keys {
		src := h.m[k]
		cp := make([]Entry, len(src))
		copy(cp, src)
		snap[i] = cp
	}
	h.mu.RUnlock()
	for i, k := range keys {
		for _, e := range snap[i] {
			fn(k, e)
		}
	}
}
