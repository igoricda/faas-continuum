package history

import (
	"errors"
	"log"
	"strings"
	"sync"
	"time"
)

// History keeps the history segmented by the scenario of load. It is safe
// for concurrent use through a read-write mutex. Each segment has a FIFO
// capacity, which avoids an out-of-memory condition on the devices of the
// edge, and there is also retention by age: entries older than the window
// are pruned on load and periodically, and segments that become empty
// disappear, so neither the memory nor the file grows without bound.
//
// The persistence is atomic, through a temporary file, a rename, restrictive
// permissions and a flush to the disk. The persistence is batched and
// asynchronous, with a ticker of 500 milliseconds: an append marks the
// history as dirty without blocking, and the background goroutine flushes
// periodically only when there is a pending change, without rewriting an
// idle file, and the close performs the final flush.
//
// There is no cache of the percentile: it now depends on the time window,
// so an all-time cache would return a stale value, and every query
// recomputes over at most the capacity of the segment.
type History struct {
	mu            sync.RWMutex
	m             map[string][]Entry
	capPerSegment int
	// retention is the window of retention by age, and a non-positive value
	// disables the cut. It is read and written under the mutex, and it
	// only holds the policy, since the pruning is a separate operation.
	retention time.Duration
	// dirtyFlag indicates a pending snapshot under the mutex. The channel
	// is only the wake-up of the loop, and the flag avoids rewriting the
	// file without changes.
	dirtyFlag bool
	path      string
	dirty     chan struct{} // buf 1, coalesce signals
	stop      chan struct{} // signal loop to exit
	wg        sync.WaitGroup
	closeOnce sync.Once  // garante Close() idempotente
	closeErr  error      // erro da primeira chamada a Close()
	strict    bool       // se true, AppendWithGroup persiste imediatamente via FlushSync
	fileMu    sync.Mutex // guards the durable write and the rename in saveSnapshot, which avoids a race between a synchronous flush of an append and the flush of the loop over the same temporary file
	// A single discipline governs the three caches guarded by a
	// generation: the same mutex, the same map of generations keyed by the
	// pair of group and node, the same ceiling, the same points of
	// invalidation, where an append or a supersede bumps the generation
	// of the pair and the aggregated fan-out never bumps it, and the same
	// temporal guards of the computation time, the newest entry, the
	// future horizon and the oldest entry with the window. A miss is
	// detected under the read lock, the fit runs pure outside the lock,
	// and the publication under the write lock only happens when the
	// generation of the pair did not move. The key of the cost cache is
	// the pair, and the keys of the pressure and usage caches are the
	// pair and the layer.
	costGen    map[string]uint64
	costCache  map[string]costCacheEntry
	psiCache   map[string]psiCacheEntry
	usageCache map[string]usageCacheEntry
	// observer is the hook of the second stage: it is called once per
	// append with the group and the entry, after the mutex is released.
	// It is the single feeding point of the risk selector, without any
	// change in the gateway, and an absent observer disables it.
	observer EntryObserver
}

// NewHistory creates a history without a cut by age, which is compatible
// with the previous behavior for tests and simulations. The arbiter uses
// the constructor with retention and the configured window.
func NewHistory(path string, capPerSegment int) (*History, error) {
	return NewHistoryWithRetention(path, capPerSegment, 0)
}

// NewHistoryWithRetention creates a history. The capacity per segment must
// be positive, with no silent default, and the wiring validates it and
// aborts on an invalid value. A positive retention loads and prunes by age
// and a non-positive value disables the cut by age, while the capacity per
// segment always applies. An empty path keeps the history in memory, which
// is the case of the tests. When the path is not empty and the file exists,
// it is loaded in streaming, segment by segment, applying the capacity and
// the retention before storing, so a large file does not become a spike of
// memory, and the version marker is required, since its absence gives a
// fail-closed error that points to the migration tool. When the path is not
// empty the constructor also starts the background goroutine of the batched
// persistence and the periodic pruning.
func NewHistoryWithRetention(path string, capPerSegment int, retention time.Duration) (*History, error) {
	if capPerSegment <= 0 {
		return nil, errors.New("history: history cap must be >0")
	}
	if retention < 0 {
		retention = 0
	}
	h := &History{
		m:             make(map[string][]Entry),
		capPerSegment: capPerSegment,
		retention:     retention,
		path:          path,
		costGen:       make(map[string]uint64),
		costCache:     make(map[string]costCacheEntry),
		psiCache:      make(map[string]psiCacheEntry),
		usageCache:    make(map[string]usageCacheEntry),
	}
	if path == "" {
		return h, nil
	}
	loaded, removed, err := loadHistoryFile(path, capPerSegment, retention, time.Now())
	if err != nil {
		return nil, err
	}
	h.m = loaded
	if removed > 0 {
		// The loaded file had data outside the capacity or the retention,
		// so the history is marked as dirty for the first flush to
		// rewrite the already pruned version.
		h.dirtyFlag = true
		log.Printf("arbitro: history: %d entradas antigas descartadas no load (retention=%s)", removed, retention)
	}
	h.startLoop()
	return h, nil
}

// startLoop starts the goroutine of the batched persistence, with a ticker
// of 500 milliseconds and a coalesced dirty signal. It is called by the
// constructor only when the path is not empty.
func (h *History) startLoop() {
	h.dirty = make(chan struct{}, 1)
	h.stop = make(chan struct{})
	h.wg.Add(1)
	go h.loop()
}

// Keys returns a copy of the keys of the history under the read lock. It
// is used by the summary of the nodes.
func (h *History) Keys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.m))
	for k := range h.m {
		out = append(out, k)
	}
	return out
}

// KeysForNode returns a copy of the primary keys of the given node under
// the read lock. Aggregated keys never match, since the aggregated prefix is
// filtered before the parsing. The primary format is the group, the node and
// the load bucket, so cutting twice extracts the node. The slice is
// pre-allocated from a heuristic of about a quarter of the keys per
// node.
func (h *History) KeysForNode(nodeID string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.m) == 0 {
		return nil
	}
	est := len(h.m)/4 + 1
	out := make([]string, 0, est)
	for k := range h.m {
		if IsAggLayerKey(k) {
			continue
		}
		_, afterGroup, ok := strings.Cut(k, "\x00")
		if !ok {
			continue
		}
		thisNodeID, _, ok := strings.Cut(afterGroup, "\x00")
		if !ok {
			continue
		}
		if thisNodeID == nodeID {
			out = append(out, k)
		}
	}
	return out
}

// KeysForLayer returns a copy of the aggregated keys of the given layer
// under the read lock. It is used by the summary per node to show the dense
// curve of the layer together with the primary keys.
func (h *History) KeysForLayer(layer string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.m) == 0 || layer == "" {
		return nil
	}
	var out []string
	for k := range h.m {
		if !IsAggLayerKey(k) {
			continue
		}
		rest := strings.TrimPrefix(k, aggLayerPrefix)
		_, afterGroup, ok := strings.Cut(rest, "\x00")
		if !ok {
			continue
		}
		thisLayer, _, ok := strings.Cut(afterGroup, "\x00")
		if !ok {
			continue
		}
		if thisLayer == layer {
			out = append(out, k)
		}
	}
	return out
}
