package history

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/igoricda/faas-continuum/internal/fsutil"
)

// Version marker of the file of the current format, implemented as a
// sentinel key.
//
// The sentinel key is a segment key that never occurs in real data, since
// the primary keys are the group, the node and the bucket, where the group
// may be empty but would still require the node to be the version name and
// the bucket to be the version value, and the aggregated keys start with the
// aggregated prefix. Its value is an empty list, so it changes no count and
// the direct readers of the JSON keep working.
//
// In its lifecycle the save always writes the sentinel, so a new or an
// existing file becomes the current version on the next flush, and the
// constructor requires the sentinel and rejects a file without it with a
// fail-closed error that names the migration tool, with no permanent dual
// write and no panic. The sentinel is removed from the in-memory map on load,
// so the keys and the statistics never see it, and it is reinjected on the
// save. A zero entry serializes without the pressure keys, so a file of the
// current version without pressure is byte for byte the previous version
// plus one empty sentinel key.
const historySchemaSentinelKey = "\x00schema_version\x00r4"

// errHistoryNeedsMigration is the fail-closed text for a file of the
// previous version without the marker. That version was retired, and with it
// the migration tool, since the functions of that version were replaced by
// implementations about six times faster. The guidance to the operator is
// therefore to remove the file, since the history of the old format is data
// of a format that will not be read again.
const errHistoryNeedsMigration = "history: arquivo de persistência %q em formato r3 (sem marcador r4): o formato r3 foi aposentado em 2026-09-28; remova o arquivo (ou renomeie para .r3.bak) e deixe o Árbitro recriar o histórico em r4 — não há migrador"

// loop is the background goroutine of the batched persistence. It runs a
// ticker of 500 milliseconds for the flush of the pending changes and a
// ticker for the retention pruning, and it responds to the coalesced dirty
// signal. It is only started when the path is not empty.
func (h *History) loop() {
	defer h.wg.Done()
	flushTicker := time.NewTicker(500 * time.Millisecond)
	defer flushTicker.Stop()
	trimTicker := time.NewTicker(trimEvery)
	defer trimTicker.Stop()
	for {
		select {
		case <-h.dirty:
			h.flush()
		case <-flushTicker.C:
			h.flush()
		case <-trimTicker.C:
			if removed, segments := h.compact(time.Now()); removed > 0 {
				log.Printf("arbitro: history: podadas %d entradas e %d segmentos vazios (retention=%s)",
					removed, segments, h.Retention())
			}
			h.flush()
		case <-h.stop:
			h.flush()
			return
		}
	}
}

// flush persists the current snapshot when there is a pending change. It is
// called by the loop, on the ticker, on the dirty signal and on the pruning,
// and by the close. It does not return an error: a failure is logged and the
// dirty signal is restored for the next tick to try again, without blocking
// or bringing the loop down.
func (h *History) flush() {
	if h.path == "" {
		return
	}
	if !h.takeDirty() {
		return
	}
	if err := h.saveSnapshot(); err != nil {
		h.restoreDirty()
		log.Printf("arbitro: history flush falhou: %v", err)
	}
}

// takeDirty consumes and returns the signal of a pending change, under the
// mutex. It is consumed before the snapshot, so a concurrent append during
// the save marks it again and the next tick persists it, and the signal is
// never lost, with at most a redundant flush.
func (h *History) takeDirty() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	d := h.dirtyFlag
	h.dirtyFlag = false
	return d
}

// restoreDirty restores the signal after a failed flush, for the retry on the next tick.
func (h *History) restoreDirty() {
	h.mu.Lock()
	h.dirtyFlag = true
	h.mu.Unlock()
}

// signalDirty wakes the persistence loop without blocking. The content is
// the dirty flag and the channel is only the wake-up. It does nothing in a
// history that only lives in memory.
func (h *History) signalDirty() {
	if h.dirty == nil {
		return
	}
	select {
	case h.dirty <- struct{}{}:
	default:
	}
}

// saveSnapshot takes a deep snapshot under the read lock, injects the
// sentinel of the current version, so a saved file always carries it even
// when empty, serializes outside the lock and writes it durably with an
// atomic rename. The format is preserved: a compact serialization, a
// trailing newline, a temporary file, the rename, restrictive permissions
// and a flush to the disk. The mutex of the file serializes the durable
// write and the rename, which avoids a race between a synchronous flush of
// an append and the flush of the loop over the same temporary file.
func (h *History) saveSnapshot() error {
	h.mu.RLock()
	snapshot := make(map[string][]Entry, len(h.m)+1)
	for k, v := range h.m {
		cp := make([]Entry, len(v))
		copy(cp, v)
		snapshot[k] = cp
	}
	snapshot[historySchemaSentinelKey] = nil
	h.mu.RUnlock()

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("history: falha ao serializar: %w", err)
	}
	data = append(data, '\n')

	h.fileMu.Lock()
	defer h.fileMu.Unlock()
	tmp := h.path + ".tmp"
	if err := fsutil.WriteFileDurable(tmp, data); err != nil {
		return fmt.Errorf("history: falha ao gravar %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, h.path); err != nil {
		return fmt.Errorf("history: falha ao renomear %q para %q: %w", tmp, h.path, err)
	}
	return nil
}

// Close finalizes the background goroutine, performs the final flush and
// waits for it. For an empty path, in the memory-only mode, it does nothing
// and returns no error. The final snapshot is unconditional, even without a
// pending change, so the file carries the version marker when the history
// closes, which is what the tests and the operator expect. It is safe to
// call multiple times, since it is idempotent.
func (h *History) Close() error {
	if h.path == "" || h.stop == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		close(h.stop)
		h.wg.Wait()
		// The final flush runs after the loop ends. The loop already
		// flushed on the stop signal, and one more run is a safety
		// measure.
		h.closeErr = h.saveSnapshot()
	})
	return h.closeErr
}

// FlushSync persists the current snapshot immediately and synchronously. It
// consumes the dirty signal before the save, so a concurrent append during
// the save marks it again and the next flush persists it, without losing the
// signal.
func (h *History) FlushSync() error {
	if h.path == "" {
		return nil
	}
	h.takeDirty()
	if err := h.saveSnapshot(); err != nil {
		h.restoreDirty()
		return err
	}
	return nil
}

// SetStrict defines the persistence mode: in the strict mode an append persists synchronously through the immediate flush.
func (h *History) SetStrict(strict bool) {
	h.strict = strict
}
