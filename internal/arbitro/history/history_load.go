package history

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// loadHistoryFile loads the file of the current format in streaming: it
// decodes the object segment by segment and applies the capacity and the
// retention before storing, so that a large file does not become a spike of
// memory, since the part outside the window is discarded during the decoding
// and never adopted. It returns the already pruned map, the total of
// discarded entries and an error, where a missing file gives an empty map
// and no error. It keeps the fail-closed behavior: corrupted JSON gives an
// error and a missing version marker gives the migration error.
func loadHistoryFile(path string, capPerSegment int, retention time.Duration, now time.Time) (map[string][]Entry, int, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string][]Entry{}, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("history: falha ao ler %q: %w", path, err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	tok, err := dec.Token()
	if err != nil {
		return nil, 0, fmt.Errorf("history: arquivo de persistência %q corrompido: %w", path, err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, 0, fmt.Errorf("history: arquivo de persistência %q corrompido: esperado objeto JSON", path)
	}

	out := make(map[string][]Entry)
	sentinel := false
	removed := 0
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, 0, fmt.Errorf("history: arquivo de persistência %q corrompido: %w", path, err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, 0, fmt.Errorf("history: arquivo de persistência %q corrompido: chave não-string", path)
		}
		var entries []Entry
		if err := dec.Decode(&entries); err != nil {
			return nil, 0, fmt.Errorf("history: arquivo de persistência %q corrompido: %w", path, err)
		}
		if key == historySchemaSentinelKey {
			sentinel = true
			continue
		}
		podadas, dropped := trimSegment(entries, capPerSegment, retention, now)
		removed += dropped
		if len(podadas) > 0 {
			out[key] = podadas
		}
	}
	// It consumes the closing brace of the object and requires the end of
	// the file, with only whitespace allowed, as the previous decoding did
	// with trailing garbage after the object.
	if _, err := dec.Token(); err != nil {
		return nil, 0, fmt.Errorf("history: arquivo de persistência %q corrompido: %w", path, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, 0, fmt.Errorf("history: arquivo de persistência %q corrompido: conteúdo após o objeto JSON", path)
	}
	if !sentinel {
		return nil, 0, fmt.Errorf(errHistoryNeedsMigration, path)
	}
	return out, removed, nil
}

// trimSegment applies retention by age and a FIFO capacity to a single list
// of entries, without touching shared state. It returns the pruned slice,
// which may be the same one when nothing was removed, and how many entries
// left.
//
// The retention keeps the timestamps after the window, including future
// timestamps from a clock skew, which the reads handle separately. The cut
// is by value and not by prefix, since the late reports arrive out of order
// on purpose. The capacity keeps the last entries in FIFO order.
func trimSegment(entries []Entry, capPerSegment int, retention time.Duration, now time.Time) ([]Entry, int) {
	removed := 0
	if retention > 0 {
		cutoff := now.Add(-retention)
		keep := entries[:0]
		for i := range entries {
			if entries[i].Timestamp.After(cutoff) {
				keep = append(keep, entries[i])
			} else {
				removed++
			}
		}
		if removed > 0 {
			// It copies into a new slice, since the original may hold
			// references to the entries removed by the tail of the
			// backing array.
			podadas := make([]Entry, len(keep))
			copy(podadas, keep)
			entries = podadas
		}
	}
	if len(entries) > capPerSegment {
		removed += len(entries) - capPerSegment
		podadas := make([]Entry, capPerSegment)
		copy(podadas, entries[len(entries)-capPerSegment:])
		entries = podadas
	}
	return entries, removed
}
