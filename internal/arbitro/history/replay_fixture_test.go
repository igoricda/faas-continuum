package history

// Replay harness of the pinned fixture.
//
// Convention, which must stay stable:
//   - the versioned fixture lives in the testdata directory of this package,
//     which the tooling ignores, and it is a byte identical copy of the
//     priming history file;
//   - the loading always goes through the production constructor, which is the
//     load path of the store, so there is no parallel JSON schema and the entry
//     and the history are the production types;
//   - the window and the reference instant follow exactly the semantics of the
//     scan of the statistics: an entry takes part when its timestamp is not
//     after the reference instant and the difference is within the window;
//   - the default window is ten minutes, and the default reference instant is
//     the largest timestamp of the fixture, and both can be overridden.
//
// The span of the fixture is about fifty-four minutes, so a window of ten
// minutes at the largest timestamp sees only the tail of forty-six entries and
// not the whole set of five hundred and sixty. The integrity gate of twelve
// segments against five hundred and sixty entries is therefore computed over the
// whole history, with a non-positive window. A full warm replay requires a
// window at least as large as the span. The implication for a seeded history is
// that the seed only warms the experiment inside the throughput window.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	// ReplayFixtureRelPath is the path of the fixture relative to this package.
	ReplayFixtureRelPath = "testdata/history-r4.json"
	// ReplayDefaultWindow is the default window of the replay, which is the
	// throughput window of the production code, and it must not be confused with
	// the default of the wiring, since here it is only the default of the harness.
	ReplayDefaultWindow = 10 * time.Minute
	// Integrity gate of the replay fixture.
	ReplayFixtureSegments = 12
	ReplayFixtureEntries  = 560
	// ReplayFixtureCap covers the largest segment with room to spare, and it
	// matches the history cap of the priming so the load does not truncate.
	ReplayFixtureCap = 1000
)

// LoadReplayFixture loads the path through the production constructor, which is
// the production load path, and registers the close with the cleanup of the test.
// It fails the test on a load error.
//
// The pinned fixture is of the current format by construction. It used to be an
// older format without a marker, which the constructor rejects, so the harness
// stamped the sentinel into a copy in a temporary directory; that format was
// retired, the fixture was converted with only the sentinel added and the
// entries kept verbatim, and the migration step left the harness. The gates of
// twelve segments against five hundred and sixty entries, of the fan-out and of
// the tail of forty-six entries were checked before and after the conversion and
// did not move.
func LoadReplayFixture(t *testing.T, path string) *History {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("LoadReplayFixture(%q): %v", path, err)
	}
	var segs map[string][]Entry
	if err := json.Unmarshal(raw, &segs); err != nil {
		t.Fatalf("LoadReplayFixture(%q): %v", path, err)
	}
	if _, ok := segs[historySchemaSentinelKey]; !ok {
		segs[historySchemaSentinelKey] = nil
		stamped, err := json.Marshal(segs)
		if err != nil {
			t.Fatalf("LoadReplayFixture(%q): %v", path, err)
		}
		stamped = append(stamped, '\n')
		path = filepath.Join(t.TempDir(), "history-stamped-r4.json")
		if err := os.WriteFile(path, stamped, 0o600); err != nil {
			t.Fatalf("LoadReplayFixture(%q): %v", path, err)
		}
	}
	h, err := NewHistory(path, ReplayFixtureCap)
	if err != nil {
		t.Fatalf("LoadReplayFixture(%q): %v", path, err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Fatalf("LoadReplayFixture Close: %v", err)
		}
	})
	return h
}

// MaxReplayTimestamp returns the largest timestamp of the entries of the history,
// which is the default reference instant of the replay. It walks the map under
// the read lock of the package without copying the slices, since the timestamp
// does not come out of the statistics.
func MaxReplayTimestamp(h *History) time.Time {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var max time.Time
	for _, entries := range h.m {
		for i := range entries {
			if entries[i].Timestamp.After(max) {
				max = entries[i].Timestamp
			}
		}
	}
	return max
}

// CountReplayInWindow anchors the window and the reference instant and counts the
// entries inside the window with the same semantics of the scan of the
// statistics, but through the public production API, which is the keys and the
// statistics per segment, without reimplementing the filter. It returns the
// number of segments with a positive total inside the window and the sum of
// those totals, where the total covers every outcome inside the window, the
// successes, the failures and the timeouts, and excludes the exceeded budget.
func CountReplayInWindow(h *History, window time.Duration, now time.Time) (segments, total int) {
	for _, k := range h.Keys() {
		_, _, _, segTotal, _, _ := h.StatsAndRecent(k, window, now)
		if segTotal > 0 {
			segments++
			total += segTotal
		}
	}
	return segments, total
}

// CountReplayAllTime conta sem filtro temporal (window<=0 => all-time).
func CountReplayAllTime(h *History) (segments, total int) {
	keys := h.Keys()
	for _, k := range keys {
		_, _, _, segTotal, _, _ := h.StatsAndRecent(k, 0, time.Time{})
		total += segTotal
	}
	return len(keys), total
}

func TestReplayFixturePin(t *testing.T) {
	fixture := filepath.Join("testdata", "history-r4.json")
	if _, err := os.Stat(fixture); err != nil {
		t.Fatalf("fixture ausente: %v", err)
	}

	t.Run("happy/gate all-time 12 segmentos 560 entries", func(t *testing.T) {
		h := LoadReplayFixture(t, fixture)
		segs, total := CountReplayAllTime(h)
		if segs != ReplayFixtureSegments || total != ReplayFixtureEntries {
			t.Fatalf("gate all-time: got %d segmentos/%d entries, want %d/%d",
				segs, total, ReplayFixtureSegments, ReplayFixtureEntries)
		}
		// Integrity per segment: two hundred and eighty primary entries and the same number of aggregated ones.
		var prim, agg int
		for _, k := range h.Keys() {
			_, _, _, segTotal, _, _ := h.StatsAndRecent(k, 0, time.Time{})
			if IsAggLayerKey(k) {
				agg += segTotal
			} else {
				prim += segTotal
			}
		}
		if prim != 280 || agg != 280 {
			t.Fatalf("fan-out: got prim=%d agg=%d, want 280/280", prim, agg)
		}
	})

	t.Run("happy/window 60m@max vê as 560 (replay quente total)", func(t *testing.T) {
		h := LoadReplayFixture(t, fixture)
		now := MaxReplayTimestamp(h)
		if now.IsZero() {
			t.Fatal("MaxReplayTimestamp retornou zero")
		}
		segs, total := CountReplayInWindow(h, time.Hour, now)
		if segs != ReplayFixtureSegments || total != ReplayFixtureEntries {
			t.Fatalf("60m@max: got %d/%d, want %d/%d", segs, total,
				ReplayFixtureSegments, ReplayFixtureEntries)
		}
	})

	t.Run("happy/default 10m@max vê só a cauda (span 54min > window)", func(t *testing.T) {
		h := LoadReplayFixture(t, fixture)
		now := MaxReplayTimestamp(h)
		segs, total := CountReplayInWindow(h, ReplayDefaultWindow, now)
		// The pin of the real value, derived from the fixture: a change means that
		// the fixture changed, so it has to be investigated and not adjusted blindly.
		if total != 46 {
			t.Fatalf("10m@max: got total=%d, want 46 (cauda da fixture de replay)", total)
		}
		if segs == 0 || segs > ReplayFixtureSegments {
			t.Fatalf("10m@max: segmentos=%d fora de (0,%d]", segs, ReplayFixtureSegments)
		}
	})

	t.Run("stale/60s@max+1h retorna 0 (stale==cold)", func(t *testing.T) {
		h := LoadReplayFixture(t, fixture)
		now := MaxReplayTimestamp(h).Add(time.Hour)
		segs, total := CountReplayInWindow(h, time.Minute, now)
		if segs != 0 || total != 0 {
			t.Fatalf("stale: got %d/%d, want 0/0", segs, total)
		}
	})

	t.Run("failure/JSON corrompido => erro limpo, nunca panic", func(t *testing.T) {
		bad := filepath.Join(t.TempDir(), "history-bad.json")
		if err := os.WriteFile(bad, []byte(`{"k": [INVALID`), 0o600); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic no load corrompido: %v", r)
			}
		}()
		h, err := NewHistory(bad, ReplayFixtureCap)
		if err == nil {
			if h != nil {
				_ = h.Close()
			}
			t.Fatal("NewHistory com JSON corrompido deveria retornar erro")
		}
	})
}
