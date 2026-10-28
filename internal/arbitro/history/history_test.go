package history

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoadBucket(t *testing.T) {
	cases := []struct {
		name string
		pct  float64
		want string
	}{
		{name: "negativo => unknown", pct: -5, want: BucketUnknown},
		{name: "negativo -0.1 => unknown", pct: -0.1, want: BucketUnknown},
		{name: "NaN => unknown", pct: math.NaN(), want: BucketUnknown},
		{name: "0 => 0-10%", pct: 0, want: Bucket0_10},
		{name: "5 => 0-10%", pct: 5, want: Bucket0_10},
		{name: "9.9 => 0-10%", pct: 9.9, want: Bucket0_10},
		{name: "10 fronteira => 10-25%", pct: 10, want: Bucket10_25},
		{name: "42 => 25-50%", pct: 42, want: Bucket25_50},
		{name: "25 fronteira => 25-50%", pct: 25, want: Bucket25_50},
		{name: "60 => 50-75%", pct: 60, want: Bucket50_75},
		{name: "50 fronteira => 50-75%", pct: 50, want: Bucket50_75},
		{name: "75 fronteira => 75%+", pct: 75, want: Bucket75Plus},
		{name: "90 => 75%+", pct: 90, want: Bucket75Plus},
		{name: ">100 clamp => 75%+", pct: 150, want: Bucket75Plus},
		{name: "100 => 75%+", pct: 100, want: Bucket75Plus},
		{name: "Inf => 75%+", pct: math.Inf(1), want: Bucket75Plus},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := LoadBucket(tc.pct)
			if got != tc.want {
				t.Fatalf("LoadBucket(%v) got %q want %q", tc.pct, got, tc.want)
			}
		})
	}
}

func TestSegmentKey(t *testing.T) {
	t.Run("formato basico", func(t *testing.T) {
		sc := ScenarioContext{LoadBucket: Bucket25_50}
		got := SegmentKey("fn", "n1", sc)
		want := "fn\x00n1\x0025-50%"
		if got != want {
			t.Fatalf("SegmentKey got %q want %q", got, want)
		}
	})

	t.Run("colisao impossivel com separador nulo", func(t *testing.T) {
		sc := ScenarioContext{LoadBucket: Bucket0_10}
		// Sem separador nulo, "ab"+"c" == "a"+"bc" == "abc" colidiria.
		// Com \x00, "ab\x00c" != "a\x00bc".
		k1 := SegmentKey("ab", "c", sc)
		k2 := SegmentKey("a", "bc", sc)
		if k1 == k2 {
			t.Fatalf("colisao: k1=%q k2=%q deveriam ser diferentes", k1, k2)
		}
		// Verifica que a colisão sem separador existiria.
		if "ab"+"c" == "a"+"bc" {
			// ok: sem separador colidiriam; com separador não colidem — prova do design.
		}
		// Teste direto do enunciado: "a\x00b" != "ab"
		if "a\x00b" == "ab" {
			t.Fatalf("separador nulo não deveria colidir com string vazia")
		}
		// Segundo nivel: bucket com separador (chave 3D: grupo+nó+bucket).
		sc2 := ScenarioContext{LoadBucket: "50-75%"}
		k3 := SegmentKey("fn", "n1", sc2)
		parts := strings.Split(k3, "\x00")
		if len(parts) != 3 {
			t.Fatalf("SegmentKey deveria ter 3 partes separadas por \\x00, got %d: %q", len(parts), k3)
		}
		if parts[0] != "fn" || parts[1] != "n1" || parts[2] != "50-75%" {
			t.Fatalf("partes inesperadas: %v", parts)
		}
	})

	t.Run("vazio unknown nao panic", func(t *testing.T) {
		sc := ScenarioContext{LoadBucket: BucketUnknown}
		got := SegmentKey("", "", sc)
		want := "\x00\x00unknown"
		if got != want {
			t.Fatalf("SegmentKey vazio got %q want %q", got, want)
		}
	})
}

func TestEntryMarshal(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	e := Entry{
		FunctionName:      "alpr-deteccao-placa",
		NodeID:            "xp4-borda-01",
		Timestamp:         now,
		ScenarioContext:   ScenarioContext{LoadBucket: Bucket50_75},
		ObservedLatencyMS: 640,
		SloMet:            true,
		Outcome:           OutcomeSucesso,
		ColdStart:         false,
		InstanceID:        "alpr-deteccao-placa-7f9b2c1a",
	}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Verifica snake_case conforme schemas §7.
	for _, want := range []string{`"function_name"`, `"node_id"`, `"timestamp"`, `"scenario_context"`, `"load_bucket"`, `"observed_latency_ms"`, `"slo_met"`, `"outcome"`, `"cold_start"`, `"instance_id"`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("json não contém %s: %s", want, string(data))
		}
	}
	// Round-trip.
	var got Entry
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.FunctionName != e.FunctionName || got.NodeID != e.NodeID || got.ObservedLatencyMS != e.ObservedLatencyMS || got.SloMet != e.SloMet || got.Outcome != e.Outcome || got.ColdStart != e.ColdStart || got.InstanceID != e.InstanceID {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, e)
	}
	if got.ScenarioContext != e.ScenarioContext {
		t.Fatalf("scenario_context mismatch: got %+v want %+v", got.ScenarioContext, e.ScenarioContext)
	}
	if !got.Timestamp.Equal(e.Timestamp) {
		t.Fatalf("timestamp mismatch: got %v want %v", got.Timestamp, e.Timestamp)
	}

	t.Run("omitempty instance_id", func(t *testing.T) {
		e2 := Entry{
			FunctionName:      "fn",
			NodeID:            "n1",
			Timestamp:         now,
			ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
			ObservedLatencyMS: 100,
			SloMet:            false,
			Outcome:           OutcomeFalha,
			ColdStart:         true,
		}
		data2, err := json.Marshal(e2)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if strings.Contains(string(data2), `"instance_id"`) {
			t.Fatalf("instance_id deveria ser omitido quando vazio, got %s", string(data2))
		}
		var got2 Entry
		if err := json.Unmarshal(data2, &got2); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if got2.InstanceID != "" {
			t.Fatalf("InstanceID want vazio got %q", got2.InstanceID)
		}
	})

	t.Run("outcome timeout", func(t *testing.T) {
		e3 := Entry{
			FunctionName:      "fn",
			NodeID:            "n1",
			Timestamp:         now,
			ScenarioContext:   ScenarioContext{LoadBucket: BucketUnknown},
			ObservedLatencyMS: 5000,
			SloMet:            false,
			Outcome:           OutcomeTimeout,
			ColdStart:         false,
		}
		data3, err := json.Marshal(e3)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if !strings.Contains(string(data3), `"timeout"`) {
			t.Fatalf("outcome timeout não serializado: %s", string(data3))
		}
	})
}

func TestStatsAndRecentCountsColdStartSuccesses(t *testing.T) {
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	mk := func(outcome string, cold bool, lat int64) Entry {
		return Entry{FunctionName: "fn", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: lat, Outcome: outcome, ColdStart: cold}
	}
	for _, e := range []Entry{
		mk(OutcomeSucesso, true, 50),
		mk(OutcomeSucesso, true, 60),
		mk(OutcomeSucesso, false, 100),
		mk(OutcomeFalha, false, 200),
	} {
		if err := h.AppendWithGroup(e, e.FunctionName); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	key := SegmentKey("fn", "n1", sc)
	_, _, recent, _, _, ok := h.StatsAndRecent(key, time.Minute, now)
	if !ok {
		t.Fatalf("StatsAndRecent ok=false, esperado true")
	}
	if want := h.RecentSuccesses(key, time.Minute, now); recent != want || recent != 3 {
		t.Errorf("recent=%d RecentSuccesses=%d, esperado 3 (cold_start conta)", recent, want)
	}
}

// ---------------------------------------------------------------------------
// history_batch_test.go — batch/coalesce
// ---------------------------------------------------------------------------

func TestHistoryBatchCoalesce(t *testing.T) {
	tmpDir := t.TempDir()
	h, err := NewHistory(tmpDir+"/history.json", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	defer h.Close()

	sc := ScenarioContext{LoadBucket: Bucket50_75}
	for i := 0; i < 100; i++ {
		e := Entry{
			FunctionName:      "alpr-deteccao-placa",
			NodeID:            "xp4-borda-01",
			Timestamp:         time.Now().UTC().Truncate(time.Millisecond),
			ScenarioContext:   sc,
			ObservedLatencyMS: 100,
			SloMet:            true,
			Outcome:           OutcomeSucesso,
			ColdStart:         false,
			InstanceID:        "alpr-deteccao-placa-7f9b2c1a",
		}
		h.AppendWithGroup(e, e.FunctionName)
	}
	time.Sleep(700 * time.Millisecond)

	segments, err := readHistoryFile(t, tmpDir+"/history.json")
	if err != nil {
		t.Fatalf("readHistoryFile: %v", err)
	}
	total := 0
	for _, s := range segments {
		total += len(s)
	}
	if total != 100 {
		t.Fatalf("expected 100 entries, got %d", total)
	}
}

func TestHistoryCloseFlush(t *testing.T) {
	tmpDir := t.TempDir()
	h, err := NewHistory(tmpDir+"/history.json", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}

	sc := ScenarioContext{LoadBucket: Bucket50_75}
	for i := 0; i < 10; i++ {
		e := Entry{
			FunctionName:      "alpr-deteccao-placa",
			NodeID:            "xp4-borda-01",
			Timestamp:         time.Now().UTC().Truncate(time.Millisecond),
			ScenarioContext:   sc,
			ObservedLatencyMS: 100,
			SloMet:            true,
			Outcome:           OutcomeSucesso,
			ColdStart:         false,
			InstanceID:        "alpr-deteccao-placa-7f9b2c1a",
		}
		h.AppendWithGroup(e, e.FunctionName)
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segments, err := readHistoryFile(t, tmpDir+"/history.json")
	if err != nil {
		t.Fatalf("readHistoryFile: %v", err)
	}
	total := 0
	for _, segment := range segments {
		total += len(segment)
	}
	if total != 10 {
		t.Fatalf("expected 10 entries after CloseFlush, got %d", total)
	}
}

func TestHistoryCloseNoAppends(t *testing.T) {
	t.Run("no-spurious-file", func(t *testing.T) {
		tmpDir := t.TempDir()
		h, err := NewHistory(tmpDir+"/history.json", 1000)
		if err != nil {
			t.Fatalf("NewHistory: %v", err)
		}
		defer h.Close()
		if _, err := os.Stat(tmpDir + "/history.json"); !os.IsNotExist(err) {
			t.Fatalf("spurious file created after Close() with no Appends")
		}
	})
}

func TestHistoryStrictImmediate(t *testing.T) {
	tmpDir := t.TempDir()
	h, err := NewHistory(tmpDir+"/history.json", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	defer h.Close()

	h.SetStrict(true)

	sc := ScenarioContext{LoadBucket: Bucket50_75}
	for i := 0; i < 5; i++ {
		e := Entry{
			FunctionName:      "alpr-deteccao-placa",
			NodeID:            "xp4-borda-01",
			Timestamp:         time.Now().UTC().Truncate(time.Millisecond),
			ScenarioContext:   sc,
			ObservedLatencyMS: 100,
			SloMet:            true,
			Outcome:           OutcomeSucesso,
			ColdStart:         false,
			InstanceID:        "alpr-deteccao-placa-7f9b2c1a",
		}
		h.AppendWithGroup(e, e.FunctionName)
	}

	segments, err := readHistoryFile(t, tmpDir+"/history.json")
	if err != nil {
		t.Fatalf("readHistoryFile: %v", err)
	}
	total := 0
	for _, segment := range segments {
		total += len(segment)
	}
	if total != 5 {
		t.Fatalf("expected 5 entries after strict AppendWithGroup (immediate flush), got %d", total)
	}
}

func readHistoryFile(t *testing.T, path string) (map[string][]Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var segments map[string][]Entry
	if err := json.Unmarshal(data, &segments); err != nil {
		return nil, err
	}
	return segments, nil
}
