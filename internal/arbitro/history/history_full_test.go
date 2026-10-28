package history

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// fullFixture pré-carrega um segmento (grupo "grp", nó "n1", bucket 0-10%)
// com TODAS as classes: solo (3, um sem CPU), contended (2, um sem CPU),
// cold (2), Inflight>=3 (1), Inflight==0 (1), falha (1) e timeout (1).
// Retorna o History e a chave primária.
func fullFixture(t *testing.T, now time.Time) (*History, string) {
	t.Helper()
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	mk := func(lat int64, outcome string, cold bool, inflight int, cpu float64) Entry {
		return Entry{
			FunctionName:      "grp",
			NodeID:            "n1",
			Timestamp:         now,
			ScenarioContext:   sc,
			ObservedLatencyMS: lat,
			Outcome:           outcome,
			ColdStart:         cold,
			Inflight:          inflight,
			CPUPctAvg:         cpu,
			SampleCount:       5,
			PSICpuSomeAvg10:   5,
			PSIMemFullAvg10:   3,
		}
	}
	for _, e := range []Entry{
		mk(100, OutcomeSucesso, false, 1, 20),
		mk(200, OutcomeSucesso, false, 1, 30),
		mk(300, OutcomeSucesso, false, 1, 0),
		mk(1000, OutcomeSucesso, false, 2, 40),
		mk(2000, OutcomeSucesso, false, 2, 0),
		mk(30000, OutcomeSucesso, true, 0, 0),
		mk(40000, OutcomeSucesso, true, 0, 0),
		mk(9999, OutcomeSucesso, false, 3, 50),
		mk(8888, OutcomeSucesso, false, 0, 0),
		mk(5000, OutcomeFalha, false, 1, 0),
		mk(6000, OutcomeTimeout, false, 1, 0),
	} {
		if err := h.AppendWithGroup(e, "grp"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	return h, SegmentKey("grp", "n1", sc)
}

func TestStatsFullClasses(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	h, key := fullFixture(t, now)
	st, ok := h.StatsFull(key, time.Minute, now)
	if !ok {
		t.Fatalf("StatsFull ok=false, esperado true")
	}
	checks := []struct {
		name string
		got  int64
		want int64
	}{
		{"Count", int64(st.Count), 7},
		{"P95MS", st.P95MS, 9999},
		{"Recent", int64(st.Recent), 9},
		{"Total", int64(st.Total), 11},
		{"SoloN", int64(st.SoloN), 3},
		{"ContN", int64(st.ContN), 2},
		{"ColdN", int64(st.ColdN), 2},
		{"SoloP95MS", st.SoloP95MS, 300},
		{"ContP95MS", st.ContP95MS, 2000},
		{"ColdMeanMS", st.ColdMeanMS, 35000},
		{"SoloCPUSamples", int64(st.SoloCPUSamples), 2},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, esperado %d", c.name, c.got, c.want)
		}
	}
	if st.SoloCPUPctMean != 25 {
		t.Errorf("SoloCPUPctMean = %v, esperado 25", st.SoloCPUPctMean)
	}

	cost, n, ok, seeded := h.ExpectedCost("grp", "n1", time.Minute, now)
	if !ok || n != 2 || cost != 25 {
		t.Errorf("ExpectedCost = (%v,%d,%v), esperado (25,2,true)", cost, n, ok)
	}
	if seeded {
		t.Errorf("ExpectedCost seeded=true, esperado false (média local, sem Layer => sem seed)")
	}
}

func TestStatsFullUnknownInflightExcluded(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	for i, inflight := range []int{0, 3, 0, 3} {
		e := Entry{
			FunctionName:      "grp",
			NodeID:            "n1",
			Timestamp:         now,
			ScenarioContext:   sc,
			ObservedLatencyMS: int64(100 * (i + 1)),
			Outcome:           OutcomeSucesso,
			Inflight:          inflight,
			CPUPctAvg:         50,
		}
		if err := h.AppendWithGroup(e, "grp"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	key := SegmentKey("grp", "n1", sc)
	st, ok := h.StatsFull(key, time.Minute, now)
	if !ok {
		t.Fatalf("StatsFull ok=false, esperado true")
	}
	if st.SoloN != 0 || st.ContN != 0 {
		t.Errorf("SoloN/ContN = %d/%d, esperado 0/0 (Inflight 0/>=3 fora das classes)", st.SoloN, st.ContN)
	}
	if st.Count != 4 || st.P95MS != 400 {
		t.Errorf("p95 misto = (%d,%d), esperado (4,400) intacto", st.Count, st.P95MS)
	}
}

func TestStatsFullColdOnly(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	sc := ScenarioContext{LoadBucket: Bucket0_10}
	for _, lat := range []int64{30000, 40000} {
		e := Entry{FunctionName: "grp", NodeID: "n1", Timestamp: now, ScenarioContext: sc, ObservedLatencyMS: lat, Outcome: OutcomeSucesso, ColdStart: true}
		if err := h.AppendWithGroup(e, "grp"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	key := SegmentKey("grp", "n1", sc)
	st, ok := h.StatsFull(key, time.Minute, now)
	if !ok {
		t.Fatalf("StatsFull ok=false, esperado true (Total>0)")
	}
	if st.ColdN != 2 || st.ColdMeanMS != 35000 {
		t.Errorf("cold = (%d,%d), esperado (2,35000)", st.ColdN, st.ColdMeanMS)
	}
	if st.Count != 0 || st.P95MS != 0 {
		t.Errorf("warm p95 = (%d,%d), esperado ausente (0,0)", st.Count, st.P95MS)
	}
	if _, _, _, _, ok := h.Stats(key); ok {
		t.Errorf("Stats ok=true, esperado false (sem sucesso warm)")
	}
	if _, _, _, _, _, ok := h.StatsAndRecent(key, time.Minute, now); ok {
		t.Errorf("StatsAndRecent ok=true, esperado false (sem sucesso warm)")
	}
}

func TestStatsFullEmptyKey(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	st, ok := h.StatsFull(SegmentKey("grp", "n1", ScenarioContext{LoadBucket: Bucket0_10}), time.Minute, now)
	if ok {
		t.Fatalf("StatsFull ok=true, esperado false (chave ausente)")
	}
	if st.Count != 0 || st.Total != 0 || st.SuccessRate != 0.5 {
		t.Errorf("empty StatsFull = %+v, esperado zero com SuccessRate 0.5", st)
	}
	cost, n, ok, seeded := h.ExpectedCost("grp", "n1", time.Minute, now)
	if ok || n != 0 || cost != 0 {
		t.Errorf("ExpectedCost vazio = (%v,%d,%v), esperado (0,0,false)", cost, n, ok)
	}
	if seeded {
		t.Errorf("ExpectedCost vazio seeded=true, esperado false")
	}
}

func TestExpectedCostSoloOnlyAcrossBuckets(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	h, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	mk := func(group, node, bucket, outcome string, inflight int, cold bool, cpu float64) Entry {
		return Entry{
			FunctionName:    group,
			NodeID:          node,
			Timestamp:       now,
			ScenarioContext: ScenarioContext{LoadBucket: bucket},
			Outcome:         outcome,
			Inflight:        inflight,
			ColdStart:       cold,
			CPUPctAvg:       cpu,
			PSICpuSomeAvg10: 5,
			PSIMemFullAvg10: 3,
		}
	}
	entries := []Entry{
		mk("grp", "n1", Bucket0_10, OutcomeSucesso, 1, false, 20),
		mk("grp", "n1", Bucket50_75, OutcomeSucesso, 1, false, 40),
		mk("grp", "n1", Bucket0_10, OutcomeSucesso, 2, false, 100),           // contended: excluído
		mk("grp", "n1", Bucket0_10, OutcomeSucesso, 1, true, 100),            // cold: excluído
		mk("grp", "n1", Bucket0_10, OutcomeSucesso, 1, false, 0),             // cpu desconhecido: excluído
		mk("grp", "n1", Bucket0_10, OutcomeOrcamentoExcedido, 1, false, 100), // não é medida: excluído
		mk("outro", "n1", Bucket0_10, OutcomeSucesso, 1, false, 100),         // outro grupo: excluído
		mk("grp", "n2", Bucket0_10, OutcomeSucesso, 1, false, 100),           // outro nó: excluído
	}
	for _, e := range entries {
		if err := h.AppendWithGroup(e, e.FunctionName); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	cost, n, ok, seeded := h.ExpectedCost("grp", "n1", time.Minute, now)
	if !ok || n != 2 || cost != 30 {
		t.Fatalf("ExpectedCost = (%v,%d,%v), esperado (30,2,true)", cost, n, ok)
	}
	if seeded {
		t.Fatalf("ExpectedCost seeded=true, esperado false (média local)")
	}

	// Só contended => n==0 => ok=false.
	h2, err := NewHistory("", 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	e := mk("grp", "n1", Bucket0_10, OutcomeSucesso, 2, false, 80)
	if err := h2.AppendWithGroup(e, "grp"); err != nil {
		t.Fatalf("AppendWithGroup: %v", err)
	}
	if cost, n, ok, seeded := h2.ExpectedCost("grp", "n1", time.Minute, now); ok || n != 0 || cost != 0 || seeded {
		t.Fatalf("ExpectedCost contended-only = (%v,%d,%v,seeded=%v), esperado (0,0,false,false)", cost, n, ok, seeded)
	}

	// Receiver nil e window<=0 => (0,0,false,false) sem panic.
	var nilH *History
	if _, n, ok, seeded := nilH.ExpectedCost("grp", "n1", time.Minute, now); ok || n != 0 || seeded {
		t.Fatalf("ExpectedCost nil receiver = ok=%v n=%d, esperado false/0", ok, n)
	}
	if _, n, ok, seeded := h.ExpectedCost("grp", "n1", 0, now); ok || n != 0 || seeded {
		t.Fatalf("ExpectedCost window<=0 = ok=%v n=%d, esperado false/0", ok, n)
	}
}

func TestStatsFullMatchesStatsAndRecent(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	h, key := fullFixture(t, now)
	windows := []time.Duration{time.Minute, 0}
	for _, w := range windows {
		count, p95, recent, total, rate, ok := h.StatsAndRecent(key, w, now)
		st, okFull := h.StatsFull(key, w, now)
		if ok != okFull {
			t.Fatalf("window=%v ok mismatch: StatsAndRecent=%v StatsFull=%v", w, ok, okFull)
		}
		if count != st.Count || p95 != st.P95MS || recent != st.Recent || total != st.Total || rate != st.SuccessRate {
			t.Fatalf("window=%v StatsFull=%+v diverge de StatsAndRecent=(%d,%d,%d,%d,%v)", w, st, count, p95, recent, total, rate)
		}
	}
}

func TestScenarioFromProjected(t *testing.T) {
	cases := []struct {
		name      string
		pkg       telemetry.Package
		ok        bool
		projected float64
		want      string
	}{
		{"sem telemetria nem projeção => unknown", telemetry.Package{}, false, 0, BucketUnknown},
		{"sem telemetria com projeção quente => bucket projetado", telemetry.Package{}, false, 80, Bucket75Plus},
		{"sem telemetria projeção negativa => unknown", telemetry.Package{}, false, -5, BucketUnknown},
		{"telemetria com projeção quente vence mem", telemetry.Package{CPUUsagePct: 5, MemoryUsagePct: 30}, true, 80, Bucket75Plus},
		{"telemetria projeção zero e mem manda", telemetry.Package{CPUUsagePct: 5, MemoryUsagePct: 30}, true, 0, Bucket25_50},
		{"telemetria mem 50-75 vence cpu projetada baixa", telemetry.Package{CPUUsagePct: 5, MemoryUsagePct: 60}, true, 0, Bucket50_75},
		{"mem NaN não derruba cpu projetada", telemetry.Package{CPUUsagePct: 5, MemoryUsagePct: math.NaN()}, true, 5, Bucket0_10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ScenarioFromProjected(tc.pkg, tc.ok, tc.projected)
			if got.LoadBucket != tc.want {
				t.Fatalf("ScenarioFromProjected(%+v,%v,%v) = %q, esperado %q", tc.pkg, tc.ok, tc.projected, got.LoadBucket, tc.want)
			}
		})
	}

	t.Run("projeção igual à observada é idêntica a ScenarioFrom", func(t *testing.T) {
		pkg := telemetry.Package{CPUUsagePct: 42, MemoryUsagePct: 60}
		if got, want := ScenarioFromProjected(pkg, true, pkg.CPUUsagePct), ScenarioFrom(pkg, true); got != want {
			t.Fatalf("ScenarioFromProjected=%+v ScenarioFrom=%+v, deveriam coincidir", got, want)
		}
	})
}

func TestEntryNewFieldsRoundTrip(t *testing.T) {
	e := Entry{
		FunctionName:      "fn",
		NodeID:            "n1",
		Timestamp:         time.Now().UTC().Truncate(time.Millisecond),
		ScenarioContext:   ScenarioContext{LoadBucket: Bucket0_10},
		ObservedLatencyMS: 100,
		Outcome:           OutcomeSucesso,
		Inflight:          2,
		CPUPctAvg:         42.5,
		SampleCount:       13,
		ProjectedLoadPct:  77.5,
	}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, want := range []string{`"inflight"`, `"cpu_pct_avg"`, `"sample_count"`, `"projected_load_pct"`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("json não contém %s: %s", want, string(data))
		}
	}
	var got Entry
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Inflight != 2 || got.CPUPctAvg != 42.5 || got.SampleCount != 13 || got.ProjectedLoadPct != 77.5 {
		t.Fatalf("round-trip mismatch: got %+v", got)
	}

	t.Run("omitempty zera campos ausentes", func(t *testing.T) {
		zero := Entry{FunctionName: "fn", NodeID: "n1", Timestamp: e.Timestamp, ScenarioContext: e.ScenarioContext, Outcome: OutcomeSucesso}
		data, err := json.Marshal(zero)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		for _, unwanted := range []string{`"inflight"`, `"cpu_pct_avg"`, `"sample_count"`, `"projected_load_pct"`} {
			if strings.Contains(string(data), unwanted) {
				t.Fatalf("%s deveria ser omitido quando zero: %s", unwanted, string(data))
			}
		}
	})
}
