package server

// Testes do history_summary estendido (selflearning-cold-contention, todo 8):
// expõe por segmento as classes solo/contended/cold e o fator de contenção
// APRENDIDO (LEARNED-ONLY, via selection.EffectiveContentionFactor). O gate é
// Total==0 (existência), então segmento só-cold ainda reporta cold_count.
// count/p95_ms/recent_rps permanecem o contrato congelado (sempre presentes).

import (
	"encoding/json"
	"math"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/auth"
)

func newSummaryEnv(t *testing.T, now time.Time) (http.Handler, *history.History, string) {
	t.Helper()
	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	mon := monitor.NewMonitor()
	gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}})
	gen, err := auth.Generate()
	if err != nil {
		t.Fatalf("auth.Generate: %v", err)
	}
	cloudStore, err := cloud.NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	h, err := history.NewHistory(filepath.Join(t.TempDir(), "history.json"), 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	srv := NewServerWithHistory(ns, mon, gw, gen.Cred, 90*time.Second, defaultLimits, cloudStore, h, 60*time.Second, func() time.Time { return now })
	handler := srv.Handler()
	body := map[string]any{"node_id": "n1", "layer": "borda", "host": "10.0.0.1:8080"}
	if rr := doReq(t, handler, "POST", "/nodes", body, gen.PlainToken); rr.Code != http.StatusCreated {
		t.Fatalf("POST /nodes: status=%d body=%s", rr.Code, rr.Body.String())
	}
	return handler, h, gen.PlainToken
}

func getSummary(t *testing.T, handler http.Handler, token string) (map[string]HistorySummaryEntry, string) {
	t.Helper()
	rr := doReq(t, handler, "GET", "/nodes", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes: status=%d body=%s", rr.Code, rr.Body.String())
	}
	var list struct {
		Nodes []struct {
			HistorySummary map[string]HistorySummaryEntry `json:"history_summary,omitempty"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, rr.Body.String())
	}
	if len(list.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1 body=%s", len(list.Nodes), rr.Body.String())
	}
	return list.Nodes[0].HistorySummary, rr.Body.String()
}

// TestHistorySummaryColdContentionStats é o fixture cheio do plano: 5 solo
// @1s (inflight==1, cpu 40) + 5 contended @2.3s (inflight==2) + 2 cold @30s =>
// solo_p95 1000, contended_p95 2300, fator 2.3, cold_count 2, cold_mean 30000,
// solo_cpu_pct_mean 40.
func TestHistorySummaryColdContentionStats(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	handler, h, token := newSummaryEnv(t, now)
	sc := history.ScenarioContext{LoadBucket: history.Bucket75Plus}
	appendN := func(n, inflight int, latency int64, cold bool, cpu float64) {
		t.Helper()
		for i := 0; i < n; i++ {
			e := history.Entry{
				FunctionName:      "alpr-yolo",
				NodeID:            "n1",
				Timestamp:         now,
				ScenarioContext:   sc,
				ObservedLatencyMS: latency,
				Outcome:           history.OutcomeSucesso,
				Inflight:          inflight,
				CPUPctAvg:         cpu,
				ColdStart:         cold,
			}
			if err := h.AppendWithGroup(e, "alpr-yolo"); err != nil {
				t.Fatalf("AppendWithGroup: %v", err)
			}
		}
	}
	appendN(5, 1, 1000, false, 40)
	appendN(5, 2, 2300, false, 0)
	appendN(2, 1, 30000, true, 0)

	summary, _ := getSummary(t, handler, token)
	key := history.SegmentKey("alpr-yolo", "n1", sc)
	got, ok := summary[key]
	if !ok {
		t.Fatalf("summary sem chave %q (keys=%v)", key, keysOf(summary))
	}
	if got.Count != 10 {
		t.Fatalf("count=%d, want 10", got.Count)
	}
	if got.P95MS != 2300 {
		t.Fatalf("p95_ms=%d, want 2300", got.P95MS)
	}
	if got.SoloP95MS != 1000 {
		t.Fatalf("solo_p95_ms=%d, want 1000", got.SoloP95MS)
	}
	if got.ContP95MS != 2300 {
		t.Fatalf("contended_p95_ms=%d, want 2300", got.ContP95MS)
	}
	if math.Abs(got.ContentionFactor-2.3) > 1e-9 {
		t.Fatalf("contention_factor=%v, want 2.3", got.ContentionFactor)
	}
	if got.ColdN != 2 {
		t.Fatalf("cold_count=%d, want 2", got.ColdN)
	}
	if got.ColdMeanMS != 30000 {
		t.Fatalf("cold_mean_ms=%d, want 30000", got.ColdMeanMS)
	}
	if math.Abs(got.SoloCPUPctMean-40) > 1e-9 {
		t.Fatalf("solo_cpu_pct_mean=%v, want 40", got.SoloCPUPctMean)
	}
	wantRPS := 12.0 / 60.0
	if math.Abs(got.RecentRPS-wantRPS) > 1e-9 {
		t.Fatalf("recent_rps=%v, want %v", got.RecentRPS, wantRPS)
	}
}

// TestHistorySummaryThinClassesOmitFactor: classes finas (solo<5) => helper
// EffectiveContentionFactor devolve ok=false e o campo é omitido do JSON,
// mas os campos de classe disponíveis (contended_p95, cold) seguem.
func TestHistorySummaryThinClassesOmitFactor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	handler, h, token := newSummaryEnv(t, now)
	sc := history.ScenarioContext{LoadBucket: history.Bucket75Plus}
	mk := func(inflight int, latency int64, cold bool) history.Entry {
		return history.Entry{
			FunctionName:      "alpr-yolo",
			NodeID:            "n1",
			Timestamp:         now,
			ScenarioContext:   sc,
			ObservedLatencyMS: latency,
			Outcome:           history.OutcomeSucesso,
			Inflight:          inflight,
			ColdStart:         cold,
		}
	}
	// Apenas 2 solo (< MinClassSamples) + 5 contended + 2 cold.
	for i := 0; i < 2; i++ {
		if err := h.AppendWithGroup(mk(1, 1000, false), "alpr-yolo"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := h.AppendWithGroup(mk(2, 2300, false), "alpr-yolo"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := h.AppendWithGroup(mk(1, 30000, true), "alpr-yolo"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}

	summary, raw := getSummary(t, handler, token)
	key := history.SegmentKey("alpr-yolo", "n1", sc)
	got, ok := summary[key]
	if !ok {
		t.Fatalf("summary sem chave %q (keys=%v)", key, keysOf(summary))
	}
	if got.ContentionFactor != 0 {
		t.Fatalf("contention_factor=%v, want 0 (omitido)", got.ContentionFactor)
	}
	if strings.Contains(raw, `"contention_factor"`) {
		t.Fatalf("contention_factor deveria estar omitido (classe solo fina): %s", raw)
	}
	if got.SoloP95MS != 1000 {
		t.Fatalf("solo_p95_ms=%d, want 1000", got.SoloP95MS)
	}
	if got.ContP95MS != 2300 {
		t.Fatalf("contended_p95_ms=%d, want 2300", got.ContP95MS)
	}
	if got.ColdN != 2 || got.ColdMeanMS != 30000 {
		t.Fatalf("cold_count=%d cold_mean_ms=%d, want 2/30000", got.ColdN, got.ColdMeanMS)
	}
}

// TestHistorySummaryColdOnlyReportsCold: segmento SEM solo (fator omitido),
// mas o gate Total==0 deixa o só-cold reportar cold_count/cold_mean_ms.
func TestHistorySummaryColdOnlyReportsCold(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	handler, h, token := newSummaryEnv(t, now)
	sc := history.ScenarioContext{LoadBucket: history.Bucket75Plus}
	for i := 0; i < 2; i++ {
		e := history.Entry{
			FunctionName:      "alpr-yolo",
			NodeID:            "n1",
			Timestamp:         now,
			ScenarioContext:   sc,
			ObservedLatencyMS: 30000,
			Outcome:           history.OutcomeSucesso,
			Inflight:          1,
			ColdStart:         true,
		}
		if err := h.AppendWithGroup(e, "alpr-yolo"); err != nil {
			t.Fatalf("AppendWithGroup: %v", err)
		}
	}

	summary, raw := getSummary(t, handler, token)
	key := history.SegmentKey("alpr-yolo", "n1", sc)
	got, ok := summary[key]
	if !ok {
		t.Fatalf("summary sem chave %q (keys=%v)", key, keysOf(summary))
	}
	// Contrato congelado presente mesmo sem sucesso warm (count/p95 zero).
	if got.Count != 0 || got.P95MS != 0 {
		t.Fatalf("count=%d p95_ms=%d, want 0/0", got.Count, got.P95MS)
	}
	if !strings.Contains(raw, `"count"`) || !strings.Contains(raw, `"p95_ms"`) || !strings.Contains(raw, `"recent_rps"`) {
		t.Fatalf("contrato congelado ausente no JSON: %s", raw)
	}
	if got.ColdN != 2 || got.ColdMeanMS != 30000 {
		t.Fatalf("cold_count=%d cold_mean_ms=%d, want 2/30000", got.ColdN, got.ColdMeanMS)
	}
	if got.ContentionFactor != 0 || strings.Contains(raw, `"contention_factor"`) {
		t.Fatalf("contention_factor deveria estar omitido sem solo: %s", raw)
	}
	if strings.Contains(raw, `"solo_p95_ms"`) {
		t.Fatalf("solo_p95_ms deveria estar omitido sem solo: %s", raw)
	}
	if strings.Contains(raw, `"contended_p95_ms"`) {
		t.Fatalf("contended_p95_ms deveria estar omitido sem contended: %s", raw)
	}
}
