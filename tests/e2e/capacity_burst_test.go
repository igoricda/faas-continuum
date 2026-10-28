// Package e2e_test, the burst of capacity with a sleep and a single instance.
//
// It proves the behavior end to end with a real gateway, a real history and a
// real invocation log, plus two simulated nodes over a test server, without
// containerd and without an external network. The scenario is fixed and is
// reused for the comparative measurement of the piggyback against the periodic
// report, so the number of requests, the concurrency and the sleep have to stay.
//
// Determinism: the saturated node applies a single instance with a real gate, a
// semaphore of one slot where the excess is rejected immediately, and an HTTP
// blocker holds the only slot during the whole burst, which announces that it
// acquired it without sleeping and hoping. The synchronization uses channels and
// a wait group, with a global timeout so the suite never hangs.
package e2e_test

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// Fixed parameters of the burst, which the comparative measurement reuses, so the
// number of requests, the concurrency and the sleep have to stay.
const (
	burstN       = 10
	burstConc    = 10
	burstSleepMS = 200
)

// The invocation token is forwarded verbatim by the gateway to the node, since the
// arbiter neither interprets nor records it and the enforcement is on the node,
// as in production.
const burstInvokeToken = "Bearer e2e-invoke-token"

var burstLimits = selection.LimitsConfig{
	MaxCPUPct:   85,
	MaxMemPct:   90,
	MaxPSIAvg10: 25,
}

// simNode is a test server node with a real gate of the maximum of instances: a
// nil value means an unlimited capacity, while a gate of one slot rejects the
// excess immediately with an exhausted pool, without queueing it.
type simNode struct {
	id       string
	token    string
	sem      chan struct{} // nil = sem teto
	acquired chan struct{} // signal of the acquisition, buffered, which proves that the slot is taken
	hits     atomic.Int64
	rejected atomic.Int64
}

func (s *simNode) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != s.token {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"UNAUTHORIZED","message":"credencial inválida"}`))
			return
		}
		if r.URL.Path != "/function/sleep/invoke" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"FUNCTION_NOT_FOUND","message":"função não encontrada"}`))
			return
		}
		var body struct {
			Input map[string]any `json:"input"`
		}
		// Parse tolerante: extrai sleep_ms do input; default = burstSleepMS.
		sleepMS := burstSleepMS
		if data := readBody(r); json.Unmarshal(data, &body) == nil {
			if f, ok := body.Input["sleep_ms"].(float64); ok && f > 0 {
				sleepMS = int(f)
			}
		}
		s.hits.Add(1)
		if s.sem != nil {
			select {
			case s.sem <- struct{}{}:
				defer func() { <-s.sem }()
			default:
				s.rejected.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"code":"POOL_EXHAUSTED","message":"pool esgotado para sleep"}`))
				return
			}
		}
		select {
		case s.acquired <- struct{}{}:
		default:
		}
		start := time.Now()
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
		elapsed := time.Since(start).Milliseconds()
		w.Header().Set("Content-Type", "application/json")
		resp, _ := json.Marshal(map[string]any{
			"result": "ok-from-" + s.id,
			"execution_metrics": map[string]any{
				"response_time_ms": elapsed,
				"function_time_ms": elapsed,
				"cold_start":       false,
				"instance_id":      "e2e-" + s.id,
			},
		})
		_, _ = w.Write(resp)
	})
}

// burstResult holds the outcome of one invocation through the gateway.
type burstResult struct {
	status int
	code   string
	latMS  int64
}

// runBurst fires concurrent invocations, one at a time under a starting barrier,
// against the handler of the gateway and returns the results. A global timeout
// of thirty seconds fails the test, so the suite never hangs.
func runBurst(t *testing.T, gw *gateway.Gateway, n, conc, sleepMS int) []burstResult {
	t.Helper()
	out := make([]burstResult, n)
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	sem := make(chan struct{}, conc)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			<-startGate
			payload, _ := json.Marshal(map[string]any{
				"function": "sleep",
				"input":    map[string]any{"sleep_ms": sleepMS},
			})
			req := httptest.NewRequest(http.MethodPost, "/invoke", bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", burstInvokeToken)
			rec := httptest.NewRecorder()
			start := time.Now()
			gw.Handle(rec, req)
			lat := time.Since(start).Milliseconds()
			var er struct {
				Code string `json:"code"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &er)
			out[i] = burstResult{status: rec.Code, code: er.Code, latMS: lat}
		}(i)
	}
	close(startGate)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return out
	case <-time.After(30 * time.Second):
		t.Fatal("runBurst: timeout de 30s — possível hang no Gateway/nós")
		return nil
	}
}

// burstArbiter builds the real arbiter, with the store of nodes, the monitor, the
// history, the invocation log, the risk selector and the gateway, over the
// simulated nodes.
func burstArbiter(t *testing.T, dir, histName, logName string, regs []nodes.Node) (*gateway.Gateway, *history.History, string, string) {
	t.Helper()
	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	for _, n := range regs {
		if _, err := ns.Upsert(n); err != nil {
			t.Fatalf("Upsert %s: %v", n.NodeID, err)
		}
	}
	mon := monitor.NewMonitor()
	histPath := filepath.Join(dir, histName)
	h, err := history.NewHistory(histPath, 1000)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	logPath := filepath.Join(dir, logName)
	l, err := gateway.NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	// The canonical selector, with a learner fed by the replay of the seed and a
	// live hook, so the history of the burst teaches the model during the run
	// itself.
	riskCfg := selection.DefaultRiskConfig()
	learner := learn.NewLearner(riskCfg.LearnConfig())
	selection.ReplayRiskHistory(h, learner)
	selection.AttachRiskObserver(h, learner)
	sel := &selection.RiskSelector{
		Base:             selection.QueueSelector{Mon: mon, Cfg: burstLimits},
		Hist:             h,
		Criteria:         selection.DefaultCriteria,
		ThroughputWindow: time.Minute,
		Clock:            time.Now,
		Learner:          learner,
		Cfg:              riskCfg,
	}
	gw := gateway.NewGatewayWithHistory(gateway.GatewayConfig{
		Nodes:         ns,
		Monitor:       mon,
		Selector:      sel,
		History:       h,
		Criteria:      selection.DefaultCriteria,
		Clock:         time.Now,
		DefaultBudget: 10 * time.Second,
		MinSamples:    5,
		MaxRaceFanout: 0,
	})
	gw.SetInvocationLog(l)
	t.Cleanup(func() {
		_ = l.Close()
		_ = h.Close()
	})
	return gw, h, histPath, logPath
}

// holdSlot holds the only slot of the node over a real HTTP call with a long
// sleep and returns only after the slot was acquired, so there is no sleep and
// no hope.
func holdSlot(t *testing.T, nodeURL string, sleepMS int) (waitDone func()) {
	t.Helper()
	type blockerRes struct {
		status int
	}
	done := make(chan blockerRes, 1)
	go func() {
		payload, _ := json.Marshal(map[string]any{
			"function": "sleep",
			"input":    map[string]any{"sleep_ms": sleepMS},
		})
		req, _ := http.NewRequest(http.MethodPost, nodeURL+"/function/sleep/invoke", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", burstInvokeToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- blockerRes{status: -1}
			return
		}
		defer resp.Body.Close()
		drainResp(resp)
		done <- blockerRes{status: resp.StatusCode}
	}()
	return func() {
		select {
		case r := <-done:
			if r.status != http.StatusOK {
				t.Fatalf("bloqueador: status = %d, want 200", r.status)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("bloqueador: timeout aguardando liberação do slot")
		}
	}
}

// p95deOrdenados replica a regra do History (ceil(0.95*n)-1 sobre ordenados).
func p95deOrdenados(vals []int64) int64 {
	cp := append([]int64(nil), vals...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(math.Ceil(0.95*float64(len(cp)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

// archiveEvidence copia os dumps do run para /tmp/e2e-capacity/.
func archiveEvidence(t *testing.T, srcHist, srcLog, dstHist, dstLog string) {
	t.Helper()
	if err := os.MkdirAll("/tmp/e2e-capacity", 0755); err != nil {
		t.Fatalf("MkdirAll /tmp/e2e-capacity: %v", err)
	}
	for _, cp := range [][2]string{{srcHist, dstHist}, {srcLog, dstLog}} {
		data, err := os.ReadFile(cp[0])
		if err != nil {
			t.Fatalf("ReadFile %s: %v", cp[0], err)
		}
		if err := os.WriteFile(cp[1], data, 0600); err != nil {
			t.Fatalf("WriteFile %s: %v", cp[1], err)
		}
	}
	t.Logf("evidência arquivada: %s (%dB), %s", dstHist, len(mustReadFile(t, dstHist)), dstLog)
}

// sleepSegmentKeys returns the keys of the history for the sleep function.
func sleepSegmentKeys(h *history.History) []string {
	var out []string
	for _, k := range h.Keys() {
		if strings.HasPrefix(k, "sleep\x00") {
			out = append(out, k)
		}
	}
	return out
}

// TestE2E_CapacityBurstWithAlternative is the first scenario: the saturated edge
// with a single instance and the free unlimited fog. The order is deterministic
// from the layer, with the edge before the fog, so every invocation tries the
// edge first, which rejects it with an exhausted pool, and the history records
// the failure without a latency while the log keeps it and the variant is
// marked as exhausted, and then the request fails over to the fog, which
// succeeds. The invariants are that almost no invocation reports an overload,
// that the history of the fog holds only the successes, that the history of the
// edge holds only the rejections without a latency, that the log holds every
// attempt and that the percentile of the segment of the fog is the percentile of
// the successes.
func TestE2E_CapacityBurstWithAlternative(t *testing.T) {
	dir := t.TempDir()
	nodeB := &simNode{id: "edge-hot", token: burstInvokeToken, sem: make(chan struct{}, 1), acquired: make(chan struct{}, 64)}
	nodeA := &simNode{id: "fog-cold", token: burstInvokeToken, acquired: make(chan struct{}, 64)}
	srvB := httptest.NewServer(nodeB.handler())
	defer srvB.Close()
	srvA := httptest.NewServer(nodeA.handler())
	defer srvA.Close()

	hostOf := func(url string) string { return strings.TrimPrefix(url, "http://") }
	gw, h, histPath, logPath := burstArbiter(t, dir, "history-a.json", "invlog-a.jsonl", []nodes.Node{
		{NodeID: "edge-hot", Layer: nodes.LayerBorda, Host: hostOf(srvB.URL), Functions: []string{"sleep"}, FunctionCapacity: map[string]int{"sleep": 1}},
		{NodeID: "fog-cold", Layer: nodes.LayerNevoa, Host: hostOf(srvA.URL), Functions: []string{"sleep"}},
	})

	// Saturate the edge: the HTTP blocker holds the only slot for much longer than the burst.
	waitBlocker := holdSlot(t, srvB.URL, 3000)
	select {
	case <-nodeB.acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("bloqueador não adquiriu o slot de B em 5s")
	}
	baseB := nodeB.hits.Load() // the blocker hit, outside the burst count

	results := runBurst(t, gw, burstN, burstConc, burstSleepMS)

	var nOK int
	var successLat []int64
	var nOverloaded int
	for _, r := range results {
		if r.status == http.StatusOK {
			nOK++
			successLat = append(successLat, r.latMS)
		}
		if r.status == http.StatusServiceUnavailable && r.code == "NODE_OVERLOADED" {
			nOverloaded++
		}
	}
	t.Logf("rajada com alternativa: ok=%d/%d overloaded=%d hitsA=%d hitsB=%d rejB=%d",
		nOK, burstN, nOverloaded, nodeA.hits.Load(), nodeB.hits.Load()-baseB, nodeB.rejected.Load())
	if nOK*10 < burstN*9 {
		t.Fatalf("roteamento: ok=%d/%d, want >=90%% sem NODE_OVERLOADED", nOK, burstN)
	}
	burstHitsB := nodeB.hits.Load() - baseB
	if burstHitsB < 1 {
		t.Fatal("failover não exercitado: B (saturado) recebeu 0 tentativas da rajada")
	}
	if nodeB.rejected.Load() != burstHitsB {
		t.Fatalf("rejB=%d, want == tentativas da rajada em B (%d): B saturado rejeita tudo", nodeB.rejected.Load(), burstHitsB)
	}
	if nodeA.hits.Load() != int64(nOK) {
		t.Fatalf("hitsA=%d, want == sucessos (%d): A deve servir cada sucesso 1x", nodeA.hits.Load(), nOK)
	}

	// The history holds two segments for the sleep function, the cold fog with only
	// the successes and the hot edge with only the capacity failures, where the
	// total grows without a latency and the count and the percentile stay
	// untouched.
	keys := sleepSegmentKeys(h)
	if len(keys) != 2 {
		t.Fatalf("segmentos sleep = %d (%v), want 2 (sucessos em A + falhas em B)", len(keys), keys)
	}
	group := selection.ResolveRequestGroup("sleep", []nodes.Node{
		{NodeID: "edge-hot", Functions: []string{"sleep"}},
		{NodeID: "fog-cold", Functions: []string{"sleep"}},
	})
	sc := history.ScenarioContext{LoadBucket: history.BucketUnknown}
	wantKeyA := history.SegmentKey(group, "fog-cold", sc)
	wantKeyB := history.SegmentKey(group, "edge-hot", sc)
	temChave := map[string]bool{}
	for _, k := range keys {
		temChave[k] = true
	}
	if !temChave[wantKeyA] || !temChave[wantKeyB] {
		t.Fatalf("segmentos = %v, want %q e %q", keys, wantKeyA, wantKeyB)
	}
	countA, segP95, totalA, _, okA := h.Stats(wantKeyA)
	if !okA {
		t.Fatal("Stats do segmento de sucesso: ok=false, want true")
	}
	if countA != nOK || totalA != nOK {
		t.Fatalf("History de A: Count=%d Total=%d, want Count=Total=%d", countA, totalA, nOK)
	}
	countB, p95B, totalB, _, okB := h.Stats(wantKeyB)
	if okB {
		t.Fatal("Stats do segmento de falhas: ok=true, want false (sem latência)")
	}
	if countB != 0 || p95B != 0 {
		t.Fatalf("History de B: Count=%d p95=%d, want 0/0 (falha de capacidade sem latência)", countB, p95B)
	}
	// The rejections of the edge are saturation: outside the service rate but
	// counted separately. Before they entered the total and lowered the rate,
	// which is what banned the saturated nodes, and that is not what this test
	// means to say.
	if totalB != 0 {
		t.Fatalf("History de B: Total=%d, want 0 (rejeição é saturação, não serviço)", totalB)
	}
	if fullB, okFullB := h.StatsFull(wantKeyB, time.Minute, time.Now()); !okFullB || fullB.Saturation != int(burstHitsB) {
		t.Fatalf("History de B: Saturação=%d ok=%v, want %d/true", fullB.Saturation, okFullB, burstHitsB)
	}
	clientP95 := p95deOrdenados(successLat)
	t.Logf("p95: segmento=%dms cliente-fim-a-fim=%dms (inclui failover B→A)", segP95, clientP95)

	// The log holds every attempt, the successes of the fog and the rejections of the edge.
	lines := nonEmptyLines(t, mustReadFile(t, logPath))
	wantLines := int64(nOK) + burstHitsB
	if int64(len(lines)) != wantLines {
		t.Fatalf("invLog linhas=%d, want %d (sucessos + tentativas em B)", len(lines), wantLines)
	}
	var okLines, rejLines int
	var invLogSuccessLat []int64
	for i, ln := range lines {
		var rec struct {
			FunctionName      string `json:"function_name"`
			NodeID            string `json:"node_id"`
			Outcome           string `json:"outcome"`
			ObservedLatencyMS int64  `json:"observed_latency_ms"`
		}
		if err := json.Unmarshal(ln, &rec); err != nil {
			t.Fatalf("invLog linha %d inválida: %v", i, err)
		}
		if rec.FunctionName != "sleep" {
			t.Fatalf("invLog linha %d function=%q, want sleep", i, rec.FunctionName)
		}
		switch rec.Outcome {
		case history.OutcomeSucesso:
			okLines++
			invLogSuccessLat = append(invLogSuccessLat, rec.ObservedLatencyMS)
			if rec.NodeID != "fog-cold" {
				t.Fatalf("invLog sucesso no nó %q, want fog-cold", rec.NodeID)
			}
		default:
			rejLines++
			if rec.NodeID != "edge-hot" {
				t.Fatalf("invLog rejeição no nó %q, want edge-hot", rec.NodeID)
			}
		}
	}
	if okLines != nOK || int64(rejLines) != burstHitsB {
		t.Fatalf("invLog ok=%d rej=%d, want ok=%d rej=%d", okLines, rejLines, nOK, burstHitsB)
	}
	// The percentile of the segment of the fog is the percentile of the successes:
	// the log holds every attempt, the successes and the fast rejections, but the
	// rejections enter the history without a latency, so the percentile of the
	// segment stays the one of the successes.
	successP95 := p95deOrdenados(invLogSuccessLat)
	t.Logf("p95 só-sucesso (invLog)=%dms", successP95)
	if d := segP95 - successP95; d < -1 || d > 1 {
		t.Fatalf("p95 segmento=%dms, p95 só-sucesso=%dms: diff > ±1ms (rejeições vazaram para o History?)", segP95, successP95)
	}

	waitBlocker()
	archiveEvidence(t, histPath, logPath, "/tmp/e2e-capacity/history.json", "/tmp/e2e-capacity/invlog.jsonl")
}

// TestE2E_CapacityBurstSingleSaturatedNode is the second scenario: a single node
// with one instance, saturated by a blocker and with no alternative. The burst
// must receive the overload rejection quickly, much faster than the sleep, with
// the rejections in the history as failures without a latency and every attempt
// in the log.
func TestE2E_CapacityBurstSingleSaturatedNode(t *testing.T) {
	dir := t.TempDir()
	nodeC := &simNode{id: "edge-solo", token: burstInvokeToken, sem: make(chan struct{}, 1), acquired: make(chan struct{}, 64)}
	srvC := httptest.NewServer(nodeC.handler())
	defer srvC.Close()

	hostOf := func(url string) string { return strings.TrimPrefix(url, "http://") }
	gw, h, histPath, logPath := burstArbiter(t, dir, "history-b.json", "invlog-b.jsonl", []nodes.Node{
		{NodeID: "edge-solo", Layer: nodes.LayerBorda, Host: hostOf(srvC.URL), Functions: []string{"sleep"}, FunctionCapacity: map[string]int{"sleep": 1}},
	})

	waitBlocker := holdSlot(t, srvC.URL, 3000)
	select {
	case <-nodeC.acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("bloqueador não adquiriu o slot de C em 5s")
	}
	baseC := nodeC.hits.Load() // the blocker hit, outside the burst count

	results := runBurst(t, gw, burstN, burstConc, burstSleepMS)

	for i, r := range results {
		if r.status != http.StatusServiceUnavailable || r.code != "NODE_OVERLOADED" {
			t.Fatalf("req %d: status=%d code=%q, want 503 NODE_OVERLOADED", i, r.status, r.code)
		}
		if r.latMS >= burstSleepMS {
			t.Fatalf("req %d: lat=%dms, want rejeição rápida (<< %dms sleep)", i, r.latMS, burstSleepMS)
		}
	}
	t.Logf("rajada sem alternativa: 503=%d/%d hitsC=%d rejC=%d", burstN, burstN, nodeC.hits.Load()-baseC, nodeC.rejected.Load())

	// The history holds one segment for the sleep function with the rejections, where
	// the total grows without a latency and the count and the percentile stay
	// untouched.
	keys := sleepSegmentKeys(h)
	if len(keys) != 1 {
		t.Fatalf("segmentos sleep = %v, want 1 (falhas de C sem latência)", keys)
	}
	group := selection.ResolveRequestGroup("sleep", []nodes.Node{
		{NodeID: "edge-solo", Functions: []string{"sleep"}},
	})
	wantKey := history.SegmentKey(group, "edge-solo", history.ScenarioContext{LoadBucket: history.BucketUnknown})
	if keys[0] != wantKey {
		t.Fatalf("segmento = %q, want %q", keys[0], wantKey)
	}
	count, segP95, total, rate, ok := h.Stats(wantKey)
	if ok {
		t.Fatal("Stats do segmento: ok=true, want false (falha sem latência)")
	}
	if count != 0 || segP95 != 0 {
		t.Fatalf("Count=%d p95=%d, want 0/0 (falha de capacidade sem latência)", count, segP95)
	}
	// A rejection is saturation, outside the service rate and counted apart. What
	// this test ensures is that the saturated node is recorded as such and that the
	// client received the overload rejection in every case.
	if total != 0 {
		t.Fatalf("Total=%d, want 0 (rejeição é saturação, não serviço)", total)
	}
	if full, okFull := h.StatsFull(wantKey, time.Minute, time.Now()); !okFull || full.Saturation != burstN {
		t.Fatalf("Saturação=%d ok=%v, want %d/true", full.Saturation, okFull, burstN)
	}
	// What matters in the rate is that the node cannot be vetoed for having
	// refused, so the assertion checks that the rate is above the threshold and
	// not the other way around, since the previous assertion passed vacuously.
	if history.VetoByRate(total, rate) {
		t.Fatalf("taxa=%.2f: rejeição de capacidade vetou o nó (saturação não veta)", rate)
	}

	// The log holds every attempt, all of them rejections of the node.
	lines := nonEmptyLines(t, mustReadFile(t, logPath))
	if burstHitsC := nodeC.hits.Load() - baseC; int64(len(lines)) != burstHitsC || len(lines) != burstN {
		t.Fatalf("invLog linhas=%d hitsC=%d, want %d (1 tentativa por invoke, sem failover)", len(lines), burstHitsC, burstN)
	}
	for i, ln := range lines {
		var rec struct {
			FunctionName string `json:"function_name"`
			NodeID       string `json:"node_id"`
			Outcome      string `json:"outcome"`
		}
		if err := json.Unmarshal(ln, &rec); err != nil {
			t.Fatalf("invLog linha %d inválida: %v", i, err)
		}
		if rec.NodeID != "edge-solo" || rec.FunctionName != "sleep" {
			t.Fatalf("invLog linha %d = %+v, want sleep/edge-solo", i, rec)
		}
		if rec.Outcome == history.OutcomeSucesso {
			t.Fatalf("invLog linha %d com sucesso — nó estava saturado", i)
		}
	}

	waitBlocker()
	archiveEvidence(t, histPath, logPath, "/tmp/e2e-capacity/history-single.json", "/tmp/e2e-capacity/invlog-single.jsonl")
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	return data
}

func nonEmptyLines(t *testing.T, raw []byte) [][]byte {
	t.Helper()
	var out [][]byte
	for _, ln := range bytes.Split(raw, []byte("\n")) {
		if len(bytes.TrimSpace(ln)) == 0 {
			continue
		}
		out = append(out, ln)
	}
	return out
}

func readBody(r *http.Request) []byte {
	buf := make([]byte, 64*1024)
	var out []byte
	for {
		n, err := r.Body.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out
		}
	}
}

func drainResp(resp *http.Response) {
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n == 0 || err != nil {
			return
		}
	}
}
