package gateway

// Tests of the race between the cloud and the wait, which is a single policy
// without a switch: the cloud wins with the pending reports cancelled, an orphan
// wins with the cloud cancelled and recorded, without pending reports the path is
// the previous one, the cloud sees the hard deadline, and when the hard deadline
// expires the losing cloud records alone.
//
// Each case protects a distinct branch of the race. The credible regressions are
// a sequential cloud after the wait, which loses deliveries, a loser without a
// record, which blinds the replay, and a deadline on the overall budget instead
// of the hard one, which kills the window of a cold cloud.
import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// raceRemote is the controllable remote executor of the race: it delays or gates
// the answer, captures the deadline of the context, which proves the hard
// deadline, and honors the cancellation, which makes the join of the loser
// deterministic.
type raceRemote struct {
	configured bool
	delay      time.Duration
	release    chan struct{}
	body       []byte
	err        error
	calls      *int32
	mu         sync.Mutex
	deadline   time.Time
	hasDL      bool
}

func (f *raceRemote) Configured() bool { return f.configured }

func (f *raceRemote) Invoke(ctx context.Context, fn string, input json.RawMessage) ([]byte, error) {
	if f.calls != nil {
		atomic.AddInt32(f.calls, 1)
	}
	if dl, ok := ctx.Deadline(); ok {
		f.mu.Lock()
		f.deadline, f.hasDL = dl, true
		f.mu.Unlock()
	}
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	} else if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.body, nil
}

func (f *raceRemote) getDeadline() (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deadline, f.hasDL
}

func raceTestGateway(t *testing.T, ns *nodes.NodeStore, logPath string, remote RemoteExecutor) *Gateway {
	t.Helper()
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: mon,
		Selector:       selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		RemoteExecutor: remote,
	}).WithInflight(NewInflightGauge())
	if logPath != "" {
		l, err := NewInvocationLog(logPath)
		if err != nil {
			t.Fatalf("NewInvocationLog: %v", err)
		}
		t.Cleanup(func() { _ = l.Close() })
		gw.SetInvocationLog(l)
	}
	return gw
}

func blockedNodeServer(t *testing.T) (*httptest.Server, chan struct{}) {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"result":"solto"}`))
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	return srv, release
}

func hasCloudLine(recs []InvocationRecord) bool {
	for _, r := range recs {
		if r.NodeID == cloudFallbackNodeID {
			return true
		}
	}
	return false
}

// The cloud wins: it delivers the body of the cloud, and the local orphan is not
// cancelled, so it runs to completion, is settled with the real outcome, which
// feeds the learning and supersedes the exceeded budget, and is not delivered.
func TestCloudRaceCloudWins(t *testing.T) {
	srv, release := blockedNodeServer(t)
	var releaseOnce sync.Once
	releaseOrphan := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseOrphan()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	var calls int32
	remote := &raceRemote{configured: true, delay: 100 * time.Millisecond, body: []byte(`{"cloud":"ok"}`), calls: &calls}
	logPath := t.TempDir() + "/inv.jsonl"
	gw := raceTestGateway(t, ns, logPath, remote)

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":200},"input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Result           json.RawMessage `json:"result"`
		ExecutionMetrics struct {
			CloudFallback bool `json:"cloud_fallback"`
		} `json:"execution_metrics"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.ExecutionMetrics.CloudFallback {
		t.Fatalf("sem cloud_fallback:true: %s", rec.Body.String())
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("calls=%d want 1", calls)
	}
	recs := readInvLogLines(t, logPath)
	if !hasCloudLine(recs) {
		t.Fatalf("nuvem vencedora sem linha no invLog")
	}
	// Releasing the orphan lets the drain after the answer settle it with the real
	// outcome, which supersedes the exceeded budget in the history without a delivery to the client.
	releaseOrphan()
	deadline := time.Now().Add(3 * time.Second)
	for {
		raw, _ := os.ReadFile(logPath)
		settled := false
		for _, ln := range strings.Split(string(raw), "\n") {
			if strings.Contains(ln, `"node_id":"n1"`) && strings.Contains(ln, `"outcome":"sucesso"`) {
				settled = true
				break
			}
		}
		if settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("órfã não assentada no invLog em 3s: %s", string(raw))
		}
		time.Sleep(20 * time.Millisecond)
	}
	recs = readInvLogLines(t, logPath)
	var orphan *InvocationRecord
	for i := range recs {
		if recs[i].NodeID == "n1" && recs[i].Outcome == history.OutcomeSucesso {
			orphan = &recs[i]
		}
	}
	if orphan == nil || orphan.LateDelivered {
		t.Fatalf("órfã sem Entry real ou com late_delivered=true: %+v", recs)
	}
	if _, served, _, cancelled := gw.BestEffortCounters(); served != 0 || cancelled != 0 {
		t.Fatalf("served=%d cancelled=%d want 0/0 (órfã assentada, não entregue nem cancelada)", served, cancelled)
	}
}

// An orphan wins: the cloud is cancelled but the attempt stays in the log, which feeds the replay and the warm path.
func TestCloudRaceOrphanWins(t *testing.T) {
	srv := lateSuccessServer(t, 300*time.Millisecond, `{"result":"tardio"}`)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	remote := &raceRemote{configured: true, delay: 10 * time.Second, body: []byte(`{"cloud":"ok"}`)}
	logPath := t.TempDir() + "/inv.jsonl"
	gw := raceTestGateway(t, ns, logPath, remote)

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":200},"input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != `{"result":"tardio"}` {
		t.Fatalf("body=%q want o corpo do órfão", rec.Body.String())
	}
	if got := rec.Header().Get("X-Faas-Late"); got != "true" {
		t.Fatalf("X-Faas-Late=%q want true", got)
	}
	recs := readInvLogLines(t, logPath)
	var orphanOK, cloudTried bool
	for _, r := range recs {
		if r.NodeID == "n1" && r.Outcome == history.OutcomeSucesso {
			orphanOK = true
		}
		if r.NodeID == cloudFallbackNodeID {
			cloudTried = true
		}
	}
	if !orphanOK || !cloudTried {
		t.Fatalf("orfão=%v nuvem=%v want true/true (perdedora registrada): %+v", orphanOK, cloudTried, recs)
	}
	if _, served, _, _ := gw.BestEffortCounters(); served != 1 {
		t.Fatalf("served=%d want 1", served)
	}
}

// Without pending reports, from a legacy node, the path is the previous one,
// with a sequential cloud after the queue, without the late mark and without the counters.
func TestCloudRaceNoPendencyLegacyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"POOL_EXHAUSTED"}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL), Functions: []string{"fn"}})
	var calls int32
	remote := &raceRemote{configured: true, body: []byte(`{"cloud":"ok"}`), calls: &calls}
	logPath := t.TempDir() + "/inv.jsonl"
	gw := raceTestGateway(t, ns, logPath, remote)

	rec := invokeRecorder(t, gw, `{"function":"fn","input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"cloud_fallback":true`) {
		t.Fatalf("sem cloud_fallback: %s", rec.Body.String())
	}
	if got := rec.Header().Get("X-Faas-Late"); got != "" {
		t.Fatalf("X-Faas-Late=%q want ausente (caminho de hoje)", got)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("calls=%d want 1", calls)
	}
	if armed, served, expired, cancelled := gw.BestEffortCounters(); armed+served+expired+cancelled != 0 {
		t.Fatalf("contadores=%d/%d/%d/%d want zeros (sem pendência)", armed, served, expired, cancelled)
	}
}

// The cloud sees the hard deadline, and not the overall budget.
func TestCloudRaceSeesHardDeadline(t *testing.T) {
	srv, release := blockedNodeServer(t)
	defer close(release)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 2000))
	remote := &raceRemote{configured: true, body: []byte(`{"cloud":"ok"}`)}
	gw := raceTestGateway(t, ns, "", remote)

	testStart := time.Now()
	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":200},"input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (nuvem imediata) body=%s", rec.Code, rec.Body.String())
	}
	dl, ok := remote.getDeadline()
	if !ok {
		t.Fatalf("nuvem sem deadline no contexto")
	}
	// hard = max(200ms, ~2000ms) a partir do invoke.
	got := dl.Sub(testStart)
	if got < 1*time.Second || got > 3*time.Second {
		t.Fatalf("deadline=%v após o invoke want ~2s (hard, não overall de 200ms)", got)
	}
}

// The hard deadline expires with the cloud still alive, so the loser records
// alone in the log, for the replay and the warm path, even when the handler returns before the append.
func TestCloudRaceExpiryRegistersCloud(t *testing.T) {
	srv, release := blockedNodeServer(t)
	defer close(release)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 400)) // hard ≈ attemptStart+400ms
	remote := &raceRemote{configured: true, release: make(chan struct{})}
	logPath := t.TempDir() + "/inv.jsonl"
	gw := raceTestGateway(t, ns, logPath, remote)

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":200},"input":{"a":1}}`)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504 body=%s", rec.Code, rec.Body.String())
	}
	if _, served, expired, _ := gw.BestEffortCounters(); served != 0 || expired > 1 {
		t.Fatalf("served=%d expired=%d want 0/<=1 (sem entrega; cap×prazo empatam)", served, expired)
	}
	// The cloud dies at its own deadline, which is the hard one, and records in the
	// goroutine, possibly after the handler returns, so the test waits a short
	// deadline and looks for the node identifier in the raw text, which avoids a partial line.
	deadline := time.Now().Add(3 * time.Second)
	for {
		raw, _ := os.ReadFile(logPath)
		if strings.Contains(string(raw), cloudFallbackNodeID) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nuvem perdedora não registrada no invLog em %v", 3*time.Second)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
