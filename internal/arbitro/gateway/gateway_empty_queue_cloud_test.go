package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// Regression of the empty queue: no candidate, either from the absence of an
// active or hosting node, from every node being overloaded or projected above
// the ceiling, unviable or filtered by a mark, has to reach the cloud fallback
// when it is configured. Without a cloud, the answer of no node available stays
// byte identical to the legacy path, including the recording of the absence.

// newEmptyQueueInvLog opens an isolated invocation log and returns the logger and its path.
func newEmptyQueueInvLog(t *testing.T) (*InvocationLog, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "invocations.jsonl")
	l, err := NewInvocationLog(path)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	return l, path
}

// readEmptyQueueInvRecords closes the log and returns the decoded lines.
func readEmptyQueueInvRecords(t *testing.T, l *InvocationLog, path string) []InvocationRecord {
	t.Helper()
	if err := l.Close(); err != nil {
		t.Fatalf("Close invLog: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile invLog: %v", err)
	}
	var recs []InvocationRecord
	for _, ln := range splitLines(t, raw) {
		var r InvocationRecord
		if err := json.Unmarshal(ln, &r); err != nil {
			t.Fatalf("linha inválida %q: %v", ln, err)
		}
		recs = append(recs, r)
	}
	return recs
}

func newCloudOKServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cloud":"ok"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

type emptyQueueCloudResp struct {
	Result           json.RawMessage `json:"result"`
	ExecutionMetrics struct {
		CloudFallback bool `json:"cloud_fallback"`
	} `json:"execution_metrics"`
}

// An empty queue with a configured cloud and a successful forwarding gives a
// success marked as a cloud fallback, and the absence of a node is never
// recorded when the cloud served the answer.
func TestGateway_EmptyQueueCloudFallback(t *testing.T) {
	cloudSrv := newCloudOKServer(t)

	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:          ns,
		Monitor:        monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
	})
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	var resp emptyQueueCloudResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v body=%s", err, rec.Body.String())
	}
	if !resp.ExecutionMetrics.CloudFallback {
		t.Fatalf("cloud_fallback=false want true body=%s", rec.Body.String())
	}

	for _, r := range readEmptyQueueInvRecords(t, il, path) {
		if r.NodeID == noNodeAvailableID {
			t.Fatalf("recordNoNode chamado mesmo com a nuvem servindo a resposta")
		}
	}
}

// An empty queue without a cloud gives the same answer as before, with the
// status, the body and the retryable flag pinned, and the absence of a node is
// recorded as before, so a future change cannot silently alter the envelope of
// the legacy path.
func TestGateway_EmptyQueueNoCloud503ByteIdentical(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	gw := newTestGateway(ns)
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	const wantBody = `{"code":"NO_NODE_AVAILABLE","message":"nenhum nó disponível para função \"fn\"","retryable":true,"fallback_applied":false}`
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content-type=%q want application/json", got)
	}
	if rec.Body.String() != wantBody {
		t.Fatalf("body divergiu do fixture byte-idêntico:\n got=%q\nwant=%q", rec.Body.String(), wantBody)
	}

	recs := readEmptyQueueInvRecords(t, il, path)
	if len(recs) != 1 {
		t.Fatalf("invLog lines=%d want 1 (recordNoNode)", len(recs))
	}
	if recs[0].NodeID != noNodeAvailableID || recs[0].Outcome != "falha" || recs[0].SloMet {
		t.Fatalf("linha no-cloud = %+v, want node=%s outcome=falha slo_met=false", recs[0], noNodeAvailableID)
	}
}

// Every candidate excluded by overload, projected or observed, with a cloud
// configured, reaches the cloud, and the overloaded nodes are never touched.
func TestGateway_AllOverloadedCloudFallback(t *testing.T) {
	var nodeHits int32
	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nodeHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node":true}`))
	}))
	t.Cleanup(nodeSrv.Close)
	cloudSrv := newCloudOKServer(t)

	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "over", Layer: nodes.LayerBorda, Host: hostFromURL(t, nodeSrv.URL)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	mon := monitor.NewMonitor()
	// A processor usage of ninety-five is above the ceiling of eighty-five, so
	// the node is excluded both by the overload filter and by the hard guard of
	// the projected load, of ninety-five against eighty-five, and the queue
	// reaches the cloud empty.
	mon.Update(newPkg("over", 95))

	now := time.Now()
	risk := &selection.RiskSelector{
		Base:             selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		Hist:             nil,
		ThroughputWindow: 60 * time.Second,
		Criteria:         selection.DefaultCriteria,
		Clock:            fixedClock(now),
		Learner:          learn.NewLearner(selection.DefaultRiskConfig().LearnConfig()),
		Cfg:              selection.DefaultRiskConfig(),
	}
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:          ns,
		Monitor:        mon,
		Selector:       risk,
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
		MinSamples:     1,
		Criteria:       selection.DefaultCriteria,
		Clock:          time.Now,
	})
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	var resp emptyQueueCloudResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v body=%s", err, rec.Body.String())
	}
	if !resp.ExecutionMetrics.CloudFallback {
		t.Fatalf("cloud_fallback=false want true body=%s", rec.Body.String())
	}
	if got := atomic.LoadInt32(&nodeHits); got != 0 {
		t.Fatalf("nó sobrecarregado foi contatado: hits=%d want 0", got)
	}
	for _, r := range readEmptyQueueInvRecords(t, il, path) {
		if r.NodeID == noNodeAvailableID {
			t.Fatalf("recordNoNode chamado mesmo com a nuvem servindo a resposta")
		}
	}
}

// An empty queue with a configured cloud that fails gives a bad gateway marked
// as a failed cloud fallback, and the retryable flag is preserved from the root
// cause, which is the absence of a node.
func TestGateway_EmptyQueueCloudFailure(t *testing.T) {
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("cloud down"))
	}))
	t.Cleanup(cloudSrv.Close)

	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:          ns,
		Monitor:        monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
	})
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "CLOUD_FALLBACK_FAILED" || !er.Retryable || !er.FallbackApplied {
		t.Fatalf("code=%q retryable=%v fallback=%v want CLOUD_FALLBACK_FAILED/true/true", er.Code, er.Retryable, er.FallbackApplied)
	}
	for _, r := range readEmptyQueueInvRecords(t, il, path) {
		if r.NodeID == noNodeAvailableID {
			t.Fatalf("recordNoNode chamado mesmo na tentativa de nuvem")
		}
	}
}

// A tiny objective makes the budget expire before the call to the cloud, so
// the answer of no node available is written, the cloud is not called and the
// absence of a node is not recorded, since the path of the cloud answered.
func TestGateway_EmptyQueueCloudBudgetExpired(t *testing.T) {
	var cloudCalls int32
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&cloudCalls, 1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"cloud":"ok"}`))
	}))
	t.Cleanup(cloudSrv.Close)

	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:          ns,
		Monitor:        monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: newTestRemoteExecutor(t, cloudSrv.URL),
	})
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","slo":{"max_latency_ms":1},"input":{"a":1}}`))
	// A deadline of the parent that already expired guarantees a non-positive
	// remaining budget deterministically, while the overall objective stays at
	// one millisecond.
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	req = req.WithContext(expired)

	rec := httptest.NewRecorder()
	gw.Handle(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NO_NODE_AVAILABLE" || !er.Retryable || er.FallbackApplied {
		t.Fatalf("code=%q retryable=%v fallback=%v want NO_NODE_AVAILABLE/true/false", er.Code, er.Retryable, er.FallbackApplied)
	}
	if got := atomic.LoadInt32(&cloudCalls); got != 0 {
		t.Fatalf("nuvem chamada com remaining<=0: calls=%d want 0", got)
	}
	if recs := readEmptyQueueInvRecords(t, il, path); len(recs) != 0 {
		t.Fatalf("invLog lines=%d want 0 (nem nuvem nem recordNoNode no caminho remaining<=0)", len(recs))
	}
}
