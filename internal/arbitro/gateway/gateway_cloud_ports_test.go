package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// The connectors satisfy the port structurally, without importing the consumer,
// which inverts the dependency and respects the interface segregation.
var (
	_ RemoteExecutor = (*cloud.Connector)(nil)
	_ RemoteExecutor = (*cloud.AzureFunctionsConnector)(nil)
	_ RemoteExecutor = (*cloud.GenericHTTPConnector)(nil)
	_ ColdEstimator  = (*cloud.Connector)(nil)
	_ ColdEstimator  = (*cloud.AzureFunctionsConnector)(nil)
	_ ColdEstimator  = (*cloud.GenericHTTPConnector)(nil)
)

// fakeRemote is the minimal mock of the port, so the gateway is tested isolated
// from the cloud infrastructure.
type fakeRemote struct {
	configured bool
	body       []byte
	err        error
	cold       bool
	calls      *int32
	lastFn     string
}

func (f *fakeRemote) Configured() bool { return f.configured }

func (f *fakeRemote) EstimateCold(fn string) bool { return f.cold }

func (f *fakeRemote) Invoke(ctx context.Context, fn string, input json.RawMessage) ([]byte, error) {
	if f.calls != nil {
		atomic.AddInt32(f.calls, 1)
	}
	f.lastFn = fn
	if f.err != nil {
		return nil, f.err
	}
	return f.body, nil
}

func cloudTestFailure() gatewayFailure {
	return gatewayFailure{httpStatus: http.StatusServiceUnavailable, code: "NO_NODE_AVAILABLE", msg: `nenhum nó disponível para função "fn"`, retryable: true}
}

func cloudTestReq(fn string) InvocationRequest {
	return InvocationRequest{Function: fn, Input: json.RawMessage(`{"a":1}`)}
}

// Without a port or without a configuration the result is false, so the caller
// answers with the last failure of the queue.
func TestGatewayCloudPort_UnconfiguredReturnsFalse(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: monitor.NewMonitor(),
		Selector: selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/invoke", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if gw.serveCloudFallback(rec, req, ctx, 30*time.Second, time.Now(), cloudTestFailure(), cloudTestReq("fn")) {
		t.Fatalf("sem porta deveria retornar false")
	}

	var calls int32
	gw2 := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: &fakeRemote{configured: false, calls: &calls},
	})
	rec2 := httptest.NewRecorder()
	if gw2.serveCloudFallback(rec2, req, ctx, 30*time.Second, time.Now(), cloudTestFailure(), cloudTestReq("fn")) {
		t.Fatalf("não configurado deveria retornar false")
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("Invoke chamado sem config: calls=%d", calls)
	}
}

// A success through the port gives a success marked as a cloud fallback, with
// the instance of the cloud and the stages, and a non-JSON body becomes a JSON
// string.
func TestGatewayCloudPort_Success(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	var calls int32
	remote := &fakeRemote{configured: true, body: []byte(`{"cloud":"ok"}`), calls: &calls}
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: remote,
	})
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/invoke", nil)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !gw.serveCloudFallback(rec, req, ctx, 30*time.Second, start, cloudTestFailure(), cloudTestReq("fn")) {
		t.Fatalf("configurado deveria retornar true")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("calls=%d, esperado 1", calls)
	}
	if remote.lastFn != "fn" {
		t.Fatalf("fn repassado = %q, esperado fn", remote.lastFn)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, esperado 200 body=%s", rec.Code, rec.Body.String())
	}
	var resp emptyQueueCloudResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if !resp.ExecutionMetrics.CloudFallback {
		t.Fatalf("cloud_fallback=false body=%s", rec.Body.String())
	}
	if string(resp.Result) != `{"cloud":"ok"}` {
		t.Fatalf("result não verbatim: %s", resp.Result)
	}
	var cloudLogged bool
	for _, r := range readEmptyQueueInvRecords(t, il, path) {
		if r.NodeID == cloudFallbackNodeID {
			cloudLogged = true
		}
	}
	if !cloudLogged {
		t.Fatalf("recordCloudAttempt não registrou a nuvem no invLog")
	}
}

// A failure through the port gives a bad gateway marked as a failed cloud
// fallback, with the retryable flag inherited from the root cause.
func TestGatewayCloudPort_Failure(t *testing.T) {
	for _, wantRetry := range []bool{true, false} {
		ns, _ := nodes.NewNodeStore("")
		remote := &fakeRemote{configured: true, err: errors.New("boom")}
		gw := NewGatewayWithHistory(GatewayConfig{
			Nodes: ns, Monitor: monitor.NewMonitor(),
			Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
			RemoteExecutor: remote,
		})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/invoke", nil)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		failure := cloudTestFailure()
		failure.retryable = wantRetry
		if !gw.serveCloudFallback(rec, req, ctx, 30*time.Second, time.Now(), failure, cloudTestReq("fn")) {
			t.Fatalf("falha da nuvem deveria retornar true")
		}
		cancel()
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status=%d, esperado 502 body=%s", rec.Code, rec.Body.String())
		}
		er := decodeErrResp(t, rec)
		if er.Code != "CLOUD_FALLBACK_FAILED" || er.Retryable != wantRetry || !er.FallbackApplied {
			t.Fatalf("code=%q retryable=%v fallback=%v, esperado CLOUD_FALLBACK_FAILED/%v/true", er.Code, er.Retryable, er.FallbackApplied, wantRetry)
		}
	}
}

// An exhausted budget does not call the cloud and returns the last failure,
// without recording a cloud attempt.
func TestGatewayCloudPort_BudgetExpired(t *testing.T) {
	ns, _ := nodes.NewNodeStore("")
	var calls int32
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: &fakeRemote{configured: true, body: []byte(`{}`), calls: &calls},
	})
	il, path := newEmptyQueueInvLog(t)
	defer il.Close()
	gw.SetInvocationLog(il)

	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/invoke", nil).WithContext(expired)
	if !gw.serveCloudFallback(rec, req, expired, time.Millisecond, time.Now(), cloudTestFailure(), cloudTestReq("fn")) {
		t.Fatalf("orçamento esgotado deveria responder (true)")
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("nuvem chamada com remaining<=0: calls=%d", calls)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, esperado 503 body=%s", rec.Code, rec.Body.String())
	}
	if recs := readEmptyQueueInvRecords(t, il, path); len(recs) != 0 {
		t.Fatalf("invLog lines=%d, esperado 0", len(recs))
	}
}

// The cold start propagates from the optional estimator to the metrics, and
// without an estimator or with a false value it stays false as before.
func TestGatewayCloudPort_ColdStartPropagation(t *testing.T) {
	for _, cold := range []bool{false, true} {
		ns, _ := nodes.NewNodeStore("")
		gw := NewGatewayWithHistory(GatewayConfig{
			Nodes: ns, Monitor: monitor.NewMonitor(),
			Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
			RemoteExecutor: &fakeRemote{configured: true, body: []byte(`{"ok":true}`), cold: cold},
		})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/invoke", nil)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if !gw.serveCloudFallback(rec, req, ctx, 30*time.Second, time.Now(), cloudTestFailure(), cloudTestReq("fn")) {
			t.Fatalf("cold=%v: deveria retornar true", cold)
		}
		cancel()
		var resp struct {
			ExecutionMetrics struct {
				ColdStart     bool `json:"cold_start"`
				CloudFallback bool `json:"cloud_fallback"`
			} `json:"execution_metrics"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.ExecutionMetrics.ColdStart != cold {
			t.Fatalf("cold_start=%v, esperado %v body=%s", resp.ExecutionMetrics.ColdStart, cold, rec.Body.String())
		}
	}
}

// Shedding end to end, with an admission ceiling: with a single slot and the
// call in flight stuck in the cloud, the second invocation is refused without
// touching the network, with a bad gateway marked as a failed cloud fallback, and
// once the slot is released the next one passes.
func TestGatewayCloudPort_CapacityShedsWithoutNetwork(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(entered)
			<-release
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))
	defer srv.Close()

	cs, err := cloud.NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	if err := cs.Set(cloud.CloudFallbackConfig{Connector: cloud.ConnectorAzureFunctions,
		Functions: map[string]cloud.FunctionEndpoint{"fn": {EndpointURL: srv.URL, InputContentType: "application/json", AuthScheme: "none", MaxInstances: 1}}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: cloud.NewConnector(cs),
	})

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{}}`))
		rec := httptest.NewRecorder()
		gw.Handle(rec, req)
		first <- rec
	}()
	<-entered // the single slot is stuck in the cloud

	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("sob teto status=%d, esperado 502 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "CLOUD_FALLBACK_FAILED" || !er.FallbackApplied {
		t.Fatalf("code=%q fallback=%v, esperado CLOUD_FALLBACK_FAILED/true", er.Code, er.FallbackApplied)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("chamada recusada alcançou a rede: hits=%d", got)
	}

	close(release)
	select {
	case r := <-first:
		if r.Code != http.StatusOK {
			t.Fatalf("primeira status=%d, esperado 200 body=%s", r.Code, r.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("primeira chamada não completou após release")
	}
}

// Error inside a success, end to end, with the real connector over a test
// server that answers an error status, which gives a failed cloud fallback
// through the handler with an empty queue.
func TestGatewayCloudPort_AppErrorEndToEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"error","message":"imagem inválida"}`))
	}))
	defer srv.Close()

	cs, err := cloud.NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	if err := cs.Set(cloud.CloudFallbackConfig{Connector: cloud.ConnectorAzureFunctions,
		Functions: map[string]cloud.FunctionEndpoint{"fn": {EndpointURL: srv.URL, InputContentType: "application/json", AuthScheme: "none"}}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: cloud.NewConnector(cs),
	})
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"image_base64":"xyz"}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d, esperado 502 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "CLOUD_FALLBACK_FAILED" || !er.FallbackApplied {
		t.Fatalf("code=%q fallback=%v, esperado CLOUD_FALLBACK_FAILED/true", er.Code, er.FallbackApplied)
	}
}

// Routing per function end to end: two functions reach two distinct endpoints
// through the handler with an empty queue.
func TestGatewayCloudPort_PerFunctionRouting(t *testing.T) {
	mk := func(tag string, calls *int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(calls, 1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","tag":"` + tag + `"}`))
		}))
	}
	var callsA, callsB int32
	srvA, srvB := mk("A", &callsA), mk("B", &callsB)
	defer srvA.Close()
	defer srvB.Close()

	cs, err := cloud.NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	if err := cs.Set(cloud.CloudFallbackConfig{Connector: cloud.ConnectorAzureFunctions,
		Functions: map[string]cloud.FunctionEndpoint{
			"alpr-deteccao-placa": {EndpointURL: srvA.URL, InputContentType: "application/json", AuthScheme: "none"},
			"ocr":                 {EndpointURL: srvB.URL, InputContentType: "application/json", AuthScheme: "none"},
		}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ns, _ := nodes.NewNodeStore("")
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes: ns, Monitor: monitor.NewMonitor(),
		Selector:       selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		RemoteExecutor: cloud.NewConnector(cs),
	})
	for fn, tag := range map[string]string{"alpr-deteccao-placa": "A", "ocr": "B"} {
		req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"`+fn+`","input":{"image_base64":"xyz"}}`))
		rec := httptest.NewRecorder()
		gw.Handle(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("fn=%s status=%d, esperado 200 body=%s", fn, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"tag":"`+tag+`"`) {
			t.Fatalf("fn=%s não alcançou o endpoint %s: %s", fn, tag, rec.Body.String())
		}
	}
	if atomic.LoadInt32(&callsA) != 1 || atomic.LoadInt32(&callsB) != 1 {
		t.Fatalf("callsA=%d callsB=%d, esperado 1/1", callsA, callsB)
	}
}
