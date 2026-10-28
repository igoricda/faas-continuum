package gateway

// Ingestion of the capacity in the gateway: the piggyback of a successful
// answer through the recording of the attempt merges into the monitor, a parsed
// rejection marks the variant as exhausted, and an absent monitor does nothing
// while the invocation succeeds normally.

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func piggybackMetrics(t *testing.T, capacity map[string]telemetry.FunctionCapacity) json.RawMessage {
	t.Helper()
	pkg := telemetry.Package{NodeID: "n1", Capacity: capacity}
	rawPkg, err := json.Marshal(pkg)
	if err != nil {
		t.Fatalf("marshal pkg: %v", err)
	}
	wrapper, err := json.Marshal(map[string]json.RawMessage{"telemetry": rawPkg})
	if err != nil {
		t.Fatalf("marshal wrapper: %v", err)
	}
	return wrapper
}

func TestIngestPiggybackMergesCapacity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Monitor: mon, Clock: fixedClock(now)})

	node := nodes.Node{NodeID: "n1"}
	metrics := piggybackMetrics(t, map[string]telemetry.FunctionCapacity{
		"sleep": {MaxInstances: 1, InstancesAvailable: 0, QueueDepth: 2},
	})
	// Without history or log only the ingestion happens, and it never panics.
	gw.recordAttempt(node, 5*time.Millisecond, nil, nil, "sleep", "sleep", metrics, true, "", 0, attemptCapture{})

	got, ok := mon.GetCapacity("n1", "sleep", now)
	if !ok {
		t.Fatalf("GetCapacity após piggyback = false, esperado true")
	}
	if got.MaxInstances != 1 || got.InstancesAvailable != 0 || got.QueueDepth != 2 {
		t.Fatalf("capacidade fundida = %+v, esperado {1 0 2}", got)
	}
}

func TestIngestPiggybackTolerant(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Monitor: mon, Clock: fixedClock(now)})
	node := nodes.Node{NodeID: "n1"}

	// A metric without capacity, from an older node or from an omitted collection, is skipped without an error.
	for name, metrics := range map[string]json.RawMessage{
		"nil":                    nil,
		"vazio":                  json.RawMessage(`{}`),
		"sem_telemetry":          json.RawMessage(`{"response_time_ms":5}`),
		"invalido":               json.RawMessage(`not-json{{{`),
		"telemetry_sem_capacity": piggybackMetrics(t, nil),
	} {
		gw.recordAttempt(node, time.Millisecond, nil, nil, "sleep", "sleep", metrics, true, "", 0, attemptCapture{})
		if _, ok := mon.GetCapacity("n1", "sleep", now); ok {
			t.Fatalf("%s: GetCapacity = true, esperado false (skip tolerante)", name)
		}
	}
}

func TestIngestMarksExhaustedOnParsedRejection(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	mon := monitor.NewMonitor()
	gw := NewGatewayWithHistory(GatewayConfig{Monitor: mon, Clock: fixedClock(now)})
	node := nodes.Node{NodeID: "n1"}

	// A fresh baseline with available instances, from the previous periodic report.
	mon.UpdateCapacity("n1", "sleep", telemetry.FunctionCapacity{MaxInstances: 1, InstancesAvailable: 1}, now)

	gw.recordAttempt(node, 3*time.Millisecond, capacityErr(t, "POOL_EXHAUSTED", "n1"), nil, "sleep", "sleep", nil, true, "", 0, attemptCapture{})

	got, ok := mon.GetCapacity("n1", "sleep", now)
	if !ok || got.InstancesAvailable != 0 {
		t.Fatalf("após rejeição = (%+v,%v), esperado (avail=0,true)", got, ok)
	}

	// A generic error, which is not about capacity, marks nothing.
	mon2 := monitor.NewMonitor()
	gw2 := NewGatewayWithHistory(GatewayConfig{Monitor: mon2, Clock: fixedClock(now)})
	gw2.recordAttempt(node, 3*time.Millisecond, errors.New("boom"), nil, "sleep", "sleep", nil, true, "", 0, attemptCapture{})
	if _, ok := mon2.GetCapacity("n1", "sleep", now); ok {
		t.Fatalf("erro genérico marcou capacidade, esperado skip")
	}
}

func TestIngestNilMonitorNoOp(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	gw := NewGatewayWithHistory(GatewayConfig{Clock: fixedClock(now)})
	node := nodes.Node{NodeID: "n1"}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("recordAttempt com mon==nil causou panic: %v", r)
		}
	}()
	metrics := piggybackMetrics(t, map[string]telemetry.FunctionCapacity{
		"sleep": {MaxInstances: 1, InstancesAvailable: 1},
	})
	gw.recordAttempt(node, 5*time.Millisecond, nil, nil, "sleep", "sleep", metrics, true, "", 0, attemptCapture{})
	gw.recordAttempt(node, 3*time.Millisecond, capacityErr(t, "QUEUE_FULL", "n1"), nil, "sleep", "sleep", nil, true, "", 0, attemptCapture{})
}

func TestHandle2xxNilMonitor(t *testing.T) {
	body := `{"result":{"ok":true},"execution_metrics":{"response_time_ms":4,"telemetry":{"capacity":{"sleep":{"max_instances":1,"instances_available":1}}}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:    ns,
		Monitor:  nil,
		Selector: selection.QueueSelector{Mon: nil, Cfg: defaultLimits},
		Clock:    time.Now,
	})
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"sleep","input":{"a":1}}`))
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s (mon==nil deve ser no-op)", rec.Code, rec.Body.String())
	}
}
