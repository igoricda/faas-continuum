package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// TestPressaoDoCorpo504ChegaAoEntry closes the wiring that the two unit tests
// do not cover: a real node answering timed out with the pressure block in the
// body, passing through the dispatch, the parser and the construction of the
// entry, all the way to the invocation log. The producer is tested in the node
// interface and the consumer here, and the seam between them was trusted, so an
// error in the read of the timed out body, in the name of the JSON field or in
// the path of the captured pressure would pass both sides.
//
// Beyond the three fields of the entry, it checks the effect on the history:
// the admission timeout is a node saturation, with a zero total, one saturation
// and one node saturation, which is the same contract that the selection
// consumes.
func TestPressaoDoCorpo504ChegaAoEntry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		_, _ = io.WriteString(w, `{"code":"ACQUIRE_TIMEOUT","message":"estourou na fila","pressure":{"pool_alive":36,"queue_depth":5,"acquire_wait_ms":14998}}`)
	}))
	t.Cleanup(srv.Close)

	now := time.Now().UTC().Truncate(time.Millisecond)
	h, err := history.NewHistory("", 500)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	ns, _ := nodes.NewNodeStore("")
	if _, err := ns.Upsert(nodes.Node{NodeID: "nevoa-1", Layer: nodes.LayerNevoa, Host: hostFromURL(t, srv.URL)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	mon := monitor.NewMonitor()
	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:    ns,
		Monitor:  mon,
		Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		History:  h,
		Clock:    fixedClock(now),
	})
	gw.SetInvocationLog(l)

	rec := httptest.NewRecorder()
	gw.Handle(rec, httptest.NewRequest(http.MethodPost, "/invoke",
		strings.NewReader(`{"function":"fn","input":{"a":1}}`)))

	e := leEntryDoLog(t, logPath)
	if e.InstancesAvailable != 36 {
		t.Errorf("InstancesAvailable = %d, esperado 36 (pool_alive do corpo 504)", e.InstancesAvailable)
	}
	if e.QueueDepth != 5 {
		t.Errorf("QueueDepth = %d, esperado 5", e.QueueDepth)
	}
	if e.AcquireWaitMS != 14998 {
		t.Errorf("AcquireWaitMS = %d, esperado 14998", e.AcquireWaitMS)
	}
	if keys := h.Keys(); len(keys) == 0 {
		t.Fatal("history vazio: o 504 de admissão não foi gravado")
	} else {
		full, okFull := h.StatsFull(keys[0], time.Hour, now)
		if !okFull || full.Total != 0 || full.Saturation != 1 || full.NodeSaturation != 1 {
			t.Fatalf("StatsFull=(total=%d sat=%d nodesat=%d ok=%v), esperado (0,1,1,true)",
				full.Total, full.Saturation, full.NodeSaturation, okFull)
		}
	}
}

// TestNodeAdmissionPressureParse covers the real parser with the cases that
// matter: a complete block, a body without the block, an invalid or empty body,
// a null block and a body above the ceiling. Previously the test lived in the
// node interface as a shim that reimplemented the decoding, which pinned the
// shape of the JSON but not the parser that runs in production, and could
// diverge from it silently.
func TestNodeAdmissionPressureParse(t *testing.T) {
	grande := fmt.Sprintf(`{"code":"ACQUIRE_TIMEOUT","pressure":{"pool_alive":1},"x":%q}`,
		strings.Repeat("a", int(maxCapacityErrorBodyBytes)+1))
	casos := []struct {
		nome     string
		body     string
		ok       bool
		vivas    int
		fila     int
		esperaMS int64
	}{
		{"completo", `{"code":"ACQUIRE_TIMEOUT","message":"x","pressure":{"pool_alive":36,"queue_depth":5,"acquire_wait_ms":14998}}`, true, 36, 5, 14998},
		{"no antigo, sem bloco", `{"code":"ACQUIRE_TIMEOUT","message":"x"}`, false, 0, 0, 0},
		{"nao e JSON", `nao e json`, false, 0, 0, 0},
		{"vazio", ``, false, 0, 0, 0},
		{"pressure nulo explicito", `{"code":"X","pressure":null}`, false, 0, 0, 0},
		{"so o codigo", `{"code":"QUEUE_FULL"}`, false, 0, 0, 0},
		{"acima do teto de corpo", grande, false, 0, 0, 0},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			p, ok := nodeAdmissionPressure([]byte(c.body))
			if ok != c.ok {
				t.Fatalf("ok = %v, esperado %v (body=%d bytes)", ok, c.ok, len(c.body))
			}
			if p.PoolAlive != c.vivas || p.QueueDepth != c.fila || p.AcquireWaitMS != c.esperaMS {
				t.Fatalf("retrato = %+v, esperado vivas=%d fila=%d espera=%d", p, c.vivas, c.fila, c.esperaMS)
			}
		})
	}
}

// TestRetratoDeAdmissaoNoEntryDo504 is the other side of the capture.
//
// The producer already wrote the reading in the body of the timed out answer,
// but the arbiter discarded it for a structural reason: the construction of the
// entry only read the occupancy from the capacity piggyback, which exists only
// on a success. A request that never ran, which is the only case in which the
// admission pressure is directly observable, was therefore recorded without the
// queue depth, the available instances and the acquisition wait.
//
// This test pins the effect of the consumer: the reading that the node sent has
// to appear in the entry, since without it the change in the producer would be a
// no-op from the point of view of the decision.
func TestRetratoDeAdmissaoNoEntryDo504(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := nodes.Node{NodeID: "nevoa-1"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	group := selection.ResolveRequestGroup("fn-adm", []nodes.Node{node})

	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	h, _ := history.NewHistory("", 100)
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Monitor: monitor.NewMonitor(), Clock: fixedClock(now)})
	gw.SetInvocationLog(l)

	pr := NodeAdmissionPressure{PoolAlive: 36, QueueDepth: 5, AcquireWaitMS: 14998}
	captured := attemptCapture{concrete: "fn-adm", admPressure: &pr}
	gw.recordAttempt(node, 15000*time.Millisecond, ErrAttemptAdmissionTimeout,
		slo, "fn-adm", group, nil, true, "req-1", 1, captured)

	e := leEntryDoLog(t, logPath)
	if e.InstancesAvailable != 36 {
		t.Errorf("InstancesAvailable = %d, esperado 36 (containers VIVAS, nao livres)", e.InstancesAvailable)
	}
	if e.QueueDepth != 5 {
		t.Errorf("QueueDepth = %d, esperado 5 (profundidade da fila no instante da desistencia)", e.QueueDepth)
	}
	if e.AcquireWaitMS != 14998 {
		t.Errorf("AcquireWaitMS = %d, esperado 14998", e.AcquireWaitMS)
	}
}

// TestRetratoAusenteNaoViraZero covers an older node or a truncated body, in
// which there is no block. The entry has to keep the values it already had, of
// zero, and the history cannot gain an invented number, since the absence and
// the zero are different things: an unknown queue is not an empty queue.
func TestRetratoAusenteNaoViraZero(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := nodes.Node{NodeID: "nevoa-1"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	group := selection.ResolveRequestGroup("fn-adm2", []nodes.Node{node})

	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	h, _ := history.NewHistory("", 100)
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Monitor: monitor.NewMonitor(), Clock: fixedClock(now)})
	gw.SetInvocationLog(l)
	gw.recordAttempt(node, 15000*time.Millisecond, ErrAttemptAdmissionTimeout,
		slo, "fn-adm2", group, nil, true, "req-2", 1, attemptCapture{concrete: "fn-adm2"})

	e := leEntryDoLog(t, logPath)
	if e.QueueDepth != 0 || e.AcquireWaitMS != 0 {
		t.Errorf("sem retrato o Entry nao deveria ter valor: %+v", e)
	}
}

// TestRetratoNaoContaminaOSucesso covers the successful path, which has its own
// reading, from the piggyback, and that reading wins. If the reading of the
// timed out body invaded the success, a healthy node would receive numbers from
// an outcome that was not its own.
func TestRetratoNaoContaminaOSucesso(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := nodes.Node{NodeID: "borda-1"}
	slo := &selection.SLO{MaxLatencyMS: 5000}
	group := selection.ResolveRequestGroup("fn-ok", []nodes.Node{node})

	logPath := t.TempDir() + "/invocations.jsonl"
	l, err := NewInvocationLog(logPath)
	if err != nil {
		t.Fatalf("NewInvocationLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	h, _ := history.NewHistory("", 100)
	gw := NewGatewayWithHistory(GatewayConfig{History: h, Monitor: monitor.NewMonitor(), Clock: fixedClock(now)})
	gw.SetInvocationLog(l)
	captured := attemptCapture{concrete: "fn-ok"}
	captured.admPressure = &NodeAdmissionPressure{PoolAlive: 99, QueueDepth: 99, AcquireWaitMS: 99}
	// The piggyback of the success carries the same concrete variant, so this is
	// the reading that has to win, with a queue depth of three and not the ninety-nine of the timeout.
	metrics := json.RawMessage(`{"telemetry":{"capacity":{"fn-ok":{"max_instances":2,"instances_available":1,"queue_depth":3}}}}`)
	gw.recordAttempt(node, 200*time.Millisecond, nil, slo, "fn-ok", group, metrics, true, "req-3", 1, captured)

	e := leEntryDoLog(t, logPath)
	if e.QueueDepth != 3 {
		t.Errorf("QueueDepth = %d, esperado 3 (o do piggyback do 2xx, nao o 99 do 504)", e.QueueDepth)
	}
	if e.AcquireWaitMS == 99 {
		t.Error("AcquireWaitMS = 99: o retrato do 504 invadiu o caminho de sucesso")
	}
	if e.InstancesAvailable == 99 {
		t.Error("InstancesAvailable = 99: o retrato do 504 invadiu o caminho de sucesso")
	}
}

// leEntryDoLog reads the last line of the invocation log and returns the entry,
// which the log flattens in the JSON. Reading here instead of through a getter of
// the history covers the whole path, from the construction to the store and to
// the disk, and not only the assembly of the entry.
func leEntryDoLog(t *testing.T, path string) history.Entry {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile invLog: %v", err)
	}
	linhas := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(linhas) == 0 || linhas[len(linhas)-1] == "" {
		t.Fatalf("invLog vazio (%d bytes)", len(data))
	}
	var e history.Entry
	if err := json.Unmarshal([]byte(linhas[len(linhas)-1]), &e); err != nil {
		t.Fatalf("unmarshal da linha %q: %v", linhas[len(linhas)-1], err)
	}
	return e
}
