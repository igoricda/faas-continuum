package gateway

// Tests of the late delivery marked on the wire, with the late header and the
// objective flag derived from the real latency, the normal path byte identical
// without a new header, and the counters of the delivery.
//
// They protect the wire contract of the delivery, which the client observes, and
// the operational counters. The credible regressions are marking a normal
// delivery as late, which is a false positive in a rerun, and forgetting the
// objective flag. The scenarios reuse the fixtures of the wait.
import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

func lateSuccessServer(t *testing.T, delay time.Duration, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(delay):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A late delivery that exceeds the objective is marked late and unmet, with one delivery.
func TestLateHeadersSloMetFalse(t *testing.T) {
	srv := lateSuccessServer(t, 300*time.Millisecond, `{"result":"tardio"}`)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	gw := waitTestGateway(t, ns, "")

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Faas-Late"); got != "true" {
		t.Fatalf("X-Faas-Late=%q want true", got)
	}
	if got := rec.Header().Get("X-Faas-Slo-Met"); got != "false" {
		t.Fatalf("X-Faas-Slo-Met=%q want false (latência 300 > SLO 100)", got)
	}
	if _, served, _, _ := gw.BestEffortCounters(); served != 1 {
		t.Fatalf("served=%d want 1", served)
	}
}

// A late delivery within the objective is marked late and met.
func TestLateHeadersSloMetTrue(t *testing.T) {
	slow := lateSuccessServer(t, 3*time.Second, `{"result":"tardio"}`)
	releaseLonger := make(chan struct{})
	longer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-releaseLonger:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"result":"longa"}`))
		case <-r.Context().Done():
		}
	}))
	defer longer.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "a-slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slow.URL), Functions: []string{"fn"}, FunctionTimeoutMS: map[string]int{"fn": 15000}})
	_, _ = ns.Upsert(nodes.Node{NodeID: "b-longer", Layer: nodes.LayerBorda, Host: hostFromURL(t, longer.URL), Functions: []string{"fn"}, FunctionTimeoutMS: map[string]int{"fn": 15000}})
	gw := waitTestGateway(t, ns, "")

	// The overall budget of five seconds gives a slice of about two and a half for
	// the first node, which is below its three seconds, so the report is armed,
	// while the loose objective covers the three seconds. The second node stays
	// stuck until the release, which happens after the victory, so the close does
	// not wait for the ceiling.
	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":5000},"input":{"a":1}}`)
	close(releaseLonger)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != `{"result":"tardio"}` {
		t.Fatalf("body=%q want o corpo da primeira (vencedora)", rec.Body.String())
	}
	if got := rec.Header().Get("X-Faas-Late"); got != "true" {
		t.Fatalf("X-Faas-Late=%q want true", got)
	}
	if got := rec.Header().Get("X-Faas-Slo-Met"); got != "true" {
		t.Fatalf("X-Faas-Slo-Met=%q want true (latência 3s < SLO 5s)", got)
	}
	// The loser is no longer cancelled, so it completes when the release frees it
	// and the drain settles it as a real sample.
	if _, served, _, cancelled := gw.BestEffortCounters(); served != 1 || cancelled != 0 {
		t.Fatalf("served=%d cancelled=%d want 1/0 (vencedora; perdedora segue viva)", served, cancelled)
	}
}

// The normal path, within the slice, gains no new header and stays byte identical.
func TestSyncHeadersAbsent(t *testing.T) {
	srv := lateSuccessServer(t, 10*time.Millisecond, `{"result":"ok"}`)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	gw := waitTestGateway(t, ns, "")

	rec := invokeRecorder(t, gw, `{"function":"fn","input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	if got := rec.Header().Get("X-Faas-Late"); got != "" {
		t.Fatalf("X-Faas-Late=%q want ausente no caminho normal", got)
	}
	if got := rec.Header().Get("X-Faas-Slo-Met"); got != "" {
		t.Fatalf("X-Faas-Slo-Met=%q want ausente no caminho normal", got)
	}
	if armed, served, expired, cancelled := gw.BestEffortCounters(); armed+served+expired+cancelled != 0 {
		t.Fatalf("contadores=%d/%d/%d/%d want todos zero", armed, served, expired, cancelled)
	}
}

// The ceiling of the node, which is the service timeout, answers timed out without
// counting a delivery or a cancellation. The hard deadline is the defensive path
// and ties with the ceiling by construction, since it is at least the ceiling, so
// the test pins the invariant: no delivery, no cancellation and at most one exit,
// which is never a double count.
func TestBestEffortCapTimeoutCounters(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			_, _ = w.Write([]byte(`{}`))
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 400))
	gw := waitTestGateway(t, ns, "")

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":200},"input":{"a":1}}`)
	close(release)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504", rec.Code)
	}
	if _, served, expired, cancelled := gw.BestEffortCounters(); served != 0 || cancelled != 0 || expired > 1 {
		t.Fatalf("served=%d expired=%d cancelled=%d want 0/<=1/0 (cap do nó, sem corte nosso)", served, expired, cancelled)
	}
}

// A synchronous winner does not cancel the armed report, which concludes when
// released, is settled as a real sample in the log, and the counter of
// cancellations stays at zero. Previously the winner cancelled everything and killed the execution.
// It was the perpetual cold start measured in the batch.
func TestBestEffortWinnerKeepsArmedRunning(t *testing.T) {
	releaseSlow := make(chan struct{})
	var slowCalls int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&slowCalls, 1)
		select {
		case <-releaseSlow:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"result":"lento"}`))
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSlow) }) }
	defer release()
	fast := lateSuccessServer(t, 10*time.Millisecond, `{"result":"rapido"}`)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "a-slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slow.URL), Functions: []string{"fn"}, FunctionTimeoutMS: map[string]int{"fn": 15000}})
	_, _ = ns.Upsert(nodes.Node{NodeID: "b-fast", Layer: nodes.LayerBorda, Host: hostFromURL(t, fast.URL), Functions: []string{"fn"}, FunctionTimeoutMS: map[string]int{"fn": 15000}})
	logPath := t.TempDir() + "/inv.jsonl"
	gw := waitTestGateway(t, ns, logPath)

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":200},"input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	if _, served, _, cancelled := gw.BestEffortCounters(); served != 0 || cancelled != 0 {
		t.Fatalf("served=%d cancelled=%d want 0/0 (vencedora síncrona; armada segue viva)", served, cancelled)
	}
	release()
	// A armada conclui e o drain a assenta com o desfecho real (sem entrega).
	deadline := time.Now().Add(3 * time.Second)
	for {
		raw, _ := os.ReadFile(logPath)
		settled := false
		for _, ln := range strings.Split(string(raw), "\n") {
			if strings.Contains(ln, `"node_id":"a-slow"`) && strings.Contains(ln, `"outcome":"sucesso"`) {
				settled = true
				break
			}
		}
		if settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("armada não assentada no invLog em 3s: %s", string(raw))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&slowCalls); got != 1 {
		t.Fatalf("nó a-slow chamado %d vez(es), want 1 (execução não foi morta)", got)
	}
}
