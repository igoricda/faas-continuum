package gateway

// Battery of effects: the fail-fast cancels the pending reports, the ceiling of
// the body applies to a pending report, an older node stays synchronous and the
// pool and the join behave under concurrency. The other scenarios live in the
// tests of the wait and of the headers, referenced below.
//
//   - the edge exceeds the slice and delivers later;
//   - the fog wins and the edge is cancelled;
//   - a late rejection supersedes the exceeded budget;
//   - a client disconnection aborts without writing;
//   - the hard deadline falls through to the cloud or to the failure.
//
// Each case locks an invariant or a fail-fast branch. The credible regressions
// are a fail-fast that ignores the orphans, which leak up to the ceiling, a late
// framing treated as a success, an older node entering the asynchronous path, and
// a release of the pool before the join, which corrupts the body.
import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

// The revised fail-fast: a synchronous unauthorized or bad request answers
// immediately and the armed orphan is not cancelled, so it completes later and is
// settled with the real outcome, which feeds the learning, and the counter of
// cancellations stays at zero. Previously the fail-fast killed the announced
// execution that was ahead in the queue, which produced a perpetual cold start in
// the measured batch.
func TestFailFastKeepsArmedRunning(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		code   string
	}{
		{"401", http.StatusUnauthorized, "UNAUTHORIZED"},
		{"400", http.StatusBadRequest, "NODE_BAD_REQUEST"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			var slowCalls int32
			slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&slowCalls, 1)
				select {
				case <-release:
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"result":"orfao"}`))
				case <-r.Context().Done():
				}
			}))
			defer slow.Close()
			var releaseOnce sync.Once
			releaseArmed := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseArmed()
			strict := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer strict.Close()
			ns, _ := nodes.NewNodeStore("")
			// The announced node comes before the older one, and the armed orphan stays alive after the fail-fast of the next node.
			_, _ = ns.Upsert(nodes.Node{NodeID: "a-slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slow.URL), Functions: []string{"fn"}, FunctionTimeoutMS: map[string]int{"fn": 15000}})
			_, _ = ns.Upsert(nodes.Node{NodeID: "b-strict", Layer: nodes.LayerBorda, Host: hostFromURL(t, strict.URL), Functions: []string{"fn"}})
			logPath := t.TempDir() + "/inv.jsonl"
			gw := waitTestGateway(t, ns, logPath)

			start := time.Now()
			rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
			elapsed := time.Since(start)
			if rec.Code != tc.status {
				t.Fatalf("status=%d want %d body=%s", rec.Code, tc.status, rec.Body.String())
			}
			if er := decodeErrResp(t, rec); er.Code != tc.code {
				t.Fatalf("code=%q want %s", er.Code, tc.code)
			}
			if elapsed > 3*time.Second {
				t.Fatalf("fail-fast em %v: segurou a resposta até a órfã", elapsed)
			}
			if armed, _, _, cancelled := gw.BestEffortCounters(); armed != 1 || cancelled != 0 {
				t.Fatalf("armed=%d cancelled=%d want 1/0 (fail-fast não cancela mais)", armed, cancelled)
			}
			// Release the orphan, so the drain after the answer settles it with the real outcome.
			releaseArmed()
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
					t.Fatalf("órfã não assentada no invLog em 3s: %s", string(raw))
				}
				time.Sleep(20 * time.Millisecond)
			}
			if got := atomic.LoadInt32(&slowCalls); got != 1 {
				t.Fatalf("nó a-slow chamado %d vez(es), want 1 (execução não foi morta)", got)
			}
		})
	}
}

// A body above the ceiling in a pending report gives the same error as the
// synchronous path, which is a bad gateway attributed to the node and only in the log, since a framing error is not recordable.
func TestLateOversizeBodyFraming(t *testing.T) {
	large := bytes.Repeat([]byte("a"), 9*1024*1024)
	bigJSON := append(append([]byte(`{"data":"`), large...), []byte(`"}`)...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bigJSON)
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	logPath := t.TempDir() + "/inv.jsonl"
	gw := waitTestGateway(t, ns, logPath)

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502 body=%s", rec.Code, rec.Body.String())
	}
	if er := decodeErrResp(t, rec); er.Code != "NODE_ERROR" {
		t.Fatalf("code=%q want NODE_ERROR (mesmo do síncrono)", er.Code)
	}
	recs := readInvLogLines(t, logPath)
	if len(recs) != 2 {
		t.Fatalf("linhas=%d want 2 (orcamento + falha de framing)", len(recs))
	}
	if recs[1].Outcome != history.OutcomeFalha {
		t.Fatalf("última outcome=%q want falha", recs[1].Outcome)
	}
}

// A target without a function timeout follows the synchronous path, so an
// exceeded slice gives the timeout of the budget without a late delivery, which is the defensive behavior.
func TestLegacyNodeStaysSynchronous(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"tarde-demais"}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL), Functions: []string{"fn"}})
	gw := waitTestGateway(t, ns, "")

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d want 504 (sem espera best-effort) body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Faas-Late"); got != "" {
		t.Fatalf("X-Faas-Late=%q want ausente (nó antigo)", got)
	}
	if armed, served, expired, cancelled := gw.BestEffortCounters(); armed+served+expired+cancelled != 0 {
		t.Fatalf("contadores=%d/%d/%d/%d want zeros", armed, served, expired, cancelled)
	}
}

// Pool and join under concurrency: several concurrent invocations with distinct
// inputs, all armed and delivered late, where each returned body belongs to its
// own invocation, since the release never recycles a buffer read by a live goroutine.
func TestPooledBufferJoinUnderRace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		var fwd struct {
			Input json.RawMessage `json:"input"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &fwd); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp, _ := json.Marshal(map[string]json.RawMessage{"echo": fwd.Input})
		_, _ = w.Write(resp)
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	mon := monitor.NewMonitor()
	ig := NewInflightGauge()
	gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}}).WithInflight(ig)

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			want := fmt.Sprintf(`{"data":"call-%d"}`, i)
			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","slo":{"max_latency_ms":20},"input":`+want+`}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			gw.Handle(rec, req)
			if rec.Code != http.StatusOK {
				errs[i] = fmt.Errorf("call %d: status=%d body=%s", i, rec.Code, rec.Body.String())
				return
			}
			var resp map[string]json.RawMessage
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				errs[i] = fmt.Errorf("call %d: decode: %v", i, err)
				return
			}
			if strings.TrimSpace(string(resp["echo"])) != want {
				errs[i] = fmt.Errorf("call %d: eco=%q want %q (corrupção de buffer?)", i, resp["echo"], want)
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("invoke %d: %v", i, err)
		}
	}
	if got := ig.NodeK("n1"); got != 0 {
		t.Errorf("inflight=%d want 0 (Dec de todas as pendências)", got)
	}
	if _, served, _, _ := gw.BestEffortCounters(); served != n {
		t.Errorf("served=%d want %d (todas tardias)", served, n)
	}
}
