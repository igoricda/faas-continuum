package nodeapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/auth"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// ---------------------------------------------------------------------------
// Helpers: node server with access to the server and to the late sender
// ---------------------------------------------------------------------------

func newLateTestServer(t *testing.T, backend *fakeInstanceBackend) (*httptest.Server, *Server, string) {
	t.Helper()
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("falha ao gerar credencial de teste: %v", err)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(backend, nil)
	srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, newTestCollector(t), newFakeSampler(), 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend})
	return httptest.NewServer(srv.Handler()), srv, g.PlainToken
}

// ---------------------------------------------------------------------------
// Best-effort sending and discarding under pressure
// ---------------------------------------------------------------------------

func TestLateSend(t *testing.T) {
	received := make(chan lateReportMsg, 1)
	arb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg lateReportMsg
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			t.Errorf("decode: %v", err)
			return
		}
		received <- msg
		w.WriteHeader(http.StatusAccepted)
	}))
	defer arb.Close()

	ts, srv, mgmtToken := newLateTestServer(t, newFakeInstanceBackend(t, slowHandler))
	defer ts.Close()
	srv.configureLateSender(arb.URL, "node-token", "node1", lateQueueCap, 0)
	defer close(srv.lateQueue) // It stops the sender loop, which would otherwise leak and race with the log buffers of the other tests

	resp := doRegister(t, ts, mgmtToken, "func-late", registerRequest{ImageRef: "img:1.0", MinInstances: 1, TimeoutMS: 3000, LateReportable: true})
	_, invTok := mustRegister(t, resp)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-late/invoke", bytes.NewReader([]byte(`{"input":{}}`)))
	req.Header.Set(auth.DefaultHeaderName, invTok)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Faas-Attempt-ID", "req-1")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, esperado %d (corpo: %s)", resp.StatusCode, http.StatusOK, string(bodyBytes))
	}

	select {
	case msg := <-received:
		if msg.RequestID != "req-1" {
			t.Errorf("request_id = %q, esperado req-1", msg.RequestID)
		}
		if msg.Function != "func-late" {
			t.Errorf("function = %q, esperado func-late", msg.Function)
		}
		if msg.Outcome != "sucesso" {
			t.Errorf("outcome = %q, esperado sucesso", msg.Outcome)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout aguardando late report")
	}
}

// The cold start reaches the arbiter in the late report as the cold start
// flag, and it is omitted in the warm case, which keeps the wire
// compatible and decodable by an older arbiter.
func TestLateReportColdStart(t *testing.T) {
	bodies := make(chan []byte, 4)
	arb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies <- b
		w.WriteHeader(http.StatusAccepted)
	}))
	defer arb.Close()

	ts, srv, mgmtToken := newLateTestServer(t, newFakeInstanceBackend(t, echoHandler))
	defer ts.Close()
	srv.configureLateSender(arb.URL, "node-token", "node1", lateQueueCap, 0)
	defer close(srv.lateQueue) // It stops the sender loop, which would otherwise leak and race with the log buffers of the other tests

	// With no minimum instance, the first invocation provisions the
	// instance and the second one reuses the released one.
	resp := doRegister(t, ts, mgmtToken, "func-cold", registerRequest{ImageRef: "img:1.0", MinInstances: 0, TimeoutMS: 3000, LateReportable: true})
	_, invTok := mustRegister(t, resp)

	invokeDetached := func(attempt string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-cold/invoke", bytes.NewReader([]byte(`{"input":{}}`)))
		req.Header.Set(auth.DefaultHeaderName, invTok)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Faas-Attempt-ID", attempt)
		r, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("invoke %s status = %d, esperado 200 (corpo: %s)", attempt, r.StatusCode, body)
		}
	}

	waitBody := func(name string) []byte {
		t.Helper()
		select {
		case b := <-bodies:
			return b
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout aguardando late report %s", name)
			return nil
		}
	}

	invokeDetached("cold-1")
	invokeDetached("warm-1")

	cases := []struct {
		name    string
		wantKey bool
		wantVal string
	}{
		{"cold", true, "true"},
		{"warm", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := waitBody(tc.name)
			var got map[string]json.RawMessage
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("decode late-report: %v (corpo: %s)", err, body)
			}
			raw, ok := got["cold_start"]
			if ok != tc.wantKey {
				t.Fatalf("cold_start presente = %v, esperado %v (corpo: %s)", ok, tc.wantKey, body)
			}
			if tc.wantKey && string(raw) != tc.wantVal {
				t.Errorf("cold_start = %s, esperado %s", raw, tc.wantVal)
			}
		})
	}
}

// lateBlockingProvisioner holds the provisioning until the test releases
// it or the context expires: with a provisioning in flight, a second
// acquisition enters the queue of the pool, which is the state that used
// to shed the detachment.
type lateBlockingProvisioner struct{ liberar chan struct{} }

func (b *lateBlockingProvisioner) Provision(ctx context.Context, _, _ string) error {
	select {
	case <-b.liberar:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *lateBlockingProvisioner) Terminate(context.Context, string) error { return nil }

// TestDetachAllowed pins the admission of the orphans: a standalone node
// never detaches, a full late queue drops the request, and the pressure
// of the pool no longer drops it. The shedding based on the depth of the
// queue was self-contradictory, because the pool fills exactly when the
// attempts are being cut, which is when the late report is the only
// sample of the real latency of the node.
func TestDetachAllowed(t *testing.T) {
	blk := &lateBlockingProvisioner{liberar: make(chan struct{})}
	pl := pool.NewManagerWithQueueCap(blk, nil)

	// It leaves a provisioning pending and a second acquisition waiting,
	// so the queue of the function is not empty.
	provCtx, cancelProv := context.WithCancel(context.Background())
	defer cancelProv()
	go func() { _, _, _ = pl.AcquireWaitCold(provCtx, "fn", "img:1.0", 0) }()
	filaCtx, cancelFila := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFila()
	go func() { _, _, _ = pl.AcquireWaitCold(filaCtx, "fn", "img:1.0", 0) }()
	deadline := time.Now().Add(3 * time.Second)
	for pl.QueueLen("fn") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("fila do pool não encheu: a fixture não exercita a pressão")
		}
		time.Sleep(5 * time.Millisecond)
	}

	srv := &Server{pool: pl}
	if srv.detachAllowed() {
		t.Fatal("detachAllowed sem sender (fila nil) = true; standalone nunca desanexa")
	}
	srv.lateQueue = make(chan lateReportMsg, 2)
	if !srv.detachAllowed() {
		t.Fatal("detachAllowed com QueueLen>0 = false; o shed por QueueLen foi removido (C4)")
	}
	srv.lateQueue <- lateReportMsg{}
	srv.lateQueue <- lateReportMsg{}
	if srv.detachAllowed() {
		t.Fatal("detachAllowed com fila de late cheia = true; fila cheia ainda derruba")
	}
}

func TestLateSendQueueFull(t *testing.T) {
	ts, srv, mgmtToken := newLateTestServer(t, newFakeInstanceBackend(t, echoHandler))
	defer ts.Close()
	srv.configureLateSender("http://127.0.0.1:1", "node-token", "node1", lateQueueCap, 0)
	defer close(srv.lateQueue) // It stops the sender loop, which would otherwise leak and race with the log buffers of the other tests

	resp := doRegister(t, ts, mgmtToken, "func-late", registerRequest{ImageRef: "img:1.0", MinInstances: 1, LateReportable: true})
	_, invTok := mustRegister(t, resp)

	for i := 0; i < lateQueueCap+10; i++ {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-late/invoke", bytes.NewReader([]byte(`{"input":{}}`)))
		req.Header.Set(auth.DefaultHeaderName, invTok)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Faas-Attempt-ID", fmt.Sprintf("req-%d", i))
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	// The sender is asynchronous, so the drops caused by a failure of the
	// sending only appear after the loop drains the queue. The test
	// waits with a deadline instead of asserting immediately, which
	// avoids a pre-existing race.
	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadInt64(&srv.lateDropped) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("lateDropped = 0, esperado > 0 com árbitro fora do ar")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
