package nodeapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/auth"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// sleepyReadiness simulates a warm instance with a slow readiness: it
// sleeps and then declares itself ready, without provisioning anything.
type sleepyReadiness struct{ d time.Duration }

func (s *sleepyReadiness) WaitReady(ctx context.Context, _ string) (int, error) {
	select {
	case <-time.After(s.d):
		return 1, nil
	case <-ctx.Done():
		return 1, ctx.Err()
	}
}

// timeoutProvisioner simulates a cold provisioning that exceeds the
// deadline without wrapping the deadline error, as the errors of the
// container daemon do: it blocks until the context dies and returns an
// opaque error, since the cause is classified by the clock of the
// handler and not by the wrapper of the error.
type timeoutProvisioner struct{}

func (timeoutProvisioner) Provision(ctx context.Context, _, _ string) error {
	<-ctx.Done()
	return errors.New("provisionamento interrompido pelo deadline (erro opaco, sem %w)")
}

func (timeoutProvisioner) Terminate(context.Context, string) error { return nil }

func newAuditTestServer(t *testing.T, p pool.Provisioner, addresser InstanceAddresser, readiness ReadinessChecker) (*httptest.Server, string) {
	t.Helper()
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("falha ao gerar credencial de teste: %v", err)
	}
	reg, _ := registry.NewPersistentRegistry(filepath.Join(t.TempDir(), "functions.json"))
	pl := pool.NewManagerWithQueueCap(p, nil)
	srv := NewServer("xp4-borda-01:8080", reg, pl, addresser, g.Cred, 5*time.Second, newTestCollector(t), newFakeSampler(), 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, readiness)
	return httptest.NewServer(srv.Handler()), g.PlainToken
}

func mustErrorCode(t *testing.T, resp *http.Response, status int, code string) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != status {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, esperado %d; corpo: %s", resp.StatusCode, status, body)
	}
	var errResp errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("corpo não é JSON de erro: %v", err)
	}
	if errResp.Code != code {
		t.Errorf("code = %q, esperado %q", errResp.Code, code)
	}
}

// The size of the queue reaches the API, is persisted and is validated,
// with an invalid parameter answer and never an internal failure nor a
// silent discard.
func TestAuditRegisterQueueMax(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()

	fn, _ := mustRegister(t, doRegister(t, ts, token, "func-fila", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 0, QueueMax: 4}))
	if fn.QueueMax != 4 {
		t.Errorf("QueueMax = %d, esperado 4 (campo inalcançável antes da correção)", fn.QueueMax)
	}

	mustErrorCode(t, doRegister(t, ts, token, "func-teto-fila", registerRequest{ImageRef: "img:1.0", MaxInstances: 3, QueueMax: 4}),
		http.StatusBadRequest, "INVALID_SCALING_PARAMS")
	mustErrorCode(t, doRegister(t, ts, token, "func-fila-grande", registerRequest{ImageRef: "img:1.0", QueueMax: 999}),
		http.StatusBadRequest, "INVALID_SCALING_PARAMS")
}

// The invocation timeout is validated like the idle timeout: a negative
// value or one beyond the ceiling gives an invalid parameter answer,
// never a silent default nor a slot held for days.
func TestAuditRegisterTimeoutMS(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()

	mustErrorCode(t, doRegister(t, ts, token, "func-neg", registerRequest{ImageRef: "img:1.0", TimeoutMS: -5}),
		http.StatusBadRequest, "INVALID_TIMEOUT_MS")
	mustErrorCode(t, doRegister(t, ts, token, "func-eterna", registerRequest{ImageRef: "img:1.0", TimeoutMS: 1<<31 - 1}),
		http.StatusBadRequest, "INVALID_TIMEOUT_MS")

	fn, _ := mustRegister(t, doRegister(t, ts, token, "func-ok", registerRequest{ImageRef: "img:1.0", TimeoutMS: 1500}))
	if fn.TimeoutMS != 1500 {
		t.Errorf("TimeoutMS = %d, esperado 1500", fn.TimeoutMS)
	}
}

// A timeout during the provisioning becomes a gateway timeout, even when
// the provisioner returns an opaque error without the deadline error in
// the chain.
func TestAuditInvokeProvisionTimeoutIs504(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token := newAuditTestServer(t, timeoutProvisioner{}, backend, &SocketReadinessChecker{Addresser: backend})
	defer ts.Close()

	resp := doRegister(t, ts, token, "func-lenta-provisao", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 0, TimeoutMS: 150})
	_, invTok := mustRegister(t, resp)
	// The error is an acquisition timeout and not an invocation timeout:
	// the deadline expired in the admission stage, during the
	// provisioning, so the function never ran. The arbiter uses that
	// distinction to avoid counting an admission starvation as a
	// service failure, which is why the test pins the stage and not
	// only the answer.
	mustErrorCode(t, doInvoke(t, ts, "func-lenta-provisao", invTok, invokeRequest{Input: json.RawMessage(`{}`)}),
		http.StatusGatewayTimeout, "ACQUIRE_TIMEOUT")
}

// A warm instance with a slow readiness is not a cold start, since the
// signal comes from the pool and not from a heuristic of the duration.
func TestAuditInvokeWarmSlowReadinessIsNotCold(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token := newAuditTestServer(t, backend, backend, &sleepyReadiness{d: 8 * time.Millisecond})
	defer ts.Close()

	resp := doRegister(t, ts, token, "func-morna", registerRequest{ImageRef: "img:1.0", MinInstances: 1, TimeoutMS: 5000})
	_, invTok := mustRegister(t, resp)
	invResp := doInvoke(t, ts, "func-morna", invTok, invokeRequest{Input: json.RawMessage(`{"x":1}`)})
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(invResp.Body)
		t.Fatalf("invoke status = %d, esperado 200; corpo: %s", invResp.StatusCode, body)
	}
	var result invokeResponse
	if err := json.NewDecoder(invResp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	m := result.ExecutionMetrics
	if m.Phases.ReadinessMS <= 0 {
		t.Fatalf("precondição do teste quebrada: readiness_ms = %d, esperado > 0", m.Phases.ReadinessMS)
	}
	if m.ColdStart {
		t.Errorf("cold_start = true para instância warm com readiness de %dms (deveria ser só isCold)", m.Phases.ReadinessMS)
	}
}

// An update with an image that does not come up degrades the function
// with an actionable recorded error, which carries the previous
// configuration and the restoration command.
func TestAuditUpdateFailureHasActionableLastError(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()

	resp := doRegister(t, ts, token, "func-imagem", registerRequest{ImageRef: "img:1.0", MinInstances: 1})
	_, invTok := mustRegister(t, resp)

	backend.mu.Lock()
	backend.failProvision = true
	backend.mu.Unlock()
	upd := doUpdate(t, ts, token, "func-imagem", registerRequest{ImageRef: "img:versao-quebrada", MinInstances: 1})
	mustErrorCode(t, upd, http.StatusInternalServerError, "POOL_PROVISION_FAILED")

	getResp := doGet(t, ts, "func-imagem", invTok)
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		t.Fatalf("GET após update falho: status = %d, esperado 200; corpo: %s", getResp.StatusCode, body)
	}
	var fn registry.Function
	if err := json.NewDecoder(getResp.Body).Decode(&fn); err != nil {
		t.Fatal(err)
	}
	if !fn.Degraded() {
		t.Errorf("função deveria estar degraded após reprovisionamento falho")
	}
	for _, trecho := range []string{"docker.io/library/img:1.0", "para restaurar"} {
		if !strings.Contains(fn.LastError, trecho) {
			t.Errorf("LastError deveria conter %q (runbook acionável); obtido: %q", trecho, fn.LastError)
		}
	}
}

// UnmarshalJSON implements the JSON unmarshaler, symmetrically to the
// marshaler: the flat body becomes a function plus its invocation token,
// which the tests use to read the envelope that shows the token once.
func (r *registerResponse) UnmarshalJSON(data []byte) error {
	var tok struct {
		InvokeToken string `json:"invoke_token"`
	}
	if err := json.Unmarshal(data, &tok); err != nil {
		return err
	}
	r.InvokeToken = tok.InvokeToken
	// The unknown key of the invocation token is ignored here, while the
	// unmarshaler of the function handles the migration of the legacy
	// fields of the degraded state and of the last error.
	return json.Unmarshal(data, &r.Function)
}

// The scaler only reaps and never scales up, and the reconciliation
// only runs at startup, so a deletion that tears the pool down but fails
// to unregister the function, because the persistence failed and the
// removal was reverted in memory, would leave the function registered
// without its minimum floor until the restart. The handler restores the
// floor and keeps answering with an internal failure.
func TestAuditDeleteUnregisterFailureRestoresMinInstances(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	dir := t.TempDir()
	regPath := filepath.Join(dir, "functions.json")
	reg, err := registry.NewPersistentRegistry(regPath)
	if err != nil {
		t.Fatalf("falha ao criar registro persistente de teste: %v", err)
	}
	pl := pool.NewManagerWithQueueCap(backend, nil)
	g, err := auth.Generate()
	if err != nil {
		t.Fatalf("falha ao gerar credencial de teste: %v", err)
	}
	srv := NewServer("xp4-borda-01:8080", reg, pl, backend, g.Cred, 5*time.Second, newTestCollector(t), newFakeSampler(), 64<<10, 8<<20, 10*time.Second, 120*time.Second, 200*time.Millisecond, &SocketReadinessChecker{Addresser: backend})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	mustRegister(t, doRegister(t, ts, g.PlainToken, "func-aud49", registerRequest{ImageRef: "img:1.0", MinInstances: 2}))
	if got := pl.Count("func-aud49"); got != 2 {
		t.Fatalf("precondição quebrada: pool.Count() = %d, esperado 2", got)
	}

	// It injects a persistence failure that works even as root: the
	// file of the registry is replaced by a directory, so that the save
	// of the unregistration fails even for a privileged user, since a
	// read-only directory does not stop it. The update then reverts the
	// removal in memory.
	if err := os.Remove(regPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(regPath, 0o755); err != nil {
		t.Fatal(err)
	}

	mustErrorCode(t, doDelete(t, ts, g.PlainToken, "func-aud49"), http.StatusInternalServerError, "UNREGISTER_FAILED")

	if _, ok := reg.Get("func-aud49"); !ok {
		t.Fatalf("função deveria seguir registrada após Unregister malsucedido (rollback em memória)")
	}
	if got := pl.Count("func-aud49"); got != 2 {
		t.Errorf("pool.Count() após Unregister malsucedido = %d, esperado 2 (piso mínimo restaurado)", got)
	}
}
