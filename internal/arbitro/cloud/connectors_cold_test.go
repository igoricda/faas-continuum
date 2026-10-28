package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Validation of the estimate fields: reaper per connector (top level) and
// max_instances per function.
func TestCloudConfig_ColdFieldsValidation(t *testing.T) {
	good := func() FunctionEndpoint {
		return FunctionEndpoint{EndpointURL: "https://example.com/api/f", InputContentType: "application/json", AuthScheme: "none", MaxInstances: 10}
	}
	cases := []struct {
		name    string
		cfg     CloudFallbackConfig
		wantErr bool
	}{
		{"reaper + max válidos", CloudFallbackConfig{Connector: ConnectorAzureFunctions, ReaperTimeout: "5m", Functions: map[string]FunctionEndpoint{"a": good()}}, false},
		{"reaper inválido", CloudFallbackConfig{Connector: ConnectorAzureFunctions, ReaperTimeout: "abc", Functions: map[string]FunctionEndpoint{"a": good()}}, true},
		{"reaper zero", CloudFallbackConfig{Connector: ConnectorAzureFunctions, ReaperTimeout: "0s", Functions: map[string]FunctionEndpoint{"a": good()}}, true},
		{"reaper negativo", CloudFallbackConfig{Connector: ConnectorAzureFunctions, ReaperTimeout: "-5m", Functions: map[string]FunctionEndpoint{"a": good()}}, true},
		{"max negativo", CloudFallbackConfig{Connector: ConnectorAzureFunctions, Functions: map[string]FunctionEndpoint{"a": {EndpointURL: "https://example.com/api/f", InputContentType: "application/json", AuthScheme: "none", MaxInstances: -1}}}, true},
		{"reaper no plano legado", CloudFallbackConfig{EndpointURL: "https://example.com/i", InputContentType: "application/json", AuthScheme: "none", ReaperTimeout: "5m"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewCloudStore("")
			if err != nil {
				t.Fatalf("NewCloudStore: %v", err)
			}
			err = s.Set(tc.cfg)
			if tc.wantErr && !errors.Is(err, ErrInvalidCloudConfig) {
				t.Fatalf("Set err=%v, esperado ErrInvalidCloudConfig", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Set válido err=%v", err)
			}
		})
	}
}

func coldTestStore(t *testing.T, url, reaper string, max int) (*CloudStore, *AzureFunctionsConnector) {
	t.Helper()
	s, err := NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	ep := FunctionEndpoint{EndpointURL: url, InputContentType: "application/json", AuthScheme: "none", MaxInstances: max}
	if err := s.Set(CloudFallbackConfig{Connector: ConnectorAzureFunctions, ReaperTimeout: reaper, Functions: map[string]FunctionEndpoint{"f": ep}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	return s, NewAzureFunctionsConnector(s, nil)
}

func okServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// No evidence (first call, absent fields) yields false.
func TestCold_FirstCallAndNoCriteria(t *testing.T) {
	srv := okServer(t)
	_, c := coldTestStore(t, srv.URL, "", 0)
	if c.EstimateCold("f") {
		t.Fatalf("primeira chamada deveria ser false")
	}
	if _, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if c.EstimateCold("f") {
		t.Fatalf("sem critérios configurados deveria seguir false")
	}
	if c.EstimateCold("inexistente") {
		t.Fatalf("função desconhecida deveria ser false")
	}
}

// Idleness beyond the connector reaper is cold; within it is warm. The fake
// clock keeps the test deterministic without sleeps.
func TestCold_IdleBeyondReaper(t *testing.T) {
	srv := okServer(t)
	_, c := coldTestStore(t, srv.URL, "50ms", 0)
	now := time.Now()
	c.cold.now = func() time.Time { return now }

	if _, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if c.EstimateCold("f") {
		t.Fatalf("logo após 2xx deveria ser warm")
	}
	now = now.Add(30 * time.Millisecond)
	if c.EstimateCold("f") {
		t.Fatalf("ociosidade < reaper deveria ser warm")
	}
	now = now.Add(30 * time.Millisecond) // total 60ms > 50ms
	if !c.EstimateCold("f") {
		t.Fatalf("ociosidade > reaper deveria ser cold")
	}
}

// A non-2xx response does not renew the mark: idleness keeps counting from the
// last 2xx.
func TestCold_Non2xxDoesNotMarkAlive(t *testing.T) {
	var fail int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&fail) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("down"))
			return
		}
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))
	t.Cleanup(srv.Close)

	_, c := coldTestStore(t, srv.URL, "50ms", 0)
	now := time.Now()
	c.cold.now = func() time.Time { return now }

	if _, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Invoke 2xx: %v", err)
	}
	atomic.StoreInt32(&fail, 1)
	now = now.Add(100 * time.Millisecond) // already cold
	if _, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`)); err == nil {
		t.Fatalf("Invoke 500 deveria falhar")
	}
	// The 500 did not renew the mark, so it stays cold at the same instant.
	if !c.EstimateCold("f") {
		t.Fatalf("após erro sem 2xx, deveria seguir cold")
	}
}

// Occupancy: in-flight calls at max_instances make EstimateCold true, and
// Invoke is rejected by the admission gate without touching the network.
func TestCold_OverCapacity(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(entered)
			<-release // the first call holds the only slot
			_, _ = w.Write([]byte(`{"status":"success"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))
	t.Cleanup(srv.Close)

	_, c := coldTestStore(t, srv.URL, "", 1)
	done := make(chan error, 1)
	go func() {
		_, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`))
		done <- err
	}()
	<-entered // the single slot is taken
	if !c.EstimateCold("f") {
		t.Fatalf("com slot lotado deveria ser cold")
	}
	if _, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`)); !errors.Is(err, ErrCloudAtCapacity) {
		t.Fatalf("acima do teto err=%v, esperado ErrCloudAtCapacity", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("chamada recusada alcançou a rede: hits=%d", got)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if c.EstimateCold("f") {
		t.Fatalf("slot livre e sem reaper deveria ser warm")
	}
	if _, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Invoke após liberar: %v", err)
	}
}

// Burst: with max=2 and 5 concurrent calls, exactly 2 enter and 3 are rejected;
// the counter returns to 0 afterwards without leaking.
func TestGate_BurstShedsExcessLeakFree(t *testing.T) {
	release := make(chan struct{})
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		<-release
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))
	t.Cleanup(srv.Close)

	_, c := coldTestStore(t, srv.URL, "", 2)
	const burst = 5
	type res struct{ err error }
	out := make(chan res, burst)
	for i := 0; i < burst; i++ {
		go func() {
			_, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`))
			out <- res{err}
		}()
	}
	// Atomic cap: with 2 slots held, the remaining calls are rejected without
	// I/O, so the first 3 completions are the rejections (admitted calls hold
	// their slots).
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt32(&calls) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("hits=%d, esperado 2 (teto)", got)
	}
	for i := 0; i < 3; i++ {
		select {
		case r := <-out:
			if !errors.Is(r.err, ErrCloudAtCapacity) {
				t.Fatalf("recusa %d err=%v, esperado ErrCloudAtCapacity", i, r.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("shed não chegou (deadlock?)")
		}
	}
	close(release) // release the 2 admitted calls
	for i := 0; i < 2; i++ {
		select {
		case r := <-out:
			if r.err != nil {
				t.Fatalf("admitida err=%v", r.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("admitida não completou após release")
		}
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("hits=%d, esperado 2 (recusadas sem I/O)", got)
	}
	if st := c.cold.state["f"]; st == nil || st.inflight != 0 {
		t.Fatalf("vazamento no tracker: %+v", st)
	}
}

// Pruning: the internal map never exceeds the functions of the current
// configuration.
func TestCold_PruneBoundsMap(t *testing.T) {
	srv := okServer(t)
	s, c := coldTestStore(t, srv.URL, "1ms", 0)
	if _, err := c.Invoke(context.Background(), "f", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if n := len(c.cold.state); n != 1 {
		t.Fatalf("state=%d, esperado 1", n)
	}
	// Switch the configuration: "f" leaves and "g" enters with the same
	// endpoint.
	if err := s.Set(CloudFallbackConfig{Connector: ConnectorAzureFunctions, ReaperTimeout: "1ms",
		Functions: map[string]FunctionEndpoint{"g": {EndpointURL: srv.URL, InputContentType: "application/json", AuthScheme: "none"}}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if c.EstimateCold("g") {
		t.Fatalf("função nova deveria ser false")
	}
	if n := len(c.cold.state); n != 0 {
		t.Fatalf("state=%d após troca, esperado 0 (poda)", n)
	}
}

// The generic connector always returns false, matching the legacy path.
func TestCold_GenericAlwaysFalse(t *testing.T) {
	srv := okServer(t)
	s, err := NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	if err := s.Set(CloudFallbackConfig{EndpointURL: srv.URL, InputContentType: "application/json", AuthScheme: "none", ReaperTimeout: "1ms"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	g := NewGenericHTTPConnector(s, nil)
	if g.EstimateCold("f") {
		t.Fatalf("genérico deveria ser sempre false")
	}
	if _, err := g.Invoke(context.Background(), "f", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if g.EstimateCold("f") {
		t.Fatalf("genérico após Invoke deveria seguir false")
	}
}

// The dispatcher forwards the estimate of the format in effect.
func TestCold_DispatcherForwards(t *testing.T) {
	srv := okServer(t)
	s, err := NewCloudStore("")
	if err != nil {
		t.Fatalf("NewCloudStore: %v", err)
	}
	d := NewConnector(s)
	if d.EstimateCold("f") {
		t.Fatalf("sem config deveria ser false")
	}
	ep := FunctionEndpoint{EndpointURL: srv.URL, InputContentType: "application/json", AuthScheme: "none"}
	if err := s.Set(CloudFallbackConfig{Connector: ConnectorAzureFunctions, ReaperTimeout: "1ms", Functions: map[string]FunctionEndpoint{"f": ep}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	now := time.Now()
	d.azure.cold.now = func() time.Time { return now }
	if _, err := d.Invoke(context.Background(), "f", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	now = now.Add(time.Second)
	d.azure.cold.now = func() time.Time { return now }
	if !d.EstimateCold("f") {
		t.Fatalf("despachante deveria repassar cold=true")
	}
}
