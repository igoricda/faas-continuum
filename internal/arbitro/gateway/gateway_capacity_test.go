package gateway

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/pool"
)

func TestClassifyCapacity(t *testing.T) {
	poolErr := capacityAttemptErr([]byte(`{"code":"POOL_EXHAUSTED","message":"busy"}`), "n1", "fallback")
	if poolErr == nil {
		t.Fatal("POOL_EXHAUSTED: got nil, want capacity error")
	}
	if !errors.Is(poolErr, pool.ErrPoolExhausted) {
		t.Fatalf("POOL_EXHAUSTED: errors.Is sentinel = false: %v", poolErr)
	}
	var capErr *CapacityError
	if !errors.As(poolErr, &capErr) {
		t.Fatalf("POOL_EXHAUSTED: errors.As *CapacityError = false: %v", poolErr)
	}
	if !IsCapacityRejection(poolErr) {
		t.Fatalf("POOL_EXHAUSTED: IsCapacityRejection = false: %v", poolErr)
	}

	queueErr := capacityAttemptErr([]byte(`{"code":"QUEUE_FULL","message":"full"}`), "n1", "fallback")
	if queueErr == nil {
		t.Fatal("QUEUE_FULL: got nil, want capacity error")
	}
	if !errors.Is(queueErr, pool.ErrQueueFull) {
		t.Fatalf("QUEUE_FULL: errors.Is sentinel = false: %v", queueErr)
	}
	if !errors.As(queueErr, &capErr) {
		t.Fatalf("QUEUE_FULL: errors.As *CapacityError = false: %v", queueErr)
	}
	if !IsCapacityRejection(queueErr) {
		t.Fatalf("QUEUE_FULL: IsCapacityRejection = false: %v", queueErr)
	}

	genericCases := map[string][]byte{
		"unknown_code": []byte(`{"code":"SOME_FUTURE_CODE"}`),
		"empty_code":   []byte(`{"code":""}`),
		"no_code":      []byte(`{"message":"busy"}`),
		"empty_body":   []byte{},
		"nil_body":     nil,
		"garbage":      []byte(`not-json{{{\x00\x01`),
	}
	for name, body := range genericCases {
		if got := capacityAttemptErr(body, "n1", "fallback"); got != nil {
			t.Fatalf("%s: got %v, want nil (generic path)", name, got)
		}
	}

	big := `{"code":"POOL_EXHAUSTED","pad":"` + strings.Repeat("x", maxCapacityErrorBodyBytes) + `"}`
	if got := capacityAttemptErr([]byte(big), "n1", "fallback"); got != nil {
		t.Fatalf("oversized: got %v, want nil (truncated generic path)", got)
	}

	if IsCapacityRejection(errors.New("generic failure")) {
		t.Fatal("IsCapacityRejection(generic) = true, want false")
	}
	if IsCapacityRejection(nil) {
		t.Fatal("IsCapacityRejection(nil) = true, want false")
	}
}

func TestGateway503Capacity(t *testing.T) {
	bigBody := `{"code":"POOL_EXHAUSTED","pad":"` + strings.Repeat("y", maxCapacityErrorBodyBytes) + `"}`
	cases := []struct {
		name        string
		body        string
		wantHistory bool
	}{
		{"pool_exhausted", `{"code":"POOL_EXHAUSTED","message":"pool busy"}`, true},
		{"queue_full", `{"code":"QUEUE_FULL","message":"queue full"}`, true},
		// The rejection carries the semantics of the status and not of the body, so
		// a truncated, empty or non-JSON body or an unknown code also trains the
		// history as a failure without latency, in the total and in the rate, with
		// the percentile and the count intact, while the contract of the client
		// stays the retryable overload.
		{"unknown_code", `{"code":"FUTURE_CODE"}`, true},
		{"empty_body", ``, true},
		{"garbage_body", `not-json{{{`, true},
		{"oversized_body", bigBody, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.body
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()
			ns, _ := nodes.NewNodeStore("")
			if _, err := ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL)}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			h, err := history.NewHistory("", 100)
			if err != nil {
				t.Fatalf("NewHistory: %v", err)
			}
			mon := monitor.NewMonitor()
			gw := NewGatewayWithHistory(GatewayConfig{Nodes: ns, Monitor: mon, Selector: selection.QueueSelector{Mon: mon, Cfg: defaultLimits}, History: h})
			req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"fn","input":{"a":1}}`))
			rec := httptest.NewRecorder()
			gw.Handle(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
			}
			er := decodeErrResp(t, rec)
			if er.Code != "NODE_OVERLOADED" {
				t.Fatalf("code=%q want NODE_OVERLOADED", er.Code)
			}
			if !er.Retryable {
				t.Fatalf("retryable=false want true")
			}
			if tc.wantHistory {
				if len(h.Keys()) != 2 {
					t.Fatalf("history tem %d chaves, esperado 2 (primária + agregada)", len(h.Keys()))
				}
				// A capacity rejection is a saturation, so it is recorded without
				// latency, outside the percentile and outside the service rate, since
				// the veto by rate cannot punish the node for refusing.
				count, p95, total, rate, ok := h.Stats(h.Keys()[0])
				if ok || count != 0 || p95 != 0 || total != 0 {
					t.Fatalf("stats=(count=%d p95=%d total=%d ok=%v), esperado sem taxa e sem latência", count, p95, total, ok)
				}
				if history.VetoByRate(total, rate) {
					t.Fatalf("rejeição vetou o nó (rate=%v)", rate)
				}
				if full, okFull := h.StatsFull(h.Keys()[0], time.Hour, time.Now()); !okFull || full.Saturation != 1 {
					t.Fatalf("Saturation=(%d,%v), esperado (1,true)", full.Saturation, okFull)
				}
			}
			if !tc.wantHistory && len(h.Keys()) != 0 {
				t.Fatalf("history tem %d chaves, esperado 0", len(h.Keys()))
			}
		})
	}
}
