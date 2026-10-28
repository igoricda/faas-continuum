package gateway

// Table tests of the routing by quality: a qualifying variant is served
// without the header, a miss is served with the header and SloMet false, a
// malformed domain_metrics yields 400, a missing host preserves the 503 and
// a nil SLO goes through the path verbatim.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/registry"
)

func qualityFixtureNode(t *testing.T, srvURL string) nodes.Node {
	t.Helper()
	return nodes.Node{
		NodeID:    "n1",
		Layer:     nodes.LayerBorda,
		Host:      hostFromURL(t, srvURL),
		Functions: []string{"alpr-fast", "alpr-strong"},
		FunctionCategories: map[string]string{
			"alpr-fast":   "alpr",
			"alpr-strong": "alpr",
		},
		FunctionExpected: map[string]map[string]registry.ExpectedMetric{
			"alpr-fast":   {"accuracy": {Value: 0.90, HigherIsBetter: true}},
			"alpr-strong": {"accuracy": {Value: 0.97, HigherIsBetter: true}},
		},
	}
}

func qualityServeFixture(t *testing.T, capturedPath *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
}

func qualityInvoke(t *testing.T, gw *Gateway, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	return rec
}

func qualityLastRecord(t *testing.T, path string) InvocationRecord {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := splitLines(t, raw)
	if len(lines) == 0 {
		t.Fatalf("invocation log vazio")
	}
	var rec InvocationRecord
	if err := json.Unmarshal(lines[len(lines)-1], &rec); err != nil {
		t.Fatalf("linha inválida: %v", err)
	}
	return rec
}

func TestGateway_QualityRouting(t *testing.T) {
	tests := []struct {
		name          string
		slo           string
		wantPath      string
		wantDowngrade bool
		wantSloMet    bool
	}{
		{
			name:          "qualifying-fast served sem header",
			slo:           `{"accuracy":0.85}`,
			wantPath:      "/function/alpr-fast/invoke",
			wantDowngrade: false,
			wantSloMet:    true,
		},
		{
			name:          "qualifying-strong served sem header",
			slo:           `{"accuracy":0.95}`,
			wantPath:      "/function/alpr-strong/invoke",
			wantDowngrade: false,
			wantSloMet:    true,
		},
		{
			name:          "miss servido com downgrade",
			slo:           `{"accuracy":0.99}`,
			wantPath:      "/function/alpr-fast/invoke",
			wantDowngrade: true,
			wantSloMet:    false,
		},
		{
			name:          "nil SLO caminho verbatim sem header",
			slo:           "",
			wantPath:      "/function/alpr-fast/invoke",
			wantDowngrade: false,
			wantSloMet:    true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedPath string
			srv := qualityServeFixture(t, &capturedPath)
			defer srv.Close()

			ns, _ := nodes.NewNodeStore("")
			if _, err := ns.Upsert(qualityFixtureNode(t, srv.URL)); err != nil {
				t.Fatalf("Upsert: %v", err)
			}
			logPath := filepath.Join(t.TempDir(), "invocations.jsonl")
			l, err := NewInvocationLog(logPath)
			if err != nil {
				t.Fatalf("NewInvocationLog: %v", err)
			}
			gw := newTestGateway(ns)
			gw.SetInvocationLog(l)

			var body string
			if tc.slo == "" {
				body = `{"function":"alpr","input":{"img":1}}`
			} else {
				body = `{"function":"alpr","slo":{"domain_metrics":` + tc.slo + `},"input":{"img":1}}`
			}
			rec := qualityInvoke(t, gw, body)

			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
			}
			if capturedPath != tc.wantPath {
				t.Fatalf("concreto=%q want %q", capturedPath, tc.wantPath)
			}
			gotHeader := rec.Header().Get("X-Faas-Quality-Downgrade")
			if tc.wantDowngrade {
				if gotHeader != "true" {
					t.Fatalf("header X-Faas-Quality-Downgrade=%q want %q (deve estar no wire antes do WriteHeader)", gotHeader, "true")
				}
			} else if gotHeader != "" {
				t.Fatalf("header X-Faas-Quality-Downgrade=%q want ausente", gotHeader)
			}
			if err := l.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			gotRec := qualityLastRecord(t, logPath)
			if gotRec.SloMet != tc.wantSloMet {
				t.Fatalf("SloMet=%v want %v", gotRec.SloMet, tc.wantSloMet)
			}
		})
	}
}

func TestGateway_QualityMalformed400(t *testing.T) {
	cases := []struct {
		name          string
		domainMetrics string
	}{
		{"array nao-objeto", `[1,2]`},
		{"string nao-objeto", `"high"`},
		{"numero nao-objeto", `0.9`},
		{"valor string", `{"accuracy":"high"}`},
		{"valor null", `{"accuracy":null}`},
		{"valor bool", `{"accuracy":true}`},
		{"mapa misto numerico+nao-numerico", `{"accuracy":0.9,"label":"high"}`},
		{"json invalido", `{`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns, _ := nodes.NewNodeStore("")
			gw := newTestGateway(ns)
			body := `{"function":"alpr","slo":{"domain_metrics":` + tc.domainMetrics + `},"input":{"img":1}}`
			rec := qualityInvoke(t, gw, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d want 400 body=%s", rec.Code, rec.Body.String())
			}
			er := decodeErrResp(t, rec)
			if er.Code != "INVALID_REQUEST" {
				t.Fatalf("code=%q want INVALID_REQUEST", er.Code)
			}
			if er.Retryable {
				t.Fatalf("retryable true want false")
			}
			if got := rec.Header().Get("X-Faas-Quality-Downgrade"); got != "" {
				t.Fatalf("header presente no 400: %q", got)
			}
		})
	}
}

func TestGateway_QualityNoHost503(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer srv.Close()

	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: hostFromURL(t, srv.URL), Functions: []string{"other"}})
	gw := newTestGateway(ns)

	rec := qualityInvoke(t, gw, `{"function":"alpr","slo":{"domain_metrics":{"accuracy":0.99}},"input":{"img":1}}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	er := decodeErrResp(t, rec)
	if er.Code != "NO_NODE_AVAILABLE" {
		t.Fatalf("code=%q want NO_NODE_AVAILABLE", er.Code)
	}
	if !er.Retryable {
		t.Fatalf("retryable false want true")
	}
	if got := rec.Header().Get("X-Faas-Quality-Downgrade"); got != "" {
		t.Fatalf("header presente no 503: %q", got)
	}
}
