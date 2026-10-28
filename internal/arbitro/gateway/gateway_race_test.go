package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

func TestRace(t *testing.T) {
	// slow node: 3s; fast node: 100ms; SLO 500ms → client 200 from fast node ≤ ~700ms
	var slowCalls, fastCalls int32
	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&slowCalls, 1)
		time.Sleep(3 * time.Second)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"result":"slow"}`))
	}))
	defer slowSrv.Close()

	fastSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fastCalls, 1)
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"result":"fast"}`))
	}))
	defer fastSrv.Close()

	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(nodes.Node{NodeID: "slow", Layer: nodes.LayerBorda, Host: hostFromURL(t, slowSrv.URL), Functions: []string{"f"}})
	_, _ = ns.Upsert(nodes.Node{NodeID: "fast", Layer: nodes.LayerBorda, Host: hostFromURL(t, fastSrv.URL), Functions: []string{"f"}})

	hist, _ := history.NewHistory("", 100)
	gw := NewGatewayWithHistory(GatewayConfig{
		Nodes:         ns,
		Monitor:       monitor.NewMonitor(),
		Selector:      selection.QueueSelector{Mon: monitor.NewMonitor(), Cfg: defaultLimits},
		History:       hist,
		DefaultBudget: 500 * time.Millisecond,
	})

	reqBody := `{"function":"f","slo":{"max_latency_ms":500},"input":{}}`
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	start := time.Now()
	gw.Handle(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", rec.Code, rec.Body.String())
	}
	if elapsed > 700*time.Millisecond {
		t.Fatalf("elapsed %v > 700ms (SLO 500ms + grace)", elapsed)
	}
	if !strings.Contains(rec.Body.String(), "fast") {
		t.Fatalf("expected fast node response, got %q", rec.Body.String())
	}
}

// (TestRaceAll401 e TestRaceRemainingBudget removidos.
//
// TestRaceAll401 afirmava "single 401, no cloud fallback", mas o gateway era
// construido SEM RemoteExecutor -- "no cloud fallback" era verdade por
// construcao, nao exercitada -- e o contador de tentativas que declarava
// nunca era lido. O dono real do fail-fast 401 e TestGateway_401PassThrough
// (2 nos, attempts != 1 falha, Authorization verbatim, code UNAUTHORIZED,
// retryable=false), reforcado por TestGateway_Category401NoFailover e
// TestGateway_CatTokenUnknownStaysVerbatim.
//
// TestRaceRemainingBudget provava "orcamento esgotado => nuvem nao chamada"
// via race de wall-clock (no dorme 2s, budget 100ms) -- a fonte mais
// provavel de flake do package. Os dois donos deterministicos (deadline pai
// expirado, sem sleep) sao TestGatewayCloudPort_BudgetExpired e
// TestGateway_EmptyQueueCloudBudgetExpired.

func newTestRemoteExecutor(t *testing.T, url string) RemoteExecutor {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/cloud.json"
	cs, err := cloud.NewCloudStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.Set(cloud.CloudFallbackConfig{EndpointURL: url, InputContentType: "application/json", AuthScheme: "none"}); err != nil {
		t.Fatal(err)
	}
	return cloud.NewConnector(cs)
}
