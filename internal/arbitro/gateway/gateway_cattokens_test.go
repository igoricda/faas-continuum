package gateway

// Translation of the token per category: with a valid category token, each node
// receives the token it expects in the concrete variant and never the token of
// the client. An unauthorized answer after the translation continues the queue,
// since a stale mapping is the fault of the operator, while the legacy verbatim
// path does not fail over on an unauthorized answer.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/cattokens"
	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

func invokeFunctionWithToken(t *testing.T, gw *Gateway, function, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{"function":"`+function+`","input":{"x":1}}`))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	rec := httptest.NewRecorder()
	gw.Handle(rec, req)
	return rec
}

func catTokenTestPair(t *testing.T, bordaAuth, nevoaAuth *string, bordaStatus, nevoaStatus int) (*nodes.NodeStore, func(), *cattokens.Store, string) {
	t.Helper()
	var mu sync.Mutex
	var got []string
	mk := func(id string, slot *string, status int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			got = append(got, id+":"+r.Header.Get("Authorization"))
			*slot = r.Header.Get("Authorization")
			mu.Unlock()
			if status != http.StatusOK {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"code":"UNAUTHORIZED"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"result":"ok"}`))
		}))
	}
	srvB := mk("borda", bordaAuth, bordaStatus)
	srvN := mk("nevoa", nevoaAuth, nevoaStatus)
	ns, err := nodes.NewNodeStore("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ns.Upsert(nodes.Node{NodeID: "borda", Layer: nodes.LayerBorda, Host: hostFromURL(t, srvB.URL), Functions: []string{"alpr-yolo-tiny"}, FunctionCategories: map[string]string{"alpr-yolo-tiny": "alpr-yolo"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ns.Upsert(nodes.Node{NodeID: "nevoa", Layer: nodes.LayerNevoa, Host: hostFromURL(t, srvN.URL), Functions: []string{"alpr-yolo-full"}, FunctionCategories: map[string]string{"alpr-yolo-full": "alpr-yolo"}}); err != nil {
		t.Fatal(err)
	}
	cs, err := cattokens.NewStore("")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := cs.Issue("alpr-yolo")
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.SetDownstream("alpr-yolo", "borda", "alpr-yolo-tiny", "tok-borda"); err != nil {
		t.Fatal(err)
	}
	if err := cs.SetDownstream("alpr-yolo", "nevoa", "alpr-yolo-full", "tok-nevoa"); err != nil {
		t.Fatal(err)
	}
	cleanup := func() { srvB.Close(); srvN.Close() }
	_ = got
	return ns, cleanup, cs, plain
}

func catTokenTestGateway(ns *nodes.NodeStore, h *history.History, cs *cattokens.Store) *Gateway {
	mon := monitor.NewMonitor()
	return NewGatewayWithHistory(GatewayConfig{
		Nodes:     ns,
		Monitor:   mon,
		Selector:  selection.QueueSelector{Mon: mon, Cfg: defaultLimits},
		History:   h,
		CatTokens: cs,
	})
}

// Each node receives its own downstream token, and never the token of the category.
func TestGateway_CatTokenTranslatedPerNode(t *testing.T) {
	var bordaAuth, nevoaAuth string
	ns, cleanup, cs, plain := catTokenTestPair(t, &bordaAuth, &nevoaAuth, http.StatusOK, http.StatusOK)
	defer cleanup()
	h, _ := history.NewHistory("", 1000)
	rec := invokeFunctionWithToken(t, catTokenTestGateway(ns, h, cs), "alpr-yolo", plain)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if bordaAuth != "tok-borda" {
		t.Fatalf("borda recebeu %q, want tok-borda (tradução)", bordaAuth)
	}
	if nevoaAuth != "" {
		t.Fatalf("nevoa não deveria ser tentada, recebeu %q", nevoaAuth)
	}
}

// An unauthorized answer with a translated token continues the queue, and the
// second node receives its own token and serves.
func TestGateway_CatTokenTranslated401Failover(t *testing.T) {
	var bordaAuth, nevoaAuth string
	ns, cleanup, cs, plain := catTokenTestPair(t, &bordaAuth, &nevoaAuth, http.StatusUnauthorized, http.StatusOK)
	defer cleanup()
	h, _ := history.NewHistory("", 1000)
	rec := invokeFunctionWithToken(t, catTokenTestGateway(ns, h, cs), "alpr-yolo", plain)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (failover pós-401 traduzido) body=%s", rec.Code, rec.Body.String())
	}
	if bordaAuth != "tok-borda" {
		t.Fatalf("borda recebeu %q, want tok-borda", bordaAuth)
	}
	if nevoaAuth != "tok-nevoa" {
		t.Fatalf("nevoa recebeu %q, want tok-nevoa", nevoaAuth)
	}
}

// An unknown token, which is not even a category, follows the legacy verbatim
// path, so the node rejects it with its own unauthorized answer and no
// translation happens.
func TestGateway_CatTokenUnknownStaysVerbatim(t *testing.T) {
	var bordaAuth string
	var nevoaAuth string
	ns, cleanup, cs, _ := catTokenTestPair(t, &bordaAuth, &nevoaAuth, http.StatusUnauthorized, http.StatusOK)
	defer cleanup()
	h, _ := history.NewHistory("", 1000)
	rec := invokeFunctionWithToken(t, catTokenTestGateway(ns, h, cs), "alpr-yolo", "token-qualquer")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401 (verbatim legado sem failover)", rec.Code)
	}
	if bordaAuth != "token-qualquer" {
		t.Fatalf("borda recebeu %q, want verbatim token-qualquer", bordaAuth)
	}
	if nevoaAuth != "" {
		t.Fatalf("nevoa não deveria ser tentada no 401 verbatim, recebeu %q", nevoaAuth)
	}
}
