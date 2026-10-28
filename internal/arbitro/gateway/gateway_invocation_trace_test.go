package gateway

// The invocation log groups the attempts by invocation: an invocation with a race
// produces lines with the same request identifier, attempt numbers from one to
// the number of attempts and exactly one line marked as delivered.
import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

func TestInvocationTraceGroupsByRequest(t *testing.T) {
	srv := lateSuccessServer(t, 300*time.Millisecond, `{"result":"tardio"}`)
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	logPath := t.TempDir() + "/inv.jsonl"
	gw := waitTestGateway(t, ns, logPath)

	rec := invokeRecorder(t, gw, `{"function":"fn","slo":{"max_latency_ms":100},"input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rec.Code, rec.Body.String())
	}

	lines := readInvLogLines(t, logPath)
	if len(lines) != 2 {
		t.Fatalf("linhas=%d want 2 (arm + assentamento vencedor)", len(lines))
	}
	rid := lines[0].RequestID
	if rid == "" {
		t.Fatalf("request_id vazio na linha 0: %+v", lines[0])
	}
	for i, ln := range lines {
		if ln.RequestID != rid {
			t.Fatalf("linha %d request_id=%q want %q", i, ln.RequestID, rid)
		}
		if ln.AttemptN != 1 {
			t.Fatalf("linha %d attempt_n=%d want 1", i, ln.AttemptN)
		}
		if !ln.Armed {
			t.Fatalf("linha %d armed=false want true (pendência best-effort)", i)
		}
	}
	delivered := 0
	for _, ln := range lines {
		if ln.Delivered {
			delivered++
		}
	}
	if delivered != 1 {
		t.Fatalf("delivered=%d want exatamente 1", delivered)
	}
	if lines[0].Delivered {
		t.Fatalf("linha 0 (arm orcamento_excedido) delivered=true want false")
	}
	if !lines[1].Delivered {
		t.Fatalf("linha 1 (assentamento vencedor) delivered=false want true")
	}
}

func TestInvocationTraceSyncWin(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer srv.Close()
	ns, _ := nodes.NewNodeStore("")
	_, _ = ns.Upsert(announcedNode(t, srv.URL, 15000))
	logPath := t.TempDir() + "/inv.jsonl"
	gw := waitTestGateway(t, ns, logPath)

	rec := invokeRecorder(t, gw, `{"function":"fn","input":{"a":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	lines := readInvLogLines(t, logPath)
	if len(lines) != 1 {
		t.Fatalf("linhas=%d want 1 (vitória síncrona sem corrida)", len(lines))
	}
	if lines[0].RequestID == "" {
		t.Fatalf("request_id vazio: %+v", lines[0])
	}
	if lines[0].AttemptN != 1 {
		t.Fatalf("attempt_n=%d want 1", lines[0].AttemptN)
	}
	if lines[0].Armed {
		t.Fatalf("armed=true want false (sem estouro de fatia)")
	}
	if !lines[0].Delivered {
		t.Fatalf("delivered=false want true (resposta entregue)")
	}
}
