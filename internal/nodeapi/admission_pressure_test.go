package nodeapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestAdmissionPressureNoCorpoDo504 checks that the gateway timeout
// body now states what the node knows at the instant it gives up.
//
// The gap was on both sides and was the same: the producer fills the wait
// of the acquisition and the depth of the queue on the successful path,
// after the answer, and the arbiter only reads them from the capacity
// piggyback, which also only exists on a successful answer. A request
// that never ran, which is the only case in which the admission
// pressure is directly observable, therefore reported nothing.
func TestAdmissionPressureNoCorpoDo504(t *testing.T) {
	// The path that has to carry the snapshot is the readiness one: the
	// container comes up but never becomes ready in time, so the
	// function never runs. A handler that answers quickly with a
	// instantaneous socket overflows on the call itself, which is the
	// service timeout and does not receive the block.
	backend := newDelayedBackend(t, echoHandler, 0, 30*time.Second)
	ts, token, _ := newDelayedTestServer(t, backend)
	defer ts.Close()
	resp := doRegister(t, ts, token, "fn-adm", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 5, TimeoutMS: 150})
	_, invTok := mustRegister(t, resp)

	inv := doInvoke(t, ts, "fn-adm", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
	defer inv.Body.Close()
	if inv.StatusCode != http.StatusGatewayTimeout {
		b, _ := readAll(inv)
		t.Fatalf("status = %d, want 504; body: %s", inv.StatusCode, string(b))
	}
	var body errorResponse
	raw, _ := readAll(inv)
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("corpo nao e JSON: %s", string(raw))
	}
	if body.Code != "INSTANCE_NOT_READY_TIMEOUT" {
		t.Fatalf("code = %q, esperado INSTANCE_NOT_READY_TIMEOUT", body.Code)
	}
	if body.Pressure == nil {
		t.Fatalf("corpo sem o bloco `pressure`: %s — o no calou no unico momento em que tem o que dizer", string(raw))
	}
	// The signature of this path: the admission was instantaneous, since
	// the container was provisioned quickly, and what timed out was the
	// readiness. The number of live instances is therefore at least one,
	// since the instance exists and it is the one that never became
	// ready, and the wait of the acquisition is about zero.
	//
	// The first version of this test required a positive wait and failed
	// with zero. The zero was correct: the admission takes milliseconds
	// and the whole budget goes to the readiness. Requiring a positive
	// wait would assert that the two gateway timeouts are the same, which
	// is exactly what this field distinguishes.
	if body.Pressure.PoolAlive < 1 {
		t.Errorf("pool_alive = %d: o no tem instancia viva (foi ela que nao ficou pronta)", body.Pressure.PoolAlive)
	}
	if body.Pressure.AcquireWaitMS > 50 {
		t.Errorf("acquire_wait_ms = %d: neste caminho a admissao e instantanea; se for grande, o caso e ACQUIRE_TIMEOUT e nao readiness", body.Pressure.AcquireWaitMS)
	}
	t.Logf("pressure: pool_alive=%d queue_depth=%d acquire_wait_ms=%d",
		body.Pressure.PoolAlive, body.Pressure.QueueDepth, body.Pressure.AcquireWaitMS)
}

// TestAdmissionPressureAusenteNosOutrosCodigos checks that the block is
// additive: in the codes that already existed the body must be exactly
// what it was, otherwise the correction becomes a break of the contract
// instead of an addition.
func TestAdmissionPressureAusenteNosOutrosCodigos(t *testing.T) {
	ts, token, _ := newDelayedTestServer(t, newDelayedBackend(t, errorHandler, 0, 0))
	defer ts.Close()
	resp := doRegister(t, ts, token, "fn-adm-2", registerRequest{ImageRef: "img:1.0", MinInstances: 0, MaxInstances: 5, TimeoutMS: 5000})
	_, invTok := mustRegister(t, resp)
	inv := doInvoke(t, ts, "fn-adm-2", invTok, invokeRequest{Input: json.RawMessage(`{}`)})
	defer inv.Body.Close()
	raw, _ := readAll(inv)
	// It compares the JSON key and not the word, since the name of the
	// function appears in the error message.
	if strings.Contains(string(raw), `"pressure"`) {
		t.Fatalf("corpo de erro sem 504 ganhou o bloco pressure: %s", string(raw))
	}
}

// TestAdmissionPressurePoolNil returns a zeroed snapshot instead of
// panicking: with no pool, in a node without a pool, in a test or in the
// degraded mode, the block stays present with zeros. It is deliberate and
// deserves its own test, because the reading of a zero depth and a zero
// number of live instances is ambiguous by nature: a consumer has to know
// that an absence and a zero are different things.
func TestAdmissionPressurePoolNil(t *testing.T) {
	s := &Server{}
	p := s.admissionPressure("fn", 1234*time.Millisecond)
	if p.AcquireWaitMS != 1234 {
		t.Errorf("AcquireWaitMS = %d, esperado 1234", p.AcquireWaitMS)
	}
	if p.PoolAlive != 0 || p.QueueDepth != 0 {
		t.Errorf("retrato com pool nil = %+v, esperado tudo zero", p)
	}
}

// The parser of the pressure block is tested in the package that owns it,
// in the gateway. Here the contract covered is the one of the producer:
// the body that this package writes. A shim that reimplemented the
// decoding was removed, because it pinned the shape of the JSON without
// exercising the production parser.
