package nodeapi

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/igoricda/faas-continuum/internal/auth"
)

func TestSLOStrip(t *testing.T) {
	backend := newFakeInstanceBackend(t, echoHandler)
	ts, token, _ := newTestServer(t, backend)
	defer ts.Close()

	resp := doRegister(t, ts, token, "func-strip", registerRequest{ImageRef: "img:1.0", MinInstances: 1, TimeoutMS: 2000})
	_, invTok := mustRegister(t, resp)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/function/func-strip/invoke", bytes.NewReader([]byte(`{"slo":{"max_latency_ms":1},"input":{}}`)))
	req.Header.Set(auth.DefaultHeaderName, invTok)
	req.Header.Set("Content-Type", "application/json")
	invResp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer invResp.Body.Close()
	if invResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, esperado %d (slo estranho deve ser ignorado)", invResp.StatusCode, http.StatusOK)
	}
}
