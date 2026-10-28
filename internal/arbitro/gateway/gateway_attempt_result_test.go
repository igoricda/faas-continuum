package gateway

// Tabela status→classe do Passo 4.2 (adendo 2026-09-30): trava o refactor
// que extraiu readAttemptResult/classifyAttemptResult do closure do dispatch.
// Cada linha fixa kind + failure (status/code/retryable) + classe do
// attemptErr + reason/pressure.
//
// Gate test-audit: protege o mapeamento (contrato comportamental extraído,
// não a forma do código). Regressão crível: drift no switch durante o
// refactor (ex.: ramo 504 que perde a pressão — pego por este teste na
// extração). TestClassifyAttempt cobre err→outcome, não status→classe
// (sem duplicação). Sem seam novo (funções puras, entradas sintéticas).
import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/registry"
)

func classifyTarget() nodes.Node {
	return nodes.Node{
		NodeID:    "n1",
		Layer:     nodes.LayerBorda,
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

func TestClassifyAttemptResultTable(t *testing.T) {
	big := bytes.Repeat([]byte("a"), int(maxInvokeBodyBytes)+1)
	truncated := append([]byte(`{"code":"POOL_EXHAUSTED","x":`), bytes.Repeat([]byte("a"), int(maxCapacityErrorBodyBytes)+1)...)
	target := classifyTarget()
	tests := []struct {
		name       string
		res        attemptResult
		reqs       map[string]float64
		declared   bool
		translated bool
		wantKind   attemptKind
		wantStatus int
		wantCode   string
		wantRetry  bool
		wantReason string
		checkErr   func(t *testing.T, err error)
		checkMore  func(t *testing.T, failure gatewayFailure, pressure *NodeAdmissionPressure)
	}{
		{
			name: "200 válido sem qualidade",
			res:  attemptResult{status: 200, body: []byte(`{"result":1}`), metrics: []byte(`{}`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("err=%v want nil", err)
				}
			},
			wantKind: attemptSuccess, wantReason: "OK",
		},
		{
			name:     "200 com downgrade",
			res:      attemptResult{status: 200, body: []byte(`{"result":1}`), metrics: []byte(`{}`)},
			reqs:     map[string]float64{"accuracy": 0.99},
			declared: true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("err=%v want nil", err)
				}
			},
			wantKind: attemptDowngrade, wantReason: "DOWNGRADE",
		},
		{
			name:     "200 qualificado não é downgrade",
			res:      attemptResult{status: 200, body: []byte(`{"result":1}`), metrics: []byte(`{}`)},
			reqs:     map[string]float64{"accuracy": 0.85},
			declared: true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("err=%v want nil", err)
				}
			},
			wantKind: attemptSuccess, wantReason: "OK",
		},
		{
			name: "200 acima de 8MiB",
			res:  attemptResult{status: 200, body: big, framingErr: errors.New("framing")},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil {
					t.Fatalf("err=nil want framing")
				}
			},
			wantKind: attemptNext, wantStatus: 502, wantCode: "NODE_ERROR",
			wantRetry: true, wantReason: "response_too_large",
			checkMore: func(t *testing.T, f gatewayFailure, _ *NodeAdmissionPressure) {
				t.Helper()
				if f.msg != "resposta do nó excede o limite de 8 MiB" {
					t.Fatalf("msg=%q", f.msg)
				}
			},
		},
		{
			name: "200 JSON inválido",
			res:  attemptResult{status: 200, body: []byte(`não-json`), framingErr: errors.New("framing")},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil {
					t.Fatalf("err=nil want framing")
				}
			},
			wantKind: attemptNext, wantStatus: 502, wantCode: "NODE_ERROR",
			wantRetry: true, wantReason: "invalid_json",
			checkMore: func(t *testing.T, f gatewayFailure, _ *NodeAdmissionPressure) {
				t.Helper()
				if f.msg != "resposta do nó não é JSON válido" {
					t.Fatalf("msg=%q", f.msg)
				}
			},
		},
		{
			name: "401 verbatim fail-fast",
			res:  attemptResult{status: 401, body: []byte(`{}`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, ErrAttemptUnauthorized) {
					t.Fatalf("err=%v want ErrAttemptUnauthorized", err)
				}
			},
			wantKind: attemptFailFast, wantStatus: 401, wantCode: "UNAUTHORIZED",
			wantRetry: false, wantReason: "UNAUTHORIZED",
			checkMore: func(t *testing.T, f gatewayFailure, _ *NodeAdmissionPressure) {
				t.Helper()
				if f.msg != "credencial de invocação inválida ou ausente" {
					t.Fatalf("msg=%q", f.msg)
				}
			},
		},
		{
			name:       "401 traduzido continua",
			res:        attemptResult{status: 401, body: []byte(`{}`)},
			translated: true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, ErrAttemptUnauthorized) {
					t.Fatalf("err=%v want ErrAttemptUnauthorized", err)
				}
			},
			wantKind: attemptNext, wantStatus: 401, wantCode: "UNAUTHORIZED",
			wantRetry: true, wantReason: "UNAUTHORIZED_TRANSLATED",
		},
		{
			name: "400 fail-fast",
			res:  attemptResult{status: 400, body: []byte(`{}`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, ErrAttemptBadRequest) {
					t.Fatalf("err=%v want ErrAttemptBadRequest", err)
				}
			},
			wantKind: attemptFailFast, wantStatus: 400, wantCode: "NODE_BAD_REQUEST",
			wantRetry: false, wantReason: "BAD_REQUEST_FAIL_FAST",
		},
		{
			name: "404 continua",
			res:  attemptResult{status: 404, body: []byte(`{}`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, ErrAttemptNotFound) {
					t.Fatalf("err=%v want ErrAttemptNotFound", err)
				}
			},
			wantKind: attemptNext, wantStatus: 404, wantCode: "FUNCTION_NOT_FOUND",
			wantRetry: false, wantReason: "FUNCTION_NOT_FOUND",
		},
		{
			name: "503 POOL_EXHAUSTED com identidade",
			res:  attemptResult{status: 503, body: []byte(`{"code":"POOL_EXHAUSTED"}`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !IsCapacityRejection(err) {
					t.Fatalf("err=%v want identidade de capacidade", err)
				}
			},
			wantKind: attemptNext, wantStatus: 503, wantCode: "NODE_OVERLOADED",
			wantRetry: true, wantReason: "NODE_OVERLOADED",
		},
		{
			name: "503 QUEUE_FULL com identidade",
			res:  attemptResult{status: 503, body: []byte(`{"code":"QUEUE_FULL"}`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !IsCapacityRejection(err) {
					t.Fatalf("err=%v want identidade de capacidade", err)
				}
			},
			wantKind: attemptNext, wantStatus: 503, wantCode: "NODE_OVERLOADED",
			wantRetry: true, wantReason: "NODE_OVERLOADED",
		},
		{
			name: "503 genérico sem identidade",
			res:  attemptResult{status: 503, body: []byte(`ocupado`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, ErrAttemptOverloaded) || IsCapacityRejection(err) {
					t.Fatalf("err=%v want ErrAttemptOverloaded sem identidade", err)
				}
			},
			wantKind: attemptNext, wantStatus: 503, wantCode: "NODE_OVERLOADED",
			wantRetry: true, wantReason: "NODE_OVERLOADED",
		},
		{
			name: "503 acima do teto vira genérico",
			res:  attemptResult{status: 503, body: truncated},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, ErrAttemptOverloaded) || IsCapacityRejection(err) {
					t.Fatalf("err=%v want genérico (corpo truncado)", err)
				}
			},
			wantKind: attemptNext, wantStatus: 503, wantCode: "NODE_OVERLOADED",
			wantRetry: true, wantReason: "NODE_OVERLOADED",
		},
		{
			name: "504 INVOCATION_TIMEOUT é falha de serviço",
			res:  attemptResult{status: 504, body: []byte(`{"code":"INVOCATION_TIMEOUT"}`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, ErrAttemptTimeout) || errors.Is(err, ErrAttemptAdmissionTimeout) {
					t.Fatalf("err=%v want ErrAttemptTimeout (não admissão)", err)
				}
			},
			wantKind: attemptNext, wantStatus: 504, wantCode: "NODE_INVOCATION_TIMEOUT",
			wantRetry: true, wantReason: "NODE_INVOCATION_TIMEOUT",
			checkMore: func(t *testing.T, _ gatewayFailure, p *NodeAdmissionPressure) {
				t.Helper()
				if p != nil {
					t.Fatalf("pressure=%+v want nil (sem bloco)", p)
				}
			},
		},
		{
			name: "504 ACQUIRE_TIMEOUT com pressão",
			res:  attemptResult{status: 504, body: []byte(`{"code":"ACQUIRE_TIMEOUT","pressure":{"pool_alive":36,"queue_depth":5,"acquire_wait_ms":14998}}`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, ErrAttemptAdmissionTimeout) {
					t.Fatalf("err=%v want ErrAttemptAdmissionTimeout", err)
				}
			},
			wantKind: attemptNext, wantStatus: 504, wantCode: "NODE_INVOCATION_TIMEOUT",
			wantRetry: true, wantReason: "NODE_INVOCATION_TIMEOUT",
			checkMore: func(t *testing.T, _ gatewayFailure, p *NodeAdmissionPressure) {
				t.Helper()
				if p == nil || p.PoolAlive != 36 || p.QueueDepth != 5 || p.AcquireWaitMS != 14998 {
					t.Fatalf("pressure=%+v want {36 5 14998}", p)
				}
			},
		},
		{
			name: "504 INSTANCE_NOT_READY_TIMEOUT é admissão",
			res:  attemptResult{status: 504, body: []byte(`{"code":"INSTANCE_NOT_READY_TIMEOUT"}`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, ErrAttemptAdmissionTimeout) {
					t.Fatalf("err=%v want ErrAttemptAdmissionTimeout", err)
				}
			},
			wantKind: attemptNext, wantStatus: 504, wantCode: "NODE_INVOCATION_TIMEOUT",
			wantRetry: true, wantReason: "NODE_INVOCATION_TIMEOUT",
		},
		{
			name: "500 default",
			res:  attemptResult{status: 500, body: []byte(`oops`)},
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil || errors.Is(err, ErrAttemptTimeout) || errors.Is(err, ErrAttemptOverloaded) {
					t.Fatalf("err=%v want genérico", err)
				}
			},
			wantKind: attemptNext, wantStatus: 502, wantCode: "NODE_ERROR",
			wantRetry: true, wantReason: "NODE_ERROR",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attemptErr, failure, kind, reason, pressure := classifyAttemptResult(tc.res, target, "alpr", "alpr", tc.reqs, tc.declared, tc.translated)
			if kind != tc.wantKind {
				t.Fatalf("kind=%d want %d", kind, tc.wantKind)
			}
			if reason != tc.wantReason {
				t.Fatalf("reason=%q want %q", reason, tc.wantReason)
			}
			if tc.checkErr != nil {
				tc.checkErr(t, attemptErr)
			}
			if tc.wantCode != "" {
				if failure.httpStatus != tc.wantStatus || failure.code != tc.wantCode || failure.retryable != tc.wantRetry {
					t.Fatalf("failure=%+v want status=%d code=%s retry=%v", failure, tc.wantStatus, tc.wantCode, tc.wantRetry)
				}
			}
			if tc.checkMore != nil {
				tc.checkMore(t, failure, pressure)
			} else if pressure != nil {
				t.Fatalf("pressure=%+v want nil", pressure)
			}
		})
	}
}

type closeTracker struct {
	io.Reader
	closed bool
}

func (c *closeTracker) Close() error {
	c.closed = true
	return nil
}

// readAttemptResult preserva o enquadramento do caminho legado: 2xx até
// 8MiB+1 com métricas, erro até 64KiB+1 verbatim para o parse de código, e o
// corpo sempre fechado.
func TestReadAttemptResultFraming(t *testing.T) {
	newResp := func(status int, body []byte) (*http.Response, *closeTracker) {
		ct := &closeTracker{Reader: bytes.NewReader(body)}
		return &http.Response{StatusCode: status, Body: ct}, ct
	}
	t.Run("2xx válido extrai métricas", func(t *testing.T) {
		resp, ct := newResp(200, []byte(`{"result":1,"execution_metrics":{"cold_start":true}}`))
		res := readAttemptResult(resp)
		if !ct.closed {
			t.Fatalf("corpo não fechado")
		}
		if res.framingErr != nil {
			t.Fatalf("framingErr=%v want nil", res.framingErr)
		}
		if !strings.Contains(string(res.metrics), "cold_start") {
			t.Fatalf("metrics=%s want bloco execution_metrics", res.metrics)
		}
	})
	t.Run("2xx acima de 8MiB", func(t *testing.T) {
		big := append([]byte(`{"d":"`), bytes.Repeat([]byte("a"), int(maxInvokeBodyBytes)+1)...)
		resp, ct := newResp(200, big)
		res := readAttemptResult(resp)
		if !ct.closed {
			t.Fatalf("corpo não fechado")
		}
		if res.framingErr == nil || !errors.Is(res.framingErr, ErrAttemptFraming) {
			t.Fatalf("framingErr=%v want ErrAttemptFraming", res.framingErr)
		}
		if res.metrics != nil {
			t.Fatalf("metrics=%s want nil", res.metrics)
		}
	})
	t.Run("2xx inválido", func(t *testing.T) {
		resp, ct := newResp(200, []byte(`não-json`))
		res := readAttemptResult(resp)
		if !ct.closed {
			t.Fatalf("corpo não fechado")
		}
		if res.framingErr == nil || !errors.Is(res.framingErr, ErrAttemptFraming) {
			t.Fatalf("framingErr=%v want ErrAttemptFraming", res.framingErr)
		}
	})
	t.Run("503 preserva corpo para o parse de código", func(t *testing.T) {
		resp, ct := newResp(503, []byte(`{"code":"POOL_EXHAUSTED"}`))
		res := readAttemptResult(resp)
		if !ct.closed {
			t.Fatalf("corpo não fechado")
		}
		if string(res.body) != `{"code":"POOL_EXHAUSTED"}` {
			t.Fatalf("body=%s want verbatim", res.body)
		}
		if res.metrics != nil || res.framingErr != nil {
			t.Fatalf("erro não carrega metrics/framing: %+v", res)
		}
	})
}
