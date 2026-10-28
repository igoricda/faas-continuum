package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/cloud"
)

// PUT/GET/DELETE com o formato por função (ADR-0011, item 3): mesmos
// códigos da rota legada (400 INVALID_CLOUD_CONFIG, 413, 404, 204).
func TestServer_CloudFallbackPerFunction(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	perFn := `{"connector":"azure-functions","reaper_timeout":"5m","functions":{` +
		`"alpr-deteccao-placa":{"endpoint_url":"https://a.example/api/alpr","input_content_type":"application/json","auth_scheme":"none","max_instances":10},` +
		`"ocr":{"endpoint_url":"https://b.example/api/ocr","input_content_type":"application/json","auth_scheme":"none"}}}`

	rr := doReq(t, h, http.MethodPut, "/cloud-fallback", perFn, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("PUT por função status=%d, esperado 200 body=%s", rr.Code, rr.Body.String())
	}
	var saved cloud.CloudFallbackConfig
	if err := json.Unmarshal(rr.Body.Bytes(), &saved); err != nil {
		t.Fatalf("decode PUT: %v", err)
	}
	if saved.Connector != cloud.ConnectorAzureFunctions || len(saved.Functions) != 2 {
		t.Fatalf("eco do PUT = %+v", saved)
	}
	if saved.ReaperTimeout != "5m" || saved.Functions["alpr-deteccao-placa"].MaxInstances != 10 {
		t.Fatalf("eco sem campos cold (reaper/max) = %+v", saved)
	}

	rr = doReq(t, h, http.MethodGet, "/cloud-fallback", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status=%d, esperado 200 body=%s", rr.Code, rr.Body.String())
	}
	var got cloud.CloudFallbackConfig
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode GET: %v", err)
	}
	if got.Functions["ocr"].EndpointURL != "https://b.example/api/ocr" {
		t.Fatalf("GET = %+v", got)
	}

	// Connector desconhecido => 400 INVALID_CLOUD_CONFIG (fail-closed).
	rr = doReq(t, h, http.MethodPut, "/cloud-fallback", `{"connector":"lambda","functions":{"f":{"endpoint_url":"https://x.example/","input_content_type":"application/json","auth_scheme":"none"}}}`, token)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("PUT inválido status=%d, esperado 400 body=%s", rr.Code, rr.Body.String())
	}
	var er struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &er); err != nil || er.Code != "INVALID_CLOUD_CONFIG" {
		t.Fatalf("code=%q body=%s, esperado INVALID_CLOUD_CONFIG", er.Code, rr.Body.String())
	}

	rr = doReq(t, h, http.MethodDelete, "/cloud-fallback", nil, token)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("DELETE status=%d, esperado 204", rr.Code)
	}
	rr = doReq(t, h, http.MethodGet, "/cloud-fallback", nil, token)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("GET após DELETE status=%d, esperado 404", rr.Code)
	}
}

// O formato plano legado continua aceito sem migração (ADR-0011, item 5).
func TestServer_CloudFallbackLegacyStillAccepted(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()

	rr := doReq(t, h, http.MethodPut, "/cloud-fallback",
		`{"endpoint_url":"https://nuvem.example.com/invoke","input_content_type":"application/json","auth_scheme":"none"}`, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("PUT legado status=%d, esperado 200 body=%s", rr.Code, rr.Body.String())
	}
	rr = doReq(t, h, http.MethodGet, "/cloud-fallback", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status=%d, esperado 200", rr.Code)
	}
	var got cloud.CloudFallbackConfig
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.EndpointURL != "https://nuvem.example.com/invoke" || got.Functions != nil {
		t.Fatalf("GET legado = %+v", got)
	}
}
