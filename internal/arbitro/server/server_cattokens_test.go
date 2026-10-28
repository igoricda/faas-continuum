package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/cattokens"
)

// POST emite show-once; PUT registra downstream; GET lista sem plaintext;
// categoria vazia => 400; store ausente => 501.
func TestServer_CategoryTokensLifecycle(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	cs, err := cattokens.NewStore("")
	if err != nil {
		t.Fatal(err)
	}
	srv.SetCatTokens(cs)
	h := srv.Handler()

	rr := doReq(t, h, http.MethodPost, "/category-tokens/lpr", nil, token)
	if rr.Code != http.StatusCreated {
		t.Fatalf("POST status=%d, esperado 201 body=%s", rr.Code, rr.Body.String())
	}
	var issued struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &issued); err != nil || issued.Token == "" {
		t.Fatalf("POST sem token: %s", rr.Body.String())
	}

	rr = doReq(t, h, http.MethodPut, "/category-tokens/lpr/downstream",
		`{"node_id":"xp4","concrete":"alpr-ncnn","token":"tok-no"}`, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("PUT status=%d, esperado 200 body=%s", rr.Code, rr.Body.String())
	}

	rr = doReq(t, h, http.MethodGet, "/category-tokens", nil, token)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status=%d, esperado 200 body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if stringContains(body, "tok-no") {
		t.Fatalf("GET vazou plaintext: %s", body)
	}
	var got struct {
		Categories map[string]map[string][]string `json:"categories"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode GET: %v", err)
	}
	if len(got.Categories["lpr"]["xp4"]) != 1 || got.Categories["lpr"]["xp4"][0] != "alpr-ncnn" {
		t.Fatalf("GET = %v", got.Categories)
	}
	if !cs.Verify("lpr", issued.Token) {
		t.Fatal("token emitido não verifica no store")
	}
	if tok, ok := cs.DownstreamFor("lpr", "xp4", "alpr-ncnn"); !ok || tok != "tok-no" {
		t.Fatalf("downstream=%q,%v", tok, ok)
	}

	// Categoria vazia (%20 não casa \x00 mas Issue rejeita " "?) — caminho
	// inválido: downstream sem node.
	rr = doReq(t, h, http.MethodPut, "/category-tokens/lpr/downstream",
		`{"node_id":"","concrete":"alpr-ncnn","token":"x"}`, token)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("PUT sem node status=%d, esperado 400", rr.Code)
	}
}

func TestServer_CategoryTokensNotConfigured(t *testing.T) {
	srv, token, _, _ := newTestServerEnv(t)
	h := srv.Handler()
	rr := doReq(t, h, http.MethodPost, "/category-tokens/lpr", nil, token)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("POST sem store status=%d, esperado 501", rr.Code)
	}
	rr = doReq(t, h, http.MethodGet, "/category-tokens", nil, token)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("GET sem store status=%d, esperado 501", rr.Code)
	}
}

func stringContains(hay, needle string) bool {
	for i := 0; i+len(needle) <= len(hay); i++ {
		if hay[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
