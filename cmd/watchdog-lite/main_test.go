// Testes do polling de prontidão do watchdog-lite:
// cobrem waitForReady puro (retorna error; sem log.Fatal dentro) só com
// httptest.Server — sem rede real, sem socket, tetos em ms para a suíte
// rodar em <<5s. O proxy de newServer fica fora de escopo aqui.
package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// waitForReady retorna nil assim que o upstream responde 200 — cenários
// de sucesso (200 imediato, transição 503→200, READY_PATH customizado).
func TestWaitForReady_BecomesReady(t *testing.T) {
	tests := []struct {
		name string
		// failFirst: nº de hits iniciais respondidos com 503 antes do
		// primeiro 200 (0 = 200 imediato).
		failFirst int
		// path: sufixo pedido ao servidor de teste; o handler só
		// responde 200 nesse caminho (404 nos demais), provando que
		// o poll respeita o READY_PATH customizado.
		path string
		// wantMinHits: nº mínimo de polls observados no servidor.
		wantMinHits int64
	}{
		{name: "200-imediato", failFirst: 0, path: "", wantMinHits: 1},
		{name: "503-entao-200", failFirst: 2, path: "", wantMinHits: 3},
		{name: "ready-path-customizado", failFirst: 0, path: "/pronto", wantMinHits: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hits atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := hits.Add(1)
				if tt.path != "" && r.URL.Path != tt.path {
					http.NotFound(w, r)
					return
				}
				if int(n) <= tt.failFirst {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(srv.Close)

			url := srv.URL + tt.path
			start := time.Now()
			if err := waitForReady(url, 2*time.Second, 10*time.Millisecond); err != nil {
				t.Fatalf("waitForReady(%q) = %v, want nil", url, err)
			}
			if got := hits.Load(); got < tt.wantMinHits {
				t.Errorf("polls observados = %d, want >= %d", got, tt.wantMinHits)
			}
			// Sucesso sai no primeiro 200, sem dormir o teto.
			if elapsed := time.Since(start); elapsed >= 2*time.Second {
				t.Errorf("waitForReady dormiu o teto: elapsed = %s", elapsed)
			}
		})
	}
}

// waitForReady nunca vê 200: qualquer status != 200 (503, 500, ...)
// conta como "ainda não pronto" e o poll só sai pelo teto (timeout),
// retornando context.DeadlineExceeded.
func TestWaitForReady_Timeout(t *testing.T) {
	tests := []struct {
		name   string
		status int
	}{
		{name: "sempre-503", status: http.StatusServiceUnavailable},
		{name: "500-conta-como-nao-pronto", status: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hits atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.WriteHeader(tt.status)
			}))
			t.Cleanup(srv.Close)

			const timeout = 300 * time.Millisecond
			start := time.Now()
			err := waitForReady(srv.URL, timeout, 20*time.Millisecond)
			elapsed := time.Since(start)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("waitForReady = %v, want %v", err, context.DeadlineExceeded)
			}
			if got := hits.Load(); got < 2 {
				t.Errorf("polls observados = %d, want >= 2 (poll repetiu até o teto)", got)
			}
			// Saiu pelo teto (ms), não por espera de segundos.
			if elapsed < timeout || elapsed > 5*time.Second {
				t.Errorf("elapsed = %s, want em [%s, 5s]", elapsed, timeout)
			}
		})
	}
}
