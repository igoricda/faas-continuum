// Tests of the readiness polling of the watchdog: they cover the pure wait for
// readiness, which returns an error instead of terminating the process, only
// with a test server, without a real network and without a socket, and with the
// ceilings in milliseconds so the suite runs fast. The proxy of the server is
// out of scope here.
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

// The wait for readiness returns as soon as the upstream answers with a success,
// in the success scenarios of an immediate success, of a transition from a
// rejection to a success and of a custom readiness path.
func TestWaitForReady_BecomesReady(t *testing.T) {
	tests := []struct {
		name string
		// failFirst is the number of initial hits answered with a rejection before
		// the first success, where zero means an immediate success.
		failFirst int
		// The path is the suffix requested from the test server, whose handler
		// answers only on that path and with a not found on the others, which proves
		// that the polling honors the custom readiness path.
		path string
		// wantMinHits is the minimum number of polls observed by the server.
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
			// The success exits on the first success, without sleeping the whole ceiling.
			if elapsed := time.Since(start); elapsed >= 2*time.Second {
				t.Errorf("waitForReady dormiu o teto: elapsed = %s", elapsed)
			}
		})
	}
}

// The wait for readiness never sees a success: any status other than a success
// counts as not ready yet, and the polling only leaves through the ceiling, with
// a timeout of the context.
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
			// It left through the ceiling, in milliseconds, and not by waiting seconds.
			if elapsed < timeout || elapsed > 5*time.Second {
				t.Errorf("elapsed = %s, want em [%s, 5s]", elapsed, timeout)
			}
		})
	}
}
