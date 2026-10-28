// Command watchdog-lite is the main process executed inside each function
// container. It starts the real process of the function, given by the FPROCESS
// environment variable, a single time, and proxies the invocations received
// on a Unix socket to that process, which listens on the loopback address at
// the port of the container.
//
// The image of the function has to include this binary, set FPROCESS and use
// the watchdog as its command or entry point.

// The limited polling of the readiness endpoint, with a ceiling, is the only
// exception to the event-driven design. The node side is event-driven through
// the notification of the file system, and bilateral polling is not the
// default.
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	socketPath := getenv("SOCKET_PATH", "/run/faas-continuum-socket/handle.sock")
	upstreamPort := getenv("UPSTREAM_PORT", "8081")
	fprocess := getenv("FPROCESS", "")
	readyPath := getenv("READY_PATH", "/_/ready")
	readyTimeout := getDuration("READY_TIMEOUT", 90*time.Second)
	readyInterval := getDuration("READY_INTERVAL", 250*time.Millisecond)

	if fprocess == "" {
		log.Fatal("watchdog-lite: variável de ambiente FPROCESS é obrigatória")
	}

	cmd, err := startFprocess(fprocess, upstreamPort)
	if err != nil {
		log.Fatalf("watchdog-lite: falha ao iniciar fprocess: %v", err)
	}
	log.Printf("watchdog-lite: fprocess iniciado (pid=%d)", cmd.Process.Pid)

	upstreamAddr := "127.0.0.1:" + upstreamPort
	readyURL := "http://" + upstreamAddr + readyPath
	if err := waitForReady(readyURL, readyTimeout, readyInterval); err != nil {
		log.Fatalf("watchdog-lite: fprocess não ficou pronto em %s: %v", readyTimeout, err)
	}
	log.Printf("watchdog-lite: fprocess pronto em %s", upstreamAddr)

	srv, listener, err := newServer(socketPath, upstreamAddr)
	if err != nil {
		log.Fatalf("watchdog-lite: %v", err)
	}

	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("watchdog-lite: servidor encerrado com erro: %v", err)
		}
	}()
	log.Printf("watchdog-lite: escutando em %q", socketPath)

	waitForShutdownSignal()
	log.Println("watchdog-lite: encerrando...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)

	if cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}
	// Reaping of the function process: without a wait the process becomes a
	// zombie while the watchdog lives, which matters when the watchdog is not
	// the first process of the container, since nothing else would collect the
	// child.
	_, _ = cmd.Process.Wait()
}

// startFprocess starts the process of the function a single time, telling it
// on which loopback port it should listen.
func startFprocess(fprocess, upstreamPort string) (*exec.Cmd, error) {
	cmd := exec.Command("sh", "-c", fprocess)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "PORT="+upstreamPort)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

// waitForReady probes the readiness endpoint until it answers success, with
// polling bounded by the deadline of the bootstrap. The socket only opens
// after that answer, and a single client avoids stuck goroutines.
func waitForReady(readyURL string, timeout, interval time.Duration) error {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(readyURL)
		if err == nil {
			status := resp.StatusCode
			resp.Body.Close()
			if status == http.StatusOK {
				return nil
			}
		}
		// An error, from a refused connection or a dial failure, or a status
		// other than success means that it is not ready yet, so the probe
		// continues until the ceiling.
		time.Sleep(interval)
	}
	return context.DeadlineExceeded
}

// newServer builds the reverse proxy and the listener on the Unix socket.
func newServer(socketPath, upstreamAddr string) (*http.Server, net.Listener, error) {
	target, err := url.Parse("http://" + upstreamAddr)
	if err != nil {
		return nil, nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)

	os.Remove(socketPath) // socket of a previous execution, when one is left
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, nil, err
	}

	// The header timeout closes connections that never finish the headers,
	// since even inside an isolated network namespace a hanging connection
	// consumes a goroutine of the resident process.
	return &http.Server{Handler: proxy, ReadHeaderTimeout: 10 * time.Second}, listener, nil
}

func waitForShutdownSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("watchdog-lite: valor inválido para %s=%q, usando padrão %s", key, v, fallback)
		return fallback
	}
	return d
}
