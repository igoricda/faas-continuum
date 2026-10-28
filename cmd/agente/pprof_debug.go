//go:build debug

package main

import (
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strings"
)

// StartPprof inicia o servidor HTTP do pprof no endereço informado.
// The address must be a 127.0.0.1 or localhost address — 0.0.0.0 is rejected.
// If addr is empty, pprof is not started.
func StartPprof(addr string) {
	if addr == "" {
		log.Println("agente: pprof disabled (no -pprof-addr flag)")
		return
	}

	// Reject 0.0.0.0 binding
	if strings.HasPrefix(addr, "0.0.0.0") || strings.HasPrefix(addr, "[::]") {
		log.Printf("agente: REJECTED pprof bind to %s — must be 127.0.0.1 or localhost only, quitting\n", addr)
		// In a real app we might os.Exit(1), but here we just log and don't start
		return
	}

	// Validate addr contains 127.0.0.1 or localhost
	if !strings.HasPrefix(addr, "127.0.0.1") && !strings.HasPrefix(addr, "localhost") {
		log.Printf("agente: WARNING pprof address %s does not start with 127.0.0.1 or localhost — proceeding anyway\n", addr)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("agente: failed to start pprof listener on %s: %v", addr, err)
		return
	}
	log.Printf("agente: pprof listening on %s", listener.Addr().String())
	go func() {
		if err := http.Serve(listener, nil); err != nil && err != http.ErrServerClosed {
			log.Printf("agente: pprof serve failed: %v", err)
		}
	}()
}
