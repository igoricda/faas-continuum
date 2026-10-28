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
func StartPprof(addr string) {
	if addr == "" {
		log.Println("arbitro: pprof disabled (no -pprof-addr flag)")
		return
	}
	if strings.HasPrefix(addr, "0.0.0.0") || strings.HasPrefix(addr, "[::]") {
		log.Printf("arbitro: REJECTED pprof bind to %s — must be 127.0.0.1 or localhost only, quitting\n", addr)
		return
	}
	if !strings.HasPrefix(addr, "127.0.0.1") && !strings.HasPrefix(addr, "localhost") {
		log.Printf("arbitro: WARNING pprof address %s does not start with 127.0.0.1 or localhost — proceeding anyway\n", addr)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("arbitro: failed to start pprof listener on %s: %v", addr, err)
		return
	}
	log.Printf("arbitro: pprof listening on %s", listener.Addr().String())
	go func() {
		if err := http.Serve(listener, nil); err != nil && err != http.ErrServerClosed {
			log.Printf("arbitro: pprof serve failed: %v", err)
		}
	}()
}
