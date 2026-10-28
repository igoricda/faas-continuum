package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/igoricda/faas-continuum/internal/auth"
	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/containerruntime"
	"github.com/igoricda/faas-continuum/internal/nodeapi"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func setupServer(cfg config.Config, publicHost string, reg *registry.Registry, pl *pool.Manager, poolAdapter *containerruntime.PoolAdapter, cred auth.Credential, collector *telemetry.Collector) (*nodeapi.Server, *http.Server) {
	srv := nodeapi.NewServer(publicHost, reg, pl, poolAdapter, cred, time.Duration(cfg.NodeAPI.DefaultTimeout), collector, poolAdapter, int(cfg.NodeAPI.MaxManagementBodyBytes), int(cfg.NodeAPI.MaxFunctionOutputBytes), time.Duration(cfg.NodeAPI.ReadHeaderTimeout), time.Duration(cfg.NodeAPI.IdleTimeout), time.Duration(cfg.Telemetry.InvocationSampleInterval), &nodeapi.SocketReadinessChecker{Addresser: poolAdapter})
	srv.WithInvokeBodyBytes(int(cfg.NodeAPI.MaxInvokeBodyBytes))

	srvHTTP := &http.Server{
		Addr:              cfg.NodeAPI.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: time.Duration(cfg.NodeAPI.ReadHeaderTimeout),
		IdleTimeout:       time.Duration(cfg.NodeAPI.IdleTimeout),
	}

	return srv, srvHTTP
}

func startServer(srvHTTP *http.Server) chan error {
	serveErr := make(chan error, 1)
	go func() {
		log.Printf("agente: escutando em %q via HTTP (host público %q)", srvHTTP.Addr, "")
		serveErr <- srvHTTP.ListenAndServe()
		return
	}()
	return serveErr
}

func handleShutdown(serveErr chan error, srvHTTP *http.Server, stopCtx context.Context) {
	select {
	case ferr := <-serveErr:
		if ferr != nil && !errors.Is(ferr, http.ErrServerClosed) {
			log.Fatalf("agente: servidor encerrado com erro: %v", ferr)
		}
	case <-stopCtx.Done():
		log.Println("agente: sinal de encerramento recebido — desligando com elegância")
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancelShutdown()
		if err := srvHTTP.Shutdown(shutdownCtx); err != nil {
			log.Printf("agente: shutdown não completou no prazo (conexões forçadas a fechar): %v", err)
		}
	}
}
