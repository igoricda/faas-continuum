package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/registry"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func registerLoop(ctx context.Context, arbitroURL, nodeID, layer, host string, psiSupported bool, minInstances int, getFuncs func() []string, categories map[string]string, getExpected func() map[string]map[string]registry.ExpectedMetric, getCapacity func() map[string]int, getTimeouts func() map[string]int, token string) {
	client := &http.Client{Timeout: 5 * time.Second}
	// A single timer with stages: a retry of ten seconds until the first
	// registration, then a heartbeat of sixty seconds. Two independent tickers
	// used to emit about seven requests per minute in steady state.
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	doRegister := func() bool {
		funcs := getFuncs()
		var capacity map[string]int
		if getCapacity != nil {
			capacity = getCapacity()
		}
		body, err := buildRegistrationPayload(nodeID, layer, host, psiSupported, minInstances, funcs, categories, getExpected, capacity, getTimeouts)
		if err != nil {
			log.Printf("agente: falha ao montar payload de registro no Árbitro %q: %v", arbitroURL, err)
			return false
		}
		target := strings.TrimRight(arbitroURL, "/") + "/nodes"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
		if err != nil {
			log.Printf("agente: falha ao criar requisição de registro no Árbitro %q: %v", arbitroURL, err)
			return false
		}
		req.Header.Set("Authorization", token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			log.Printf("agente: falha ao registrar nó %q no Árbitro %q: %v", nodeID, arbitroURL, err)
			return false
		}
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			log.Printf("agente: Árbitro %q respondeu %d ao registro do nó %q", arbitroURL, resp.StatusCode, nodeID)
			return false
		}
		log.Printf("agente: nó %q registrado no Árbitro %q (camada %q, host %q, %d funções)", nodeID, arbitroURL, layer, host, len(funcs))
		return true
	}

	registered := doRegister()
	if registered {
		log.Printf("agente: registro inicial no Árbitro %q concluído", arbitroURL)
		tick.Reset(60 * time.Second)
	} else {
		log.Printf("agente: registro inicial no Árbitro %q falhou — retentando a cada 10s", arbitroURL)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if doRegister() && !registered {
				registered = true
				tick.Reset(60 * time.Second)
			}
		}
	}
}

func setupArbitro(cfg config.Config) (string, string, int, string) {
	arbURL := strings.TrimSpace(cfg.Arbitro.URL)
	nodeLayer := cfg.Arbitro.Layer
	if nodeLayer == "" {
		nodeLayer = "borda"
	}
	nodeMinInstances := cfg.Arbitro.MinInstances
	tokenFile := strings.TrimSpace(cfg.Arbitro.TokenFile)
	token := strings.TrimSpace(cfg.Arbitro.Token)

	if tokenFile != "" {
		b, err := os.ReadFile(tokenFile)
		if err != nil {
			log.Fatalf("agente: falha ao ler arquivo de token do árbitro %q: %v", tokenFile, err)
		}
		token = strings.TrimSpace(string(b))
	}

	if arbURL != "" {
		if token == "" {
			log.Fatalf("agente: -arbitro-url exige -arbitro-token ou -arbitro-token-file com token não vazio")
		}
		if len(token) > 4096 {
			log.Fatalf("agente: token do árbitro excede limite de 4096 bytes")
		}
		log.Printf("agente: modo Continuum — Árbitro em %q, camada %q (ver Reporter em internal/telemetry e Server em internal/arbitro/server)", arbURL, nodeLayer)
	} else {
		log.Printf("agente: modo Standalone — nenhum Árbitro configurado (ver Server em internal/nodeapi)")
	}

	return arbURL, nodeLayer, nodeMinInstances, token
}

func setupArbitroIntegration(cfg config.Config, arbURL, nodeLayer, publicHost string, collector *telemetry.Collector, nodeName string, nodeMinInstances int, reg *registry.Registry, token string, stopCtx context.Context) {
	if arbURL != "" {
		go registerLoop(stopCtx, arbURL, nodeName, nodeLayer, publicHost, collector.PSISupported(), nodeMinInstances, func() []string {
			funcs := reg.List()
			names := make([]string, 0, len(funcs))
			for _, f := range funcs {
				names = append(names, f.Name)
			}
			return names
		}, cfg.Arbitro.FunctionCategories, func() map[string]map[string]registry.ExpectedMetric {
			var out map[string]map[string]registry.ExpectedMetric
			for _, f := range reg.List() {
				if len(f.ExpectedMetrics) == 0 {
					continue
				}
				if out == nil {
					out = make(map[string]map[string]registry.ExpectedMetric)
				}
				out[f.Name] = f.ExpectedMetrics
			}
			return out
		}, func() map[string]int {
			funcs := reg.List()
			if len(funcs) == 0 {
				return nil
			}
			out := make(map[string]int, len(funcs))
			for _, f := range funcs {
				out[f.Name] = f.MaxInstances
			}
			return out
		}, func() map[string]int {
			return effectiveFunctionTimeouts(reg.List(), time.Duration(cfg.NodeAPI.DefaultTimeout), time.Duration(cfg.NodeAPI.LateCapMS)*time.Millisecond)
		}, token)

		if cfg.Telemetry.Enabled {
			telemetryTarget := strings.TrimRight(arbURL, "/") + "/nodes/" + nodeName + "/telemetry"
			hdr := http.Header{"Authorization": {token}}
			reporter := telemetry.NewReporter(telemetryTarget, time.Duration(cfg.Telemetry.ReportInterval), collector, nil, hdr)
			go reporter.Run(stopCtx)
			log.Printf("agente: envio periódico de telemetria ativo (%s → %s) via Árbitro", time.Duration(cfg.Telemetry.ReportInterval), telemetryTarget)
		} else {
			log.Printf("agente: telemetria desativada (telemetry.enabled=false) — Reporter não iniciado apesar de modo Continuum")
		}
	} else if cfg.NodeAPI.MonitorURL != "" {
		if !cfg.Telemetry.Enabled {
			log.Printf("agente: telemetria desativada (telemetry.enabled=false) — Reporter não iniciado apesar de monitor-url")
		} else {
			reporter := telemetry.NewReporter(cfg.NodeAPI.MonitorURL, time.Duration(cfg.Telemetry.ReportInterval), collector, nil, nil)
			go reporter.Run(stopCtx)
			log.Printf("agente: envio periódico de telemetria ativo (%s → %s)", time.Duration(cfg.Telemetry.ReportInterval), cfg.NodeAPI.MonitorURL)
		}
	}
}
