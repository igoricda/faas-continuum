// Command agente is the process executed in the edge and fog nodes. It runs
// the container runtime, the registry, the pool, the telemetry and the node
// interface.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/igoricda/faas-continuum/internal/containerruntime"
)

func main() {
	configPath := flag.String("config", "./config.jsonc", "caminho do config.jsonc (JSONC com comentários; sobrepõe defaults; flags ainda vencem)")
	flag.String("socket", containerruntime.DefaultSocket, "caminho do socket do containerd")
	flag.String("namespace", containerruntime.DefaultNamespace, "namespace do containerd usado pelo framework")
	flag.String("listen", ":8080", "endereço em que a API HTTP do nó escuta")
	flag.String("host", "", "endereço público deste nó, usado para compor host/function/nome-unico (padrão: mesmo valor de -listen)")
	credentialPath := flag.String("credential", "./faas-continuum-credential.json", "caminho do arquivo de credencial local (ver LoadOrGenerate em internal/auth); gerado automaticamente no primeiro uso")
	flag.String("registry", "./faas-continuum-functions.json", "caminho do arquivo de persistência do registro de funções (ver NewPersistentRegistry em internal/registry); gerado automaticamente no primeiro registro")
	flag.String("socket-root", containerruntime.DefaultHostSocketRoot, "diretório base onde os sockets Unix de invocação de cada instância são criados (ver SocketPath em internal/containerruntime)")
	flag.Duration("default-timeout", 90*time.Second, "timeout padrão de invocação, usado quando a função e o SLO da requisição não declaram um (ver DefaultTimeout em internal/config)")
	flag.Duration("reconcile-timeout", 60*time.Second, "prazo máximo para a reconciliação de startup (ver Run em internal/reconcile) antes de o agente subir mesmo assim")

	flag.String("node-id", "", "identificador deste nó na telemetria e no Árbitro (padrão: hostname)")
	flag.String("monitor-url", "", "URL para envio periódico de telemetria; vazio desativa")
	flag.Duration("report-interval", 10*time.Second, "intervalo do envio periódico de telemetria (ver Reporter em internal/telemetry)")
	flag.String("cgroup-root", "", "raiz do cgroups v2 (padrão: /sys/fs/cgroup; sem cgroups v2 utilizável, o uso agregado cai automaticamente no fallback /proc/stat + /proc/meminfo — ver New em internal/telemetry/usage)")
	flag.Duration("default-scale-down-timeout", 15*time.Minute, "timeout ocioso padrão antes de escalar para baixo até min_instances; usado quando a função não declara idle_timeout (ver Manager em internal/pool)")
	arbitroURL := flag.String("arbitro-url", "", "URL base do Árbitro; vazio = modo Standalone (ver Server em internal/nodeapi)")
	layer := flag.String("layer", "borda", "camada deste nó no continuum: borda | nevoa")
	arbTokenFile := flag.String("arbitro-token-file", "", "arquivo só com token plaintext do Árbitro (0600); alternativo a -arbitro-token")
	arbToken := flag.String("arbitro-token", "", "token plaintext do Árbitro (preferir -arbitro-token-file; env FAAS_ARBITRO_TOKEN)")
	lateQueueCap := flag.Int("late-queue-cap", 1024, "teto da fila de relatórios tardios; além do teto, descarta e contabiliza")
	lateCapMS := flag.Int("late-cap-ms", 90000, "teto, em milissegundos, da execução desanexada autorizada")
	minInstances := flag.Int("node-min-instances", 0, "informativo enviado no registro do nó")
	flag.String("function-categories", "", "mapa explícito variante→categoria em JSON (ex. '{\"alpr-yolo-tiny\":\"alpr-yolo\"}') enviado como function_categories no POST /nodes; vazio = chave omitida (sem auto-derivação do registry local)")
	pprofAddr := flag.String("pprof-addr", "", "endereço do pprof (só builds debug, desligado por padrão; ex. 127.0.0.1:6060)")
	flag.Parse()

	StartPprof(*pprofAddr)

	// Load the configuration and the dependencies.
	cfg, nodeName, publicHost, cred := setupConfig(*configPath, *credentialPath)
	reg, rt, poolAdapter, pl := setupRuntime(cfg)
	defer rt.Close()

	// Reconcile the state during the initialization.
	setupReconcile(cfg, poolAdapter, reg, pl)

	// Configure the telemetry collector.
	collector := setupTelemetry(cfg, nodeName)

	// Configure the HTTP server.
	srv, srvHTTP := setupServer(cfg, publicHost, reg, pl, poolAdapter, cred, collector)

	// The flags of the continuum feed the configuration loader through the
	// visited flags above.
	var _, _, _, _, _, _, _ = *arbitroURL, *layer, *minInstances, *arbTokenFile, *arbToken, *lateQueueCap, *lateCapMS

	// Load the configuration of the arbiter.
	arbURL, nodeLayer, nodeMinInstances, token := setupArbitro(cfg)

	// The late reports are only sent in the continuum mode, and the standalone
	// mode keeps the local behavior.
	if arbURL != "" {
		srv.ConfigureLateReports(strings.TrimRight(arbURL, "/")+"/internal/late-report", token, nodeName, cfg.NodeAPI.LateQueueCap, time.Duration(cfg.NodeAPI.LateCapMS)*time.Millisecond)
		log.Printf("agente: late-report sender ativo (-> %s/internal/late-report, queue_cap=%d, late_cap=%s)", strings.TrimRight(arbURL, "/"), cfg.NodeAPI.LateQueueCap, time.Duration(cfg.NodeAPI.LateCapMS)*time.Millisecond)
	}

	// Start HTTP server
	serveErr := startServer(srvHTTP)

	// Configure the handling of the signals.
	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Configure and start the scaler.
	setupScaler(cfg, pl, reg, stopCtx)

	// Wire the arbiter integration and the telemetry when they are enabled.
	setupArbitroIntegration(cfg, arbURL, nodeLayer, publicHost, collector, nodeName, nodeMinInstances, reg, token, stopCtx)

	// Wait for shutdown
	handleShutdown(serveErr, srvHTTP, stopCtx)
}
