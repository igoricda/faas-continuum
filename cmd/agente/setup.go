package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/igoricda/faas-continuum/internal/auth"
	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/containerruntime"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/reconcile"
	"github.com/igoricda/faas-continuum/internal/registry"
	"github.com/igoricda/faas-continuum/internal/scaler"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func setupConfig(configPath, credentialPath string) (config.Config, string, string, auth.Credential) {
	effectiveConfigPath := configPath
	flagOverrides := map[string]string{}
	flag.Visit(func(f *flag.Flag) { flagOverrides[f.Name] = f.Value.String() })
	cfg, err := config.Load(effectiveConfigPath, flagOverrides)
	if err != nil {
		log.Fatalf("agente: falha ao carregar config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		log.Fatalf("agente: config inválida: %v", err)
	}
	if _, err := os.Stat(effectiveConfigPath); os.IsNotExist(err) {
		log.Printf("agente: config não encontrado em %q, usando flags/defaults", effectiveConfigPath)
	}

	publicHost := cfg.NodeAPI.Host
	if publicHost == "" {
		publicHost = cfg.NodeAPI.Listen
	}

	nodeName := cfg.NodeAPI.NodeID
	if nodeName == "" {
		hn, herr := os.Hostname()
		if herr != nil {
			log.Fatalf("agente: -node-id vazio e falha ao obter o hostname do sistema: %v", herr)
		}
		nodeName = hn
	}

	if cfg.NodeAPI.MonitorURL != "" && time.Duration(cfg.Telemetry.ReportInterval) <= 0 {
		log.Fatalf("agente: -report-interval deve ser positivo quando -monitor-url é usado")
	}

	if cfg.NodeAPI.LateQueueCap <= 0 {
		log.Fatalf("agente: -late-queue-cap deve ser >0 (recebido %d)", cfg.NodeAPI.LateQueueCap)
	}
	if cfg.NodeAPI.LateCapMS <= 0 {
		log.Fatalf("agente: -late-cap-ms deve ser >0 (recebido %d)", cfg.NodeAPI.LateCapMS)
	}

	cred, plainToken, cerr := auth.LoadOrGenerate(credentialPath)
	if cerr != nil {
		log.Fatalf("agente: falha ao preparar credencial local: %v", cerr)
	}
	log.Printf("agente: credencial local pronta em %q (cabeçalho %q)", credentialPath, cred.HeaderName)

	if plainToken != "" {
		fmt.Printf(`
========================================================================
ATENÇÃO: credencial provisionada agora (primeiro uso do agente neste nó).

Este token em texto plano NÃO será exibido novamente. Guarde-o agora
para configurar os clientes que consomem a API de gerenciamento do nó.

  Cabeçalho: %s
  Token:     %s

========================================================================
`, cred.HeaderName, plainToken)
	}

	return cfg, nodeName, publicHost, cred
}

func setupRuntime(cfg config.Config) (*registry.Registry, *containerruntime.Runtime, *containerruntime.PoolAdapter, *pool.Manager) {
	reg, rerr := registry.NewPersistentRegistry(cfg.Registry.Path, cfg.Registry.AllowedRegistries...)
	if rerr != nil {
		log.Fatalf("agente: falha ao carregar registro de funções persistido: %v", rerr)
	}
	log.Printf("agente: registro de funções persistido em %q (%d função(ões) carregada(s), allowed_registries=%v)", cfg.Registry.Path, len(reg.List()), cfg.Registry.AllowedRegistries)

	rt, rterr := containerruntime.New(containerruntime.Config{
		SocketPath: cfg.Runtime.Socket,
		Namespace:  cfg.Runtime.Namespace,
	})
	if rterr != nil {
		log.Fatalf("agente: não foi possível conectar ao containerd: %v", rterr)
	}

	poolAdapter := containerruntime.NewPoolAdapterWithSocketRoot(rt, cfg.Runtime.SocketRoot)
	// The linear scan per provision is kept on purpose: the registry changes
	// at runtime, through the registration, the update and the removal in the
	// node interface, so a map built once here would be born stale, since a
	// function registered after the boot would lose the device and a removed
	// one would leak the flag. The number of functions is in the dozens in
	// practice and each provisioning already pays the pull, the creation and
	// the start, so the scan is noise. It should be revisited only if the
	// number of functions grows to some hundreds with frequent warm
	// provisioning.
	poolAdapter.GPUForImage = func(imageRef string) bool {
		for _, fn := range reg.List() {
			if fn.ImageRef == imageRef {
				return fn.GPU
			}
		}
		return false
	}
	pl := pool.NewManagerWithQueueCap(poolAdapter, func(fn string) int {
		f, ok := reg.Get(fn)
		if !ok || f.QueueMax <= 0 {
			return 0
		}
		return f.QueueMax
	})

	return reg, rt, poolAdapter, pl
}

func setupReconcile(cfg config.Config, poolAdapter *containerruntime.PoolAdapter, reg *registry.Registry, pl *pool.Manager) {
	reconcileCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.NodeAPI.ReconcileTimeout))
	defer cancel()

	if err := reconcile.Run(reconcileCtx, poolAdapter, reg, pl); err != nil {
		log.Printf("agente: reconciliação de startup terminou com problemas (agente sobe mesmo assim, funções afetadas ficam degraded): %v", err)
	} else {
		log.Printf("agente: reconciliação de startup concluída sem problemas")
	}
}

func setupTelemetry(cfg config.Config, nodeName string) *telemetry.Collector {
	collector := telemetry.NewCollector(nodeName, cfg.Telemetry.Enabled, cfg.Telemetry.CgroupRoot, cfg.Telemetry.ProcStatPath, cfg.Telemetry.MeminfoPath, cfg.Telemetry.PowercapRoot, cfg.Telemetry.PowerSupplyRoot)
	log.Printf("agente: telemetria pronta (nó %q, usage_source=%s, psi_supported=%t, energy_source=%s)", nodeName, collector.UsageSource(), collector.PSISupported(), collector.EnergySource())
	if !collector.PSISupported() {
		log.Printf("agente: PSI indisponível neste kernel — modo de compatibilidade, pacote de telemetria sem campos de pressão (ver Supported em internal/telemetry/psi)")
	}
	if collector.EnergySource() == "none" {
		log.Printf("agente: energia indisponível neste dispositivo — modo de compatibilidade, campo energy omitido do pacote (ver Source em internal/telemetry/energy); use cmd/spike-energia para diagnosticar e preveja instrumentação externa nos experimentos do TCC")
	}

	return collector
}

func setupScaler(cfg config.Config, pl *pool.Manager, reg *registry.Registry, stopCtx context.Context) {
	scalerMgr := scaler.New(pl, reg, time.Duration(cfg.Scaling.DefaultScaleDownTimeout), scaler.DefaultInterval)
	go scalerMgr.Run(stopCtx)
	log.Printf("agente: scaler pronto (default_scale_down_timeout=%s interval=%s)", cfg.Scaling.DefaultScaleDownTimeout, scaler.DefaultInterval)
}
