// Command spike-energia determines which energy mechanism the kernel
// exposes: the cumulative RAPL counter, the instantaneous power_supply
// reading, or none of them.
//
// Usage:
//
//	go run ./cmd/spike-energia [-config <path>] [-powercap-root DIR] [-power-supply-root DIR]
//
// The -config flag takes a JSONC or JSON configuration file that replaces
// the telemetry roots. The -powercap-root and -power-supply-root flags
// override the values coming from that file.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/telemetry/energy"
)

func main() {
	powercapRoot := flag.String("powercap-root", "", "raiz do Power Capping Framework (padrão: /sys/class/powercap)")
	powerSupplyRoot := flag.String("power-supply-root", "", "raiz da classe power_supply (padrão: /sys/class/power_supply)")
	configPath := flag.String("config", "", "caminho para o arquivo de configuração JSONC/JSON (padrão: vazio)")
	flag.Parse()

	var pcapRoot, psRoot string

	if *configPath != "" {
		cfg, err := config.Load(*configPath, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "falha ao carregar config: %v\n", err)
			os.Exit(1)
		}
		pcapRoot = cfg.Telemetry.PowercapRoot
		psRoot = cfg.Telemetry.PowerSupplyRoot
	}

	// The -powercap-root and -power-supply-root flags take precedence over
	// the configuration file.
	if *powercapRoot != "" {
		pcapRoot = *powercapRoot
	}
	if *powerSupplyRoot != "" {
		psRoot = *powerSupplyRoot
	}

	r := energy.New(pcapRoot, psRoot)

	fmt.Printf("energy_source: %s\n", r.Source())

	if r.Source() == energy.SourceNone {
		fmt.Println("Nenhuma fonte de energia disponível — sem pacote intel-rapl legível em /sys/class/powercap e sem entrada power_supply qualificada.")
		fmt.Println("Ação: registrar este dispositivo em modo de compatibilidade (ver Source em internal/telemetry/energy) e usar instrumentação externa nos experimentos do TCC (ex.: medidor INA219+ESP8266 na alimentação).")
		os.Exit(0)
	}

	t0, err := r.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "erro ao ler energia (primeira leitura): %v\n", err)
		os.Exit(1)
	}

	inicio := time.Now()
	time.Sleep(time.Second)

	t1, err := r.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "erro ao ler energia (segunda leitura): %v\n", err)
		os.Exit(1)
	}

	switch r.Source() {
	case energy.SourceRAPL:
		w := energy.WindowWatts(r.Source(), t0, t1, time.Since(inicio))
		fmt.Printf("energia_cumulativa_t0_uj: %d\n", t0.CumulativeUJoules)
		fmt.Printf("energia_cumulativa_t1_uj: %d\n", t1.CumulativeUJoules)
		fmt.Printf("potencia_media_mw: %.1f\n", w*1000)
	case energy.SourcePowerSupply:
		fmt.Printf("potencia_instantanea_t0_mw: %.1f\n", t0.InstantWatts*1000)
		fmt.Printf("potencia_instantanea_t1_mw: %.1f\n", t1.InstantWatts*1000)
	default:
		fmt.Fprintf(os.Stderr, "fonte de energia desconhecida: %s\n", r.Source())
		os.Exit(1)
	}
}
