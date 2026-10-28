// Command spike-psi checks whether a Linux kernel exposes Pressure Stall
// Information through cgroups v2.
//
// Usage:
//
//	go run ./cmd/spike-psi [-cgroup /sys/fs/cgroup]
//
// Run it on the target device: the answer decides whether the telemetry
// collector can use PSI or has to fall back to aggregate usage counters.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/igoricda/faas-continuum/internal/telemetry/psi"
)

func main() {
	cgroupPath := flag.String("cgroup", "", "caminho do cgroup a inspecionar (padrão: raiz do cgroups v2 do sistema)")
	flag.Parse()

	reader := psi.NewReader(*cgroupPath)

	supported := reader.Supported()
	fmt.Printf("psi_supported: %v\n", supported)

	if !supported {
		fmt.Println("PSI indisponível — kernel provavelmente sem CONFIG_PSI ou o sistema está em cgroups v1.")
		fmt.Println("Ação: registrar este dispositivo em modo de compatibilidade (ver Supported em internal/telemetry/psi) e seguir para o próximo.")
		os.Exit(0)
	}

	snap, err := reader.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "erro ao ler PSI apesar de psi_supported=true: %v\n", err)
		os.Exit(1)
	}

	out, _ := json.MarshalIndent(snap, "", "  ")
	fmt.Println(string(out))
}
