// Package psi reads the Pressure Stall Information (PSI) files exported by
// cgroups v2.
//
// When the kernel does not expose PSI (CONFIG_PSI disabled or cgroups v1),
// Supported reports false and the rest of the framework runs in
// compatibility mode, using only aggregate usage counters.
package psi

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// DefaultCgroupRoot is the default mount point of the unified cgroups v2
// hierarchy on modern Linux distributions.
const DefaultCgroupRoot = "/sys/fs/cgroup"

// Pressure is one PSI metric line (some or full), as documented in
// kernel.org/Documentation/accounting/psi.rst.
// Only avg10 is read: the collector in ../telemetry.go samples the hot
// path.
type Pressure struct {
	Avg10 float64
}

// Snapshot aggregates the CPU and memory pressure readings taken at a
// single instant. MemoryFull is the memory "full" signal, the strongest
// predictor of an imminent out-of-memory condition.
type Snapshot struct {
	CPUSome    Pressure
	MemorySome Pressure
	MemoryFull Pressure
}

// Reader reads the PSI files of one cgroup. It defaults to the system root
// cgroup, which gives the aggregate view of the device and is enough to
// triage nodes. One instance per node agent is enough, since readings are
// taken on demand.
type Reader struct {
	cgroupPath string
}

// NewReader returns a reader for the given cgroup path. An empty path
// selects DefaultCgroupRoot.
func NewReader(cgroupPath string) *Reader {
	if cgroupPath == "" {
		cgroupPath = DefaultCgroupRoot
	}
	return &Reader{cgroupPath: cgroupPath}
}

// Supported reports whether the device exposes PSI through cgroups v2,
// which it determines by looking for a readable cpu.pressure file. The
// result is reported as psi_supported in the node telemetry.
func (r *Reader) Supported() bool {
	_, err := os.Stat(filepath.Join(r.cgroupPath, "cpu.pressure"))
	return err == nil
}

// Read collects the "some" pressure of CPU and memory and the "full"
// pressure of memory. It returns an error when PSI is unavailable, so
// callers should check Supported first.
func (r *Reader) Read() (Snapshot, error) {
	var snap Snapshot

	cpu, err := r.readFile("cpu.pressure")
	if err != nil {
		return snap, fmt.Errorf("psi: cpu.pressure indisponível (kernel sem CONFIG_PSI ou cgroups v1?): %w", err)
	}
	snap.CPUSome, err = parsePressureLine(cpu, "some")
	if err != nil {
		return snap, fmt.Errorf("psi: falha ao interpretar cpu.pressure: %w", err)
	}

	mem, err := r.readFile("memory.pressure")
	if err != nil {
		return snap, fmt.Errorf("psi: memory.pressure indisponível: %w", err)
	}
	snap.MemorySome, err = parsePressureLine(mem, "some")
	if err != nil {
		return snap, fmt.Errorf("psi: falha ao interpretar memory.pressure: %w", err)
	}
	snap.MemoryFull, err = parsePressureLine(mem, "full")
	if err != nil {
		return snap, fmt.Errorf("psi: falha ao interpretar memory.pressure (full): %w", err)
	}

	return snap, nil
}

func (r *Reader) readFile(name string) (string, error) {
	data, err := os.ReadFile(filepath.Join(r.cgroupPath, name))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func parsePressureLine(content string, prefix string) (Pressure, error) {
	want := prefix + " "
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, want) {
			continue
		}
		return parsePressureFields(strings.Fields(line)[1:])
	}
	if err := scanner.Err(); err != nil {
		return Pressure{}, err
	}
	return Pressure{}, fmt.Errorf("linha '%s' não encontrada", prefix)
}

func parsePressureFields(fields []string) (Pressure, error) {
	var p Pressure
	for _, f := range fields {
		kv := strings.SplitN(f, "=", 2)
		if len(kv) != 2 {
			continue
		}
		if kv[0] == "avg10" {
			p.Avg10, _ = strconv.ParseFloat(kv[1], 64)
		}
	}
	return p, nil
}
