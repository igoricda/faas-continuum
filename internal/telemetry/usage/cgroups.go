// The cgroups v2 backend, which is preferred: it reads the usage counter
// and the memory of the root of the unified hierarchy, an aggregate view
// of the whole device and a single mechanism for the usage and the
// pressure.
package usage

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// CgroupsReader reads the usage counters at the root of the cgroups v2
// hierarchy.
type CgroupsReader struct {
	cgroupRoot  string
	meminfoPath string // used when the memory ceiling is unlimited
}

// NewCgroups returns the cgroups v2 backend explicitly. Empty paths select
// the defaults of the package.
func NewCgroups(cgroupRoot, meminfoPath string) *CgroupsReader {
	if cgroupRoot == "" {
		cgroupRoot = DefaultCgroupRoot
	}
	if meminfoPath == "" {
		meminfoPath = DefaultMeminfoPath
	}
	return &CgroupsReader{cgroupRoot: cgroupRoot, meminfoPath: meminfoPath}
}

func (r *CgroupsReader) Source() string { return "cgroups-v2" }

// available is the check of the automatic detection: the backend is only
// eligible when the CPU and the memory counters exist and are readable at
// the given root.
func (r *CgroupsReader) available() bool {
	for _, name := range []string{"cpu.stat", "memory.current"} {
		f, err := os.Open(filepath.Join(r.cgroupRoot, name))
		if err != nil {
			return false
		}
		f.Close()
	}
	return true
}

func (r *CgroupsReader) Read() (Sample, error) {
	var s Sample

	cpuData, err := os.ReadFile(filepath.Join(r.cgroupRoot, "cpu.stat"))
	if err != nil {
		return s, fmt.Errorf("usage: cgroups: falha ao ler cpu.stat: %w", err)
	}
	s.CPUTotalUSec, err = parseUsageUSec(string(cpuData))
	if err != nil {
		return s, fmt.Errorf("usage: cgroups: %w", err)
	}

	currentBytes, err := readUintFile(filepath.Join(r.cgroupRoot, "memory.current"))
	if err != nil {
		return s, fmt.Errorf("usage: cgroups: %w", err)
	}
	limitBytes, err := r.memoryLimitBytes()
	if err != nil {
		return s, fmt.Errorf("usage: cgroups: %w", err)
	}
	if limitBytes > 0 {
		s.MemoryPct = clampPct(float64(currentBytes) / float64(limitBytes) * 100)
	}
	return s, nil
}

// memoryLimitBytes returns the memory limit of the root cgroup in bytes.
// The value of the memory limit file is used as is, and the value max,
// which means no limit, falls back to the total memory of the system,
// without which the percentage cannot be normalized.
func (r *CgroupsReader) memoryLimitBytes() (uint64, error) {
	data, err := os.ReadFile(filepath.Join(r.cgroupRoot, "memory.max"))
	if err != nil {
		return 0, fmt.Errorf("falha ao ler memory.max: %w", err)
	}
	content := strings.TrimSpace(string(data))
	if content == "max" {
		totalKB, err := readMeminfoKB(r.meminfoPath, "MemTotal")
		if err != nil {
			return 0, fmt.Errorf("memory.max ilimitado e %w", err)
		}
		return totalKB * 1024, nil // meminfo fala em kB
	}
	limit, err := strconv.ParseUint(content, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("memory.max com valor inválido %q: %w", content, err)
	}
	return limit, nil
}

// parseUsageUSec extracts the usage_usec line of the CPU statistics file,
// which also holds the number of periods and the throttling counters that
// are of no interest here.
func parseUsageUSec(content string) (uint64, error) {
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "usage_usec ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		return strconv.ParseUint(fields[1], 10, 64)
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("falha ao interpretar cpu.stat: %w", err)
	}
	return 0, errors.New("linha 'usage_usec' não encontrada em cpu.stat")
}

// readUintFile reads a counter file of the cgroups that holds a single
// decimal value.
func readUintFile(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("falha ao ler %s: %w", path, err)
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("valor inválido em %s: %w", path, err)
	}
	return v, nil
}
