// Package usage reads the aggregate usage counters of the device, the CPU
// and the memory, with automatic detection of the best mechanism available.
//
// The cgroups v2 backend is preferred, and the universal fallback through
// the procfs interface covers cgroups v1 and the total absence of usable
// cgroups.
//
// The central contract is that the CPU is a cumulative counter in
// microseconds in both backends, so the caller keeps the previous sample
// and computes the difference over the elapsed time. The memory is already
// instantaneous, normalized as a percentage.
package usage

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// The default paths of the two mechanisms on the device.
const (
	DefaultCgroupRoot   = "/sys/fs/cgroup"
	DefaultProcStatPath = "/proc/stat"
	DefaultMeminfoPath  = "/proc/meminfo"
)

// userHZ is the number of jiffies per second on practically every Linux
// system, so one jiffy lasts ten milliseconds. Only the procfs backend uses
// it, to convert jiffies into microseconds.
const userHZ = 100

// Sample is a raw reading of the aggregate usage of the device.
type Sample struct {
	// CPUTotalUSec is cumulative since boot and is not a percentage: the
	// percentage comes from the difference between two samples.
	CPUTotalUSec uint64
	// MemoryPct is instantaneous, already normalized to [0, 100].
	MemoryPct float64
}

// Reader returns a sample of the aggregate usage of the device. One
// instance per agent is enough. The implementations keep no state between
// readings, because the state of the difference belongs to the caller.
type Reader interface {
	Read() (Sample, error)
	// Source identifies the mechanism in use.
	Source() string
}

// New detects the available mechanism: the cgroups backend when its
// counters are readable, and the procfs backend otherwise. Empty paths
// select the defaults. There is no flag to force a backend, because the
// choice follows the real capability of the device.
func New(cgroupRoot, procStatPath, meminfoPath string) Reader {
	cg := NewCgroups(cgroupRoot, meminfoPath)
	if cg.available() {
		return cg
	}
	return NewProcfs(procStatPath, meminfoPath)
}

// CPUPercent is a pure function, testable without files, shared by both
// backends:
//
//	pct = (cur.CPUTotalUSec - prev.CPUTotalUSec) / (elapsed us * numCPU) * 100
//
// A non-positive elapsed time or a non-positive number of processors gives
// zero, a counter that went backwards gives zero, and the result is
// clamped to [0, 100]. Normalizing both backends to microseconds is what
// allows a single formula.
func CPUPercent(prev, cur Sample, elapsed time.Duration, numCPU int) float64 {
	if elapsed <= 0 || numCPU <= 0 {
		return 0
	}
	if cur.CPUTotalUSec <= prev.CPUTotalUSec {
		return 0
	}
	deltaUSec := float64(cur.CPUTotalUSec - prev.CPUTotalUSec)
	pct := deltaUSec / (elapsed.Seconds() * 1e6 * float64(numCPU)) * 100
	return clampPct(pct)
}

func clampPct(pct float64) float64 {
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// readMeminfoKB extracts a field in kilobytes from the memory interface.
// The expected line is "<Key>:      <value> kB". It is shared by both
// backends: the cgroups backend uses the total memory when the limit is
// unlimited, and the procfs backend uses the total and the available
// memory.
func readMeminfoKB(path, key string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("falha ao abrir %s: %w", path, err)
	}
	defer f.Close()

	want := key + ":"
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == want {
			kb, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("campo %s inválido em %s: %w", key, path, err)
			}
			return kb, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("falha ao ler %s: %w", path, err)
	}
	return 0, fmt.Errorf("campo %s não encontrado em %s", key, path)
}

// readMeminfoPair extracts the total and the available memory in a single
// pass over the memory interface, with the same line format as
// readMeminfoKB. Both fields are required, and a missing one produces the
// same error the single field reader would return, which fails closed on
// older kernels.
func readMeminfoPair(path string) (totalKB, availKB uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("falha ao abrir %s: %w", path, err)
	}
	defer f.Close()

	var total, avail uint64
	foundTotal := false
	foundAvail := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 {
			key := fields[0]
			kb, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, 0, fmt.Errorf("campo %s inválido em %s: %w", key, path, err)
			}
			switch key {
			case "MemTotal:":
				total = kb
				foundTotal = true
			case "MemAvailable:":
				avail = kb
				foundAvail = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, fmt.Errorf("falha ao ler %s: %w", path, err)
	}
	if !foundTotal {
		return 0, 0, fmt.Errorf("campo MemTotal não encontrado em %s", path)
	}
	if !foundAvail {
		return 0, 0, fmt.Errorf("campo MemAvailable não encontrado em %s", path)
	}
	return total, avail, nil
}
