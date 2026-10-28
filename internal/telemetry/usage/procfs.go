// The procfs backend, the universal fallback: it covers the scenarios
// where there is no usable cgroups v2, such as cgroups v1 or its total
// absence, and closes the compatibility mode without leaving a gap in the
// coverage.
package usage

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ProcfsReader reads the CPU from the statistics interface and the memory
// from the memory interface.
type ProcfsReader struct {
	statPath    string
	meminfoPath string
}

// NewProcfs returns the procfs backend explicitly. Empty paths select the
// defaults of the package.
func NewProcfs(procStatPath, meminfoPath string) *ProcfsReader {
	if procStatPath == "" {
		procStatPath = DefaultProcStatPath
	}
	if meminfoPath == "" {
		meminfoPath = DefaultMeminfoPath
	}
	return &ProcfsReader{statPath: procStatPath, meminfoPath: meminfoPath}
}

func (r *ProcfsReader) Source() string { return "procfs" }

func (r *ProcfsReader) Read() (Sample, error) {
	var s Sample

	busyJiffies, err := readBusyJiffies(r.statPath)
	if err != nil {
		return s, fmt.Errorf("usage: procfs: %w", err)
	}
	// The jiffies are converted to microseconds by multiplying by one
	// million divided by the number of jiffies per second, which gives
	// the same unit as the counter of the cgroups backend.
	s.CPUTotalUSec = busyJiffies * (1e6 / userHZ)

	totalKB, availKB, err := readMeminfoPair(r.meminfoPath)
	if err != nil {
		return s, fmt.Errorf("usage: procfs: %w", err)
	}
	if totalKB > 0 && availKB < totalKB {
		s.MemoryPct = clampPct(float64(totalKB-availKB) / float64(totalKB) * 100)
	}
	return s, nil
}

// readBusyJiffies sums every numeric field of the aggregate CPU line and
// subtracts the idle and the input wait times, which yields the cumulative
// busy time, the analogue of the counter of the cgroups backend.
func readBusyJiffies(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("falha ao abrir %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)[1:] // descarta o prefixo "cpu"
		var total, idle uint64
		for i, field := range fields {
			v, err := strconv.ParseUint(field, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("%s: campo não numérico %q na linha cpu: %w", path, field, err)
			}
			total += v
			if i == 3 || i == 4 { // idle e iowait
				idle += v
			}
		}
		if len(fields) == 0 {
			return 0, fmt.Errorf("%s: linha cpu sem campos", path)
		}
		return total - idle, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("falha ao ler %s: %w", path, err)
	}
	return 0, errors.New("linha 'cpu' não encontrada em " + path)
}
