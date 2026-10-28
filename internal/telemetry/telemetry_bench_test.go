package telemetry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// benchFixture creates the files of the procfs and cgroups v2 interfaces
// used by the automatic detection, with the same format as the fixture of
// the unit tests. The growing counters are rewritten on every iteration
// to exercise the CPU delta, which is the dominant cost of a collection.
type benchFixture struct {
	stat    string
	meminfo string
	cgroup  string
	counter uint64
}

func newBenchFixture(b *testing.B) *benchFixture {
	b.Helper()
	dir := b.TempDir()
	f := &benchFixture{
		stat:    filepath.Join(dir, "stat"),
		meminfo: filepath.Join(dir, "meminfo"),
		cgroup:  filepath.Join(dir, "cgroup"),
	}
	if err := os.MkdirAll(f.cgroup, 0o755); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(f.stat, []byte("cpu  0 0 0 0 0 0 0 0 0 0\n"), 0o644); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(f.meminfo, []byte("MemTotal:        1024 kB\nMemAvailable:     512 kB\n"), 0o644); err != nil {
		b.Fatal(err)
	}
	f.writeCounters()
	return f
}

func (f *benchFixture) writeCounters() {
	f.counter += 1000000
	content := "cpu  " + strconv.FormatUint(f.counter, 10) + " 0 0 0 0 0 0 0 0 0\n"
	_ = os.WriteFile(f.stat, []byte(content), 0o644)
	_ = os.WriteFile(filepath.Join(f.cgroup, "cpu.stat"), []byte("usage_usec "+strconv.FormatUint(f.counter, 10)+"\nnr_periods 0\n"), 0o644)
	_ = os.WriteFile(filepath.Join(f.cgroup, "memory.current"), []byte("1073741824\n"), 0o644)
	_ = os.WriteFile(filepath.Join(f.cgroup, "memory.max"), []byte("2147483648\n"), 0o644)
}

// BenchmarkCollectorCollect measures a complete collection, reading the
// counters, computing the CPU delta and building the package, against
// temporary files with growing counters. It is the cost of the piggyback
// of every invocation and of the periodic report.
func BenchmarkCollectorCollect(b *testing.B) {
	f := newBenchFixture(b)
	c := NewCollector("bench-node", true, f.cgroup, f.stat, f.meminfo, "", "")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.writeCounters()
		if _, err := c.Collect(SourcePeriodic); err != nil {
			b.Fatalf("Collect: %v", err)
		}
	}
}

// BenchmarkCollectorCollectDisabled measures the cost of the disabled
// path, an immediate return without reading any file, which is the lower
// bound of a collection when the telemetry is disabled.
func BenchmarkCollectorCollectDisabled(b *testing.B) {
	c := NewCollector("bench-node", false, "", "", "", "", "")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Collect(SourcePeriodic); err != nil {
			b.Fatalf("Collect: %v", err)
		}
	}
}

// BenchmarkReporterMarshalPackage measures the serialization of the
// telemetry package over a complete package with an energy block and the
// pressure fields, which is the cost of the autonomous arm on every tick.
func BenchmarkReporterMarshalPackage(b *testing.B) {
	now := time.Now()
	pkg := Package{
		NodeID:             "bench-node",
		Timestamp:          now,
		CPUUsagePct:        42.5,
		MemoryUsagePct:     61.25,
		CPUPressure:        &PressureFields{Avg10: 1.5},
		MemoryPressure:     &PressureFields{Avg10: 2.5},
		MemoryPressureFull: &PressureFields{Avg10: 3.5},
		Energy:             &EnergyFields{Source: "rapl", PowerMilliwatts: 12345.678},
		Source:             SourcePeriodic,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(pkg); err != nil {
			b.Fatalf("Marshal: %v", err)
		}
	}
}
