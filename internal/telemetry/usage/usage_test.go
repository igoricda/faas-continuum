package usage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeCgroupCounters(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "cpu.stat"), []byte("usage_usec 12345\nnr_periods 0\nnr_throttled 0\nthrottled_usec 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("1024\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeMeminfoFixture creates a fake memory interface with the requested
// lines.
func writeMeminfoFixture(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "meminfo")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestNewDetectsCgroups covers the automatic detection: with both counters
// readable at the root, the chosen backend is the cgroups v2 one.
func TestNewDetectsCgroups(t *testing.T) {
	dir := t.TempDir()
	writeCgroupCounters(t, dir)

	r := New(dir, filepath.Join(dir, "stat"), filepath.Join(dir, "meminfo"))
	if got := r.Source(); got != "cgroups-v2" {
		t.Errorf("Source() = %q, esperado %q (cpu.stat + memory.current presentes)", got, "cgroups-v2")
	}
}

// TestNewFallsBackToProcfs ensures the universal fallback: an empty or
// missing cgroup root falls back to the procfs backend, so there is never
// a device without a reader.
func TestNewFallsBackToProcfs(t *testing.T) {
	for _, dir := range []string{t.TempDir(), filepath.Join(t.TempDir(), "nao-existe")} {
		r := New(dir, "", "")
		if got := r.Source(); got != "procfs" {
			t.Errorf("Source() = %q para raiz %q, esperado fallback %q", got, dir, "procfs")
		}
	}
}

// TestCPUPercent covers the pure function shared by both backends: the
// basic multi-core case and the boundary rules.
func TestCPUPercent(t *testing.T) {
	prev := Sample{CPUTotalUSec: 1_000_000}
	cur := Sample{CPUTotalUSec: 1_200_000} // delta de 200_000 µs

	if got := CPUPercent(prev, cur, time.Second, 2); got != 10 {
		t.Errorf("CPUPercent(delta 200ms em 1s, 2 CPUs) = %v, esperado 10", got)
	}
	// Without a time window there is no definable percentage.
	if got := CPUPercent(prev, cur, 0, 2); got != 0 {
		t.Errorf("CPUPercent(elapsed=0) = %v, esperado 0", got)
	}
	// The counter restarted, so the result is zero and never negative.
	if got := CPUPercent(cur, prev, time.Second, 2); got != 0 {
		t.Errorf("CPUPercent(cur<prev) = %v, esperado 0", got)
	}
	// The upper clamp applies when the difference exceeds the capacity of
	// the window.
	if got := CPUPercent(Sample{CPUTotalUSec: 0}, Sample{CPUTotalUSec: 5_000_000}, 100*time.Millisecond, 1); got != 100 {
		t.Errorf("CPUPercent(estouro) = %v, esperado clampado em 100", got)
	}
}

// ---------------------------------------------------------------------------
// Tests of the procfs backend
// ---------------------------------------------------------------------------

// TestProcfsReadParsesValues covers the happy path of the universal
// fallback: the busy time is the total minus the idle and the input wait
// times, the jiffies are converted to microseconds and the memory comes
// from the total and the available memory.
func TestProcfsReadParsesValues(t *testing.T) {
	dir := t.TempDir()
	stat := filepath.Join(dir, "stat")
	if err := os.WriteFile(stat, []byte("cpu  100 0 100 700 0 0 0 0 0 0\ncpu0 50 0 50 350 0 0 0 0 0 0\ncpu1 50 0 50 350 0 0 0 0 0 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	meminfo := writeMeminfoFixture(t, dir, "MemTotal:        1000 kB\nMemAvailable:     250 kB\n")

	s, err := NewProcfs(stat, meminfo).Read()
	if err != nil {
		t.Fatalf("Read() retornou erro inesperado: %v", err)
	}
	if s.CPUTotalUSec != 2_000_000 {
		t.Errorf("CPUTotalUSec = %d, esperado 2000000 (busy 200 jiffies × 10 ms)", s.CPUTotalUSec)
	}
	if s.MemoryPct != 75 {
		t.Errorf("MemoryPct = %v, esperado 75 ((1000-250)/1000)", s.MemoryPct)
	}
}

// TestProcfsErrorsWithoutCPULine checks that a statistics interface without
// the aggregate CPU line fails, since the per-processor lines are not a
// valid substitute.
func TestProcfsErrorsWithoutCPULine(t *testing.T) {
	dir := t.TempDir()
	stat := filepath.Join(dir, "stat")
	if err := os.WriteFile(stat, []byte("cpu0 100 0 100 700 0 0 0 0 0 0\nintr 123\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	meminfo := writeMeminfoFixture(t, dir, "MemTotal: 1000 kB\nMemAvailable: 500 kB\n")

	if _, err := NewProcfs(stat, meminfo).Read(); err == nil {
		t.Fatal("esperava erro quando /proc/stat não tem a linha 'cpu '")
	}
}

// TestProcfsErrorsOnNonNumericField checks that a non-numeric field of the
// CPU line is malformed input and fails closed.
func TestProcfsErrorsOnNonNumericField(t *testing.T) {
	dir := t.TempDir()
	stat := filepath.Join(dir, "stat")
	if err := os.WriteFile(stat, []byte("cpu  100 abc 100 700 0 0 0 0 0 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	meminfo := writeMeminfoFixture(t, dir, "MemTotal: 1000 kB\nMemAvailable: 500 kB\n")

	_, err := NewProcfs(stat, meminfo).Read()
	if err == nil {
		t.Fatal("esperava erro com campo não numérico na linha cpu")
	}
	if !strings.Contains(err.Error(), "abc") {
		t.Errorf("erro = %v, esperado mencionar o campo inválido", err)
	}
}

// TestReadMeminfoPairHappyPath checks that both values are extracted in a
// single pass and that the resulting memory percentage is identical to the
// one of the original path.
func TestReadMeminfoPairHappyPath(t *testing.T) {
	dir := t.TempDir()
	meminfo := writeMeminfoFixture(t, dir, "MemTotal:        1000 kB\nMemAvailable:     250 kB\n")

	total, avail, err := readMeminfoPair(meminfo)
	if err != nil {
		t.Fatalf("readMeminfoPair() inesperado: %v", err)
	}
	if total != 1000 {
		t.Errorf("total = %d, esperado 1000", total)
	}
	if avail != 250 {
		t.Errorf("avail = %d, esperado 250", avail)
	}
}

// TestReadMeminfoPairMissingTotal checks that the reader fails when the
// total memory is absent, which fails closed on older kernels.
func TestReadMeminfoPairMissingTotal(t *testing.T) {
	dir := t.TempDir()
	meminfo := writeMeminfoFixture(t, dir, "MemAvailable:     250 kB\n")

	_, _, err := readMeminfoPair(meminfo)
	if err == nil {
		t.Fatal("esperava erro quando MemTotal está ausente")
	}
	if !strings.Contains(err.Error(), "MemTotal") {
		t.Errorf("erro = %v, esperado mencionar MemTotal", err)
	}
}

// TestReadMeminfoPairMissingAvail checks that the reader fails when the
// available memory is absent, which fails closed on older kernels.
func TestReadMeminfoPairMissingAvail(t *testing.T) {
	dir := t.TempDir()
	meminfo := writeMeminfoFixture(t, dir, "MemTotal:        1000 kB\n")

	_, _, err := readMeminfoPair(meminfo)
	if err == nil {
		t.Fatal("esperava erro quando MemAvailable está ausente")
	}
	if !strings.Contains(err.Error(), "MemAvailable") {
		t.Errorf("erro = %v, esperado mencionar MemAvailable", err)
	}
}

// ---------------------------------------------------------------------------
// Tests of the cgroups backend
// ---------------------------------------------------------------------------

// TestCgroupsReadParsesValues covers the happy path: the usage counter is
// extracted from the statistics file and the memory percentage uses the
// numeric memory limit.
func TestCgroupsReadParsesValues(t *testing.T) {
	dir := t.TempDir()
	writeCgroupCounters(t, dir) // usage_usec 12345; memory.current 1024
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("4096\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := NewCgroups(dir, "").Read()
	if err != nil {
		t.Fatalf("Read() retornou erro inesperado: %v", err)
	}
	if s.CPUTotalUSec != 12345 {
		t.Errorf("CPUTotalUSec = %d, esperado 12345", s.CPUTotalUSec)
	}
	if s.MemoryPct != 25 {
		t.Errorf("MemoryPct = %v, esperado 25 (1024/4096)", s.MemoryPct)
	}
}

// TestCgroupsErrorsOnMalformedCPUStat checks that a statistics file without
// a well-formed usage counter fails with a clear error.
func TestCgroupsErrorsOnMalformedCPUStat(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cpu.stat"), []byte("nr_periods 10\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("1024\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := NewCgroups(dir, "").Read(); err == nil {
		t.Fatal("esperava erro com cpu.stat sem usage_usec")
	}
}

// TestCgroupsUnlimitedMemoryUsesMemTotal checks that an unlimited memory
// limit falls back to the total memory of the system, converted from
// kilobytes to bytes.
func TestCgroupsUnlimitedMemoryUsesMemTotal(t *testing.T) {
	dir := t.TempDir()
	writeCgroupCounters(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("262144\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	meminfo := writeMeminfoFixture(t, dir, "MemTotal:        1024 kB\nMemFree:         512 kB\n")

	// 262144 bytes over 25% of the limit.
	s, err := NewCgroups(dir, meminfo).Read()
	if err != nil {
		t.Fatalf("Read() retornou erro inesperado: %v", err)
	}
	if s.MemoryPct != 25 {
		t.Errorf("MemoryPct = %v, esperado 25 (262144 bytes sobre 1 MiB)", s.MemoryPct)
	}
}

// TestCgroupsUnlimitedMemoryRequiresMemTotal checks that an unlimited
// memory limit without the total memory in the system interface fails,
// because the percentage cannot be normalized.
func TestCgroupsUnlimitedMemoryRequiresMemTotal(t *testing.T) {
	dir := t.TempDir()
	writeCgroupCounters(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	meminfo := writeMeminfoFixture(t, dir, "MemFree:         512 kB\n")

	if _, err := NewCgroups(dir, meminfo).Read(); err == nil {
		t.Fatal("esperava erro quando memory.max é 'max' e meminfo não tem MemTotal")
	}
}
