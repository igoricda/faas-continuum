package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Shared fixtures and helpers
// ---------------------------------------------------------------------------

// newFixture creates a directory with the three files read by the detection
// and by the two backends. The paths are always passed explicitly to the
// collector, never the defaults, so the test never reads the real procfs
// of the machine and never becomes non-deterministic.
type fixture struct {
	dir     string
	stat    string
	meminfo string
	cgroup  string
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	dir := t.TempDir()
	f := fixture{
		dir:     dir,
		stat:    filepath.Join(dir, "stat"),
		meminfo: filepath.Join(dir, "meminfo"),
		cgroup:  filepath.Join(dir, "cgroup"),
	}
	for _, d := range []string{dir, f.cgroup} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(f.stat, []byte("cpu  0 0 0 0 0 0 0 0 0 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.meminfo, []byte("MemTotal:        1024 kB\nMemAvailable:     512 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f fixture) writeCPUStat(content string) {
	if err := os.WriteFile(f.stat, []byte(content), 0o644); err != nil {
		panic(err)
	}
}

func (f fixture) writeCgroupCounters(usageUSec uint64) {
	files := map[string]string{
		filepath.Join(f.cgroup, "cpu.stat"):       "usage_usec " + strconv.FormatUint(usageUSec, 10) + "\nnr_periods 0\n",
		filepath.Join(f.cgroup, "memory.current"): "1073741824\n", // 1 GiB
		filepath.Join(f.cgroup, "memory.max"):     "2147483648\n", // 2 GiB => memória fixa em 50%
	}
	for name, content := range files {
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			panic(err)
		}
	}
}

func newTestCollector(t *testing.T, f fixture) *Collector {
	t.Helper()
	return NewCollector("xp4-borda-01", true, f.cgroup, f.stat, f.meminfo, "", "")
}

// writeRAPLFixture creates the energy counter file of a package with the
// given value and returns the path of the file, in the real format of the
// power capping framework, which is enough for the package to qualify.
func writeRAPLFixture(t *testing.T, root string, uj uint64) string {
	t.Helper()
	pkg := filepath.Join(root, "intel-rapl:0")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(pkg, "energy_uj")
	if err := os.WriteFile(path, []byte(strconv.FormatUint(uj, 10)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// capturedPackages accumulates, in a way that is safe against races, the
// packages received by the test server from the reporter.
type capturedPackages struct {
	mu     sync.Mutex
	pkgs   []Package
	bodies []string
}

func (c *capturedPackages) add(p Package, raw string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pkgs = append(c.pkgs, p)
	c.bodies = append(c.bodies, raw)
}

func (c *capturedPackages) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pkgs)
}

// newReporterFixture returns a deterministic collector, with explicit
// fixtures and no pressure support, for the tests of the reporter.
func newReporterFixture(t *testing.T) *Collector {
	t.Helper()
	f := newFixture(t)
	f.writeCgroupCounters(0)
	return NewCollector("no-teste", true, f.cgroup, f.stat, f.meminfo, "", "")
}

// baseT is the fixed reference instant of the tests of the profile tracker,
// so that no case depends on the real clock.
var baseT = time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)

// pctEpsilon is the tolerance used when comparing percentages.
const pctEpsilon = 1e-9

// ---------------------------------------------------------------------------
// Tests of the collector
// ---------------------------------------------------------------------------

// TestPSIUnavailableOmitsPressureFields covers the compatibility mode:
// without the pressure files, the pressure fields are omitted from the
// JSON and never sent as null.
func TestPSIUnavailableOmitsPressureFields(t *testing.T) {
	f := newFixture(t)
	f.writeCgroupCounters(0) // contadores presentes, PSI ausente

	pkg, err := newTestCollector(t, f).Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("Collect() retornou erro inesperado: %v", err)
	}
	if pkg.CPUPressure != nil || pkg.MemoryPressure != nil {
		t.Fatalf("campos de pressão deveriam ser nil sem PSI (cpu=%v mem=%v)", pkg.CPUPressure, pkg.MemoryPressure)
	}

	data, err := json.Marshal(pkg)
	if err != nil {
		t.Fatal(err)
	}
	for _, chave := range []string{"cpu_pressure", "memory_pressure"} {
		if strings.Contains(string(data), chave) {
			t.Errorf("JSON %s não deveria conter a chave %q", data, chave)
		}
	}
}

// TestPSIAvailableFillsPressureFields checks that the pressure files of
// the cgroup directory fill the pressure fields with the values read.
func TestPSIAvailableFillsPressureFields(t *testing.T) {
	f := newFixture(t)
	f.writeCgroupCounters(0)
	psiCPU := "some avg10=5.20 avg60=3.10 avg300=1.80 total=128400\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
	psiMem := "some avg10=7.70 avg60=4.40 avg300=2.20 total=99000\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
	if err := os.WriteFile(filepath.Join(f.cgroup, "cpu.pressure"), []byte(psiCPU), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.cgroup, "memory.pressure"), []byte(psiMem), 0o644); err != nil {
		t.Fatal(err)
	}

	pkg, err := newTestCollector(t, f).Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("Collect() retornou erro inesperado: %v", err)
	}
	if pkg.CPUPressure == nil || pkg.MemoryPressure == nil {
		t.Fatalf("campos de pressão deveriam estar preenchidos (cpu=%v mem=%v)", pkg.CPUPressure, pkg.MemoryPressure)
	}
	if pkg.CPUPressure.Avg10 != 5.20 {
		t.Errorf("CPUPressure = %+v, esperado avg10=5.20", pkg.CPUPressure)
	}
	if pkg.MemoryPressure.Avg10 != 7.70 {
		t.Errorf("MemoryPressure = %+v, esperado avg10=7.70", pkg.MemoryPressure)
	}
	if pkg.MemoryPressureFull == nil || pkg.MemoryPressureFull.Avg10 != 0.00 {
		t.Errorf("MemoryPressureFull = %+v, esperado avg10=0.00 (full line)", pkg.MemoryPressureFull)
	}
}

// TestFirstCollectPrimesCPUSample checks that the first collection primes
// the sample and returns a zero CPU percentage, and that the second one,
// over a counter that advanced, returns a positive and coherent
// percentage.
func TestFirstCollectPrimesCPUSample(t *testing.T) {
	f := newFixture(t)
	f.writeCgroupCounters(0)

	c := newTestCollector(t, f)
	first, err := c.Collect(SourcePiggyback)
	if err != nil {
		t.Fatalf("1ª Collect() retornou erro inesperado: %v", err)
	}
	if first.CPUUsagePct != 0 {
		t.Errorf("1ª coleta: CPUUsagePct = %v, esperado 0 (amostra apenas primada)", first.CPUUsagePct)
	}

	// 100 s of accumulated CPU: any real window between the two
	// collections produces a positive percentage, and the clamp keeps
	// it at 100 when the window is very short.
	f.writeCgroupCounters(100_000_000)
	second, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("2ª Collect() retornou erro inesperado: %v", err)
	}
	if second.CPUUsagePct <= 0 || second.CPUUsagePct > 100 {
		t.Errorf("2ª coleta: CPUUsagePct = %v, esperado (0, 100]", second.CPUUsagePct)
	}
}

// TestCollectorDetectsMechanism covers the automatic detection as seen by
// the collector: an empty root selects the working procfs backend and a
// complete root selects the cgroups backend.
func TestCollectorDetectsMechanism(t *testing.T) {
	f := newFixture(t)

	procfsCol := newTestCollector(t, f) // f.cgroup está vazio
	if got := procfsCol.UsageSource(); got != "procfs" {
		t.Errorf("UsageSource() = %q para raiz vazia, esperado %q", got, "procfs")
	}
	if _, err := procfsCol.Collect(SourcePeriodic); err != nil {
		t.Errorf("coleta via fallback procfs falhou: %v", err)
	}

	f.writeCgroupCounters(42)
	cgCol := newTestCollector(t, f)
	if got := cgCol.UsageSource(); got != "cgroups-v2" {
		t.Errorf("UsageSource() = %q para raiz completa, esperado %q", got, "cgroups-v2")
	}
}

// TestPackageMetadata guarantees the propagation of the mandatory
// metadata: the node identifier, the source and a nonzero timestamp.
func TestPackageMetadata(t *testing.T) {
	f := newFixture(t)
	f.writeCgroupCounters(0)

	pkg, err := newTestCollector(t, f).Collect(SourcePiggyback)
	if err != nil {
		t.Fatalf("Collect() retornou erro inesperado: %v", err)
	}
	if pkg.NodeID != "xp4-borda-01" {
		t.Errorf("NodeID = %q, esperado xp4-borda-01", pkg.NodeID)
	}
	if pkg.Source != SourcePiggyback {
		t.Errorf("Source = %q, esperado %q", pkg.Source, SourcePiggyback)
	}
	if pkg.Timestamp.IsZero() {
		t.Error("Timestamp deveria estar preenchido")
	}
	if pkg.MemoryUsagePct != 50 {
		t.Errorf("MemoryUsagePct = %v, esperado 50 (1 GiB / 2 GiB)", pkg.MemoryUsagePct)
	}
}

// TestCollectConcurrent runs under the race detector: the piggyback and
// the reporter call Collect concurrently, the state of the delta is
// protected by a mutex and no collection can corrupt another one.
func TestCollectConcurrent(t *testing.T) {
	f := newFixture(t)
	f.writeCgroupCounters(0)

	c := newTestCollector(t, f)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(source string) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				if _, err := c.Collect(source); err != nil {
					t.Errorf("Collect(%q) falhou: %v", source, err)
					return
				}
			}
		}(SourcePiggyback)
	}
	wg.Wait()
}

func TestPSICacheExpiryForcesReread(t *testing.T) {
	f := newFixture(t)
	f.writeCgroupCounters(0)
	if err := os.WriteFile(filepath.Join(f.cgroup, "cpu.pressure"), []byte("some avg10=5.00 avg60=3.00 avg300=1.00 total=100\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.cgroup, "memory.pressure"), []byte("some avg10=7.00 avg60=4.00 avg300=2.00 total=90\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	c := newTestCollector(t, f)
	first, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("1a Collect: %v", err)
	}
	if first.CPUPressure == nil || first.CPUPressure.Avg10 != 5.00 {
		t.Fatalf("1a coleta: CPUPressure=%+v, esperado avg10=5.00", first.CPUPressure)
	}

	if err := os.WriteFile(filepath.Join(f.cgroup, "cpu.pressure"), []byte("some avg10=9.00 avg60=3.00 avg300=1.00 total=100\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c.psiCache.at = c.psiCache.at.Add(-11 * time.Second)
	second, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("2a Collect: %v", err)
	}
	if second.CPUPressure == nil || second.CPUPressure.Avg10 != 9.00 {
		t.Errorf("2a coleta após expiração: CPUPressure=%+v, esperado avg10=9.00 (releitura forçada)", second.CPUPressure)
	}
}

// ---------------------------------------------------------------------------
// Tests of the energy block
// ---------------------------------------------------------------------------

// TestCollectEnergy covers the energy block of the collector: the first
// collection omits it, because the window of watts needs the previous
// sample, and the second one, over a counter that advanced, returns
// positive watts with the source of the hardware counter.
func TestCollectEnergy(t *testing.T) {
	dir := t.TempDir()
	powercap := filepath.Join(dir, "powercap")
	supply := filepath.Join(dir, "power_supply")
	if err := os.MkdirAll(supply, 0o755); err != nil {
		t.Fatal(err)
	}
	energyPath := writeRAPLFixture(t, powercap, 5_000_000)

	c := NewCollector("n", true, "", "", "", powercap, supply)
	if got := c.EnergySource(); got != "rapl" {
		t.Fatalf("EnergySource() = %q, esperado %q", got, "rapl")
	}

	first, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("1ª Collect() retornou erro inesperado: %v", err)
	}
	if first.Energy != nil {
		t.Fatalf("1ª coleta: Energy = %+v, esperado nil (amostra apenas primada)", first.Energy)
	}

	if err := os.WriteFile(energyPath, []byte("50000000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	second, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("2ª Collect() retornou erro inesperado: %v", err)
	}
	if second.Energy == nil {
		t.Fatal("2ª coleta: Energy = nil, esperado bloco preenchido")
	}
	if second.Energy.Source != "rapl" {
		t.Errorf("Energy.Source = %q, esperado %q", second.Energy.Source, "rapl")
	}
	if second.Energy.PowerMilliwatts <= 0 {
		t.Errorf("Energy.PowerMilliwatts = %v, esperado > 0 (contador avançou na janela)", second.Energy.PowerMilliwatts)
	}
}

// TestCollectEnergyNoneOmitsBlock covers the compatibility mode: without
// any source in the given directories, the energy block never appears in
// the package.
func TestCollectEnergyNoneOmitsBlock(t *testing.T) {
	powercap := filepath.Join(t.TempDir(), "powercap")
	supply := filepath.Join(t.TempDir(), "supply")
	for _, d := range []string{powercap, supply} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	c := NewCollector("n", true, "", "", "", powercap, supply)
	if got := c.EnergySource(); got != "none" {
		t.Fatalf("EnergySource() = %q, esperado %q", got, "none")
	}

	pkg, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("Collect() retornou erro inesperado: %v", err)
	}
	if pkg.Energy != nil {
		t.Fatalf("Energy = %+v, esperado nil sem fonte de energia", pkg.Energy)
	}
}

// TestCollectEnergyReadErrorExtendsWindow covers the failure invariant: a
// failed energy reading leaves the previous sample intact, so the block is
// omitted in the affected collection and the next window covers the whole
// interval. Recreating the file with the same value proves that the
// previous sample survived the error, since the delta over the extended
// window is exactly zero, while a reset state would produce a positive
// delta.
func TestCollectEnergyReadErrorExtendsWindow(t *testing.T) {
	dir := t.TempDir()
	powercap := filepath.Join(dir, "powercap")
	supply := filepath.Join(dir, "power_supply")
	if err := os.MkdirAll(supply, 0o755); err != nil {
		t.Fatal(err)
	}
	energyPath := writeRAPLFixture(t, powercap, 7_000_000)

	c := NewCollector("n", true, "", "", "", powercap, supply)
	if _, err := c.Collect(SourcePeriodic); err != nil { // primou a amostra
		t.Fatalf("coleta de priming falhou: %v", err)
	}

	if err := os.Remove(energyPath); err != nil {
		t.Fatal(err)
	}
	gap, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("Collect() durante falha de energia deveria suceder (D7): %v", err)
	}
	if gap.Energy != nil {
		t.Fatalf("Energy = %+v durante falha de leitura, esperado nil", gap.Energy)
	}

	time.Sleep(50 * time.Millisecond)
	if err := os.WriteFile(energyPath, []byte("7000000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	same, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("Collect() pós-recuperação falhou: %v", err)
	}
	if same.Energy == nil {
		t.Fatal("Energy = nil pós-recuperação, esperado bloco preenchido")
	}
	if same.Energy.PowerMilliwatts != 0 {
		t.Errorf("PowerMilliwatts = %v, esperado 0 (mesmo valor => delta zero sobre a janela estendida; estado sobreviveu à falha)", same.Energy.PowerMilliwatts)
	}

	if err := os.WriteFile(energyPath, []byte("70000000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	recovered, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("Collect() final falhou: %v", err)
	}
	if recovered.Energy == nil || recovered.Energy.PowerMilliwatts <= 0 {
		t.Fatalf("Energy pós-retomada = %+v, esperado milliwatts > 0", recovered.Energy)
	}
}

// TestPackageEnergyJSONTags pins the serialization contract: a nil energy
// omits the whole key, while inside the block no field is omitted, so
// that a legitimate zero, produced by a wraparound of the counter, still
// reaches the wire.
func TestPackageEnergyJSONTags(t *testing.T) {
	data, err := json.Marshal(Package{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "energy") {
		t.Errorf("JSON %s não deveria conter a chave \"energy\" quando Energy é nil", data)
	}

	data, err = json.Marshal(Package{Energy: &EnergyFields{Source: "rapl", PowerMilliwatts: 0}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"energy"`) {
		t.Fatalf("JSON %s deveria conter a chave \"energy\"", data)
	}
	if !strings.Contains(string(data), `"power_milliwatts":0`) {
		t.Errorf("JSON %s deveria conter \"power_milliwatts\":0 (sem omitempty interno)", data)
	}
}

// ---------------------------------------------------------------------------
// Tests of the disabled collector
// ---------------------------------------------------------------------------

func TestCollectorDisabled(t *testing.T) {
	f := newFixture(t)
	f.writeCgroupCounters(0)
	c := NewCollector("node-disabled", false, f.cgroup, f.stat, f.meminfo, "", "")
	if c.Enabled() {
		t.Fatal("Enabled() = true, want false")
	}
	pkg, err := c.Collect(SourcePiggyback)
	if err != nil {
		t.Fatalf("Collect disabled returned error: %v", err)
	}
	if pkg.NodeID != "node-disabled" {
		t.Errorf("NodeID = %q, want node-disabled", pkg.NodeID)
	}
	if pkg.Source != SourcePiggyback {
		t.Errorf("Source = %q, want %q", pkg.Source, SourcePiggyback)
	}
	if pkg.Timestamp.IsZero() {
		t.Error("Timestamp should be set")
	}
	if pkg.CPUUsagePct != 0 || pkg.MemoryUsagePct != 0 {
		t.Errorf("disabled collect should have zero usage: cpu=%v mem=%v", pkg.CPUUsagePct, pkg.MemoryUsagePct)
	}
	if pkg.CPUPressure != nil || pkg.MemoryPressure != nil {
		t.Errorf("disabled collect should have nil pressure fields")
	}
	if pkg.Energy != nil {
		t.Errorf("disabled collect should have nil Energy")
	}
	// The second collection also performs no input and output.
	pkg2, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("2nd Collect disabled returned error: %v", err)
	}
	if pkg2.Energy != nil {
		t.Errorf("2nd disabled collect Energy should be nil")
	}
}

func TestCustomSysfsRoots(t *testing.T) {
	dir := t.TempDir()
	powercap := filepath.Join(dir, "powercap")
	supply := filepath.Join(dir, "power_supply")
	if err := os.MkdirAll(supply, 0o755); err != nil {
		t.Fatal(err)
	}
	energyPath := writeRAPLFixture(t, powercap, 9_000_000)
	c := NewCollector("n", true, "", "", "", powercap, "")
	if got := c.EnergySource(); got != "rapl" {
		t.Fatalf("EnergySource() = %q, want rapl", got)
	}
	if _, err := c.Collect(SourcePeriodic); err != nil {
		t.Fatalf("priming Collect failed: %v", err)
	}
	if err := os.WriteFile(energyPath, []byte("90000000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	second, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("2nd Collect failed: %v", err)
	}
	if second.Energy == nil {
		t.Fatal("Energy = nil, expected block")
	}
	if second.Energy.PowerMilliwatts <= 0 {
		t.Errorf("PowerMilliwatts = %v, want >0", second.Energy.PowerMilliwatts)
	}
}

// ---------------------------------------------------------------------------
// Tests of the invocation profile
// ---------------------------------------------------------------------------

// TestProfileTracker covers the sequential cases of the tracker: the
// windows, the peak against the average, the memory including the initial
// sample, a counter that went backwards, the idempotence of the closing
// call and the no-op before the start. Every scenario uses two cores:
//
//	pct = deltaUSec / (elapsed_s * 1e6 * 2) * 100
//
// All the windows below have the same duration of one second, so the
// simple average and the average weighted by the duration coincide. The
// cases with different durations are covered by another test.
func TestProfileTracker(t *testing.T) {
	tests := []struct {
		name   string
		run    func(t *testing.T, tr *ProfileTracker)
		want   InvocationProfile
		wantOK bool
	}{
		{
			// A single window, from the start to the closing call without
			// any observation: one closing point, so the peak equals the
			// average, both zero, since the counter does not advance on
			// the closing call.
			name: "janela unica tem pico igual a media",
			run: func(t *testing.T, tr *ProfileTracker) {
				tr.Start(InvocationSample{CPUUsageUSec: 1_000_000, MemoryBytes: 10}, baseT)
				tr.Finish(baseT.Add(time.Second))
			},
			want:   InvocationProfile{MemPeakBytes: 10},
			wantOK: true,
		},
		{
			// A single window with a real load, read before the closing
			// call: the first window closes at ten percent, so the peak
			// and the average are both ten.
			name: "janela unica em voo tem pico igual a media",
			run: func(t *testing.T, tr *ProfileTracker) {
				tr.Start(InvocationSample{MemoryBytes: 5}, baseT)
				tr.Observe(InvocationSample{CPUUsageUSec: 200_000, MemoryBytes: 7}, baseT.Add(time.Second))
			},
			want:   InvocationProfile{CPUPctAvg: 10, CPUPctPeak: 10, MemPeakBytes: 7},
			wantOK: true,
		},
		{
			// Several windows with different loads: five percent, then
			// fifteen percent, then a closing window of zero. The peak
			// is the maximum, fifteen, and the average is the mean of
			// the three values.
			name: "multiplas janelas tem pico igual ao maximo",
			run: func(t *testing.T, tr *ProfileTracker) {
				tr.Start(InvocationSample{MemoryBytes: 100}, baseT)
				tr.Observe(InvocationSample{CPUUsageUSec: 100_000, MemoryBytes: 200}, baseT.Add(time.Second))
				tr.Observe(InvocationSample{CPUUsageUSec: 400_000, MemoryBytes: 300}, baseT.Add(2*time.Second))
				tr.Finish(baseT.Add(3 * time.Second))
			},
			want:   InvocationProfile{CPUPctAvg: 20.0 / 3.0, CPUPctPeak: 15, MemPeakBytes: 300},
			wantOK: true,
		},
		{
			// The memory peak considers the initial sample, which is larger
			// than all the following ones and must win.
			name: "pico de memoria inclui a amostra t0",
			run: func(t *testing.T, tr *ProfileTracker) {
				tr.Start(InvocationSample{MemoryBytes: 999}, baseT)
				tr.Observe(InvocationSample{CPUUsageUSec: 100_000, MemoryBytes: 50}, baseT.Add(time.Second))
				tr.Finish(baseT.Add(2 * time.Second))
			},
			want:   InvocationProfile{CPUPctAvg: 2.5, CPUPctPeak: 5, MemPeakBytes: 999},
			wantOK: true,
		},
		{
			// A counter that went backwards, because the container
			// restarted between two samples: the window becomes zero
			// percent, without a panic and without a negative
			// percentage.
			name: "contador regressivo produz janela zero sem panico",
			run: func(t *testing.T, tr *ProfileTracker) {
				tr.Start(InvocationSample{CPUUsageUSec: 1_000_000, MemoryBytes: 10}, baseT)
				tr.Observe(InvocationSample{CPUUsageUSec: 500_000, MemoryBytes: 20}, baseT.Add(time.Second))
				tr.Finish(baseT.Add(2 * time.Second))
			},
			want:   InvocationProfile{MemPeakBytes: 20},
			wantOK: true,
		},
		{
			// A second closing call is idempotent: it opens no new window
			// and does not change the profile, which keeps two windows,
			// ten percent and a closing window of zero, so the average
			// is five.
			name: "finish duplo e idempotente",
			run: func(t *testing.T, tr *ProfileTracker) {
				tr.Start(InvocationSample{MemoryBytes: 1}, baseT)
				tr.Observe(InvocationSample{CPUUsageUSec: 200_000, MemoryBytes: 2}, baseT.Add(time.Second))
				tr.Finish(baseT.Add(2 * time.Second))
				tr.Finish(baseT.Add(9 * time.Second))
			},
			want:   InvocationProfile{CPUPctAvg: 5, CPUPctPeak: 10, MemPeakBytes: 2},
			wantOK: true,
		},
		{
			// An observation or a closing call before the start does
			// nothing: there is no panic and the tracker stays in the
			// state of never having started.
			name: "observe e finish antes de start sao no-op",
			run: func(t *testing.T, tr *ProfileTracker) {
				tr.Observe(InvocationSample{CPUUsageUSec: 100, MemoryBytes: 1}, baseT)
				tr.Finish(baseT.Add(time.Second))
			},
			want:   InvocationProfile{},
			wantOK: false,
		},
		{
			// A profile read without a start reports false and a zero value.
			name: "profile sem start devolve false",
			run: func(t *testing.T, tr *ProfileTracker) {
				// Nothing, the tracker never started.
			},
			want:   InvocationProfile{},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewProfileTracker(2)
			tt.run(t, tr)

			got, ok := tr.Profile()
			if ok != tt.wantOK {
				t.Fatalf("Profile() ok = %v, esperado %v", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if math.Abs(got.CPUPctAvg-tt.want.CPUPctAvg) > pctEpsilon {
				t.Errorf("CPUPctAvg = %v, esperado %v", got.CPUPctAvg, tt.want.CPUPctAvg)
			}
			if math.Abs(got.CPUPctPeak-tt.want.CPUPctPeak) > pctEpsilon {
				t.Errorf("CPUPctPeak = %v, esperado %v", got.CPUPctPeak, tt.want.CPUPctPeak)
			}
			if got.MemPeakBytes != tt.want.MemPeakBytes {
				t.Errorf("MemPeakBytes = %d, esperado %d", got.MemPeakBytes, tt.want.MemPeakBytes)
			}
		})
	}
}

// TestProfileTrackerStartReplace guarantees the exact semantics of the
// start: repeated calls replace the initial instant only while no window
// has been closed, and after the first window the start becomes a no-op.
func TestProfileTrackerStartReplace(t *testing.T) {
	tr := NewProfileTracker(2)
	tr.Start(InvocationSample{CPUUsageUSec: 0, MemoryBytes: 10}, baseT)
	// Replacing the initial instant before any window: the new sample
	// and base, not the old ones, define the first window.
	tr.Start(InvocationSample{CPUUsageUSec: 0, MemoryBytes: 20}, baseT.Add(time.Second))
	tr.Observe(InvocationSample{CPUUsageUSec: 400_000, MemoryBytes: 30}, baseT.Add(2*time.Second))
	tr.Finish(baseT.Add(3 * time.Second))

	got, ok := tr.Profile()
	if !ok {
		t.Fatal("Profile() ok = false, esperado true")
	}
	// With the old initial instant the window would be two seconds and
	// the percentage ten. With the replacement, the delta of 400 ms over
	// one second and two cores gives twenty percent in the window of the
	// observation, and the closing window of zero percent brings the
	// average to ten.
	if math.Abs(got.CPUPctAvg-10) > pctEpsilon || math.Abs(got.CPUPctPeak-20) > pctEpsilon {
		t.Errorf("avg=%v peak=%v, esperado 10/20 (t0 substituído antes da 1ª janela)", got.CPUPctAvg, got.CPUPctPeak)
	}
	if got.MemPeakBytes != 30 {
		t.Errorf("MemPeakBytes = %d, esperado 30", got.MemPeakBytes)
	}

	// After a closed window, a repeated start is ignored: it neither
	// resets the accumulators nor restarts the timeline.
	tr.Start(InvocationSample{CPUUsageUSec: 0, MemoryBytes: 99}, baseT.Add(4*time.Second))

	depois, ok := tr.Profile()
	if !ok {
		t.Fatal("Profile() ok = false após Start tardio, esperado true")
	}
	if math.Abs(depois.CPUPctAvg-got.CPUPctAvg) > pctEpsilon ||
		math.Abs(depois.CPUPctPeak-got.CPUPctPeak) > pctEpsilon ||
		depois.MemPeakBytes != got.MemPeakBytes {
		t.Errorf("Start após janela fechada alterou o perfil: antes %+v, depois %+v", got, depois)
	}
}

// TestProfileTrackerWeightedAvg proves that the average is weighted by
// the duration of the windows: with different durations the average is
// the weighted mean and not the simple mean of the windows, so a short
// invocation is no longer diluted by long idle windows.
func TestProfileTrackerWeightedAvg(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, tr *ProfileTracker)
		want InvocationProfile
	}{
		{
			// A short invocation of 51 ms with a single tick of load: a
			// window of 50 ms at eighty percent and a synthetic closing
			// window of 1 ms at zero. The weighted average is about
			// 78.43 and the peak is eighty, while the simple mean
			// would give forty, a dilution of two to one.
			name: "invocacao curta nao e diluida pelo fechamento sintetico",
			run: func(t *testing.T, tr *ProfileTracker) {
				tr.Start(InvocationSample{MemoryBytes: 10}, baseT)
				tr.Observe(InvocationSample{CPUUsageUSec: 80_000, MemoryBytes: 11}, baseT.Add(50*time.Millisecond))
				tr.Finish(baseT.Add(51 * time.Millisecond))
			},
			want: InvocationProfile{CPUPctAvg: 4000.0 / 51.0, CPUPctPeak: 80, MemPeakBytes: 11},
		},
		{
			// Three windows of different durations: 100 ms at thirty
			// percent, 500 ms at ten percent and a closing window of
			// 100 ms at zero. The weighted mean is the sum of the
			// weighted values over the sum of the durations, while the
			// simple mean would give about 13.33.
			name: "media ponderada com tres duracoes distintas",
			run: func(t *testing.T, tr *ProfileTracker) {
				tr.Start(InvocationSample{MemoryBytes: 1}, baseT)
				tr.Observe(InvocationSample{CPUUsageUSec: 60_000, MemoryBytes: 2}, baseT.Add(100*time.Millisecond))
				tr.Observe(InvocationSample{CPUUsageUSec: 160_000, MemoryBytes: 3}, baseT.Add(600*time.Millisecond))
				tr.Finish(baseT.Add(700 * time.Millisecond))
			},
			want: InvocationProfile{CPUPctAvg: 80.0 / 7.0, CPUPctPeak: 30, MemPeakBytes: 3},
		},
		{
			// All the windows with a zero duration, because the
			// observations happen at the same instant: the sum of the
			// durations is zero, so the average is zero and never not a
			// number, since the guard checks the sum of the durations
			// and not the number of windows.
			name: "janelas de duracao zero dao media zero sem nan",
			run: func(t *testing.T, tr *ProfileTracker) {
				tr.Start(InvocationSample{MemoryBytes: 7}, baseT)
				tr.Observe(InvocationSample{CPUUsageUSec: 999, MemoryBytes: 8}, baseT)
				tr.Observe(InvocationSample{CPUUsageUSec: 1_999, MemoryBytes: 9}, baseT)
				tr.Finish(baseT)
			},
			want: InvocationProfile{MemPeakBytes: 9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewProfileTracker(2)
			tt.run(t, tr)

			got, ok := tr.Profile()
			if !ok {
				t.Fatal("Profile() ok = false, esperado true")
			}
			if math.IsNaN(got.CPUPctAvg) {
				t.Fatalf("CPUPctAvg = NaN, esperado %v", tt.want.CPUPctAvg)
			}
			if math.Abs(got.CPUPctAvg-tt.want.CPUPctAvg) > pctEpsilon {
				t.Errorf("CPUPctAvg = %v, esperado %v", got.CPUPctAvg, tt.want.CPUPctAvg)
			}
			if math.Abs(got.CPUPctPeak-tt.want.CPUPctPeak) > pctEpsilon {
				t.Errorf("CPUPctPeak = %v, esperado %v", got.CPUPctPeak, tt.want.CPUPctPeak)
			}
			if got.MemPeakBytes != tt.want.MemPeakBytes {
				t.Errorf("MemPeakBytes = %d, esperado %d", got.MemPeakBytes, tt.want.MemPeakBytes)
			}
		})
	}
}

// The old code allowed this because the sum of the durations was
// zero. It works because the number of samples blocks the
// replacement.
func TestProfileTrackerZeroDurationThenLateStart(t *testing.T) {
	tr := NewProfileTracker(2)
	tr.Start(InvocationSample{CPUUsageUSec: 1_000_000, MemoryBytes: 10}, baseT)
	tr.Observe(InvocationSample{CPUUsageUSec: 400_000, MemoryBytes: 30}, baseT)
	tr.Start(InvocationSample{CPUUsageUSec: 0, MemoryBytes: 999}, baseT.Add(time.Hour))
	got, ok := tr.Profile()
	if !ok {
		t.Fatal("Profile() ok = false, esperado true após Start+Observe+late Start")
	}
	if got.MemPeakBytes != 30 {
		t.Errorf("MemPeakBytes = %d, esperado 30 (t0 preservado)", got.MemPeakBytes)
	}
}

// TestProfileTrackerConcurrent covers the case of parallel goroutines
// calling Observe on the same tracker. The assertions are invariants that
// hold for any interleaving: the peak is at least the average, both are
// clamped to [0, 100], and the maximum memory is always captured.
func TestProfileTrackerConcurrent(t *testing.T) {
	tr := NewProfileTracker(2)
	tr.Start(InvocationSample{MemoryBytes: 1}, baseT)

	const goroutines = 8
	const observesPerGoroutine = 50

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := 0; j < observesPerGoroutine; j++ {
				sample := InvocationSample{
					CPUUsageUSec: uint64(g+1)*1_000_000 + uint64(j)*1_000,
					MemoryBytes:  uint64(j + 1),
				}
				at := baseT.Add(time.Duration(g*observesPerGoroutine+j) * time.Millisecond)
				tr.Observe(sample, at)
			}
		}(g)
	}
	wg.Wait()
	tr.Finish(baseT.Add(time.Hour))

	got, ok := tr.Profile()
	if !ok {
		t.Fatal("Profile() ok = false após Start + Observes concorrentes, esperado true")
	}
	if got.CPUPctPeak < got.CPUPctAvg {
		t.Errorf("invariante violada: peak (%v) < avg (%v)", got.CPUPctPeak, got.CPUPctAvg)
	}
	if got.CPUPctAvg < 0 || got.CPUPctAvg > 100 || got.CPUPctPeak < 0 || got.CPUPctPeak > 100 {
		t.Errorf("percentuais fora do clamp [0,100]: avg=%v peak=%v", got.CPUPctAvg, got.CPUPctPeak)
	}
	// Every goroutine observes a memory of up to fifty bytes, so the
	// peak must capture it and must never be smaller than the initial
	// sample.
	if got.MemPeakBytes < 50 {
		t.Errorf("MemPeakBytes = %d, esperado >= 50 (máximo observado)", got.MemPeakBytes)
	}
}

// ---------------------------------------------------------------------------
// Tests of the reporter
// ---------------------------------------------------------------------------

// TestReporterSendsPeriodicPackages covers the contract of the autonomous
// arm: a JSON post to the configured destination, with the periodic source
// and the correct node identifier.
func TestReporterSendsPeriodicPackages(t *testing.T) {
	captured := &capturedPackages{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("falha ao ler corpo recebido: %v", err)
		}
		var pkg Package
		if err := json.Unmarshal(body, &pkg); err != nil {
			t.Errorf("corpo recebido não é um TelemetryPackage válido: %v (%s)", err, body)
		}
		captured.add(pkg, string(body))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	rep := NewReporter(srv.URL, 20*time.Millisecond, newReporterFixture(t), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		rep.Run(ctx)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for captured.count() < 3 {
		select {
		case <-deadline:
			t.Fatalf("esperava ao menos 3 pacotes periódicos; chegaram %d", captured.count())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run não retornou após cancelamento do contexto")
	}

	captured.mu.Lock()
	defer captured.mu.Unlock()
	pkg := captured.pkgs[0]
	if pkg.Source != SourcePeriodic {
		t.Errorf("source = %q, esperado %q", pkg.Source, SourcePeriodic)
	}
	if pkg.NodeID != "no-teste" {
		t.Errorf("node_id = %q, esperado no-teste", pkg.NodeID)
	}
}

// TestReporterSurvivesMonitorErrors checks that a monitor answering with
// an error neither stalls nor brings down the loop: the reporter logs and
// keeps trying.
func TestReporterSurvivesMonitorErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	rep := NewReporter(srv.URL, 20*time.Millisecond, newReporterFixture(t), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		rep.Run(ctx)
		close(done)
	}()

	time.Sleep(80 * time.Millisecond) // ~4 ticks, todos com 500
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run não retornou após cancelamento (loop travado ou em pânico)")
	}
}

func TestReporter_Header(t *testing.T) {
	var mu sync.Mutex
	var gotAuth string
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		mu.Unlock()
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	hdr := http.Header{"Authorization": {"tok-xyz"}, "X-Custom": {"cval"}}
	origCount := len(hdr)
	rep := NewReporter(srv.URL, 20*time.Millisecond, newReporterFixture(t), nil, hdr)
	hdr.Set("Authorization", "mutated")
	hdr.Set("X-Extra", "extra")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		rep.Run(ctx)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		has := gotAuth != ""
		mu.Unlock()
		if has {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for header propagation")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run não retornou")
	}

	mu.Lock()
	defer mu.Unlock()
	if gotAuth != "tok-xyz" {
		t.Fatalf("Authorization = %q, want tok-xyz (clone nil-safe)", gotAuth)
	}
	if gotCT != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", gotCT)
	}
	if len(hdr) != origCount+1 {
		t.Fatalf("header clone failed: original mutated")
	}
}

func TestReporter_NilHeaderNoPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization = %q, want empty when header nil", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	rep := NewReporter(srv.URL, 20*time.Millisecond, newReporterFixture(t), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		rep.Run(ctx)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run não retornou")
	}
}
