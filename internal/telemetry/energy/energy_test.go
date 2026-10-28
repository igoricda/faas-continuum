package energy

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWindowWatts(t *testing.T) {
	tests := []struct {
		name          string
		source        string
		prev, cur     Sample
		elapsed       time.Duration
		expectedWatts float64
	}{
		// 3_000_000 microjoules are 3.0 J, and 3.0 J over 2 s is 1.5 W.
		{
			name:          "normal delta rapl",
			source:        "rapl",
			prev:          Sample{CumulativeUJoules: 1_000_000},
			cur:           Sample{CumulativeUJoules: 4_000_000},
			elapsed:       2 * time.Second,
			expectedWatts: 1.5,
		},
		// The battery reading passes through even when the cumulative
		// counters are zero.
		{
			name:          "power_supply passthrough",
			source:        "power_supply",
			prev:          Sample{CumulativeUJoules: 0, InstantWatts: 10.5},
			cur:           Sample{CumulativeUJoules: 0, InstantWatts: 15.2},
			elapsed:       5 * time.Second,
			expectedWatts: 15.2,
		},
		// A counter that went backwards gives zero, after a wraparound or
		// a reboot.
		{
			name:          "cur<pre rapl wraparound",
			source:        "rapl",
			prev:          Sample{CumulativeUJoules: 4_000_000},
			cur:           Sample{CumulativeUJoules: 1_000_000},
			elapsed:       2 * time.Second,
			expectedWatts: 0,
		},
		// A zero elapsed time gives zero.
		{
			name:          "elapsed zero rapl",
			source:        "rapl",
			prev:          Sample{CumulativeUJoules: 1_000_000},
			cur:           Sample{CumulativeUJoules: 4_000_000},
			elapsed:       0,
			expectedWatts: 0,
		},
		// A negative elapsed time gives zero.
		{
			name:          "elapsed negative rapl",
			source:        "rapl",
			prev:          Sample{CumulativeUJoules: 1_000_000},
			cur:           Sample{CumulativeUJoules: 4_000_000},
			elapsed:       -1 * time.Second,
			expectedWatts: 0,
		},
		// Zero samples give zero.
		{
			name:          "zero samples rapl",
			source:        "rapl",
			prev:          Sample{},
			cur:           Sample{},
			elapsed:       2 * time.Second,
			expectedWatts: 0,
		},
		// A large difference is not clamped: 500_000_000_000 microjoules
		// over one second are 500000 W, and the value is preserved.
		{
			name:          "giant delta no clamp",
			source:        "rapl",
			prev:          Sample{CumulativeUJoules: 0},
			cur:           Sample{CumulativeUJoules: 500_000_000_000},
			elapsed:       time.Second,
			expectedWatts: 500000,
		},
		// An unknown source gives zero.
		{
			name:          "unknown source foo",
			source:        "foo",
			prev:          Sample{CumulativeUJoules: 1_000_000},
			cur:           Sample{CumulativeUJoules: 4_000_000},
			elapsed:       2 * time.Second,
			expectedWatts: 0,
		},
		// The power supply passes through even with zero cumulative
		// counters.
		{
			name:          "power_supply zero cumulatives",
			source:        "power_supply",
			prev:          Sample{CumulativeUJoules: 0, InstantWatts: 0},
			cur:           Sample{CumulativeUJoules: 0, InstantWatts: 100},
			elapsed:       1 * time.Second,
			expectedWatts: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := WindowWatts(tt.source, tt.prev, tt.cur, tt.elapsed)
			if got != tt.expectedWatts {
				t.Errorf("WindowWatts(source=%q, prev=%+v, cur=%+v, elapsed=%v) = %v, esperado %v",
					tt.source, tt.prev, tt.cur, tt.elapsed, got, tt.expectedWatts)
			}
		})
	}
}

// writeEnergyUJ creates the energy counter file with the given value.
func writeEnergyUJ(t *testing.T, root, zone, value string) {
	t.Helper()
	dir := filepath.Join(root, zone)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "energy_uj"), []byte(value+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile em %s: %v", dir, err)
	}
}

// TestNewDetectsRAPLSumsPackagesOnlyTopLevel checks the detection: the sum
// covers the top-level packages only, the sub-zones are excluded because
// they would count the same energy twice, and a corrupt package does not
// qualify.
func TestNewDetectsRAPLSumsPackagesOnlyTopLevel(t *testing.T) {
	root := t.TempDir()
	writeEnergyUJ(t, root, "intel-rapl:0", "1000000")
	writeEnergyUJ(t, root, "intel-rapl:1", "2000000")
	// A decoy sub-zone of the first package with a different value: if it
	// were summed, the result would not be 3_000_000.
	writeEnergyUJ(t, root, "intel-rapl:0:0", "999999999")
	// A package with a corrupt counter does not qualify and stays out of
	// the sum.
	writeEnergyUJ(t, root, "intel-rapl:2", "corrompido")

	r := New(root, filepath.Join(root, "power_supply-inexistente"))
	if !r.Supported() {
		t.Fatalf("Supported() = false, esperado true (há pacotes RAPL qualificados)")
	}
	if got := r.Source(); got != SourceRAPL {
		t.Fatalf("Source() = %q, esperado %q", got, SourceRAPL)
	}
	s, err := r.Read()
	if err != nil {
		t.Fatalf("Read() erro inesperado: %v", err)
	}
	// Only the first two packages contribute.
	if s.CumulativeUJoules != 3_000_000 {
		t.Errorf("CumulativeUJoules = %d, esperado 3000000 (subzonas/corruptos não devem somar)", s.CumulativeUJoules)
	}
}

// TestRAPLReadErrorOnCorruptFile checks that the read fails with an error
// prefixed by the package name when a qualified package becomes corrupt
// after the detection, and that it never returns a partial sum.
func TestRAPLReadErrorOnCorruptFile(t *testing.T) {
	root := t.TempDir()
	writeEnergyUJ(t, root, "intel-rapl:0", "1000000")

	r := New(root, "")
	rr, ok := r.(raplReader)
	if !ok {
		t.Fatalf("New retornou %T, esperado raplReader", r)
	}
	// The corruption happens after the detection, so the second read must
	// fail as a whole.
	writeEnergyUJ(t, root, "intel-rapl:1", "ainda-valida-na-detecção")
	rr.packages = append(rr.packages, filepath.Join(root, "intel-rapl:1", "energy_uj"))
	if err := os.WriteFile(rr.packages[1], []byte("lixo"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	s, err := rr.Read()
	if err == nil {
		t.Fatalf("Read() = %+v, esperado erro com energy_uj corrupto", s)
	}
	if msg := err.Error(); len(msg) < 8 || msg[:7] != "energy:" {
		t.Errorf("erro = %q, esperado prefixo \"energy:\"", msg)
	}
	if s.CumulativeUJoules != 0 {
		t.Errorf("Sample retornado no erro = %+v, esperado zero (nunca soma parcial)", s)
	}
}

// TestNewFallsBackToNoneWithoutRAPL checks the terminal fallback: without
// a readable hardware counter the reader reports that it is not supported
// and returns the unavailable error.
func TestNewFallsBackToNoneWithoutRAPL(t *testing.T) {
	root := t.TempDir()
	// The directory exists but holds no hardware counter zone.
	if err := os.MkdirAll(filepath.Join(root, "outro-mecanismo"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	r := New(root, filepath.Join(root, "power_supply-inexistente"))
	if r.Supported() {
		t.Fatalf("Supported() = true, esperado false sem RAPL e sem power_supply")
	}
	if got := r.Source(); got != SourceNone {
		t.Fatalf("Source() = %q, esperado %q", got, SourceNone)
	}
	s, err := r.Read()
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Read() erro = %v, esperado ErrUnavailable", err)
	}
	if s != (Sample{}) {
		t.Errorf("Sample = %+v, esperado zero", s)
	}
}

// TestNewExcludesUnreadablePackageOnProbe covers the case of a package
// that cannot be read, which happens when only the root may read the
// counter. Such a package is excluded during the detection, and when it is
// the only candidate the chain falls back to the unsupported reader. Since
// restrictive permissions do not stop the root, the test skips itself when
// it runs as root.
func TestNewExcludesUnreadablePackageOnProbe(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("rodando como root: chmod 000 não impede a leitura da sonda")
	}
	root := t.TempDir()
	writeEnergyUJ(t, root, "intel-rapl:0", "1000000")
	energyUJ := filepath.Join(root, "intel-rapl:0", "energy_uj")
	if err := os.Chmod(energyUJ, 0o000); err != nil {
		t.Fatalf("Chmod(000) em %s: %v", energyUJ, err)
	}
	t.Cleanup(func() { _ = os.Chmod(energyUJ, 0o644) })

	r := New(root, filepath.Join(root, "power_supply-inexistente"))
	if r.Supported() {
		t.Fatalf("Supported() = true, esperado false (sonda ilegível ⇒ pacote não qualifica)")
	}
	if got := r.Source(); got != SourceNone {
		t.Errorf("Source() = %q, esperado %q", got, SourceNone)
	}
}

// TestDefaultRoots pins the exact strings of the default paths of the
// interface, because a wrong path would silently disable the energy
// telemetry on every node.
func TestDefaultRoots(t *testing.T) {
	if DefaultPowercapRoot != "/sys/class/powercap" {
		t.Errorf("DefaultPowercapRoot = %q, esperado \"/sys/class/powercap\"", DefaultPowercapRoot)
	}
	if DefaultPowerSupplyRoot != "/sys/class/power_supply" {
		t.Errorf("DefaultPowerSupplyRoot = %q, esperado \"/sys/class/power_supply\"", DefaultPowerSupplyRoot)
	}
}

// ---------------------------------------------------------------------------
// Tests of the power supply reader
// ---------------------------------------------------------------------------

// writePowerSupply creates a file of a source directory with the given
// value, as a fixture of the interface under a temporary directory.
func writePowerSupply(t *testing.T, root, name, file, value string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, file), []byte(value+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile em %s: %v", dir, err)
	}
}

// newPowerSupplyReader builds the reader through the detection chain with a
// hardware counter root that does not exist, so the detection reaches the
// power supply.
func newPowerSupplyReader(t *testing.T, psRoot string) Reader {
	t.Helper()
	r := New(filepath.Join(t.TempDir(), "powercap-inexistente"), psRoot)
	if _, ok := r.(*powersupplyReader); !ok {
		t.Fatalf("New retornou %T, esperado *powersupplyReader", r)
	}
	return r
}

// TestPowerSupplyReadPowerNowMode checks the direct mode: a source with a
// power_now of 10000000 microjoules reads exactly 10 W.
func TestPowerSupplyReadPowerNowMode(t *testing.T) {
	root := t.TempDir()
	writePowerSupply(t, root, "BAT1", "power_now", "10000000")

	r := newPowerSupplyReader(t, root)
	if !r.Supported() {
		t.Fatalf("Supported() = false, esperado true")
	}
	if got := r.Source(); got != SourcePowerSupply {
		t.Fatalf("Source() = %q, esperado %q", got, SourcePowerSupply)
	}
	s, err := r.Read()
	if err != nil {
		t.Fatalf("Read() erro inesperado: %v", err)
	}
	if s.InstantWatts != 10 {
		t.Errorf("InstantWatts = %v, esperado 10", s.InstantWatts)
	}
}

// TestPowerSupplyReadProductModeFreeName checks the product mode with a
// free name: the battery of an embedded board, whose driver name has no
// battery prefix, with a current of 2000000 microamperes and a voltage of
// 3700000 microvolts reads 7.4 W.
func TestPowerSupplyReadProductModeFreeName(t *testing.T) {
	root := t.TempDir()
	writePowerSupply(t, root, "axp20x-battery", "current_now", "2000000")
	writePowerSupply(t, root, "axp20x-battery", "voltage_now", "3700000")

	r := newPowerSupplyReader(t, root)
	s, err := r.Read()
	if err != nil {
		t.Fatalf("Read() erro inesperado: %v", err)
	}
	if s.InstantWatts != 7.4 {
		t.Errorf("InstantWatts = %v, esperado 7.4", s.InstantWatts)
	}
}

// TestPowerSupplySumsQualifiedEntriesIgnDecoy checks that the two
// qualified sources are summed, 10 W and 7.4 W, and that the decoy without
// any readable power file is excluded.
func TestPowerSupplySumsQualifiedEntriesIgnDecoy(t *testing.T) {
	root := t.TempDir()
	writePowerSupply(t, root, "BAT1", "power_now", "10000000")
	writePowerSupply(t, root, "axp20x-battery", "current_now", "2000000")
	writePowerSupply(t, root, "axp20x-battery", "voltage_now", "3700000")
	// The decoy is a mains source without any readable power file.
	writePowerSupply(t, root, "AC", "uevent", "POWER_SUPPLY_NAME=AC")

	r := newPowerSupplyReader(t, root)
	s, err := r.Read()
	if err != nil {
		t.Fatalf("Read() erro inesperado: %v", err)
	}
	if math.Abs(s.InstantWatts-17.4) > 1e-9 {
		t.Errorf("InstantWatts = %v, esperado 17.4 (chamariz não deve somar)", s.InstantWatts)
	}
}

// TestPowerSupplyAbsPerEntry pins the contract of the absolute value per
// entry, because the sign depends on the driver between charging and
// discharging while the value on the wire is always non-negative.
func TestPowerSupplyAbsPerEntry(t *testing.T) {
	root := t.TempDir()
	writePowerSupply(t, root, "BAT1", "power_now", "-5000000")

	r := newPowerSupplyReader(t, root)
	s, err := r.Read()
	if err != nil {
		t.Fatalf("Read() erro inesperado: %v", err)
	}
	if s.InstantWatts != 5 {
		t.Errorf("InstantWatts = %v, esperado 5 (abs por entrada)", s.InstantWatts)
	}
}

// TestPowerSupplyReadErrorOnCorruptFileAfterDetection checks that a
// qualified source whose power file becomes corrupt after the detection
// makes the read fail with an error prefixed by the package name, and
// that it never returns a partial sum.
func TestPowerSupplyReadErrorOnCorruptFileAfterDetection(t *testing.T) {
	root := t.TempDir()
	writePowerSupply(t, root, "BAT1", "power_now", "10000000")

	detected, ok := detectPowerSupply(root)
	if !ok {
		t.Fatalf("detectPowerSupply(%s) = (_, false), esperado true", root)
	}
	powerNow := filepath.Join(root, "BAT1", "power_now")
	if err := os.WriteFile(powerNow, []byte("corrompido"), 0o644); err != nil {
		t.Fatalf("WriteFile(corrompido): %v", err)
	}

	s, err := detected.Read()
	if err == nil {
		t.Fatalf("Read() = %+v, esperado erro com power_now corrupto", s)
	}
	if msg := err.Error(); !strings.Contains(msg, "energy:") {
		t.Errorf("erro = %q, esperado conter \"energy:\"", msg)
	}
	if s != (Sample{}) {
		t.Errorf("Sample retornado no erro = %+v, esperado zero (nunca soma parcial)", s)
	}
}

// TestPowerSupplySetFrozenAtConstruction pins the freezing of the set of
// sources: a source that appears after the construction is only seen
// after the agent restarts.
func TestPowerSupplySetFrozenAtConstruction(t *testing.T) {
	root := t.TempDir()
	writePowerSupply(t, root, "BAT1", "power_now", "10000000")

	r, ok := detectPowerSupply(root)
	if !ok {
		t.Fatalf("detectPowerSupply(%s) = (_, false), esperado true", root)
	}
	// A source that appears after the construction does not enter the
	// following readings.
	writePowerSupply(t, root, "BAT2", "power_now", "5000000")

	s, err := r.Read()
	if err != nil {
		t.Fatalf("Read() erro inesperado: %v", err)
	}
	if s.InstantWatts != 10 {
		t.Errorf("InstantWatts = %v, esperado 10 (conjunto congelado na construção)", s.InstantWatts)
	}
}

// TestPowerSupplyFrozenModeAfterDeletion checks that, once the detection
// has frozen the direct mode, removing the power file makes the read fail
// honestly instead of falling back to the product mode.
func TestPowerSupplyFrozenModeAfterDeletion(t *testing.T) {
	root := t.TempDir()
	// The fixture has the power file, so the detection freezes the direct
	// mode.
	writePowerSupply(t, root, "BAT1", "power_now", "10000000")

	detected, ok := detectPowerSupply(root)
	if !ok {
		t.Fatalf("detectPowerSupply(root) = (_, false), esperado true")
	}
	// Modo congelado: power_now = true

	// Remove the instantaneous power after the construction
	powerNowPath := filepath.Join(root, "BAT1", "power_now")
	if err := os.WriteFile(powerNowPath, []byte{}, 0o644); err != nil {
		t.Fatalf("WriteFile(vazio): %v", err)
	}
	// Escreve current_now e voltage_now para tentar enganar o fallback
	writePowerSupply(t, root, "BAT1", "current_now", "2000000")
	writePowerSupply(t, root, "BAT1", "voltage_now", "3700000")

	s, err := detected.Read()
	if err == nil {
		t.Fatalf("Read() = %+v, esperado erro (modo power_now congelado)", s)
	}
	if msg := err.Error(); !strings.Contains(msg, "energy:") {
		t.Errorf("erro = %q, esperado conter \"energy:\"", msg)
	}
	if s != (Sample{}) {
		t.Errorf("Sample retornado no erro = %+v, esperado zero (nunca soma parcial)", s)
	}
}

// TestPowerSupplyAbsentRootFallsToNone verifica o elo terminal: raiz
// power_supply inexistente ⇒ detectPowerSupply (nil, false) e New cai
// para noneReader (Supported false, ErrUnavailable).
func TestPowerSupplyAbsentRootFallsToNone(t *testing.T) {
	root := t.TempDir()
	psRoot := filepath.Join(root, "power_supply-inexistente")

	if r, ok := detectPowerSupply(psRoot); ok || r != nil {
		t.Fatalf("detectPowerSupply(raiz ausente) = (%T, %v), esperado (nil, false)", r, ok)
	}

	r := New(filepath.Join(root, "powercap-inexistente"), psRoot)
	if r.Supported() {
		t.Fatalf("Supported() = true, esperado false sem RAPL e sem power_supply")
	}
	if got := r.Source(); got != SourceNone {
		t.Errorf("Source() = %q, esperado %q", got, SourceNone)
	}
	if _, err := r.Read(); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Read() erro = %v, esperado ErrUnavailable", err)
	}
}
