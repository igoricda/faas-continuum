// Package energy provides the types and pure functions that convert energy
// samples into watts, with an explicit source.
//
// The contract is that the hardware counter is cumulative, in microjoules
// since boot, while the power supply reports instantaneous power in watts.
// WindowWatts normalizes both to window watts, using the source as the
// discriminant.
package energy

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Source identifies the mechanism used to collect the energy.
const (
	// SourceRAPL identifies the cumulative hardware counter, in microjoules
	// since boot.
	SourceRAPL = "rapl"
	// SourcePowerSupply identifies the instantaneous power source.
	SourcePowerSupply = "power_supply"
	// SourceNone identifies the absence of a known source.
	SourceNone = "none"
)

// Sample is a raw energy reading of the device.
//
// CumulativeUJoules is the cumulative counter in microjoules since boot. It
// is meaningful only for the hardware counter, which advances since the
// system boot and may wrap around after the maximum range, of at least
// 260 kJ. A window that crosses a wraparound reads zero watts, an accepted
// and documented bias.
//
// InstantWatts is the instantaneous power in watts. It is meaningful only
// for the power supply, and it is valid for the sampling window.
type Sample struct {
	// CumulativeUJoules is cumulative since boot, in microjoules.
	CumulativeUJoules uint64
	// InstantWatts is the instantaneous power in watts.
	InstantWatts float64
}

// WindowWatts converts an energy sample into watts over a time window.
//
// The source is explicit, which avoids guessing the unit from the values,
// since a large value could be cumulative microjoules or instantaneous
// watts.
//
// The rules depend on the source:
//
//	With the power supply, it returns the instantaneous power, which is
//	already in watts, and the time window is not used.
//
//	With the cumulative counter, the elapsed time must be positive and
//	the counter must have advanced. Otherwise the result is zero: a
//	non-positive elapsed time means there is no valid window, and a
//	counter that did not advance means a wraparound or a reboot. The
//	watts are the difference in microjoules divided by one million and
//	by the elapsed seconds.
//
//	With an unknown or empty source, the result is zero.
//
// There is no upper clamp: more than a hundred watts is legitimate for a
// dense server, and clamping it would hide a real operating condition.
func WindowWatts(source string, prev, cur Sample, elapsed time.Duration) float64 {
	switch source {
	case SourcePowerSupply:
		return cur.InstantWatts
	case SourceRAPL:
		// The cumulative path of the hardware counter.
		if elapsed <= 0 {
			return 0
		}
		if cur.CumulativeUJoules <= prev.CumulativeUJoules {
			return 0
		}
		deltaUJ := float64(cur.CumulativeUJoules - prev.CumulativeUJoules)
		watts := deltaUJ / 1e6 / elapsed.Seconds()
		return watts
	default:
		// An unknown source cannot be converted.
		return 0
	}
}

// The default paths of the two energy mechanisms of the target device, the
// power capping framework and the power supply class of the kernel.
const (
	DefaultPowercapRoot    = "/sys/class/powercap"
	DefaultPowerSupplyRoot = "/sys/class/power_supply"
)

// Reader returns an energy sample of the device. One instance per agent is
// enough. The implementations keep no state between readings, because the
// state of the difference belongs to the caller, which converts it with
// WindowWatts.
type Reader interface {
	Supported() bool
	Read() (Sample, error)
	Source() string
}

// ErrUnavailable is returned by Read when no energy source is available. The
// absence of a source is not a failure of the system but an expected
// operating state on hardware without a hardware counter.
var ErrUnavailable = errors.New("energy: fonte de energia indisponível")

// raplReader sums the cumulative energy counter, in microjoules since boot,
// of every qualified top-level package found during the detection.
type raplReader struct {
	packages []string // caminhos completos de <pkg>/energy_uj
}

func (r raplReader) Supported() bool { return true }

func (r raplReader) Source() string { return SourceRAPL }

// Read sums the counter of every package. A failure to read or to parse any
// of them aborts with an error, because a partial sum would be read as a
// drop in consumption by the difference of the window.
func (r raplReader) Read() (Sample, error) {
	var soma uint64
	for _, path := range r.packages {
		content, err := os.ReadFile(path)
		if err != nil {
			return Sample{}, fmt.Errorf("energy: falha ao ler energy_uj em %s: %w", path, err)
		}
		v, err := strconv.ParseUint(strings.TrimSpace(string(content)), 10, 64)
		if err != nil {
			return Sample{}, fmt.Errorf("energy: falha ao ler energy_uj em %s: %w", path, err)
		}
		soma += v
	}
	return Sample{CumulativeUJoules: soma}, nil
}

// noneReader is the terminal source of the detection chain: no mechanism is
// available. It reports that it is not supported, so the caller omits the
// energy fields, in compatibility mode.
type noneReader struct{}

func (noneReader) Supported() bool       { return false }
func (noneReader) Source() string        { return SourceNone }
func (noneReader) Read() (Sample, error) { return Sample{}, ErrUnavailable }

// New detects the available energy source in a single pass. The cumulative
// hardware counter is preferred, the power supply comes next, and the
// absence of a source comes last. Empty paths select the defaults. It never
// returns an error and never panics, because the absence of a source is a
// normal state.
//
// Since kernel 5.10 the access to the energy counter is restricted to
// root. A permission error during the probe means the source is
// unavailable and the chain moves to the next source.
func New(powercapRoot, powerSupplyRoot string) Reader {
	if powercapRoot == "" {
		powercapRoot = DefaultPowercapRoot
	}
	if powerSupplyRoot == "" {
		powerSupplyRoot = DefaultPowerSupplyRoot
	}

	if paths := detectRAPLPackages(powercapRoot); len(paths) > 0 {
		return raplReader{packages: paths}
	}
	if r, ok := detectPowerSupply(powerSupplyRoot); ok {
		return r
	}
	return noneReader{}
}

// detectRAPLPackages walks the power capping root once and collects the
// path of the energy counter of every qualified top-level package. A
// package qualifies when its name starts with the hardware counter prefix,
// has no further colon in the suffix and the suffix is a number, which
// excludes the sub-zones of a package that would be counted twice, and
// when a real probe reads its counter as an unsigned integer, which
// excludes the packages that cannot be read or that hold a corrupt
// value.
func detectRAPLPackages(powercapRoot string) []string {
	entries, err := os.ReadDir(powercapRoot)
	if err != nil {
		return nil
	}
	var paths []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "intel-rapl:") {
			continue
		}
		suffix := strings.TrimPrefix(name, "intel-rapl:")
		if strings.Contains(suffix, ":") {
			continue // subzona intel-rapl:X:Y — subset do pacote X
		}
		if _, err := strconv.Atoi(suffix); err != nil {
			continue // a non-numeric suffix is not a power zone
		}
		path := filepath.Join(powercapRoot, name, "energy_uj")
		content, err := os.ReadFile(path)
		if err != nil {
			continue // permission denied or absent: the package does not qualify
		}
		if _, err := strconv.ParseUint(strings.TrimSpace(string(content)), 10, 64); err != nil {
			continue // corrupted content: the package does not qualify
		}
		paths = append(paths, path)
	}
	return paths
}
