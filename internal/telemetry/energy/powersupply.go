// Energy reader based on the power supply class of the kernel. It measures
// the total electrical power of the whole system and never of a single
// process, because the interface does not split the energy between
// containers and attributing it to a function is outside the scope of
// this source.
//
// The set of sources is frozen at construction, during the single
// detection. A source that appears later is only seen after the agent
// restarts, and a source that disappears makes the following readings fail
// with an error, so the caller omits the energy block until the restart.
package energy

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
)

// psEntry records a qualified source directory together with the mode
// detected for it, either the direct power reading or the product of the
// current and the voltage. The mode is frozen at construction and used by
// readEntryWatts to read the value honestly.
type psEntry struct {
	dir         string
	usePowerNow bool
}

// powersupplyReader sums the instantaneous power, in watts, of every source
// qualified during the detection. The entries keep the directories and the
// detected mode, which are frozen at construction, while the values are
// read again on every call. The mode is never switched silently.
type powersupplyReader struct {
	entries []psEntry
}

func (r *powersupplyReader) Supported() bool { return true }

func (r *powersupplyReader) Source() string { return SourcePowerSupply }

// Read sums the instantaneous power of every qualified source. A failure to
// read or to parse any of them aborts with an error, because a partial sum
// would be read as a drop in consumption by the caller.
func (r *powersupplyReader) Read() (Sample, error) {
	var total float64
	for _, entry := range r.entries {
		watts, err := readEntryWatts(entry)
		if err != nil {
			return Sample{}, err
		}
		total += watts
	}
	return Sample{InstantWatts: total}, nil
}

// detectPowerSupply is the next link of the detection chain. It walks the
// root once and collects the qualified directories. The qualification
// depends on the content and never on the name, because on an embedded
// board the battery inherits the name of the fuel gauge driver instead of
// the labels of a conventional computer. The mode is decided once during
// the detection and frozen in the entry.
func detectPowerSupply(root string) (Reader, bool) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, false
	}
	dirs := make([]psEntry, 0, len(entries))
	for _, entry := range entries {
		dir := filepath.Join(root, entry.Name())
		qualified, usePowerNow := qualifiesAsPowerSupply(dir)
		if qualified {
			dirs = append(dirs, psEntry{dir: dir, usePowerNow: usePowerNow})
		}
	}
	if len(dirs) == 0 {
		return nil, false
	}
	return &powersupplyReader{entries: dirs}, true
}

// qualifiesAsPowerSupply probes a directory to decide whether it is a
// power source: it tries the direct power reading first and, when that is
// missing or unreadable, the pair of current and voltage. It reports
// whether the directory qualified and whether the direct reading was the
// one that qualified it.
func qualifiesAsPowerSupply(dir string) (bool, bool) {
	if _, err := readSysfsInt(dir, "power_now"); err == nil {
		return true, true
	}
	_, errCur := readSysfsInt(dir, "current_now")
	_, errVol := readSysfsInt(dir, "voltage_now")
	if errCur == nil && errVol == nil {
		return true, false
	}
	return false, false
}

// readEntryWatts measures the instantaneous power, in watts, of one
// qualified source, reading the files again at the time of the call while
// keeping the mode detected at construction.
//
// With the direct mode, the power file is present and readable and the
// watts are the microjoules divided by one million. A missing file is an
// honest error, without a fallback to the product mode.
//
// With the product mode, the watts are the current in microamperes times
// the voltage in microvolts, divided by one million million. A missing
// file is an honest error, without a fallback to the direct mode.
//
// The absolute value is taken per entry before summing, because the sign
// depends on the driver and on whether the device is charging or
// discharging, while the value on the wire is always non-negative. A
// corrupt file is an immediate error of that entry: an absence in the
// frozen mode is an error and never a switch of mode.
func readEntryWatts(entry psEntry) (float64, error) {
	if entry.usePowerNow {
		powerNow, err := readSysfsInt(entry.dir, "power_now")
		switch {
		case err == nil:
			return math.Abs(float64(powerNow) / 1e6), nil
		case !os.IsNotExist(err):
			return 0, fmt.Errorf("energy: falha ao ler power_now em %s: %w", entry.dir, err)
		}
		return 0, fmt.Errorf("energy: power_now ausente em %s (modo power_now congelado)", entry.dir)
	}
	currentNow, err := readSysfsInt(entry.dir, "current_now")
	if err != nil {
		return 0, fmt.Errorf("energy: falha ao ler current_now em %s: %w", entry.dir, err)
	}
	voltageNow, err := readSysfsInt(entry.dir, "voltage_now")
	if err != nil {
		return 0, fmt.Errorf("energy: falha ao ler voltage_now em %s: %w", entry.dir, err)
	}
	return math.Abs(float64(currentNow) * float64(voltageNow) / 1e12), nil
}

// readSysfsInt reads a file of the directory and parses its content as a
// decimal integer, trimming the whitespace of the newline that the
// interface adds. It returns the original error of the read or of the
// parse, and the caller decides whether an absence disqualifies the
// directory or only switches the mode.
func readSysfsInt(dir, name string) (int64, error) {
	content, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(string(bytes.TrimSpace(content)), 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}
