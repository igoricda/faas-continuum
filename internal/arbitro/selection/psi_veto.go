// Full pressure veto of the selector: the rule and the defaults follow the
// canonical risk selector, and the calibration is done by the flags of the
// processor and full memory thresholds.
//
// Rule: the veto applies when the average full memory pressure of the
// telemetry exceeds its threshold or when the average processor pressure
// exceeds its threshold, over the ten-second averages of the pressure
// interface, with the same percentage normalization as the telemetry package.
// The functions are pure and testable without a monitor or a history, since
// the pressure inputs are explicit.
//
// Defaults:
//   - the processor threshold is 28, in the range from 25 to 30, just above
//     the generic overload ceiling of 25 on purpose, so the processor arm of
//     the veto does not duplicate the second stage and only catches a severe
//     stall that escaped it, and the new value of this veto is in the full
//     memory arm;
//   - the full memory threshold is 12, in the range from 10 to 15 and below
//     the generic ceiling of 25, since high full memory pressure with low
//     processor pressure indicates memory thrashing, a leading indicator
//     that the generic ceiling would only catch late.
//
// Fail-open: an absent, stale or unknown pressure never vetoes alone, since a
// missing reading, the zero value of an unknown, a non-numeric value and a
// negative value all return false.
//
// Order of evaluation: the veto is evaluated in the per-node loop of the
// selection after the projected guard, accumulating the nodes it removed, so
// that the fail-open of the selector covers it and restores the set only when
// the veto emptied it, since the pressure veto never empties the set alone. It
// does not replace the hard guard of projected processor load, which has no
// fail-open, nor the generic overload stage, and the three vetoes coexist in
// parallel.
package selection

import (
	"fmt"
	"math"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// PSIVetoThresholds carries the thresholds of the full pressure veto, where a
// non-positive value is invalid and fails fast, without silent defaults.
type PSIVetoThresholds struct {
	CPUSomeAvg10 float64
	MemFullAvg10 float64
}

// DefaultPSIVetoThresholds returns the documented defaults, of 28 for the
// processor and 12 for the full memory.
func DefaultPSIVetoThresholds() PSIVetoThresholds {
	return PSIVetoThresholds{CPUSomeAvg10: 28, MemFullAvg10: 12}
}

// ValidatePSIVetoThresholds fails fast with an error on a non-positive
// threshold, in the style of the configuration validation, without a silent
// fallback.
func ValidatePSIVetoThresholds(th PSIVetoThresholds) error {
	if th.CPUSomeAvg10 <= 0 {
		return fmt.Errorf("selection: PSIVetoThresholds CPUSomeAvg10=%.2f inválido (exige >0)", th.CPUSomeAvg10)
	}
	if th.MemFullAvg10 <= 0 {
		return fmt.Errorf("selection: PSIVetoThresholds MemFullAvg10=%.2f inválido (exige >0)", th.MemFullAvg10)
	}
	return nil
}

// PSIVetoFull decides the full pressure veto, and it is pure. It vetoes when
// either average exceeds its threshold, with a strict comparison, so equality
// does not veto, which matches the overload and the projected guard.
// Fail-open: an absent or stale reading gives false, both non-positive averages
// give false, since zero means unknown, and a non-numeric value from a
// corrupted reading gives false. Non-positive thresholds never veto zeros,
// which guards the unknown value, but the caller should validate them once at
// startup.
func PSIVetoFull(cpuSomeAvg10, memFullAvg10 float64, hasPSI bool, th PSIVetoThresholds) bool {
	if !hasPSI {
		return false
	}
	if math.IsNaN(cpuSomeAvg10) || math.IsNaN(memFullAvg10) {
		return false
	}
	if cpuSomeAvg10 <= 0 && memFullAvg10 <= 0 {
		return false
	}
	return memFullAvg10 > th.MemFullAvg10 || cpuSomeAvg10 > th.CPUSomeAvg10
}

// PSIVetoFullFromPackage adapts a telemetry package to the full veto. An
// unknown package gives false. Absent pressure fields, in the compatibility
// mode without pressure support, fall open. Only the processor and the full
// memory arms feed the veto, since the partial memory pressure belongs to the
// generic overload stage and stays outside this veto by decision.
func PSIVetoFullFromPackage(pkg telemetry.Package, ok bool, th PSIVetoThresholds) bool {
	if !ok {
		return false
	}
	var cpuSome, memFull float64
	hasPSI := false
	if pkg.CPUPressure != nil {
		cpuSome = pkg.CPUPressure.Avg10
		hasPSI = true
	}
	if pkg.MemoryPressureFull != nil {
		memFull = pkg.MemoryPressureFull.Avg10
		hasPSI = true
	}
	return PSIVetoFull(cpuSome, memFull, hasPSI, th)
}
