// Package ntime provides a compact monotonic timestamp: nanoseconds elapsed
// since a package-level epoch taken at process start.
package ntime

import "time"

// Time is a timestamp in nanoseconds since the package epoch.
type Time int64

// epoch is the reference instant for all Time values.
var epoch = time.Now()

// Now returns the current instant relative to the package epoch.
func Now() Time {
	return Time(time.Since(epoch).Nanoseconds())
}

// FromTime converts a wall-clock time to an instant relative to the package
// epoch.
func FromTime(t time.Time) Time {
	return Time(t.Sub(epoch).Nanoseconds())
}

// Since returns the time elapsed since t.
func Since(t Time) time.Duration {
	return time.Duration(Now() - t)
}

// Sub returns the duration from u to t.
func (t Time) Sub(u Time) time.Duration {
	return time.Duration(t - u)
}

// Add returns the instant t+d.
func (t Time) Add(d time.Duration) Time {
	return t + Time(d.Nanoseconds())
}

// IsZero reports whether t is the zero Time.
func (t Time) IsZero() bool {
	return t == 0
}
