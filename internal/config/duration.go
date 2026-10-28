package config

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a time.Duration that marshals as a string ("10s", "200ms") so
// that configuration files stay human readable.
type Duration time.Duration

// MarshalJSON encodes the duration as a string via time.Duration.String. It is
// the counterpart of UnmarshalJSON.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal((time.Duration(d)).String())
}

// UnmarshalJSON decodes a duration string via time.ParseDuration.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("config: duration must be a string: %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("config: invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// String returns the duration formatted by time.Duration.String.
func (d Duration) String() string {
	return time.Duration(d).String()
}
