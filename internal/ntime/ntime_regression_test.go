package ntime

import (
	"testing"
	"time"
)

// TestTimeAlgebra checks the arithmetic of Time. Wall-clock monotonicity is not
// asserted because it is inherently flaky.
func TestTimeAlgebra(t *testing.T) {
	var zero Time
	if !zero.IsZero() {
		t.Fatal("zero Time must report IsZero")
	}
	base := FromTime(time.Now())
	if base.IsZero() {
		t.Fatal("FromTime(now) must be non-zero relative to package epoch")
	}
	if got := base.Add(time.Second).Sub(base); got != time.Second {
		t.Fatalf("Add/Sub round-trip = %v, want %v", got, time.Second)
	}
	if d := base.Add(-time.Hour).Sub(base); d >= 0 {
		t.Fatalf("negative Add must produce earlier Time, got delta %v", d)
	}
}
