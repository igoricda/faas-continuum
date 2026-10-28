// Quantile fixtures.
package learn

import (
	"math"
	"testing"
)

func TestNormalQuantileFixtures(t *testing.T) {
	for _, tc := range []struct {
		p, want float64
	}{
		{0.95, 1.6449},
		{0.975, 1.9600},
		{0.5, 0},
	} {
		if got := NormalQuantile(tc.p); math.Abs(got-tc.want) > 0.01 {
			t.Fatalf("NormalQuantile(%v) = %v, want %v ±0.01", tc.p, got, tc.want)
		}
	}
}

func TestStudentTQuantileFixtures(t *testing.T) {
	for _, tc := range []struct {
		p, nu, want float64
	}{
		{0.95, 3, 2.3534},
		{0.95, 10, 1.8125},
		{0.95, 30, 1.6973},
		{0.5, 3, 0},
	} {
		if got := StudentTQuantile(tc.p, tc.nu); math.Abs(got-tc.want) > 0.01 {
			t.Fatalf("StudentTQuantile(%v, %v) = %v, want %v ±0.01", tc.p, tc.nu, got, tc.want)
		}
	}
}

func TestQuantileEdges(t *testing.T) {
	if got := NormalQuantile(0); !math.IsInf(got, -1) {
		t.Fatalf("NormalQuantile(0) = %v, want -Inf", got)
	}
	if got := NormalQuantile(1); !math.IsInf(got, 1) {
		t.Fatalf("NormalQuantile(1) = %v, want +Inf", got)
	}
	if got := NormalQuantile(math.NaN()); !math.IsNaN(got) {
		t.Fatalf("NormalQuantile(NaN) = %v, want NaN", got)
	}
	if got := StudentTQuantile(0.9, 0); !math.IsNaN(got) {
		t.Fatalf("StudentTQuantile(nu=0) = %v, want NaN", got)
	}
}
