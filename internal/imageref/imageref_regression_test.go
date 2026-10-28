package imageref

import (
	"errors"
	"testing"
)

// This test pins the normalization rules of NormalizeImageRef.
func TestNormalizeImageRef(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "bare official name gains registry/library/latest", in: "nginx", want: "docker.io/library/nginx:latest"},
		{name: "explicit tag preserved", in: "nginx:1.25", want: "docker.io/library/nginx:1.25"},
		{name: "explicit registry preserved", in: "ghcr.io/org/img:v1", want: "ghcr.io/org/img:v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeImageRef(tc.in)
			if err != nil {
				t.Fatalf("NormalizeImageRef(%q) unexpected error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("NormalizeImageRef(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeImageRefInvalid(t *testing.T) {
	for _, in := range []string{"", "   ", ":::invalid:::"} {
		if _, err := NormalizeImageRef(in); !errors.Is(err, ErrInvalidReference) {
			t.Fatalf("NormalizeImageRef(%q) error = %v, want errors.Is ErrInvalidReference", in, err)
		}
	}
}
