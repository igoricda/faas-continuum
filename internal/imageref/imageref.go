// Package imageref normalizes OCI image references.
//
// It is kept free of any container runtime dependency so that registry
// validation can reuse it without pulling in the containerd SDK.
package imageref

import (
	"errors"
	"fmt"
	"strings"

	dockerref "github.com/distribution/reference"
)

// ErrInvalidReference is returned when a reference cannot be parsed as a
// Docker or OCI reference. Test it with errors.Is.
var ErrInvalidReference = errors.New("imageref: referência de imagem inválida")

// NormalizeImageRef applies the same normalization as the Docker CLI: it
// adds "docker.io/" when no registry is given, "library/" for official
// single-segment names, and ":latest" when neither tag nor digest is
// present. It performs no I/O.
func NormalizeImageRef(ref string) (string, error) {
	if strings.TrimSpace(ref) == "" {
		return "", fmt.Errorf("%w: referência vazia", ErrInvalidReference)
	}
	named, err := dockerref.ParseDockerRef(ref)
	if err != nil {
		return "", fmt.Errorf("%w (ref %q): %w", ErrInvalidReference, ref, err)
	}
	return named.String(), nil
}
