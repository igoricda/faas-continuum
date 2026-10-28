package registry

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/imageref"
)

var functionNamePattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,62}[A-Za-z0-9])?$`)

func validarNome(name string) error {
	if !functionNamePattern.MatchString(name) {
		return fmt.Errorf("%w: %q", ErrInvalidFunctionName, name)
	}
	return nil
}

func resolveScaling(opts RegisterOptions) (minInstances, maxInstances int, err error) {
	minInstances = opts.MinInstances
	maxInstances = opts.MaxInstances

	if minInstances < 0 {
		return 0, 0, ErrInvalidMinInstances
	}
	if maxInstances < 0 {
		return 0, 0, ErrInvalidMaxInstances
	}

	if maxInstances > 0 && maxInstances < minInstances {
		return 0, 0, ErrInvalidMaxInstances
	}
	return minInstances, maxInstances, nil
}

func validateIdleTimeout(d config.Duration) error {
	if d == 0 {
		return nil
	}
	td := time.Duration(d)
	if td < 0 || td > 24*time.Hour {
		return fmt.Errorf("%w: %q", ErrInvalidIdleTimeout, d.String())
	}
	return nil
}

func validateQueueMax(maxInstances, queueMax int) error {
	if maxInstances > 0 && queueMax != 0 {
		return ErrInvalidScalingParams
	}
	if maxInstances == 0 && queueMax != 0 && (queueMax < 1 || queueMax > 256) {
		return ErrInvalidScalingParams
	}
	return nil
}

// MaxTimeoutMS is the upper bound of timeout_ms in milliseconds (24 hours,
// the same cap as IdleTimeout): a longer timeout would hold an instance slot
// far beyond any useful request window.
const MaxTimeoutMS = 24 * 60 * 60 * 1000

// validateTimeoutMS validates timeout_ms the way validateIdleTimeout
// validates idle_timeout: 0 means the node default, and any value outside
// 1..MaxTimeoutMS is rejected instead of silently defaulted.
func validateTimeoutMS(timeoutMS int) error {
	if timeoutMS == 0 {
		return nil
	}
	if timeoutMS < 1 || timeoutMS > MaxTimeoutMS {
		return fmt.Errorf("%w: %d", ErrInvalidTimeoutMS, timeoutMS)
	}
	return nil
}

// validateExpectedMetrics validates the expected metrics map. Names are
// compared as exact bytes (case-sensitive, no trimming), so only the empty
// name is rejected; values must be finite, which rejects NaN and Inf. A nil
// or empty map advertises nothing and is valid.
func validateExpectedMetrics(m map[string]ExpectedMetric) error {
	for name, metric := range m {
		if name == "" {
			return fmt.Errorf("%w: nome de métrica vazio", ErrInvalidExpectedMetrics)
		}
		if math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) {
			return fmt.Errorf("%w: métrica %q com valor não-finito (%v)", ErrInvalidExpectedMetrics, name, metric.Value)
		}
	}
	return nil
}

// validateAndNormalizeImageRef normalizes the image reference and, when an
// allowlist is configured, validates its registry host against it. It returns
// the normalized reference or ErrInvalidImageRef.
func (r *Registry) validateAndNormalizeImageRef(imageRef string) (string, error) {
	if strings.TrimSpace(imageRef) == "" {
		return "", fmt.Errorf("%w: referência vazia", ErrInvalidImageRef)
	}
	normalized, err := imageref.NormalizeImageRef(imageRef)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidImageRef, err)
	}
	if len(r.allowedRegistries) > 0 {
		// Registry host of the normalized reference. Format:
		// registry.host/namespace/image:tag or registry.host/image:tag.
		parts := strings.SplitN(normalized, "/", 2)
		if len(parts) < 2 {
			return "", fmt.Errorf("%w: referência normalizada sem registry host: %s", ErrInvalidImageRef, normalized)
		}
		registryHost := parts[0]
		allowed := false
		for _, allowedHost := range r.allowedRegistries {
			if registryHost == allowedHost {
				allowed = true
				break
			}
		}
		if !allowed {
			return "", fmt.Errorf("%w: registry %q não está na allowlist", ErrInvalidImageRef, registryHost)
		}
	}
	return normalized, nil
}
