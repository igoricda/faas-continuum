package registry

import (
	"errors"
	"fmt"
	"time"

	"github.com/igoricda/faas-continuum/internal/config"
)

// RegisterOptions groups the configuration parameters of a function in the
// registry. It is a struct because min/max instances, queue size and timeout
// are too many same-typed positional arguments, which invites ordering
// mistakes.
type RegisterOptions struct {
	// MinInstances is the pre-allocated instance floor. Zero leaves
	// scale-to-zero enabled for this function.
	MinInstances int
	// MaxInstances caps elastic scaling. Zero means no cap: the pool grows on
	// demand until the device runs out of resources, and the Arbiter can shift
	// load to another node.
	MaxInstances int
	// QueueMax is the maximum wait queue size when the pool scales without a
	// cap (max_instances == 0). With a positive max_instances, queue_max must
	// be omitted (zero). With max_instances == 0, queue_max in 1..256 bounds
	// the queue, and zero selects the pool default of 32 (see queueCapFor in
	// internal/pool). Zero never means "no queue": a bounded queue always
	// exists while scaling up.
	QueueMax int
	// TimeoutMS is the default invocation timeout for this function in
	// milliseconds, used when the request does not declare max_latency_ms in
	// its SLO. Zero means the node default.
	TimeoutMS int
	// IdleTimeout is the idle period after which an instance is reaped. Zero
	// uses the global default.
	IdleTimeout config.Duration
	// ExpectedMetrics declares the function quality (see ExpectedMetric and
	// Function.ExpectedMetrics). Update replaces it wholesale: an omitted
	// (nil) map clears it.
	ExpectedMetrics map[string]ExpectedMetric
	// LateReportable enables late invocation reports. Defaults to false.
	LateReportable bool
	// GPU requests the NVIDIA device in the function containers (see
	// Function.GPU). Defaults to false.
	GPU bool
	// Rotate regenerates the invoke token on Update: false keeps the existing
	// hash, true issues a new token and returns its plaintext once. Register
	// ignores it and always generates a token.
	Rotate bool
}

// validateRegisterOpts runs the shared Register/Update validation chain in a
// fixed order, so the first error wins. It returns the resolved scaling
// bounds and the normalized image reference.
func (r *Registry) validateRegisterOpts(name, imageRef string, opts RegisterOptions) (min, max int, normalizedRef string, err error) {
	if err := validarNome(name); err != nil {
		return 0, 0, "", err
	}
	normalizedRef, err = r.validateAndNormalizeImageRef(imageRef)
	if err != nil {
		return 0, 0, "", err
	}
	min, max, err = resolveScaling(opts)
	if err != nil {
		return 0, 0, "", err
	}
	if err := validateQueueMax(max, opts.QueueMax); err != nil {
		return 0, 0, "", err
	}
	if err := validateTimeoutMS(opts.TimeoutMS); err != nil {
		return 0, 0, "", err
	}
	if err := validateIdleTimeout(opts.IdleTimeout); err != nil {
		return 0, 0, "", err
	}
	if err := validateExpectedMetrics(opts.ExpectedMetrics); err != nil {
		return 0, 0, "", err
	}
	return min, max, normalizedRef, nil
}

// Register adds a new function and builds its resource_url from host and
// name. The name must be unique (ErrDuplicateFunction), and every setting is
// validated with the same rules as Update; the first failing check returns
// its sentinel error.
//
// A MaxInstances of zero means no cap: the pool grows until the device runs
// out of resources, and the Arbiter can shift load to another node.
//
// Register generates the function invoke token and returns its plaintext
// once; only the hash is persisted.
func (r *Registry) Register(host, name, imageRef string, opts RegisterOptions) (Function, string, error) {
	minInstances, maxInstances, normalizedRef, err := r.validateRegisterOpts(name, imageRef, opts)
	if err != nil {
		return Function{}, "", err
	}

	plainToken, sumHex, err := generateInvokeToken()
	if err != nil {
		return Function{}, "", err
	}

	now := time.Now().UTC()
	fn := Function{
		Name:              name,
		ResourceURL:       fmt.Sprintf("%s/function/%s", host, name),
		ImageRef:          normalizedRef,
		MinInstances:      minInstances,
		MaxInstances:      maxInstances,
		QueueMax:          opts.QueueMax,
		TimeoutMS:         opts.TimeoutMS,
		IdleTimeout:       opts.IdleTimeout,
		ExpectedMetrics:   opts.ExpectedMetrics,
		LateReportable:    opts.LateReportable,
		GPU:               opts.GPU,
		InvokeTokenSHA256: sumHex,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	fn.setInvokeTokenCache(sumHex)

	err = r.apply(func(functions map[string]Function) (undo func(), err error) {
		if _, exists := functions[name]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateFunction, name)
		}
		functions[name] = fn
		return func() { delete(functions, name) }, nil
	})
	if err != nil {
		if errors.Is(err, ErrDuplicateFunction) {
			return Function{}, "", err
		}
		return Function{}, "", fmt.Errorf("registry: falha ao persistir registro de %q: %w", name, err)
	}
	return fn, plainToken, nil
}
