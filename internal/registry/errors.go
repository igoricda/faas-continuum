package registry

import (
	"errors"
)

// ErrDuplicateFunction is returned when registering a function_name that
// already exists.
var ErrDuplicateFunction = errors.New("registry: já existe uma função registrada com este nome")

// ErrFunctionNotFound is returned by Update, MarkDegraded, ClearDegraded and
// Unregister when the function is not registered.
var ErrFunctionNotFound = errors.New("registry: função não encontrada")

// ErrInvalidFunctionName is returned when the function name breaks the
// accepted format: 1 to 64 characters from [A-Za-z0-9._-], starting and
// ending with an alphanumeric character. The name becomes a URL segment, a
// container instance ID suffix and a host directory name, so invalid
// characters would leak into all three layers.
var ErrInvalidFunctionName = errors.New("registry: nome de função inválido (use 1-64 caracteres de [A-Za-z0-9._-], sem começar/terminar com . _ ou -)")

// ErrInvalidMinInstances is returned when min_instances is negative. Zero is
// valid and leaves scale-to-zero enabled for the function, while a positive
// value pre-allocates a floor of instances.
var ErrInvalidMinInstances = errors.New("registry: min_instances não pode ser negativo")

// ErrInvalidMaxInstances is returned when max_instances is negative or lower
// than min_instances, since an explicit cap cannot sit below the pre-allocated
// floor. A max_instances of zero is valid and means no cap: the pool grows
// until the device runs out of resources, and the Arbiter can shift load to
// another node.
var ErrInvalidMaxInstances = errors.New("registry: max_instances não pode ser negativo nem menor que min_instances")

// ErrInvalidScalingParams is returned for invalid combinations of
// max_instances and queue_max: queue_max set together with a positive
// max_instances, or queue_max outside 1..256 when max_instances is zero.
var ErrInvalidScalingParams = errors.New("registry: combinação inválida de max_instances e queue_max")

// ErrInvalidIdleTimeout is returned when idle_timeout is negative or exceeds
// 24 hours. Zero is valid and means the global default (see
// DefaultScaleDownTimeout in internal/config).
var ErrInvalidIdleTimeout = errors.New("registry: idle_timeout must be >=0 and <=24h")

// ErrInvalidTimeoutMS is returned when timeout_ms is negative or exceeds the
// 24 hour cap in milliseconds (see MaxTimeoutMS in validation.go). Zero is
// valid and means the node default (see DefaultTimeout in internal/config).
var ErrInvalidTimeoutMS = errors.New("registry: timeout_ms deve ser 0 (padrão do nó) ou entre 1 e 86400000 (24h)")

// ErrInvalidImageRef is returned when the image reference is invalid or not
// allowed by the registry allowlist. Callers can match it with errors.Is.
var ErrInvalidImageRef = errors.New("registry: image_ref inválido ou não permitido")

// ErrInvalidExpectedMetrics is returned when the expected metrics map is
// invalid: an empty metric name or a non-finite Value (NaN or Inf). Callers
// can match it with errors.Is.
var ErrInvalidExpectedMetrics = errors.New("registry: expected_metrics inválido")
