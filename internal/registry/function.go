package registry

import (
	"time"

	"github.com/igoricda/faas-continuum/internal/config"
)

// ExpectedMetric declares the expected quality of a function variant for one
// metric, such as accuracy 0.97: Value is the expected value and
// HigherIsBetter its polarity, where false means lower-is-better (latency,
// cost).
type ExpectedMetric struct {
	Value          float64 `json:"value"`
	HigherIsBetter bool    `json:"higher_is_better"`
}

// Function is a registered function: addressing metadata (Name, ResourceURL),
// image (ImageRef), scaling settings (MinInstances, MaxInstances, QueueMax,
// TimeoutMS, IdleTimeout) and health state (UpdatedAt, LastError as reported
// by Degraded). The JSON tags follow the snake_case used by the HTTP API.
type Function struct {
	Name         string          `json:"function_name"`
	ResourceURL  string          `json:"resource_url"`
	ImageRef     string          `json:"image_ref"`
	MinInstances int             `json:"min_instances"`
	MaxInstances int             `json:"max_instances"`        // 0 = no cap: scales until the device runs out (see AcquireWait in internal/pool)
	QueueMax     int             `json:"queue_max,omitempty"`  // 0 or omitted = pool default 32; 1..256 when set (see queueCapFor in internal/pool)
	TimeoutMS    int             `json:"timeout_ms,omitempty"` // 0 = node default (see DefaultTimeout in internal/config); 1..86400000 when set
	IdleTimeout  config.Duration `json:"idle_timeout,omitempty"`
	// ExpectedMetrics declares the quality of each variant, mapping metric
	// name to value and polarity. A nil or empty map advertises nothing and
	// leaves routing unchanged. Files without the key reload with a nil map.
	ExpectedMetrics map[string]ExpectedMetric `json:"expected_metrics,omitempty"`
	// LateReportable enables late reports: an execution keeps running after
	// the client gives up and emits a late report. Defaults to false.
	LateReportable bool `json:"late_reportable,omitempty"`
	// GPU requests the NVIDIA device in the function container: a
	// libnvidia-container prestart hook plus NVIDIA_VISIBLE_DEVICES=all. It is
	// opt-in per function because the default agent runtime is runc without
	// GPU support. Defaults to false (CPU only).
	GPU bool `json:"gpu,omitempty"`
	// InvokeTokenSHA256 is the hex SHA-256 of the function invoke token (see
	// generateInvokeToken and VerifyInvokeToken in invoketoken.go): invocation
	// requires that token, which avoids Argon2 on the hot path. The plaintext
	// is never stored here, only the hash. Files without the key reload with
	// an empty hash, which verification rejects (fail-closed).
	InvokeTokenSHA256 string `json:"invoke_token_sha256,omitempty"`
	// invokeTokenSum and invokeTokenSumValid cache the decoded hash: 32 bytes
	// copied by value on each Get under RLock, with no aliasing with Rotate.
	// They are unexported, so encoding/json ignores them and the on-disk
	// format does not change.
	invokeTokenSum      [32]byte
	invokeTokenSumValid bool
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	// LastError records why a function lost its usable instances: Update or
	// restart reconciliation could not restore min_instances after a
	// configuration change. The function stays registered but serves nothing
	// until the next successful Update or a reconciliation that restores the
	// minimum.
	LastError string `json:"last_error,omitempty"`
}

// Degraded reports whether the function is degraded, that is, LastError is
// not empty. LastError is the single source of truth and the degraded state
// is derived from it, so files with degraded:true and an empty last_error
// reload as not degraded.
func (fn Function) Degraded() bool { return fn.LastError != "" }
