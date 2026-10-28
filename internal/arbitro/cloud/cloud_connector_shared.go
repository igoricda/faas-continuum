package cloud

import (
	"errors"
	"net/http"
)

// The cloud fallback connectors satisfy the gateway.RemoteExecutor port
// structurally, without importing the consumer package, so the gateway never
// references these types.

var (
	// ErrFunctionNotMapped indicates an Invoke for a function without an
	// endpoint in the per-function map (the configuration covers other
	// functions, not this one).
	ErrFunctionNotMapped = errors.New("cloud: função sem endpoint mapeado")
	// ErrNotConfigured indicates an Invoke with no configuration in the
	// store, typically a race between Configured and Invoke such as a Clear
	// through DELETE /cloud-fallback.
	ErrNotConfigured = errors.New("cloud: fallback não configurado")
	// ErrCloudAtCapacity indicates an Invoke rejected by the concurrency cap
	// (in-flight calls at or above max_instances): the cloud would queue the
	// request instead of failing, so the connector skips the call and the
	// gateway reports CLOUD_FALLBACK_FAILED with the retry inherited from
	// the root cause. It applies only when max_instances > 0 is configured;
	// 0 or absent means no cap (legacy).
	ErrCloudAtCapacity = errors.New("cloud: teto de concorrência da função atingido")
)

// maxAppErrorMessageBytes bounds the app-level error message embedded in the
// returned error: external input has a size cap, and the message travels to the
// log and to the gateway's HTTP response.
const maxAppErrorMessageBytes = 512

// defaultCloudClient builds the HTTP client shared by a connector and reused
// across all calls rather than created per request. It uses the same parameters
// as the gateway's former client, for parity with the Forward path.
func defaultCloudClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			Proxy:             nil,
		},
		Timeout: 0,
	}
}
