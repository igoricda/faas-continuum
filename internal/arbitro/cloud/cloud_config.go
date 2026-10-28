// Package cloud implements the Arbitrator's cloud fallback: persistence of the
// global fallback configuration and forwarding of invocations to remote
// endpoints. See CloudStore and the connectors in this package.
package cloud

import "errors"

// ConnectorAzureFunctions selects the per-function endpoint map.
//
// An empty connector with no functions selects the legacy flat format;
// ConnectorGenericHTTP selects the flat format explicitly.
const (
	ConnectorAzureFunctions = "azure-functions"
	ConnectorGenericHTTP    = "generic-http"
)

// FunctionEndpoint describes the cloud endpoint of a single function. Its
// fields match the legacy flat format, and validation is the same, applied per
// map entry (see validateEndpoint).
//
// MaxInstances feeds the cold-start estimate of AzureFunctionsConnector; when
// absent the value is unknown and the occupancy rule is disabled. The reaper
// timeout is per connector (top-level field) because the scale-to-zero idle
// period belongs to the platform, not to the function.
//
// Fields are ordered from largest to smallest for 8-byte alignment.
type FunctionEndpoint struct {
	EndpointURL      string            `json:"endpoint_url"`
	InputContentType string            `json:"input_content_type"`
	AuthScheme       string            `json:"auth_scheme"`
	AuthToken        string            `json:"auth_token,omitempty"`
	HeaderValues     map[string]string `json:"header_values,omitempty"`
	RequiredHeaders  []string          `json:"required_headers,omitempty"`
	MaxInstances     int               `json:"max_instances,omitempty"`
}

// CloudFallbackConfig defines the cloud fallback configuration.
//
// Per-function format: connector plus functions. Legacy flat format: only the
// single-endpoint fields, with no connector. Detection: Connector == "" and
// Functions == nil select the legacy format; any other arrangement is
// validated as the per-function format (fail-closed, so a mix without a
// connector is rejected rather than guessed).
//
// The header_values and auth_token fields are carried in addition to the
// documented schema because the endpoint cannot be invoked without them.
//
// Fields are ordered from largest to smallest for 8-byte alignment.
type CloudFallbackConfig struct {
	Connector string                      `json:"connector,omitempty"`
	Functions map[string]FunctionEndpoint `json:"functions,omitempty"`
	// ReaperTimeout is per connector: the idle period after which the
	// platform tears the container down (scale-to-zero), as a Go duration
	// (for example "5m"). An invalid or non-positive duration is rejected
	// by Set; absent means unknown, which disables the idle rule (cold
	// stays false on that criterion).
	ReaperTimeout    string            `json:"reaper_timeout,omitempty"`
	EndpointURL      string            `json:"endpoint_url,omitempty"`
	InputContentType string            `json:"input_content_type,omitempty"`
	AuthScheme       string            `json:"auth_scheme,omitempty"`
	AuthToken        string            `json:"auth_token,omitempty"`
	HeaderValues     map[string]string `json:"header_values,omitempty"`
	RequiredHeaders  []string          `json:"required_headers,omitempty"`
}

// ErrInvalidCloudConfig is the sentinel error for cloud fallback validation.
var ErrInvalidCloudConfig = errors.New("cloud: configuração de fallback inválida")
