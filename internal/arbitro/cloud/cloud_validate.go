package cloud

import (
	"fmt"
	"mime"
	"net/url"
	"time"
)

func validateCloudConfig(cfg CloudFallbackConfig) error {
	// Reaper per connector (top level, valid for both formats): a Go
	// duration > 0 when present, fail-closed on Set and on load.
	if cfg.ReaperTimeout != "" {
		if d, err := time.ParseDuration(cfg.ReaperTimeout); err != nil || d <= 0 {
			return fmt.Errorf("%w: reaper_timeout inválido (use duração Go >0, ex. \"5m\")", ErrInvalidCloudConfig)
		}
	}
	// Format detection: the legacy flat format applies only when the
	// connector is absent and there is no map; everything else is the
	// per-function format.
	if cfg.Connector == "" && cfg.Functions == nil {
		return validateEndpoint(FunctionEndpoint{
			EndpointURL:      cfg.EndpointURL,
			InputContentType: cfg.InputContentType,
			AuthScheme:       cfg.AuthScheme,
		})
	}
	if cfg.Connector != ConnectorAzureFunctions && cfg.Connector != ConnectorGenericHTTP {
		return fmt.Errorf("%w: connector %q desconhecido (use %q ou %q)", ErrInvalidCloudConfig, cfg.Connector, ConnectorAzureFunctions, ConnectorGenericHTTP)
	}
	if cfg.Connector == ConnectorGenericHTTP {
		// Explicit name for the flat format: it requires a single endpoint
		// and no map.
		if cfg.Functions != nil {
			return fmt.Errorf("%w: connector %q não aceita mapa functions", ErrInvalidCloudConfig, ConnectorGenericHTTP)
		}
		return validateEndpoint(FunctionEndpoint{
			EndpointURL:      cfg.EndpointURL,
			InputContentType: cfg.InputContentType,
			AuthScheme:       cfg.AuthScheme,
		})
	}
	// azure-functions: a non-empty per-function map is required; top-level
	// flat fields are ignored when the map is present.
	if len(cfg.Functions) == 0 {
		return fmt.Errorf("%w: connector %q exige functions não-vazio", ErrInvalidCloudConfig, ConnectorAzureFunctions)
	}
	for fn, ep := range cfg.Functions {
		if fn == "" {
			return fmt.Errorf("%w: functions com nome de função vazio", ErrInvalidCloudConfig)
		}
		if err := validateEndpoint(ep); err != nil {
			return fmt.Errorf("%w: functions[%q]: %v", ErrInvalidCloudConfig, fn, err)
		}
	}
	return nil
}

// validateEndpoint validates one endpoint with a single rule shared by the flat
// format and by each map entry:
//   - endpoint_url through url.Parse, with scheme http or https and host != ""
//   - input_content_type through mime.ParseMediaType, and != ""
//   - auth_scheme != "" always (anonymous access uses "none" with the token
//     omitted)
//
// Validation errors return ErrInvalidCloudConfig (wrapped with %w for
// errors.Is).
func validateEndpoint(ep FunctionEndpoint) error {
	if ep.EndpointURL == "" {
		return fmt.Errorf("%w: endpoint_url vazio", ErrInvalidCloudConfig)
	}
	u, err := url.Parse(ep.EndpointURL)
	if err != nil {
		return fmt.Errorf("%w: endpoint_url inválido: %v", ErrInvalidCloudConfig, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: endpoint_url scheme deve ser http ou https", ErrInvalidCloudConfig)
	}
	if u.Host == "" {
		return fmt.Errorf("%w: endpoint_url sem host", ErrInvalidCloudConfig)
	}
	if ep.InputContentType == "" {
		return fmt.Errorf("%w: input_content_type vazio", ErrInvalidCloudConfig)
	}
	if _, _, err := mime.ParseMediaType(ep.InputContentType); err != nil {
		return fmt.Errorf("%w: input_content_type inválido: %v", ErrInvalidCloudConfig, err)
	}
	if ep.AuthScheme == "" {
		return fmt.Errorf("%w: auth_scheme vazio", ErrInvalidCloudConfig)
	}
	// Cold-start estimate fields: optional, but valid when present
	// (fail-closed).
	if ep.MaxInstances < 0 {
		return fmt.Errorf("%w: max_instances negativo", ErrInvalidCloudConfig)
	}
	return nil
}
