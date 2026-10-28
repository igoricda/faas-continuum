package cloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// GenericHTTPConnector replicates the behaviour of Forward: a single global
// endpoint, without interpreting the body. It serves the legacy flat format and
// ignores the function name.
type GenericHTTPConnector struct {
	store  *CloudStore
	client *http.Client
}

// NewGenericHTTPConnector creates the connector over the store, reading it live
// on every Invoke so no I/O runs under a lock. A nil client selects the default
// client.
func NewGenericHTTPConnector(store *CloudStore, client *http.Client) *GenericHTTPConnector {
	if client == nil {
		client = defaultCloudClient()
	}
	return &GenericHTTPConnector{store: store, client: client}
}

// Configured reports whether a legacy (flat) configuration is in effect.
func (c *GenericHTTPConnector) Configured() bool {
	if c == nil || c.store == nil {
		return false
	}
	cfg, ok := c.store.Get()
	return ok && cfg.Connector != ConnectorAzureFunctions
}

// Invoke POSTs the raw input to the single endpoint (fn is ignored for backward
// compatibility). With a per-function configuration in effect it returns an
// error instead of guessing an endpoint; format dispatch is the Connector's job.
func (c *GenericHTTPConnector) Invoke(ctx context.Context, fn string, input json.RawMessage) ([]byte, error) {
	if c == nil || c.store == nil {
		return nil, ErrNotConfigured
	}
	cfg, ok := c.store.Get()
	if !ok {
		return nil, ErrNotConfigured
	}
	if cfg.Connector == ConnectorAzureFunctions {
		return nil, fmt.Errorf("cloud: config por função requer AzureFunctionsConnector: %w", ErrFunctionNotMapped)
	}
	return doPostEndpoint(ctx, FunctionEndpoint{
		EndpointURL:      cfg.EndpointURL,
		InputContentType: cfg.InputContentType,
		AuthScheme:       cfg.AuthScheme,
		AuthToken:        cfg.AuthToken,
		HeaderValues:     cfg.HeaderValues,
		RequiredHeaders:  cfg.RequiredHeaders,
	}, c.client, input)
}

// EstimateCold always returns false for the generic connector, matching the
// legacy path, which always reported cold_start:false; estimation belongs to
// the per-function format.
func (c *GenericHTTPConnector) EstimateCold(fn string) bool {
	return false
}
