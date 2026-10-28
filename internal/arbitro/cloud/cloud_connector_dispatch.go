package cloud

import (
	"context"
	"encoding/json"
)

// Connector dispatches by the format in effect: it reads the store on every
// Invoke, so a PUT that switches the format (flat <-> per function) takes
// effect immediately without rebuilding the gateway. The main package injects
// it into the gateway port.
type Connector struct {
	store   *CloudStore
	azure   *AzureFunctionsConnector
	generic *GenericHTTPConnector
}

// NewConnector creates the dispatcher over the store with a single http.Client
// shared by both connectors.
func NewConnector(store *CloudStore) *Connector {
	client := defaultCloudClient()
	return &Connector{
		store:   store,
		azure:   NewAzureFunctionsConnector(store, client),
		generic: NewGenericHTTPConnector(store, client),
	}
}

// Configured reports whether any fallback configuration is in effect.
func (c *Connector) Configured() bool {
	if c == nil || c.store == nil {
		return false
	}
	_, ok := c.store.Get()
	return ok
}

// Invoke delegates to the connector for the format in effect.
func (c *Connector) Invoke(ctx context.Context, fn string, input json.RawMessage) ([]byte, error) {
	if c == nil || c.store == nil {
		return nil, ErrNotConfigured
	}
	cfg, ok := c.store.Get()
	if !ok {
		return nil, ErrNotConfigured
	}
	if cfg.Connector == ConnectorAzureFunctions {
		return c.azure.Invoke(ctx, fn, input)
	}
	return c.generic.Invoke(ctx, fn, input)
}

// EstimateCold delegates to the Azure connector; in the flat format it returns
// false through that connector's own check, so dispatch never reports a wrong
// estimate.
func (c *Connector) EstimateCold(fn string) bool {
	if c == nil || c.azure == nil {
		return false
	}
	return c.azure.EstimateCold(fn)
}
