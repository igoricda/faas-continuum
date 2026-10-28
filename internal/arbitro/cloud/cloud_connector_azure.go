package cloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// AzureFunctionsConnector routes each function to its own endpoint: it reads
// the function-to-endpoint map from the store on every Invoke, so a PUT
// replaces endpoints without rewiring. After a 2xx response it translates
// {"status":"error"} (an HTTP 200 returned by the app) into an error.
type AzureFunctionsConnector struct {
	store  *CloudStore
	client *http.Client
	// TranslateAppError enables translating an app-level 200 error into an
	// error (default true; GenericHTTPConnector never translates, matching
	// Forward).
	TranslateAppError bool
	// cold estimates cold starts per function.
	cold *coldTracker
}

// NewAzureFunctionsConnector creates the connector over the store, reading it
// live on every Invoke so no I/O runs under a lock. A nil client selects the
// default client.
func NewAzureFunctionsConnector(store *CloudStore, client *http.Client) *AzureFunctionsConnector {
	if client == nil {
		client = defaultCloudClient()
	}
	return &AzureFunctionsConnector{store: store, client: client, TranslateAppError: true, cold: newColdTracker()}
}

// Configured reports whether a per-function configuration (azure-functions) is
// in effect.
func (c *AzureFunctionsConnector) Configured() bool {
	if c == nil || c.store == nil {
		return false
	}
	cfg, ok := c.store.Get()
	return ok && cfg.Connector == ConnectorAzureFunctions
}

// Invoke POSTs the raw input (no {"input":...} envelope) to the function
// endpoint with the configured Content-Type, required_headers/header_values,
// and Authorization only when a token is set (same semantics as Forward).
func (c *AzureFunctionsConnector) Invoke(ctx context.Context, fn string, input json.RawMessage) ([]byte, error) {
	if c == nil || c.store == nil {
		return nil, ErrNotConfigured
	}
	cfg, ok := c.store.Get()
	if !ok {
		return nil, ErrNotConfigured
	}
	if cfg.Connector != ConnectorAzureFunctions {
		return nil, fmt.Errorf("cloud: config plana requer GenericHTTPConnector: %w", ErrFunctionNotMapped)
	}
	ep, ok := cfg.Functions[fn]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrFunctionNotMapped, fn)
	}
	// Admission cap: do not exceed max_instances, so reject atomically
	// before any I/O; the POST runs outside the lock.
	admitted, inflight := c.cold.reserve(fn, ep.MaxInstances, cfg.Functions)
	if !admitted {
		return nil, fmt.Errorf("%w: %q com %d em voo (max_instances %d)", ErrCloudAtCapacity, fn, inflight, ep.MaxInstances)
	}
	body, err := doPostEndpoint(ctx, ep, c.client, input)
	gotResponse := err == nil // only 2xx means a live container (non-2xx and transport errors do not renew lastEnd)
	c.cold.release(fn, gotResponse)
	if err != nil {
		return nil, err
	}
	if c.TranslateAppError {
		if msg, isErr := appErrorMessage(body); isErr {
			return nil, fmt.Errorf("cloud: azure functions retornou status error: %s", msg)
		}
	}
	return body, nil
}

// EstimateCold estimates whether the next call to fn would start cold (an
// optional port capability, read by the gateway before Invoke). The rule
// returns true when the idleness since the last 2xx response exceeds
// reaper_timeout (the container was reaped) or when in-flight calls reached
// max_instances (no warm slot is free). Without evidence (never seen, missing
// fields) it returns false. It is a pure observation and takes no reservation.
func (c *AzureFunctionsConnector) EstimateCold(fn string) bool {
	if c == nil || c.store == nil {
		return false
	}
	cfg, ok := c.store.Get()
	if !ok || cfg.Connector != ConnectorAzureFunctions {
		return false
	}
	ep, ok := cfg.Functions[fn]
	if !ok {
		return false
	}
	return c.cold.estimate(fn, ep.MaxInstances, parseReaper(cfg.ReaperTimeout), cfg.Functions)
}

// appErrorMessage detects the app-level error: HTTP 200 with
// {"status":"error"}. A non-JSON body or a status other than error yields
// (.., false) and the body is treated as success (the gateway wraps non-JSON as
// a string).
func appErrorMessage(body []byte) (string, bool) {
	var v struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return "", false
	}
	if v.Status != "error" {
		return "", false
	}
	msg := v.Message
	if len(msg) > maxAppErrorMessageBytes {
		msg = msg[:maxAppErrorMessageBytes] + "…"
	}
	return msg, true
}
