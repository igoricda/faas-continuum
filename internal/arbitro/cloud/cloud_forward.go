package cloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// doPostEndpoint is the HTTP core shared by the connectors: it POSTs the raw
// input, requires 2xx and enforces the 8 MiB cap. It does not interpret the
// body; translating a 200 carrying an error is exclusive to
// AzureFunctionsConnector.
//
// Contract: the body sent is the client Input as a raw json.RawMessage, not the
// {"input":...} envelope of the node protocol, so the cloud endpoint must
// accept the input directly with Content-Type = input_content_type. A 2xx
// response has a raw body (the Gateway wraps non-JSON as a string); a non-2xx
// response becomes an error carrying the status in the message, with no retry
// here (the Gateway decides retries from the queue root cause).
//
// It uses the inherited ctx without creating a new timeout: the Gateway creates
// a child with an honest remaining budget. It does not parse JSON and does not
// log tokens or secrets beyond header names. The 8 MiB cap matches the gateway,
// using LimitReader with (8<<20)+1.
//
// The contract is exercised at the connector boundary in
// TestAzureFunctionsConnector_Success.
func doPostEndpoint(ctx context.Context, ep FunctionEndpoint, client *http.Client, input json.RawMessage) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.EndpointURL, bytes.NewReader(input))
	if err != nil {
		return nil, fmt.Errorf("cloud: falha ao criar requisição de fallback: %w", err)
	}
	req.Header.Set("Content-Type", ep.InputContentType)

	for _, name := range ep.RequiredHeaders {
		if v, ok := ep.HeaderValues[name]; ok {
			req.Header.Set(name, v)
		}
	}

	if ep.AuthToken != "" {
		req.Header.Set("Authorization", ep.AuthScheme+" "+ep.AuthToken)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cloud: falha ao chamar fallback %q: %w", ep.EndpointURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("cloud: fallback respondeu com status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil {
		return nil, fmt.Errorf("cloud: falha ao ler resposta do fallback: %w", err)
	}
	if int64(len(body)) > 8<<20 {
		return nil, fmt.Errorf("cloud: resposta do fallback excede o limite de 8 MiB")
	}
	return body, nil
}
