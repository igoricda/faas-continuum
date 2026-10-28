package nodeapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/igoricda/faas-continuum/internal/registry"
)

var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// resolveTimeout returns the local execution ceiling of the node: the
// timeout of the function when it is positive, and otherwise the default
// timeout of the agent, which is always positive because it is validated
// at startup.
//
// The node is deliberately unaware of the service level objective of the
// request, which never enters this calculation: the resources of a
// request that was already accepted, with the cold start, the pull, the
// creation and the start, are not wasted because of the objective. The
// budget of the objective lives only in the gateway of the arbiter, and
// the late report path depends exactly on the slow request finishing
// here instead of being killed when the client gives up.
func (s *Server) resolveTimeout(fn registry.Function) time.Duration {
	if fn.TimeoutMS > 0 {
		return time.Duration(fn.TimeoutMS) * time.Millisecond
	}
	return s.defaultTimeout
}

var errInvalidFunctionOutput = errors.New("saída da função não é JSON válido")

type socketPathCtxKey struct{}

func (s *Server) callInstance(ctx context.Context, instanceID string, input json.RawMessage) (json.RawMessage, error) {
	socketPath := s.addresser.SocketPath(instanceID)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://faas-instance/", bytes.NewReader(input))
	if err != nil {
		return nil, fmt.Errorf("falha ao montar requisição para a instância %q: %w", instanceID, err)
	}
	httpReq = httpReq.WithContext(context.WithValue(ctx, socketPathCtxKey{}, socketPath))
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := s.invokeTransport.RoundTrip(httpReq)
	if err != nil {
		return nil, fmt.Errorf("falha ao chamar a instância %q: %w", instanceID, err)
	}
	defer httpResp.Body.Close()

	buf := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufferPool.Put(buf)
	}()

	if _, err := io.Copy(buf, io.LimitReader(httpResp.Body, int64(s.maxFunctionOutputBytes)+1)); err != nil {
		return nil, fmt.Errorf("falha ao ler resposta da instância %q: %w", instanceID, err)
	}
	if buf.Len() > s.maxFunctionOutputBytes {
		return nil, fmt.Errorf("instância %q: resposta excede o limite de %d bytes", instanceID, s.maxFunctionOutputBytes)
	}
	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("instância %q respondeu com status %d: %s", instanceID, httpResp.StatusCode, buf.String())
	}
	if !json.Valid(buf.Bytes()) {
		return nil, fmt.Errorf("instância %q: %w", instanceID, errInvalidFunctionOutput)
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}
