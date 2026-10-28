package nodeapi

import (
	"net/http"

	"github.com/igoricda/faas-continuum/internal/registry"
)

// The cheap authentication of the invocation verifies the token of the
// function with a hash and a constant-time comparison, which costs
// microseconds and keeps the expensive key derivation out of the hot path
// of the edge.
//
// The management credential is never accepted here and there is no
// fallback: whoever invokes uses the token of the function.
//
// The mandatory order in the handlers is the format of the header, the
// lookup of the function, the binding with the function, the decoding of
// the body and the acquisition. A missing or malformed header is
// rejected before the lookup, so a nonexistent function cannot be told
// apart from a request without a credential.
func checkInvokeAuthFormat(r *http.Request) bool {
	h := r.Header.Get("Authorization")
	return h != "" && len(h) <= registry.MaxPresentedInvokeTokenBytes
}

func checkInvokeAuth(r *http.Request, fn registry.Function) bool {
	return fn.VerifyPresentedToken(r.Header.Get("Authorization"))
}
