package auth

import (
	"log"
	"net"
	"net/http"
)

// Error envelopes for the protected routes; the 401 and 429 bodies share the
// same shape. The 401 text is part of the established wire contract and must
// not change. The gateway reads the retryable field.
const (
	msgUnauthorized = `{"code":"UNAUTHORIZED","message":"credencial inválida ou ausente","retryable":false}`
	msgTooManyReqs  = `{"code":"TOO_MANY_REQUESTS","message":"muitas tentativas","retryable":true}`
)

// RequireAuthWithLimiter wraps next with authentication and per-IP rate
// limiting. The limiter is shared by all routes of the same server, so
// counters are per server rather than per route. It must be created once in
// the server constructor and injected here, never per request.
func RequireAuthWithLimiter(cred Credential, limiter *RateLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := ipDeRemoteAddr(r.RemoteAddr)

		if permitido, transicao := limiter.allow(ip); !permitido {
			if transicao {
				log.Printf("auth: limite de requisições excedido ip=%s rota=%s", ip, r.URL.Path)
			}
			http.Error(w, msgTooManyReqs, http.StatusTooManyRequests)
			return
		}

		header := r.Header.Get(cred.HeaderName)
		if header == "" || len(header) > MaxPresentedTokenBytes || !VerifyToken(header, cred.TokenHash) {
			log.Printf("auth: falha de autenticação ip=%s rota=%s", ip, r.URL.Path)
			http.Error(w, msgUnauthorized, http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ipDeRemoteAddr strips the port from r.RemoteAddr with net.SplitHostPort and
// falls back to the raw string for unexpected formats such as Unix sockets.
// X-Forwarded-For is ignored: there is no trusted proxy in front of the agent
// and the header is client-controlled.
func ipDeRemoteAddr(remoteAddr string) string {
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return ip
}
