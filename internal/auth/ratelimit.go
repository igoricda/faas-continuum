package auth

import (
	"sync"
	"time"
)

// maxRateBuckets is the hard cap on tracked IPs. Above it, Allow prunes
// expired buckets and, if still over the cap, evicts one arbitrary entry.
const maxRateBuckets = 1024

// rateBucket is the request count of one IP within the current fixed window.
type rateBucket struct {
	inicio time.Time // start of the current fixed window
	count  int       // requests counted in this window
}

// RateLimiter decides whether a request from an IP may proceed, using a fixed
// window counter per IP. The bucket map is capped at maxRateBuckets entries so
// a flood of forged source addresses cannot exhaust memory. The zero value is
// not usable: use NewRateLimiter. It is safe for concurrent use.
type RateLimiter struct {
	mu      sync.Mutex
	limit   int           // maximum requests per window per IP
	window  time.Duration // fixed window duration
	buckets map[string]*rateBucket
	now     func() time.Time // injectable clock; defaults to time.Now
}

// NewRateLimiter returns a fixed-window limiter allowing up to limit requests
// per window for each IP. limit < 1 denies every request (fail-closed) and
// window <= 0 renews the window on every request; neither panics.
func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		limit:   limit,
		window:  window,
		buckets: make(map[string]*rateBucket),
		now:     time.Now,
	}
}

// Allow reports whether a request from ip may proceed: true while the current
// window has quota left, false afterwards until the window rolls over. Each IP
// opens its own window on first sight.
func (rl *RateLimiter) Allow(ip string) bool {
	permitido, _ := rl.allow(ip)
	return permitido
}

// allow is Allow with a second result: transicao marks that this denial is the
// first after an allowed request for the same IP (allow to deny). The
// middleware uses it to log throttling once per burst instead of once per
// denied request. It is unexported so the public API stays Allow(ip) bool.
func (rl *RateLimiter) allow(ip string) (permitido, transicao bool) {
	agora := rl.now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Bounded memory: above the cap, drop expired windows first and evict one
	// arbitrary entry if the map is still full (map order does not matter
	// here). The stable worst case is the cap plus the current entry, never
	// growth proportional to the number of distinct IPs.
	if len(rl.buckets) > maxRateBuckets {
		rl.pruneLocked(agora)
		if len(rl.buckets) > maxRateBuckets {
			for chave := range rl.buckets {
				delete(rl.buckets, chave)
				break
			}
		}
	}

	b, ok := rl.buckets[ip]
	if !ok || agora.Sub(b.inicio) >= rl.window {
		// First request from this IP, or the previous window expired: start
		// over. A new bucket begins at count=1, so the next denied burst logs
		// a transition again.
		rl.buckets[ip] = &rateBucket{inicio: agora, count: 1}
		return true, false
	}

	b.count++
	permitido = b.count <= rl.limit
	transicao = !permitido && b.count == rl.limit+1
	return permitido, transicao
}

// pruneLocked removes every bucket whose window has expired relative to now
// (age >= window). It takes the instant as a parameter instead of reading the
// clock, which allows synthetic timestamps in tests. The Locked suffix follows
// the stdlib convention: the caller must hold rl.mu.
func (rl *RateLimiter) pruneLocked(now time.Time) {
	for ip, b := range rl.buckets { // deleting during range is safe in Go
		if now.Sub(b.inicio) >= rl.window {
			delete(rl.buckets, ip)
		}
	}
}
