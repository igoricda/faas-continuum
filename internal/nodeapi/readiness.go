package nodeapi

import (
	"context"
	"log"
	"math/rand"
	"net"
	"time"
)

// ReadinessChecker abstracts the wait for the readiness of the Unix
// socket of an instance. WaitReady returns the number of dial attempts it
// performed together with the error, which is nil on success, so the
// number can be observed in the log of the invocation.
type ReadinessChecker interface {
	WaitReady(ctx context.Context, instanceID string) (int, error)
}

// SocketReadinessChecker is the production implementation of the
// readiness checker: it waits for an event, with a notification
// mechanism on Linux and a limited fallback elsewhere.
type SocketReadinessChecker struct {
	Addresser InstanceAddresser
}

var unixDial = net.DialTimeout

// readinessSocketName is the name of the Unix socket file inside the
// directory of the instance in production. It matches the name used by
// the container runtime package. The duplication is deliberate and
// documented: the node API cannot import the container runtime package,
// which keeps the dependency inverted, and the exception is the only
// place where the name is shared.
const readinessSocketName = "handle.sock"

// maxReadinessDials is the total ceiling of the dial attempts of a wait,
// in every path, both the event-driven one and the fallback: one
// immediate attempt, one confirmation and at most two redials for the
// window between the bind and the listen. No path dials beyond that, and
// the number of attempts never grows with the duration of the cold
// start.
const maxReadinessDials = 4

// unixDialTimeout is the deadline of each individual dial, since the
// socket may exist while the listen has not come up yet: a short dial
// fails fast and is counted.
const unixDialTimeout = 50 * time.Millisecond

// readinessRedialDelays covers the window between the bind and the listen
// after an event with a matching name: the creation event arrives at the
// bind, but the dial only succeeds after the listen. There are at most
// two redials in the same context, never a pure wait for an event, and
// they are always subject to the total ceiling of the dials.
var readinessRedialDelays = []time.Duration{20 * time.Millisecond, 50 * time.Millisecond}

// readinessFallbackDelays is the cadence of the limited fallback, used
// when the watcher is unavailable or invalidated: a ceiling of 50, 100 and
// 200 milliseconds plus a jitter of up to ten percent, in the same
// context, with the dials counted in the same counter.
var readinessFallbackDelays = []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}

// tryDial performs one dial on the socket and closes the connection on
// success. It returns nothing when the socket already accepts
// connections, which covers the warm case and the case of a listen that
// already came up.
func tryDial(path string) error {
	conn, err := unixDial("unix", path, unixDialTimeout)
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

// mapWaitReadyErr preserves the previous mapping: an expired context
// becomes a deadline error, which is the gateway timeout of the
// invocation, any other cancellation becomes the error of the context,
// which is the failure of the wait, and without an expired context the
// last error of the dial is returned.
func mapWaitReadyErr(ctx context.Context, attempts int, lastErr error) (int, error) {
	if ctx.Err() == context.DeadlineExceeded {
		return attempts, context.DeadlineExceeded
	}
	if ctx.Err() != nil {
		return attempts, ctx.Err()
	}
	return attempts, lastErr
}

// sleepCtx sleeps for the given duration, with a jitter of up to ten
// percent, or returns early when the context expires. The context governs
// everything, with no fixed timer on the happy path.
func sleepCtx(ctx context.Context, d time.Duration) error {
	jitter := time.Duration(float64(d) * 0.1 * rand.Float64())
	t, stop := context.WithTimeout(ctx, d+jitter)
	defer stop()
	<-t.Done()
	return ctx.Err()
}

// boundedFallback is the only fallback allowed: the dials are spaced by
// the fallback delays until the context expires or the total ceiling of
// the dials is reached. The count of the attempts enters with the
// attempts already consumed by the immediate attempt, the check and the
// confirmation, and every dial from here increments that same counter.
func boundedFallback(ctx context.Context, path string, attempts int, lastErr error, reason string) (int, error) {
	log.Printf("nodeapi: inotify unavailable (%s), using bounded fallback", reason)
	for _, d := range readinessFallbackDelays {
		if attempts >= maxReadinessDials {
			return attempts, lastErr
		}
		if err := sleepCtx(ctx, d); err != nil {
			return mapWaitReadyErr(ctx, attempts, lastErr)
		}
		attempts++
		if err := tryDial(path); err == nil {
			return attempts, nil
		} else {
			lastErr = err
		}
	}
	if err := ctx.Err(); err != nil {
		return mapWaitReadyErr(ctx, attempts, lastErr)
	}
	return attempts, lastErr
}

func (s *Server) waitReady(ctx context.Context, instanceID string) (int, error) {
	if s.readiness != nil {
		return s.readiness.WaitReady(ctx, instanceID)
	}
	if s.addresser == nil {
		return 0, nil
	}
	checker := &SocketReadinessChecker{Addresser: s.addresser}
	return checker.WaitReady(ctx, instanceID)
}
