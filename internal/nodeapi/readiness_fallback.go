//go:build !linux

package nodeapi

import "context"

// WaitReady outside Linux: without the notification mechanism it falls
// directly to the limited fallback, with the same ceilings, jitter,
// context and counting as the Linux path.
func (s *SocketReadinessChecker) WaitReady(ctx context.Context, instanceID string) (int, error) {
	path := s.Addresser.SocketPath(instanceID)
	attempts := 1
	if err := tryDial(path); err == nil {
		return attempts, nil
	} else {
		return boundedFallback(ctx, path, attempts, err, "plataforma sem inotify")
	}
}
