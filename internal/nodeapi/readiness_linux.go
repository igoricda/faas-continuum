//go:build linux

package nodeapi

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// WaitReady waits for the readiness of the Unix socket, driven by the
// events of the file system.
//
// The flow is an immediate dial, a miss, the arming of a watcher, a
// check-watch-check sequence whose second dial closes the race between
// the check and the watch, and then a loop of polling and reading with a
// short deadline in the same context. For each event with a matching
// name there is one confirmation dial and at most two redials for the
// window between the bind and the listen. The total ceiling of the dials
// applies to every path, and the context governs everything.
func (s *SocketReadinessChecker) WaitReady(ctx context.Context, instanceID string) (int, error) {
	path := s.Addresser.SocketPath(instanceID)
	attempts := 0
	var lastErr error

	// The immediate dial: a warm instance returns here with a single
	// attempt and without a watcher.
	attempts++
	if err := tryDial(path); err == nil {
		return attempts, nil
	} else {
		lastErr = err
	}

	// The directory of the watcher, in a fail-closed way: an error
	// while resolving it, an empty path or a traversal attempt, returns
	// before any watcher is created and never falls back to the root or
	// to a temporary directory.
	watchDir, err := s.Addresser.SocketWatchDir(instanceID)
	if err != nil {
		return attempts, err
	}
	if watchDir == "" || watchDir == "/" || watchDir == "/tmp" {
		return attempts, fmt.Errorf("nodeapi: SocketWatchDir inválido %q para %q (fail-closed)", watchDir, instanceID)
	}

	// The watcher is armed, and any error while creating it or adding
	// the watch, whether the path does not exist, the table of watches is
	// full, there are too many open files or the device is not
	// supported, falls to the single limited fallback.
	w, err := newInotifyWatcher(watchDir)
	if err != nil {
		return boundedFallback(ctx, path, attempts, lastErr, err.Error())
	}
	defer w.Close()

	// The check-watch-check sequence closes the real race between the
	// dial and the arming of the watch, since the second dial closes
	// the window in which the socket was bound before the watch
	// existed.
	attempts++
	if derr := tryDial(path); derr == nil {
		return attempts, nil
	} else {
		lastErr = derr
	}

	// The expected name of the file: in production it is the name of the
	// socket, while the second clause keeps the test doubles with a
	// per-instance name working. Any other name in the directory
	// continues to be ignored.
	wantBase := filepath.Base(path)
	for {
		switch out := w.next(ctx, wantBase); out.outcome {
		case waitCtxDone:
			return mapWaitReadyErr(ctx, attempts, lastErr)
		case waitInvalid:
			return boundedFallback(ctx, path, attempts, lastErr, out.reason)
		case waitOther:
			continue
		case waitMatch:
			// The confirmation dial and the redials for the window
			// between the bind and the listen, since the creation event
			// arrives at the bind and the dial only succeeds at the
			// listen. From here on there is never a pure wait for an
			// event.
			attempts++
			if derr := tryDial(path); derr == nil {
				return attempts, nil
			} else {
				lastErr = derr
			}
			for _, d := range readinessRedialDelays {
				if attempts >= maxReadinessDials {
					return attempts, lastErr
				}
				if serr := sleepCtx(ctx, d); serr != nil {
					return mapWaitReadyErr(ctx, attempts, lastErr)
				}
				attempts++
				if derr := tryDial(path); derr == nil {
					return attempts, nil
				} else {
					lastErr = derr
				}
			}
			return attempts, lastErr
		}
	}
}

// inotifyPollIntervalMs is the deadline of the poll: it is short enough
// to wake up quickly on the end of the context and short enough to avoid
// an auxiliary goroutine, since the loop runs on the goroutine of the
// caller and the descriptor is released when the function returns.
const inotifyPollIntervalMs = 20

// inotifyReadBuf holds a coalesced batch of events without the churn of
// a partial framing.
const inotifyReadBuf = 4096

// The mask of the watcher is deliberate: the creation and the move events
// signal the bind of the watchdog, and the deletion of the watched
// directory invalidates the watch when the directory is removed during
// the wait. The attribute events are excluded on purpose, because the
// preparation of the socket directory generates an irrelevant change of
// mode.
const inotifyWatchMask = unix.IN_CREATE | unix.IN_MOVED_TO | unix.IN_DELETE_SELF

type inotifyWatcher struct {
	fd      int
	pending []byte // resto parcial entre reads (framing nunca se perde)
}

func newInotifyWatcher(dir string) (*inotifyWatcher, error) {
	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("inotify init: %w", err)
	}
	if _, err := unix.InotifyAddWatch(fd, dir, inotifyWatchMask); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("inotify add watch: %w", err)
	}
	return &inotifyWatcher{fd: fd}, nil
}

func (w *inotifyWatcher) Close() error { return unix.Close(w.fd) }

type waitOutcome int

const (
	waitMatch   waitOutcome = iota // an event with the name of the socket
	waitOther                      // another file, ignored, or an empty batch
	waitInvalid                    // the watch was invalidated, so the single fallback applies
	waitCtxDone                    // the context expired or was cancelled
)

type waitResult struct {
	outcome waitOutcome
	reason  string // only for the invalid outcome, which goes to the contracted log
}

// next blocks until a classifiable event or the end of the context, with
// no pure blocking read: a read without a poll and a deadline would block
// forever after the removal of the directory, since the event that
// invalidates the watch only reaches the reader.
func (w *inotifyWatcher) next(ctx context.Context, wantBase string) waitResult {
	pollFds := []unix.PollFd{{Fd: int32(w.fd), Events: unix.POLLIN}}
	for {
		if err := ctx.Err(); err != nil {
			return waitResult{outcome: waitCtxDone}
		}
		n, err := unix.Poll(pollFds, inotifyPollIntervalMs)
		if err == unix.EINTR {
			continue // spike: EINTR visto 1x — repetir poll/read
		}
		if err != nil {
			return waitResult{outcome: waitInvalid, reason: err.Error()}
		}
		if n == 0 {
			continue // the poll timed out: recheck the context without a dial
		}
		matched, invalid := w.drain(wantBase)
		if invalid != "" {
			return waitResult{outcome: waitInvalid, reason: invalid}
		}
		if matched {
			return waitResult{outcome: waitMatch}
		}
	}
}

// drain empties the descriptor, without blocking until it would block,
// and classifies the batch: an invalidation takes precedence over a
// match, because with a dead watch the events are not reliable, so it
// resynchronizes through the fallback and never waits purely.
func (w *inotifyWatcher) drain(wantBase string) (matched bool, invalid string) {
	var buf [inotifyReadBuf]byte
	for {
		n, err := unix.Read(w.fd, buf[:])
		if err == unix.EINTR {
			continue
		}
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			break
		}
		if err != nil {
			return false, err.Error()
		}
		if n == 0 {
			break
		}
		w.pending = append(w.pending, buf[:n]...)
	}
	// The framing of the events: a header of sixteen bytes followed by a
	// name padded with nulls. The fields are little-endian on the usual
	// architectures. A partial remainder waits for the next read.
	for len(w.pending) >= unix.SizeofInotifyEvent {
		ln := int(binary.LittleEndian.Uint32(w.pending[12:16]))
		if len(w.pending) < unix.SizeofInotifyEvent+ln {
			break
		}
		mask := binary.LittleEndian.Uint32(w.pending[4:8])
		name := strings.TrimRight(string(w.pending[unix.SizeofInotifyEvent:unix.SizeofInotifyEvent+ln]), "\x00")
		w.pending = w.pending[unix.SizeofInotifyEvent+ln:]
		switch {
		case mask&(unix.IN_IGNORED|unix.IN_Q_OVERFLOW|unix.IN_DELETE_SELF) != 0:
			return false, fmt.Sprintf("watch invalidado (mask=%#x nome=%q)", mask, name)
		case mask&(unix.IN_CREATE|unix.IN_MOVED_TO) != 0 &&
			(name == readinessSocketName || name == wantBase):
			matched = true
		}
	}
	return matched, ""
}
