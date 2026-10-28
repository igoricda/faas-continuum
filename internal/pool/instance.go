package pool

import (
	"regexp"
	"strconv"
)

// instance is a container instance tracked by the pool.
type instance struct {
	id   string
	busy bool
}

type acquireResult struct {
	err error
}

// instanceIDPattern matches the "-<index>" suffix that EnsureMinInstances
// and Acquire use to build an instance identifier from a function name.
var instanceIDPattern = regexp.MustCompile(`^(.+)-(\d+)$`)

// ParseInstanceID undoes the naming convention used by
// EnsureMinInstances and Acquire, which is the function name, a dash and an
// index.
//
// The split is unambiguous: the part after the last dash must be digits
// only, and digits contain no dash, so at most one split is valid for an
// identifier. The parser alone never confuses an identifier with another
// owner; a name that looks like an identifier, such as "fn-1" next to
// "fn", is resolved through the explicit ownership mapping of
// ResolveOwner.
func ParseInstanceID(instanceID string) (functionName string, index int, ok bool) {
	m := instanceIDPattern.FindStringSubmatch(instanceID)
	if m == nil {
		return "", 0, false
	}
	idx, err := strconv.Atoi(m[2])
	if err != nil {
		return "", 0, false
	}
	return m[1], idx, true
}

// ResolveOwner maps an instance identifier to its explicit owner: the
// parsed owner counts only when it is registered. Without a registered
// owner the identifier is an orphan and is never adopted by the wrong
// function, because the parser and the registry decide together in a single
// place.
func ResolveOwner(instanceID string, registered func(string) bool) (functionName string, ok bool) {
	fn, _, valid := ParseInstanceID(instanceID)
	if !valid || !registered(fn) {
		return "", false
	}
	return fn, true
}

func (m *Manager) nextIndexLockedOnShard(s *shard, functionName string) int {
	next := 0
	for _, inst := range s.instances[functionName] {
		if fn, idx, ok := ParseInstanceID(inst.id); ok && fn == functionName && idx >= next {
			next = idx + 1
		}
	}
	m.globalMu.RLock()
	for id := range m.instanceByID {
		if fn, idx, ok := ParseInstanceID(id); ok && fn == functionName && idx >= next {
			next = idx + 1
		}
	}
	m.globalMu.RUnlock()
	return next
}
