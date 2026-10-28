package gateway

// Meter of the requests in flight with a cost weight: it keeps, per node
// and per pair of node and requested function, how many invocations the
// arbiter dispatched and did not conclude yet, each with the expected
// processor cost captured in the dispatch and the instant of the increment.
//
// The self-learning selector reads this same meter through function fields,
// since the gateway package already imports the selection package and the
// reverse direction would create an import cycle. That is why the meter is
// an exported and standalone type here, with no reference to the selector.
//
// It is protected by a single mutex, and the hot paths are short and do not
// do input or output. The collections are bounded: the map of nodes is
// limited by the maximum number of nodes, and the functions and the entries
// are deleted when the count reaches zero, so the size follows the live
// concurrency and never the total history of invocations.

import (
	"strings"
	"sync"
	"time"
)

// inflightEntry is a single request in flight tracked by the meter: the
// requested function, which is never the resolved concrete variant, the
// expected processor cost and the instant of the increment, for the filter
// by reception time.
type inflightEntry struct {
	function string
	cost     float64
	at       time.Time
}

// InflightGauge tracks the requests in flight per node and per function.
// The zero value is not usable, so the constructor is required. Every
// method is safe on a nil receiver, so a gateway without a meter records an
// unknown number of requests in flight and the estimator starts
// neutral.
type InflightGauge struct {
	mu      sync.Mutex
	node    map[string]int
	fn      map[string]int
	entries map[string][]inflightEntry
}

// NewInflightGauge creates an empty meter.
func NewInflightGauge() *InflightGauge {
	return &InflightGauge{
		node:    make(map[string]int),
		fn:      make(map[string]int),
		entries: make(map[string][]inflightEntry),
	}
}

// fnKey composes the internal key of the node and the function with a null
// separator that cannot collide, since the identifier of the node never
// contains a null byte.
func fnKey(nodeID, function string) string {
	return nodeID + "\x00" + function
}

// Inc records a single request in flight in the node and returns the count
// after the increment at the level of the node and not at the level of the
// function, which is the value expected by the classes of the selector.
//
// The registered function is the one requested by the client and never the
// concrete variant resolved by the node, otherwise the same logical
// invocation would fragment the map under different names. The cost is the
// expected processor cost captured in the dispatch and the instant is the
// one of the increment.
//
// A nil receiver returns zero and does nothing, so a gateway without a
// meter counts nothing.
func (g *InflightGauge) Inc(nodeID, function string, cost float64, at time.Time) (k int) {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.node[nodeID]++
	k = g.node[nodeID]
	key := fnKey(nodeID, function)
	g.fn[key]++
	g.entries[key] = append(g.entries[key], inflightEntry{function: function, cost: cost, at: at})
	return k
}

// Dec records the conclusion of a single request in flight of the node and
// of the requested function. The removal happens by the identity of the
// dispatch: the entry whose instant matches the argument is removed, which
// is the same instant passed to the increment, so the cost of the attempt
// that ended is the one that leaves and the cost of the other live entries
// of the key is preserved for the sum. The comparison uses the equality of
// the time values, so two entries of the same instant are indistinguishable
// and the first one matches. When no instant matches, from a collision of
// the clock or of a fake clock, or from an entry already removed, the
// fallback drains the oldest entry, so that the key never leaks a stale
// count. Empty maps and slices are deleted, which bounds the meter by the
// live concurrency. A nil receiver does nothing.
func (g *InflightGauge) Dec(nodeID, function string, at time.Time) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if n, ok := g.node[nodeID]; ok {
		if n <= 1 {
			delete(g.node, nodeID)
		} else {
			g.node[nodeID] = n - 1
		}
	}
	key := fnKey(nodeID, function)
	if n, ok := g.fn[key]; ok {
		if n <= 1 {
			delete(g.fn, key)
		} else {
			g.fn[key] = n - 1
		}
	}
	ents := g.entries[key]
	if len(ents) == 0 {
		return
	}
	// The identity matches the instant stamped at the increment. Without a
	// match the index zero is drained as a fallback, which is the oldest
	// entry, since the increment appends.
	i := 0
	for j := range ents {
		if ents[j].at.Equal(at) {
			i = j
			break
		}
	}
	// The copy reallocates, which avoids holding the old array while the key lives.
	rest := make([]inflightEntry, 0, len(ents)-1)
	rest = append(rest, ents[:i]...)
	rest = append(rest, ents[i+1:]...)
	if len(rest) == 0 {
		delete(g.entries, key)
	} else {
		g.entries[key] = rest
	}
}

// NodeK returns the count of requests in flight in the node, and zero when
// the node is absent. A nil receiver returns zero.
func (g *InflightGauge) NodeK(nodeID string) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.node[nodeID]
}

// FnK returns the count of requests in flight of the pair of the node and
// the requested function. It is an accessor for the tests and for the
// observability, since the selector uses the count at the level of the
// node. A nil receiver returns zero.
func (g *InflightGauge) FnK(nodeID, function string) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fn[fnKey(nodeID, function)]
}

// NodeCostSince sums the expected cost of the requests in flight of the node
// whose instant of increment is strictly after the given instant. A zero
// instant sums every live entry, which is used when no telemetry is known,
// in which case there is no double counting to avoid.
//
// There is no accessor of the total cost without the filter, since the
// filter by reception time is mandatory in both paths, the selection and
// the record, because the observed telemetry already includes the load up
// to the instant of reception and summing the entry before that instant
// would count the same work twice. A nil receiver returns zero.
func (g *InflightGauge) NodeCostSince(nodeID string, since time.Time) float64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	total := 0.0
	prefix := nodeID + "\x00"
	for key, ents := range g.entries {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, e := range ents {
			if since.IsZero() || e.at.After(since) {
				total += e.cost
			}
		}
	}
	return total
}
