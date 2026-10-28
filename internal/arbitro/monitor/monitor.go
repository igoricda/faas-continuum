// Package monitor keeps the last known state of each node: telemetry snapshots
// and per-function capacity. It is in-memory only; the server checks the node
// against the node store before updating the monitor.
package monitor

import (
	"sync"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// CapacityTTL is how long a capacity sample stays fresh: three default
// periodic reporting intervals and strictly shorter than the node staleness
// horizon, so occupancy never gates longer than liveness. An older sample is
// unknown and never causes gating.
const CapacityTTL = 30 * time.Second

// capacityEntry is the occupancy of one function on one node, stamped with the
// receive time at the arbiter. The node's own package timestamp is ignored for
// ordering, which removes clock skew between devices.
type capacityEntry struct {
	cap       telemetry.FunctionCapacity
	updatedAt time.Time
	// exhausted is the optimistic exhaustion mark set by MarkExhausted
	// (POOL_EXHAUSTED/QUEUE_FULL): "warm but at ceiling", not "no live
	// instance". MarkExhausted sets it and UpdateCapacity clears it. Only
	// GetCapacityEx reports it; GetCapacity ignores it.
	exhausted bool
}

// Monitor keeps the last known state of each node. It is in-memory only.
// Invariant: len(latest) <= MaxNodes (256), ensured by the server validating
// the node before Update; the monitor has no cap of its own.
type Monitor struct {
	mu     sync.RWMutex
	latest map[string]telemetry.Package // key: node_id
	// latestAt records the receive time at the arbiter of the latest package.
	// It bounds load projection: NodeCostSince only sums entries updated after
	// this instant, because observed telemetry already includes the load up to
	// that point. The node's own package timestamp is ignored.
	latestAt map[string]time.Time
	// capacity holds occupancy by node and function (node_id -> function).
	// Its size is bounded indirectly by latest, since the server validates the
	// same nodes before updating; the map has no cap of its own.
	capacity map[string]map[string]capacityEntry
}

// NewMonitor returns an empty monitor.
func NewMonitor() *Monitor {
	return &Monitor{
		latest:   make(map[string]telemetry.Package),
		latestAt: make(map[string]time.Time),
		capacity: make(map[string]map[string]capacityEntry),
	}
}

// Update replaces the node's latest package, using the wall clock as receive
// time. The caller must have validated that the node exists. Use UpdateAt to
// inject a clock.
func (m *Monitor) Update(pkg telemetry.Package) {
	m.UpdateAt(pkg, time.Now())
}

// UpdateAt is Update with an explicit receive time, taken from the arbiter
// clock rather than the node clock, which makes it deterministic under a fake
// clock. The node's package timestamp is ignored.
func (m *Monitor) UpdateAt(pkg telemetry.Package, recvNow time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latest[pkg.NodeID] = pkg
	m.latestAt[pkg.NodeID] = recvNow
}

// Latest returns the node's last known package. It is nil-receiver-safe: a
// typed nil *Monitor may sit inside an interface, where the caller's nil check
// does not see it, so every reader guards itself.
func (m *Monitor) Latest(nodeID string) (telemetry.Package, bool) {
	if m == nil {
		return telemetry.Package{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	pkg, ok := m.latest[nodeID]
	return pkg, ok
}

// LatestAt returns the node's last known package and its receive time at the
// arbiter, for time-bounded load accounting. ok follows presence in latest, so
// the receive time never outlives the entry.
func (m *Monitor) LatestAt(nodeID string) (pkg telemetry.Package, recvAt time.Time, ok bool) {
	if m == nil {
		return telemetry.Package{}, time.Time{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	pkg, ok = m.latest[nodeID]
	if !ok {
		return telemetry.Package{}, time.Time{}, false
	}
	return pkg, m.latestAt[nodeID], true
}

// Delete removes the node's last known package and occupancy.
func (m *Monitor) Delete(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.latest, nodeID)
	delete(m.latestAt, nodeID)
	delete(m.capacity, nodeID)
}

// ClampCapacity normalizes ingested occupancy: negative values become zero
// instead of evading the exhaustion filter. MaxInstances is a ceiling where 0
// means no ceiling; InstancesAvailable and QueueDepth are never negative.
func ClampCapacity(cap telemetry.FunctionCapacity) telemetry.FunctionCapacity {
	if cap.MaxInstances < 0 {
		cap.MaxInstances = 0
	}
	if cap.InstancesAvailable < 0 {
		cap.InstancesAvailable = 0
	}
	if cap.QueueDepth < 0 {
		cap.QueueDepth = 0
	}
	return cap
}

// UpdateCapacity merges the occupancy of one function from any ingestion
// channel, using recvNow as the arbiter receive time. The merge keeps the
// newest updatedAt and never regresses: an out-of-order or duplicated delivery
// with an older recvNow is discarded. Ordering uses recvNow only.
func (m *Monitor) UpdateCapacity(nodeID, funcName string, cap telemetry.FunctionCapacity, recvNow time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cap = ClampCapacity(cap)
	byFunc, ok := m.capacity[nodeID]
	if !ok {
		byFunc = make(map[string]capacityEntry)
		m.capacity[nodeID] = byFunc
	}
	if cur, ok := byFunc[funcName]; ok && cur.updatedAt.After(recvNow) {
		return
	}
	byFunc[funcName] = capacityEntry{cap: cap, updatedAt: recvNow}
}

// GetCapacity returns the occupancy of one function if it is fresh. A sample
// older than CapacityTTL, measured on the arbiter clock, or an absent entry
// yields ok=false, which never causes gating. The exhaustion mark is dropped,
// matching the behavior of callers that do not need it.
func (m *Monitor) GetCapacity(nodeID, funcName string, now time.Time) (telemetry.FunctionCapacity, bool) {
	if m == nil {
		return telemetry.FunctionCapacity{}, false
	}
	cap, _, ok := m.GetCapacityEx(nodeID, funcName, now)
	return cap, ok
}

// GetCapacityEx is GetCapacity plus the optimistic exhaustion mark. It returns
// exhaustedMark=true when the entry came from MarkExhausted and has not been
// cleared by UpdateCapacity. It is nil-receiver-safe, since selectors may run
// with a nil monitor.
func (m *Monitor) GetCapacityEx(nodeID, funcName string, now time.Time) (cap telemetry.FunctionCapacity, exhaustedMark bool, ok bool) {
	if m == nil {
		return telemetry.FunctionCapacity{}, false, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	byFunc, ok := m.capacity[nodeID]
	if !ok {
		return telemetry.FunctionCapacity{}, false, false
	}
	e, ok := byFunc[funcName]
	if !ok {
		return telemetry.FunctionCapacity{}, false, false
	}
	if now.Sub(e.updatedAt) > CapacityTTL {
		return telemetry.FunctionCapacity{}, false, false
	}
	return e.cap, e.exhausted, true
}

// MarkExhausted optimistically marks one function as exhausted after a parsed
// capacity rejection (POOL_EXHAUSTED or QUEUE_FULL). A successful-invocation
// piggyback never reports a saturated node, so without this mark an exhausted
// node would never send fresh occupancy and would not be skipped by the
// filter. The mark expires through updatedAt and CapacityTTL. MaxInstances and
// QueueDepth of the current entry are preserved and InstancesAvailable is set
// to 0. The merge follows the same no-regression rule as UpdateCapacity.
func (m *Monitor) MarkExhausted(nodeID, funcName string, recvNow time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byFunc, ok := m.capacity[nodeID]
	if !ok {
		byFunc = make(map[string]capacityEntry)
		m.capacity[nodeID] = byFunc
	}
	cap := telemetry.FunctionCapacity{}
	if cur, ok := byFunc[funcName]; ok {
		if cur.updatedAt.After(recvNow) {
			return
		}
		cap = cur.cap
	}
	cap.InstancesAvailable = 0
	byFunc[funcName] = capacityEntry{cap: cap, updatedAt: recvNow, exhausted: true}
}

// SanitizeForNode validates the occupancy of one function against the node
// registry. It is a pure function.
//   - An unknown function, meaning a non-empty hosted list without funcName,
//     yields ok=false and the entry is discarded, which prevents failover on a
//     stray key.
//   - An empty or nil hosted list accepts any function.
//   - Availability above the registered ceiling (ceilings[fn] > 0) is clamped.
//
// The hosted list and ceilings come from Node.Functions and
// Node.FunctionCapacity. Both ingestion channels call this before storing.
func SanitizeForNode(hosted []string, ceilings map[string]int, funcName string, cap telemetry.FunctionCapacity) (telemetry.FunctionCapacity, bool) {
	if len(hosted) > 0 {
		known := false
		for _, f := range hosted {
			if f == funcName {
				known = true
				break
			}
		}
		if !known {
			return telemetry.FunctionCapacity{}, false
		}
	}
	cap = ClampCapacity(cap)
	if ceiling, ok := ceilings[funcName]; ok && ceiling > 0 && cap.InstancesAvailable > ceiling {
		cap.InstancesAvailable = ceiling
	}
	return cap, true
}

// DeleteCapacities removes only the node's capacity entries and keeps the last
// telemetry package. Re-registration would otherwise inherit an exhausted mark
// for up to CapacityTTL without having received any load. Delete removes
// everything.
func (m *Monitor) DeleteCapacities(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.capacity, nodeID)
}

// SweepExpired removes capacity entries older than CapacityTTL. Without the
// sweep the (node, function) key set would grow monotonically, since it is
// driven by external input. Ingestion points call it, and tests observe it;
// GetCapacity treats an expired entry as unknown even without a sweep.
func (m *Monitor) SweepExpired(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for nodeID, byFunc := range m.capacity {
		for fn, e := range byFunc {
			if now.Sub(e.updatedAt) > CapacityTTL {
				delete(byFunc, fn)
			}
		}
		if len(byFunc) == 0 {
			delete(m.capacity, nodeID)
		}
	}
}
