// Package telemetry builds the telemetry package of a node, combining the
// aggregate usage counters, read through cgroups v2 or procfs, with the
// pressure information when the kernel exposes it, and propagates the
// result through a hybrid model: a piggyback in the response of the
// invocation and an autonomous periodic report.
//
// The data contract is that the usage fields are always present, while the
// pressure fields are omitted when the kernel does not expose pressure
// information, which is the compatibility mode.
package telemetry

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/igoricda/faas-continuum/internal/ntime"
	"github.com/igoricda/faas-continuum/internal/telemetry/energy"
	"github.com/igoricda/faas-continuum/internal/telemetry/psi"
	"github.com/igoricda/faas-continuum/internal/telemetry/usage"
)

// The origin of an update.
const (
	SourcePiggyback = "piggyback"
	SourcePeriodic  = "periodic"
)

type PressureFields struct {
	Avg10 float64 `json:"avg10"`
}

type EnergyFields struct {
	Source          string  `json:"source"`
	PowerMilliwatts float64 `json:"power_milliwatts"`
}

type Package struct {
	NodeID             string          `json:"node_id"`
	Timestamp          time.Time       `json:"timestamp"`
	CPUUsagePct        float64         `json:"cpu_usage_pct"`
	MemoryUsagePct     float64         `json:"memory_usage_pct"`
	CPUPressure        *PressureFields `json:"cpu_pressure,omitempty"`
	MemoryPressure     *PressureFields `json:"memory_pressure,omitempty"`
	MemoryPressureFull *PressureFields `json:"memory_pressure_full,omitempty"`
	Energy             *EnergyFields   `json:"energy,omitempty"`
	Source             string          `json:"source,omitempty"`
	// Capacity carries the occupancy per function. The piggyback fills
	// only the invoked function, while the periodic report fills the
	// complete map. A missing map means unknown, with no gating, since
	// the decoder is tolerant and the key is simply absent from the
	// JSON.
	Capacity map[string]FunctionCapacity `json:"capacity,omitempty"`
}

// FunctionCapacity is the occupancy of one function at an instant: the
// ceiling, where zero means no ceiling, plus the live picture, where
// InstancesAvailable is the total number of live instances, occupied ones
// included, and QueueDepth is the number of waiters in the bounded queue.
// It is exported so that the monitor of the arbiter can consume it.
//
// Every field is omitted when empty, so the block disappears when there
// is nothing to report and the growth of the JSON stays around 100 bytes
// per function.
//
// The pool does not expose the free and the occupied instances separately,
// only the total number of live ones, so the arbiter never receives the
// number of free slots. The capacity gating is therefore reactive, driven
// by a rejection that reports no availability, and never proactive based
// on a free slot. InstancesAvailable must not be read as idle capacity.
type FunctionCapacity struct {
	MaxInstances       int `json:"max_instances,omitempty"`
	InstancesAvailable int `json:"instances_available,omitempty"`
	QueueDepth         int `json:"queue_depth,omitempty"`
}

const psiCacheTTL = 10 * time.Second

// Collector produces snapshots of the device by combining the three
// readers. It keeps the previous sample of the CPU and of the energy,
// which the cumulative contracts of the reader interfaces require, under
// a mutex, because Collect can be called concurrently by the piggyback
// and by the reporter.
type Collector struct {
	nodeID  string
	enabled bool
	psi     *psi.Reader
	usage   usage.Reader  // cgroups-v2 ou procfs — ver New em usage/usage.go
	energy  energy.Reader // rapl, power_supply ou none — ver New em energy/energy.go
	numCPU  int

	mu             sync.Mutex
	lastCPU        usage.Sample
	lastTime       ntime.Time // zero means the first collection did not happen yet (8 bytes against 24)
	lastEnergy     energy.Sample
	lastEnergyTime ntime.Time // zero means the first energy sample is not primed yet

	// capMu guards the capacity snapshot, a callback installed by the
	// component that owns the registry and the pool. The collector never
	// imports the pool or the registry, so the coupling is only through
	// the callback and there is no reverse dependency.
	capMu   sync.RWMutex
	capSnap func() map[string]FunctionCapacity

	psiCache psiPressureCache
}

// psiPressureCache holds the last pressure reading with a lifetime of ten
// seconds. An empty cache is detected by a zero timestamp, the same idiom
// as the other cached timestamps.
type psiPressureCache struct {
	mu      sync.Mutex
	at      ntime.Time
	cpu     PressureFields
	mem     PressureFields
	memFull PressureFields
}

// load returns a copy of the pressure fields and reports whether the cached reading is still within its lifetime.
func (p *psiPressureCache) load() (cpu, mem, memFull PressureFields, fresca bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.at.IsZero() || ntime.Since(p.at) >= psiCacheTTL {
		return PressureFields{}, PressureFields{}, PressureFields{}, false
	}
	return p.cpu, p.mem, p.memFull, true
}

// store saves the fields and stamps the instant of the reading.
func (p *psiPressureCache) store(cpu, mem, memFull PressureFields) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cpu, p.mem, p.memFull = cpu, mem, memFull
	p.at = ntime.Now()
}

// NewCollector returns the collector. Empty paths select the defaults of
// the usage package and of the energy package. The pressure reader shares
// the same cgroup root, so a single subsystem feeds both the usage and
// the pressure when cgroups v2 is available.
func NewCollector(nodeID string, enabled bool, cgroupRoot, procStatPath, meminfoPath, powercapRoot, powerSupplyRoot string) *Collector {
	return &Collector{
		nodeID:  nodeID,
		enabled: enabled,
		psi:     psi.NewReader(cgroupRoot),
		usage:   usage.New(cgroupRoot, procStatPath, meminfoPath),
		energy:  energy.New(powercapRoot, powerSupplyRoot),
		numCPU:  runtime.NumCPU(),
	}
}

// Enabled reports whether the telemetry is enabled.
func (c *Collector) Enabled() bool { return c.enabled }

// PSISupported reports whether the device exposes pressure information,
// which feeds the support flag of the node registration and the startup
// log of the agent.
func (c *Collector) PSISupported() bool { return c.psi.Supported() }

// UsageSource exposes the detected usage mechanism, for the startup log
// and for diagnostics.
func (c *Collector) UsageSource() string { return c.usage.Source() }

// EnergySource exposes the detected energy source, for the startup log and
// for diagnostics.
func (c *Collector) EnergySource() string { return c.energy.Source() }

// SetCapacitySnapshot installs the provider of the complete occupancy map
// per function. It is called once by the component that owns the registry
// and the pool, and a nil provider uninstalls it.
//
// The collector consults the provider only in the periodic channel: the
// piggyback fills only the invoked function after Collect returns, so the
// two channels never mix. The call happens outside any lock of the
// collector, since no I/O and no foreign lock may happen under a lock.
func (c *Collector) SetCapacitySnapshot(fn func() map[string]FunctionCapacity) {
	c.capMu.Lock()
	defer c.capMu.Unlock()
	c.capSnap = fn
}

// capacitySnapshot returns the complete map in the periodic channel when a
// provider is installed, and nil in every other case.
func (c *Collector) capacitySnapshot(source string) map[string]FunctionCapacity {
	if source != SourcePeriodic {
		return nil
	}
	c.capMu.RLock()
	fn := c.capSnap
	c.capMu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn()
}

// Collect gathers a snapshot. It always fills the node identifier, the
// timestamp in UTC, the source given as a parameter and the usage
// percentages. The pressure fields are present only when the kernel
// supports them, and the energy block appears only from the second
// collection onwards, because the window of watts needs the previous
// sample, with the same semantics as the CPU percentage.
//
// It returns an error only when the usage counters fail. A failure of the
// pressure or of the energy omits the respective field and continues,
// since the compatibility mode is a normal operating state and not an
// error.
//
// The first call primes the CPU and energy samples and returns a zero CPU
// percentage and no energy block. The following calls return the
// percentage and the watts measured since the previous collection,
// whichever source it had, so that the piggyback and the periodic report
// share the same timeline and their values are consistent.
//
// The usage and energy readings happen outside the mutex, since doing I/O
// under a lock is a bug, and only the update of the delta state is
// protected. A failed energy reading leaves the previous sample intact, so
// the next window covers the whole interval and never reports a jump.
func (c *Collector) Collect(source string) (Package, error) {
	if !c.enabled {
		return Package{NodeID: c.nodeID, Timestamp: time.Now().UTC(), Source: source, Capacity: c.capacitySnapshot(source)}, nil
	}
	nowWall := time.Now()
	nowMono := ntime.Now()

	sample, err := c.usage.Read()
	if err != nil {
		return Package{}, fmt.Errorf("telemetry: %w", err)
	}

	var esample energy.Sample
	haveE := false
	if c.energy.Supported() {
		if es, eerr := c.energy.Read(); eerr == nil {
			esample = es
			haveE = true
		}
	}

	c.mu.Lock()
	var cpuPct float64
	if !c.lastTime.IsZero() {
		cpuPct = usage.CPUPercent(c.lastCPU, sample, nowMono.Sub(c.lastTime), c.numCPU)
	}
	c.lastCPU = sample
	c.lastTime = nowMono

	var energyFields *EnergyFields
	if haveE && !c.lastEnergyTime.IsZero() {
		w := energy.WindowWatts(c.energy.Source(), c.lastEnergy, esample, nowMono.Sub(c.lastEnergyTime))
		energyFields = &EnergyFields{Source: c.energy.Source(), PowerMilliwatts: w * 1000}
	}
	if haveE {
		c.lastEnergy = esample
		c.lastEnergyTime = nowMono
	}
	c.mu.Unlock()

	pkg := Package{
		NodeID:         c.nodeID,
		Timestamp:      nowWall.UTC(),
		CPUUsagePct:    cpuPct,
		MemoryUsagePct: sample.MemoryPct,
		Source:         source,
		Energy:         energyFields,
		Capacity:       c.capacitySnapshot(source),
	}

	if c.psi.Supported() {
		// The pressure reading is cached for ten seconds. Without the
		// cache, the collection piggybacked on every invocation would
		// read two files on every reporting interval. A cache hit
		// returns without any file access, and a miss reads the files
		// and updates the cache, which has its own mutex so it never
		// blocks the collector.
		cpu, mem, memFull, fresca := c.psiCache.load()
		if !fresca {
			if snap, perr := c.psi.Read(); perr == nil {
				cpu, mem, memFull = *pressureFields(snap.CPUSome), *pressureFields(snap.MemorySome), *pressureFields(snap.MemoryFull)
				c.psiCache.store(cpu, mem, memFull)
				fresca = true
			}
		}
		if fresca {
			pkg.CPUPressure = &cpu
			pkg.MemoryPressure = &mem
			pkg.MemoryPressureFull = &memFull
		}
	}
	return pkg, nil
}

func pressureFields(p psi.Pressure) *PressureFields {
	return &PressureFields{
		Avg10: p.Avg10,
	}
}
