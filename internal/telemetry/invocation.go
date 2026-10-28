// This file implements the per-invocation resource profile. While a
// function runs, the file accumulates the average and the peak of the CPU
// over the windows between consecutive observations, the first and the
// last included, with the average weighted by the duration of each window,
// plus the peak of the memory in bytes.
//
// The computation is pure: it reads no kernel file, and the percentage
// formula is the same one used for the aggregate usage.
package telemetry

import (
	"sync"
	"time"

	"github.com/igoricda/faas-continuum/internal/ntime"
	"github.com/igoricda/faas-continuum/internal/telemetry/usage"
)

// InvocationSample is a raw reading of the counters of one container
// during an invocation, already normalized by the collection backend: the
// cumulative CPU in microseconds and the instantaneous memory in bytes.
type InvocationSample struct {
	CPUUsageUSec uint64
	MemoryBytes  uint64
}

type InvocationProfile struct {
	CPUPctAvg    float64
	CPUPctPeak   float64
	MemPeakBytes uint64
	SampleCount  int
}

// ProfileTracker accumulates the resource profile of one invocation. It is
// safe for concurrent use through its own mutex, because Observe runs on
// the goroutine of the ticker while Start, Finish and Profile run on the
// goroutine of the request.
//
// The average of the CPU is weighted by the duration of the windows:
//
//	avg = sum(pct_i * duration_i) / sum(duration_i)
//
// Since each percentage already normalizes its window by the duration and
// by the number of cores, the weighting preserves a physical invariant:
// the average times the total time times the number of cores times ten
// thousand approximates the CPU microseconds consumed by the invocation.
// A short burst of work is not diluted by long idle windows.
//
// The lifecycle is Start, then any number of Observe calls, then Finish,
// then the reading through Profile. Outside that lifecycle the methods do
// nothing and never panic, because a telemetry failure must never affect
// the invocation.
type phase int

const (
	phaseIdle phase = iota
	phaseActive
	phaseDone
)

type ProfileTracker struct {
	numCPU int

	// mu guards the accumulators. Observe runs on the goroutine of the
	// ticker while Start, Finish and Profile run on the goroutine of
	// the request, so without this lock the accumulators would be
	// written concurrently.
	mu        sync.RWMutex
	phase     phase
	prev      usage.Sample
	prevTime  ntime.Time
	sumPctDur float64
	sumDur    float64
	peakPct   float64
	peakMem   uint64
	samples   int
}

// NewProfileTracker returns the tracker. The number of cores normalizes
// the CPU percentage to [0, 100] in a per-core view, with the same
// contract as the aggregate usage. A value of zero or less makes every
// window count as zero percent, without panicking.
func NewProfileTracker(numCPU int) *ProfileTracker {
	return &ProfileTracker{numCPU: numCPU}
}

// toUsage converts the per-container sample to the type of the shared
// formula, which is what guarantees a single formula for both scopes.
func toUsage(s InvocationSample) usage.Sample {
	return usage.Sample{CPUTotalUSec: s.CPUUsageUSec}
}

func (t *ProfileTracker) Start(s InvocationSample, now time.Time) {
	t.StartMono(s, ntime.FromTime(now))
}

func (t *ProfileTracker) StartMono(s InvocationSample, now ntime.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phase == phaseDone || (t.phase == phaseActive && t.samples > 0) {
		return
	}
	t.phase = phaseActive
	t.prev = toUsage(s)
	t.prevTime = now
	if s.MemoryBytes > t.peakMem {
		t.peakMem = s.MemoryBytes
	}
}

func (t *ProfileTracker) Observe(s InvocationSample, now time.Time) {
	t.ObserveMono(s, ntime.FromTime(now))
}

func (t *ProfileTracker) ObserveMono(s InvocationSample, now ntime.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phase != phaseActive {
		return
	}
	t.closeWindow(toUsage(s), now)
	if s.MemoryBytes > t.peakMem {
		t.peakMem = s.MemoryBytes
	}
}

func (t *ProfileTracker) Finish(now time.Time) {
	t.FinishMono(ntime.FromTime(now))
}

func (t *ProfileTracker) FinishMono(now ntime.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phase != phaseActive {
		return
	}
	t.phase = phaseDone
	t.closeWindow(t.prev, now)
}

func (t *ProfileTracker) closeWindow(cur usage.Sample, now ntime.Time) {
	dur := now.Sub(t.prevTime)
	pct := usage.CPUPercent(t.prev, cur, dur, t.numCPU)
	t.sumPctDur += pct * dur.Seconds()
	t.sumDur += dur.Seconds()
	if pct > t.peakPct {
		t.peakPct = pct
	}
	t.samples++
	t.prev = cur
	t.prevTime = now
}

func (t *ProfileTracker) Profile() (InvocationProfile, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.phase == phaseIdle {
		return InvocationProfile{}, false
	}
	var avg float64
	if t.sumDur > 0 {
		avg = t.sumPctDur / t.sumDur
	}
	return InvocationProfile{
		CPUPctAvg:    avg,
		CPUPctPeak:   t.peakPct,
		MemPeakBytes: t.peakMem,
		SampleCount:  t.samples,
	}, true
}
