package nodeapi

import (
	"context"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/igoricda/faas-continuum/internal/ntime"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// invokeProfiler is the single owner of the lifecycle of the sampler of
// an invocation: the tracker, the signal that ends it, the sampling
// context and the cancellation. The stop method closes the signal,
// cancels the context, takes the final sample and closes the tracker,
// all of it under a single guard, so that forgetting to close it or
// closing it twice is structurally impossible.
type invokeProfiler struct {
	tracker    *telemetry.ProfileTracker
	done       chan struct{}
	sampleCtx  context.Context
	cancel     context.CancelFunc
	once       sync.Once
	sampler    InstanceMetricsSampler
	interval   time.Duration
	instanceID string
}

// startProfiler enables the profiling and returns its owner. It returns
// nothing when the telemetry is disabled, when the interval is not
// positive, which means that the profiling is disabled, or when the first
// sample fails, in which case there is no tracker and no goroutine.
func (s *Server) startProfiler(r *http.Request, instanceID string) *invokeProfiler {
	if !s.telemetryActive() {
		return nil
	}
	if s.invocationSampleInterval <= 0 {
		return nil
	}
	prof := &invokeProfiler{
		sampler:    s.metricsSampler,
		interval:   s.invocationSampleInterval,
		instanceID: instanceID,
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	prof.sampleCtx, prof.cancel = ctx, cancel

	t0, terr := prof.sampler.SampleMetrics(ctx, instanceID)
	if terr != nil {
		log.Printf("nodeapi: perfil de recursos indisponível nesta invocação: %v", terr)
		cancel()
		return nil
	}

	prof.tracker = telemetry.NewProfileTracker(runtime.NumCPU())
	prof.tracker.StartMono(t0, ntime.Now())
	prof.done = make(chan struct{})
	go prof.loop()
	return prof
}

func (prof *invokeProfiler) loop() {
	ticker := time.NewTicker(prof.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if smp, serr := prof.sampler.SampleMetrics(prof.sampleCtx, prof.instanceID); serr == nil {
				prof.tracker.ObserveMono(smp, ntime.Now())
			}
		case <-prof.done:
			return
		case <-prof.sampleCtx.Done():
			return
		}
	}
}

// stop ends the sampler: it closes the signal, cancels the context, takes
// the final sample and closes the tracker. It is idempotent, so a second
// call repeats nothing and the profile read afterwards is identical. A
// failure of the final sample only omits the profile.
//
// The final sample detaches itself from the cancellation of the parent
// context, so that a sampler which respects the context still returns the
// final window even when the parent context was already cancelled. A
// child context with a short timeout is acceptable only as a fallback for
// the samplers that require a deadline.
func (prof *invokeProfiler) stop() {
	prof.once.Do(func() {
		close(prof.done)
		prof.cancel()
		sampleCtx := context.WithoutCancel(prof.sampleCtx)
		if final, ferr := prof.sampler.SampleMetrics(sampleCtx, prof.instanceID); ferr == nil {
			prof.tracker.ObserveMono(final, ntime.Now())
		}
		prof.tracker.FinishMono(ntime.Now())
	})
}
