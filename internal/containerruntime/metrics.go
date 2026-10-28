// Per-container resource sampling during an invocation: it reads the
// counters that the container daemon reports for a task, which requires
// no extra agent inside the container and no direct read of the cgroup
// paths, since the runtime is the source of truth and the cgroup path
// depends on the driver of the host.
//
// The counters are cumulative since the start of the task. Converting the
// deltas into an average and a peak is the job of the tenant that owns the
// profile.
package containerruntime

import (
	"context"
	"errors"
	"fmt"

	statsv1 "github.com/containerd/cgroups/v3/cgroup1/stats"
	statsv2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	typeurl "github.com/containerd/typeurl/v2"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// ErrUnsupportedMetricFormat is returned when the container daemon reports
// a payload format that matches neither cgroup v1 nor v2. It lets consumers
// tell an unknown format from a transport failure.
var ErrUnsupportedMetricFormat = errors.New("containerruntime: formato de métrica não suportado")

// SampleMetrics returns a point-in-time sample of the resource usage of
// the container of an instance of the pool: the accumulated total of CPU
// in microseconds and the memory in use in bytes. It fails when the
// instance is not in the pool, which also excludes the instances adopted
// by the restart reconciliation unless they entered the map of handles.
func (a *PoolAdapter) SampleMetrics(ctx context.Context, instanceID string) (telemetry.InvocationSample, error) {
	a.mu.Lock()
	handle := a.handles[instanceID]
	a.mu.Unlock()
	if handle == nil {
		return telemetry.InvocationSample{}, fmt.Errorf("containerruntime: instância %q não encontrada", instanceID)
	}

	// The container daemon requires the namespace in the context of every
	// call of its API: with a plain context the call always fails with a
	// precondition error. That is why this path is identical to the other
	// methods of the runtime, always through the context bound to the
	// namespace of the framework.
	//
	// There is no guard for a nil runtime on purpose: an adapter without a
	// runtime is a wiring error, and a silent fallback without the
	// namespace would hide exactly this class of bug. The check of a
	// nonexistent instance returns before reaching this point.
	metric, err := handle.Task.Metrics(a.runtime.ctx(ctx))
	if err != nil {
		return telemetry.InvocationSample{}, fmt.Errorf("containerruntime: falha ao obter métricas da instância %q: %w", instanceID, err)
	}

	return decodeInvocationSample(metric.Data)
}

// decodeInvocationSample decodes the metrics payload of the task in the
// official format of the container daemon, mirroring the pattern of the
// reference command line tool: it identifies the type, which corresponds
// to cgroup v1 or v2, and then decodes it, and neither step requires a
// previous registration for the generated messages.
//
// An absent component, represented by a nil field, simply leaves the
// field at zero: a partial sample is never an error, since the telemetry
// must never abort the invocation.
func decodeInvocationSample(data *anypb.Any) (telemetry.InvocationSample, error) {
	switch {
	case typeurl.Is(data, (*statsv1.Metrics)(nil)):
		var m statsv1.Metrics
		if err := typeurl.UnmarshalTo(data, &m); err != nil {
			return telemetry.InvocationSample{}, fmt.Errorf("containerruntime: falha ao decodificar métricas (cgroups v1): %w", err)
		}
		var sample telemetry.InvocationSample
		if m.CPU != nil && m.CPU.Usage != nil {
			sample.CPUUsageUSec = m.CPU.Usage.Total / 1000 // the accounting is in nanoseconds, normalized to microseconds
		}
		if m.Memory != nil && m.Memory.Usage != nil {
			sample.MemoryBytes = m.Memory.Usage.Usage // bytes
		}
		return sample, nil
	case typeurl.Is(data, (*statsv2.Metrics)(nil)):
		var m statsv2.Metrics
		if err := typeurl.UnmarshalTo(data, &m); err != nil {
			return telemetry.InvocationSample{}, fmt.Errorf("containerruntime: falha ao decodificar métricas (cgroups v2): %w", err)
		}
		var sample telemetry.InvocationSample
		if m.CPU != nil {
			sample.CPUUsageUSec = m.CPU.UsageUsec // the accounting is already in microseconds
		}
		if m.Memory != nil {
			sample.MemoryBytes = m.Memory.Usage // bytes
		}
		return sample, nil
	default:
		return telemetry.InvocationSample{}, ErrUnsupportedMetricFormat
	}
}
