package history

import (
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// ScenarioFrom builds the scenario from the telemetry package. Without
// known telemetry it returns the unknown bucket, and otherwise it applies
// the load bucket to the processor and to the memory and collapses both
// with the maximum.
func ScenarioFrom(pkg telemetry.Package, ok bool) ScenarioContext {
	if !ok {
		return ScenarioContext{LoadBucket: BucketUnknown}
	}
	return ScenarioContext{
		LoadBucket: MaxBucket(LoadBucket(pkg.CPUUsagePct), LoadBucket(pkg.MemoryUsagePct)),
	}
}

// ScenarioFromProjected builds the scenario from the telemetry package and
// from the projected processor load at the dispatch: the processor bucket
// uses the projection when it is known, even without telemetry, as long as
// the projected load is positive, and falls into unknown when there is
// neither telemetry nor projection, while the memory bucket follows only the
// observed telemetry, since there is no projection of memory. It collapses
// with the maximum, so the key remains the group, the node and the bucket,
// and the projection changes the computation of the bucket and never becomes
// part of the key. With the projected load equal to the observed load and
// with known telemetry it is identical to the plain constructor.
func ScenarioFromProjected(pkg telemetry.Package, ok bool, projectedCPU float64) ScenarioContext {
	cpuBucket := BucketUnknown
	if ok || projectedCPU > 0 {
		cpuBucket = LoadBucket(projectedCPU)
	}
	memBucket := BucketUnknown
	if ok {
		memBucket = LoadBucket(pkg.MemoryUsagePct)
	}
	return ScenarioContext{LoadBucket: MaxBucket(cpuBucket, memBucket)}
}
