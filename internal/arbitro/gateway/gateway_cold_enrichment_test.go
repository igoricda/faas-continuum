package gateway

// Enrichment of the synchronous path: the cold start flag, the instance
// identifier and the resource profile of the execution metrics block become
// fields of the history entry. The decoding is tolerant, so a nil blob, from
// any error path, invalid JSON, a wrong type or a missing key give zeros and
// never a panic.

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
)

// readInvRecords decodes each line of the invocation log as a record, with the
// entry flattened in the JSON.
func readInvRecords(t *testing.T, path string) []InvocationRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile invLog: %v", err)
	}
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil
	}
	lines := strings.Split(trimmed, "\n")
	recs := make([]InvocationRecord, 0, len(lines))
	for _, ln := range lines {
		var r InvocationRecord
		if err := json.Unmarshal([]byte(ln), &r); err != nil {
			t.Fatalf("unmarshal InvocationRecord %q: %v", ln, err)
		}
		recs = append(recs, r)
	}
	return recs
}

func TestColdEnrichDecodersTolerant(t *testing.T) {
	tests := []struct {
		name         string
		metrics      json.RawMessage
		wantCold     bool
		wantInstance string
		wantCPU      float64
		wantSamples  int
	}{
		{
			name:         "full-profile",
			metrics:      json.RawMessage(`{"cold_start":true,"instance_id":"alpr-ncnn-1","resource_profile":{"cpu_pct_avg":42.5,"sample_count":13}}`),
			wantCold:     true,
			wantInstance: "alpr-ncnn-1",
			wantCPU:      42.5,
			wantSamples:  13,
		},
		{
			name:    "absent-keys",
			metrics: json.RawMessage(`{"telemetry":{"capacity":{"sleep":{"max_instances":1,"instances_available":1}}}}`),
		},
		{
			name:    "empty-object",
			metrics: json.RawMessage(`{}`),
		},
		{
			name:    "nil-metrics",
			metrics: nil,
		},
		{
			name:    "invalid-json-garbage",
			metrics: json.RawMessage(`not-json{{{`),
		},
		{
			name:    "wrong-types",
			metrics: json.RawMessage(`{"cold_start":"yes","instance_id":7,"resource_profile":{"cpu_pct_avg":"x","sample_count":true}}`),
		},
		{
			name:        "negative-cpu-clamped",
			metrics:     json.RawMessage(`{"resource_profile":{"cpu_pct_avg":-5.0,"sample_count":3}}`),
			wantCPU:     0,
			wantSamples: 3,
		},
		{
			name:    "zero-sample-count-cpu-unknown",
			metrics: json.RawMessage(`{"resource_profile":{"cpu_pct_avg":42.5,"sample_count":0}}`),
		},
		{
			name:    "negative-sample-count-cpu-unknown",
			metrics: json.RawMessage(`{"resource_profile":{"cpu_pct_avg":42.5,"sample_count":-2}}`),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decoder panicou com metrics %q: %v", tc.metrics, r)
				}
			}()
			gotCold, gotInstance := coldInfoOfMetrics(tc.metrics)
			gotCPU, _, _, gotSamples := resourceProfileOfMetrics(tc.metrics)
			if gotCold != tc.wantCold || gotInstance != tc.wantInstance {
				t.Errorf("coldInfo = (%v,%q), esperado (%v,%q)", gotCold, gotInstance, tc.wantCold, tc.wantInstance)
			}
			if math.Abs(gotCPU-tc.wantCPU) > 1e-9 || gotSamples != tc.wantSamples {
				t.Errorf("resourceProfile = (%v,%d), esperado (%v,%d)", gotCPU, gotSamples, tc.wantCPU, tc.wantSamples)
			}
		})
	}
}

func TestColdEnrichAppendedEntry(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := nodes.Node{NodeID: "borda-cold"}
	group := selection.ResolveRequestGroup("fn-cold", []nodes.Node{node})
	slo := &selection.SLO{MaxLatencyMS: 1000}

	tests := []struct {
		name         string
		metrics      json.RawMessage
		wantCold     bool
		wantInstance string
		wantCPU      float64
		wantSamples  int
	}{
		{
			name:         "full-profile-2xx",
			metrics:      json.RawMessage(`{"cold_start":true,"instance_id":"alpr-ncnn-1","resource_profile":{"cpu_pct_avg":42.5,"sample_count":13}}`),
			wantCold:     true,
			wantInstance: "alpr-ncnn-1",
			wantCPU:      42.5,
			wantSamples:  13,
		},
		{
			name:         "warm-without-profile",
			metrics:      json.RawMessage(`{"cold_start":false,"instance_id":"alpr-ncnn-2"}`),
			wantInstance: "alpr-ncnn-2",
		},
		{
			name:    "absent-keys",
			metrics: json.RawMessage(`{"response_time_ms":4}`),
		},
		{
			name:        "negative-cpu-clamped",
			metrics:     json.RawMessage(`{"cold_start":true,"resource_profile":{"cpu_pct_avg":-9.5,"sample_count":4}}`),
			wantCold:    true,
			wantSamples: 4,
		},
		{
			name:    "wrong-types-zero",
			metrics: json.RawMessage(`{"cold_start":"yes","instance_id":7,"resource_profile":{"cpu_pct_avg":"x","sample_count":true}}`),
		},
		{
			name:    "nil-metrics",
			metrics: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, err := history.NewHistory("", 100)
			if err != nil {
				t.Fatalf("NewHistory: %v", err)
			}
			gw, logPath := capacityBurstGateway(t, h, now)
			gw.recordAttempt(node, 5*time.Millisecond, nil, slo, "fn-cold", group, tc.metrics, true, "", 0, attemptCapture{sc: history.ScenarioContext{LoadBucket: history.BucketUnknown}})
			recs := readInvRecords(t, logPath)
			if len(recs) != 1 {
				t.Fatalf("invLog = %d linhas, esperado 1", len(recs))
			}
			got := recs[0].Entry
			if got.ColdStart != tc.wantCold || got.InstanceID != tc.wantInstance {
				t.Errorf("Entry cold = (%v,%q), esperado (%v,%q)", got.ColdStart, got.InstanceID, tc.wantCold, tc.wantInstance)
			}
			if math.Abs(got.CPUPctAvg-tc.wantCPU) > 1e-9 || got.SampleCount != tc.wantSamples {
				t.Errorf("Entry profile = (%v,%d), esperado (%v,%d)", got.CPUPctAvg, got.SampleCount, tc.wantCPU, tc.wantSamples)
			}
		})
	}
}

// TestColdEnrichExcludedFromP95Count proves the semantics of a cold start: the
// entry enters the recent rate and the total, and stays out of the percentile
// and of the count, which cover only the successes.
func TestColdEnrichExcludedFromP95Count(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	h, err := history.NewHistory("", 100)
	if err != nil {
		t.Fatalf("NewHistory: %v", err)
	}
	gw, _ := capacityBurstGateway(t, h, now)
	node := nodes.Node{NodeID: "borda-cold"}
	group := selection.ResolveRequestGroup("fn-cold", []nodes.Node{node})
	slo := &selection.SLO{MaxLatencyMS: 1000}
	key := history.SegmentKey(group, node.NodeID, history.ScenarioContext{LoadBucket: history.BucketUnknown})

	warm := json.RawMessage(`{"cold_start":false,"instance_id":"warm-1","resource_profile":{"cpu_pct_avg":10.0,"sample_count":3}}`)
	cold := json.RawMessage(`{"cold_start":true,"instance_id":"cold-1","resource_profile":{"cpu_pct_avg":42.5,"sample_count":13}}`)
	gw.recordAttempt(node, 10*time.Millisecond, nil, slo, "fn-cold", group, warm, true, "", 0, attemptCapture{sc: history.ScenarioContext{LoadBucket: history.BucketUnknown}})
	gw.recordAttempt(node, 300*time.Millisecond, nil, slo, "fn-cold", group, cold, true, "", 0, attemptCapture{sc: history.ScenarioContext{LoadBucket: history.BucketUnknown}})

	count, p95, recent, total, _, ok := h.StatsAndRecent(key, time.Minute, now)
	if !ok || count != 1 || p95 != 10 {
		t.Fatalf("StatsAndRecent = (count=%d,p95=%d,ok=%v), esperado (1,10,true): cold fora do p95/Count", count, p95, ok)
	}
	if recent != 2 || total != 2 {
		t.Fatalf("recent=%d total=%d, esperado 2/2: cold conta em Recent/Total", recent, total)
	}
}
