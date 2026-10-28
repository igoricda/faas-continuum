package history

import (
	"slices"
	"strings"
	"time"
)

// queueBucketUnboundedK is the nominal ceiling of the last range, for
// depths of 41 and above: the depth has no known upper limit and the value
// only exists so that the range is a closed interval. It is never compared
// with a real depth, since the index is resolved by queueBucketIndex.
const queueBucketUnboundedK = 1 << 30

// queueBucketEdges are the ranges of queue depth of the learned delivery
// curve, where the depth is the number of requests in flight captured at the
// dispatch. They are dense at the knee, with three to five and six to ten,
// and wide in the tail. They are fixed on purpose: the range is the unit of
// evidence of the learning and not a parameter calibrated by flags.
var queueBucketEdges = [...]struct{ min, max int }{
	{1, 1},
	{2, 2},
	{3, 5},
	{6, 10},
	{11, 20},
	{21, 40},
	{41, queueBucketUnboundedK},
}

// queueBucketIndex maps a depth to the index of the range, and gives minus
// one for a non-positive depth, which is unknown and stays outside the
// curve, the same exclusion as the classes of the statistics scan. It is
// pure.
func queueBucketIndex(k int) int {
	if k <= 0 {
		return -1
	}
	for i, e := range queueBucketEdges {
		if k <= e.max {
			return i
		}
	}
	return len(queueBucketEdges) - 1
}

// QueueBucket is one range of queue depth of the learned delivery curve: the
// counts per outcome and the percentile of the warm successes of the range.
// The successes include the cold success, since it is a delivery, while the
// percentile covers only successes without a cold start, so a cold start,
// which is a provisioning cost and not a queue cost, does not inflate it.
// The failures count failures and timeouts, where the node had a chance and
// did not deliver. The saturation counts the exceeded budget, the capacity
// rejection and the admission timeout, the same taxonomy as the
// classification of the outcomes, which stays outside the delivery rate by
// doctrine but is preserved for observability.
type QueueBucket struct {
	KMin       int
	KMax       int
	P95MS      int64
	Successes  int
	Failures   int
	Saturation int
}

// QueueCurve is the delivery curve by queue depth of a single pair of group
// and node, aggregated over every scenario of load of the pair, since the
// queue effect is orthogonal to the bucket of processor usage and the
// aggregation gives evidence where an isolated segment would give none. The
// buckets cover the ranges in increasing order of depth, and a range without
// an entry in the window comes out as zero.
type QueueCurve struct {
	Buckets []QueueBucket
}

// queueBucketAccum is the accumulator of the scan, and the latencies are
// only allocated for a range that has a warm success.
type queueBucketAccum struct {
	lats                            []int64
	successes, failures, saturation int
}

// QueueCurve reads the delivery curve by queue depth of the pair of group
// and node in the window, where a non-positive window disables the temporal
// filter and reads all time, as the caller decides, and the selector uses
// the retention. It walks every segment of the pair in a single scan under
// the read lock, and an entry with an unknown number of requests in flight
// stays out, the same exclusion as the classes. The result is absent when
// there is no classifiable entry in the window, for a cold pair, so the
// caller never invents a curve.
//
// There is no cache: the curve depends on the window, for the same reason as
// the statistics scan, and the cost is linear in the entries of the pair,
// which at the production ceiling is the same order as one full statistics
// query per segment, paid only by a candidate with a predictor.
func (h *History) QueueCurve(group, nodeID string, window time.Duration, now time.Time) (QueueCurve, bool) {
	if h == nil || group == "" || nodeID == "" {
		return QueueCurve{}, false
	}
	prefix := group + "\x00" + nodeID + "\x00"
	h.mu.RLock()
	defer h.mu.RUnlock()
	var acc [len(queueBucketEdges)]queueBucketAccum
	any := false
	for key, entries := range h.m {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, e := range entries {
			if !windowFilter(e.Timestamp, now, window) {
				continue
			}
			bi := queueBucketIndex(e.Inflight)
			if bi < 0 {
				continue
			}
			switch classifyOutcomeBucket(e.Outcome) {
			case bucketSuccess:
				acc[bi].successes++
				if !e.ColdStart {
					acc[bi].lats = append(acc[bi].lats, e.ObservedLatencyMS)
				}
				any = true
			case bucketFailure:
				acc[bi].failures++
				any = true
			case bucketSaturation:
				acc[bi].saturation++
				any = true
			default:
				// An unknown outcome stays outside the curve, since it is not a measurement of the node.
			}
		}
	}
	if !any {
		return QueueCurve{}, false
	}
	curve := QueueCurve{Buckets: make([]QueueBucket, 0, len(queueBucketEdges))}
	for i, e := range queueBucketEdges {
		a := acc[i]
		b := QueueBucket{
			KMin:       e.min,
			KMax:       e.max,
			Successes:  a.successes,
			Failures:   a.failures,
			Saturation: a.saturation,
		}
		if len(a.lats) > 0 {
			slices.Sort(a.lats)
			b.P95MS = p95DeOrdenados(a.lats)
		}
		curve.Buckets = append(curve.Buckets, b)
	}
	return curve, true
}
