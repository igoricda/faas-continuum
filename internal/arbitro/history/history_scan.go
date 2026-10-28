package history

import (
	"slices"
	"time"
)

// windowFilter reports whether a timestamp takes part in the window that
// ends now: it is not after now and its age is within the window. A
// non-positive window disables the temporal filter and reads all time. It is
// pure and shared by the statistics scan, the recent successes and the
// expected cost, so that the temporal gate does not drift between the
// readers.
func windowFilter(ts, now time.Time, window time.Duration) bool {
	if window <= 0 {
		return true
	}
	if ts.After(now) {
		return false
	}
	return now.Sub(ts) <= window
}

// outcomeBucket classifies an outcome for the accounting of the statistics
// scan. The separation is the core of the saturation signal: saturation and
// service failure are different things and each one feeds a single signal.
//
//   - success feeds the total, the successes and the population of the
//     percentile.
//   - failure, which is a failure or a timeout, means the node had a chance
//     and did not deliver. It feeds the total and lowers the success rate,
//     which is the signal of reliability and the only one that the veto
//     consumes.
//   - saturation, with the exceeded budget, the capacity rejection and the
//     admission timeout, means the request was not served in time, either
//     because the arbiter gave up, because the node refused for lack of
//     room, or because the deadline expired before the request got an
//     instance. None of them is a measurement of the reliability of the
//     node, since in all of them the function did not run. They stay out of
//     the total and of the success rate and go to the saturation counter.
//     Accounting them as a failure made the veto punish the node for being
//     saturated.
//   - an unknown outcome is never a measurement of the node.
//
// The distinction that matters: a timeout here is the timeout of the
// function, either the real timeout of the node or the deadline with budget
// to spare, where the node was running and did not answer, while the
// exceeded budget is the arbiter giving up first and the capacity rejection
// is the node refusing on purpose. It is pure.
//
// Inside saturation there is a second distinction, and it has a consumer:
// the exceeded budget is a decision of the budget of the arbiter, and the
// latency of the cut is the remaining slice and not a cost of the node, so
// using it to punish the node would punish it for the decision of the
// arbiter itself. The capacity rejection and the admission timeout are the
// node saying that it cannot cope, an honest refusal and an admission
// starvation. That is why the saturation counts all three, as the total
// observable pressure, and the node saturation counts only the two of the
// node, which is the signal of the absolution floor.
type outcomeBucket uint8

const (
	bucketExcluded outcomeBucket = iota
	bucketSuccess
	bucketFailure
	bucketSaturation
)

func classifyOutcomeBucket(outcome string) outcomeBucket {
	switch outcome {
	case OutcomeSucesso:
		return bucketSuccess
	case OutcomeFalha, OutcomeTimeout:
		return bucketFailure
	case OutcomeOrcamentoExcedido, OutcomeRejeicaoCapacidade, OutcomeTimeoutAdmissao:
		return bucketSaturation
	default:
		return bucketExcluded
	}
}

// nodeSideSaturation reports whether the node is the author of the
// saturation outcome: it refused for lack of room, or the request did not
// get an instance before the deadline. The exceeded budget stays out on
// purpose, since it is the arbiter giving up the attempt and the recorded
// latency is the remaining slice and not a cost of the node. Using the
// budget cut to demote would punish the node for the decision of the
// arbiter, the same misattribution that the veto no longer makes. The
// absolution floor consumes this subset.
func nodeSideSaturation(outcome string) bool {
	switch outcome {
	case OutcomeRejeicaoCapacidade, OutcomeTimeoutAdmissao:
		return true
	default:
		return false
	}
}

// statsAccum is the widened result of the single scan core: it always
// carries the mixed aggregate and, when the classes are requested, the solo,
// contended and cold classes. The statistics queries without classes discard
// them without paying for them.
//
// The fields are ordered from the largest to the smallest for the alignment
// of eight bytes.
type statsAccum struct {
	P95MS          int64
	SoloP95MS      int64
	ContP95MS      int64
	ColdMeanMS     int64
	SuccessRate    float64
	SoloCPUPctMean float64
	Count          int
	Recent         int
	Total          int
	Saturation     int
	NodeSaturation int
	SoloN          int
	ContN          int
	ColdN          int
	SoloCPUSamples int
	OK             bool
}

// scanStatsLocked is the single linear core shared by the statistics
// queries, with never two implementations of the percentile that could
// diverge. It allocates nothing beyond the latency slices of the requested
// classes:
//
//   - The percentile has a window, an intentional break with the all-time
//     behavior: with a positive window only the entries not after now and
//     within the age participate, and the recent count and the percentile
//     share the same window, so a segment with only stale data behaves as a
//     cold segment and falls into the bootstrap by the hard limit. A
//     non-positive window preserves the all-time behavior.
//   - The latencies and the count cover only entries with a success and
//     without a cold start, since the percentile reflects the cost of a
//     success and the minimum number of samples counts warm successes before
//     the segment becomes ready. A fast failure would lower the percentile,
//     making a sick node look fast, while a long timeout would inflate it,
//     vetoing a healthy node.
//   - The total and the success rate are the signal of reliability that the
//     veto consumes: the total counts only the service outcomes in the
//     window, which are the success, the failure and the timeout of the
//     function, where the node had a chance and did not deliver. The success
//     rate is always smoothed, even without a ready percentile, so that the
//     veto bites with a zero count. The exceeded budget and the capacity
//     rejection do not enter, since they are saturation and go to their own
//     counter, and before this separation they entered the total and the
//     veto punished a saturated node.
//   - The saturation counts the exceeded budget, the capacity rejection and
//     the admission timeout in the window. It is its own signal, without a
//     consumer in the veto, and it is the total pressure that the overload
//     filter uses. Counting it is what keeps a saturated node visible after
//     it leaves the rate.
//   - The node saturation is the subset of the saturation with the node as
//     the author, which is the capacity rejection and the admission timeout.
//     It is the signal of the absolution floor, which knows that the node is
//     not delivering. Without this separation the floor demoted a healthy
//     node cut by the budget of the arbiter, and in the campaigns every
//     firing of the floor at the edge was an exceeded budget, where the node
//     was executing and the arbiter was giving up.
//   - The recent count counts every success within the window, including a
//     cold start, the same semantics as the recent successes, and a
//     non-positive window gives zero recent.
//
// Without the classes only the mixed aggregate is produced. With the classes
// the scan adds them, under the same window and the same gate of outcome as
// the aggregate:
//   - solo: warm successes with exactly one request in flight, which feed
//     the count and the percentile of the solo class, and a positive average
//     processor usage also feeds its mean and its number of samples.
//   - contended: warm successes with exactly two requests in flight.
//   - cold: a cold start with a success, retained with its latency.
//   - An unknown number of requests in flight and a count of three or more
//     stay out of every class, but remain in the mixed percentile.
//
// The caller holds at least the read lock. There is no cache, since
// correctness wins over performance: the percentile depends on the window,
// and an all-time cache would return a stale value.
func scanStatsLocked(entries []Entry, window time.Duration, now time.Time, wantClasses bool) statsAccum {
	windowed := window > 0
	var lats, soloLats, contLats []int64
	coldSum := int64(0)
	successes := 0
	var acc statsAccum
	for _, e := range entries {
		if windowed && !windowFilter(e.Timestamp, now, window) {
			continue
		}
		switch classifyOutcomeBucket(e.Outcome) {
		case bucketSuccess:
			acc.Total++
			successes++
			if windowed {
				acc.Recent++
			}
			if e.ColdStart {
				if wantClasses {
					acc.ColdN++
					coldSum += e.ObservedLatencyMS
				}
				continue
			}
			lats = append(lats, e.ObservedLatencyMS)
			if wantClasses {
				switch e.Inflight {
				case 1:
					acc.SoloN++
					soloLats = append(soloLats, e.ObservedLatencyMS)
					if e.CPUPctAvg > 0 {
						acc.SoloCPUSamples++
						acc.SoloCPUPctMean += e.CPUPctAvg
					}
				case 2:
					acc.ContN++
					contLats = append(contLats, e.ObservedLatencyMS)
				}
			}
		case bucketFailure:
			acc.Total++
		case bucketSaturation:
			// Saturation: the request was not served in time, either
			// because the arbiter gave up or because the node refused for
			// lack of room. It stays out of the total and of the success
			// rate on purpose, since contaminating the rate made the veto
			// ban the node for being saturated. A zero count is possible and
			// correct, since the segment exists without a sample of
			// reliability.
			acc.Saturation++
			// The node saturation is the subset with the node as the
			// author, an honest refusal or an admission starvation, and it
			// is what the absolution floor consumes. The budget cut stays
			// out, since demoting for it would punish the node for the
			// decision of the arbiter.
			if nodeSideSaturation(e.Outcome) {
				acc.NodeSaturation++
			}
		default:
			// An unknown outcome stays out of the total, of the saturation
			// and of the percentile, since it is not a measurement of the
			// node.
		}
	}
	acc.SuccessRate = LaplaceRate(successes, acc.Total)
	if len(lats) > 0 {
		slices.Sort(lats)
		acc.Count = len(lats)
		acc.P95MS = p95DeOrdenados(lats)
		acc.OK = true
	}
	if wantClasses {
		if len(soloLats) > 0 {
			slices.Sort(soloLats)
			acc.SoloP95MS = p95DeOrdenados(soloLats)
		}
		if len(contLats) > 0 {
			slices.Sort(contLats)
			acc.ContP95MS = p95DeOrdenados(contLats)
		}
		if acc.ColdN > 0 {
			acc.ColdMeanMS = coldSum / int64(acc.ColdN)
		}
		if acc.SoloCPUSamples > 0 {
			acc.SoloCPUPctMean /= float64(acc.SoloCPUSamples)
		}
	}
	return acc
}
