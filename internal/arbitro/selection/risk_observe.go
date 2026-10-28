// Feeding of the selector: the mapping from a history entry to an observation,
// the replay at boot and the live observation hook. The schema of the entry
// gains no new field.
package selection

import (
	"strings"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
)

// RiskObsFromEntry maps an entry to an observation. An ignored observation
// covers the exceeded budget, the capacity rejection, the admission timeout
// and the unknown outcomes, which are a decision of the arbiter, a saturation
// or strange data, and never a measurement of the node.
func RiskObsFromEntry(e history.Entry) (learn.Obs, bool) {
	switch e.Outcome {
	case history.OutcomeOrcamentoExcedido,
		history.OutcomeRejeicaoCapacidade,
		history.OutcomeTimeoutAdmissao:
		return learn.Obs{}, false
	case history.OutcomeTimeout, history.OutcomeFalha:
		// A timeout enters as a failure of reliability and does not enter the
		// regressor, which is a known optimistic bias.
		return learn.Obs{At: e.Timestamp, Outcome: learn.OutcomeFailure}, true
	case history.OutcomeSucesso:
		// Below.
	default:
		return learn.Obs{}, false
	}
	o := learn.Obs{At: e.Timestamp, Outcome: learn.OutcomeSuccess}
	if e.ColdStart {
		// The response is the natural logarithm of the observed latency, which already includes the cold start.
		o.Cold = true
		o.LatencyMS = float64(e.ObservedLatencyMS)
		return o, true
	}
	// The response is the natural logarithm of the latency without the acquisition wait, floored at one.
	lat := e.ObservedLatencyMS - e.AcquireWaitMS
	if lat < 1 {
		lat = 1
	}
	o.LatencyMS = float64(lat)
	// The effective servers are the smaller of the requests in flight and the
	// available instances, floored at one, when both are known, and the
	// requests in flight alone otherwise.
	m := e.Inflight
	if e.Inflight > 0 && e.InstancesAvailable > 0 {
		m = min(e.Inflight, e.InstancesAvailable)
	}
	if m < 1 {
		m = 1
	}
	o.M = float64(m)
	// Without the acquisition wait, and without a way to separate the wait
	// from the service, because the requests in flight are unknown or above
	// the available instances, the observation counts in the reliability but
	// does not enter the regressor.
	o.UseRegr = !(e.AcquireWaitMS == 0 && (e.Inflight <= 0 || e.Inflight > e.InstancesAvailable))
	return o, true
}

// groupOfHistoryKey extracts the group from the primary key of the group, the node and the bucket.
func groupOfHistoryKey(key string) string {
	g, _, ok := strings.Cut(key, "\x00")
	if !ok || g == "" || history.IsAggLayerKey(key) {
		return ""
	}
	return g
}

// ReplayRiskHistory rebuilds the learner from the history at boot. It skips the
// aggregated keys, since the same entry is in the fan-out and would be counted
// twice, and the ignored observations. An entry already superseded does not
// appear in the iteration, since the replay never supersedes.
func ReplayRiskHistory(h HistorySource, l LearnerPort) {
	if h == nil || l == nil {
		return
	}
	h.ForEachEntry(func(key string, e history.Entry) {
		group := groupOfHistoryKey(key)
		if group == "" {
			return
		}
		if o, ok := RiskObsFromEntry(e); ok {
			l.Observe(group, e.NodeID, o)
		}
	})
}

// AttachRiskObserver wires the observation hook, which is the single feeding
// point, since the recording of an entry and the publication of a settlement
// funnel into the append of the history, without any change in the gateway.
// The real outcome of the losers arrives through the same append after the
// exceeded budget was superseded, so there is no double counting.
func AttachRiskObserver(h HistorySource, l LearnerPort) {
	if h == nil || l == nil {
		return
	}
	h.SetObserver(func(group string, e history.Entry) {
		if o, ok := RiskObsFromEntry(e); ok {
			l.Observe(group, e.NodeID, o)
		}
	})
}
