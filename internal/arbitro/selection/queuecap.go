package selection

import (
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// PredictMaxQueueK implements the optional capacity predictor of the gateway:
// it returns the largest admissible concurrency for a new request in this
// node, learned from the delivery curve by queue depth. The first gap of
// depth in which the node shows a collapse, which is evidence of at least the
// minimum number of samples and a majority of failures under the smoothed
// rate, defines the ceiling as one below that depth, the last depth not
// refuted. A gap without evidence proves no limit and neither advances nor
// stops the scan, which fails open per gap, and without any bad gap with
// evidence the result is absent and the selector does not invent a ceiling,
// so the gate keeps the projection against the depth.
//
// The window is the retention of the history, where a zero window reads every
// entry in memory, already pruned by age. A ceiling of capacity must not
// expire together with the window of throughput, since bad evidence has to
// survive between runs. The expiration by retention reopens the exploration,
// which is how the ceiling recalibrates after a new regime: without evidence
// the node is attempted again and the next collapse recreates the ceiling.
//
// The objective is accepted by the signature of the port and does not change
// the ceiling, since the limit is of the capacity of the node and holds even
// without an objective, while the fit to the objective stays with the gate of
// viability.
//
// The helper of the learned ceiling is shared by the canonical selector, and
// the instant is explicit for a test without an injected clock.
func maxQueueKForNode(hist HistorySource, group string, n nodes.Node, now time.Time) (int, bool) {
	if hist == nil {
		return 0, false
	}
	curve, ok := hist.QueueCurve(group, n.NodeID, 0, now)
	if !ok {
		return 0, false
	}
	for _, b := range curve.Buckets {
		evidence := b.Successes + b.Failures
		if evidence < MinClassSamples {
			continue
		}
		rate := history.LaplaceRate(b.Successes, evidence)
		if history.VetoByRate(evidence, rate) {
			k := b.KMin - 1
			if k < 1 {
				k = 1
			}
			return k, true
		}
	}
	return 0, false
}
