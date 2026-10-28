// Live state per candidate: it builds the live state of the learner from the
// capacity reading reused from the stage of the mark, without a second query,
// and from the live meter, with the number of requests in flight including the
// losers, since the decrement is delayed to the settlement.
package selection

import (
	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
)

// liveStateForNode builds the live state of a candidate. The input is the
// consolidated reading of the stage of the mark, while the counts of requests
// in flight come from the meter, at the level of the node and of the pair of
// node and requested function. The default number of servers applies when the
// maximum number of instances is unknown, with an open pool or without a
// reading, which is conservative and predicts the queue early. It returns the
// live state, the cold start flag and the effective number of servers.
func liveStateForNode(cr capacityRead, kFn, kNode int, defaultServers int) (learn.LiveState, bool, int) {
	if defaultServers < 1 {
		defaultServers = 1
	}
	c := defaultServers
	alive, queueTel := 0, 0
	telFresh := cr.ok
	if cr.ok {
		alive = cr.fc.InstancesAvailable
		queueTel = cr.fc.QueueDepth
		if cr.fc.MaxInstances > 0 {
			c = cr.fc.MaxInstances
		} else if cr.fc.InstancesAvailable > c {
			// Open pool: the effective servers are the maximum between the live instances and the default.
			c = cr.fc.InstancesAvailable
		}
	}
	if kFn < 0 {
		kFn = 0
	}
	if kNode < 0 {
		kNode = 0
	}
	if cr.mark {
		// The optimistic exhaustion mark means warm at the ceiling, so a zero
		// is not cold and the known queue is at least the effective servers.
		if kFn < c {
			kFn = c
		}
	} else if cr.ok && cr.fc.QueueDepth > 0 {
		if want := c + cr.fc.QueueDepth; kFn < want {
			kFn = want
		}
	}
	cold := predictedCold(cr.fc, cr.mark, cr.ok)
	return learn.LiveState{
		KFn: kFn, KNode: kNode, Servers: c, Alive: alive,
		QueueTel: queueTel, TelFresh: telFresh, Cold: cold,
	}, cold, c
}

// liveStateSolo is the synthetic live state of the gateway ports, with no
// requests in flight and one server, so the wait is zero and the upper bound
// is only the service time, without any queue.
func liveStateSolo(kNode int) learn.LiveState {
	if kNode < 0 {
		kNode = 0
	}
	return learn.LiveState{KNode: kNode, Servers: 1, Alive: 1, TelFresh: true}
}

// riskCapCap is the static admission ceiling, as the floor of the effective
// servers multiplied by one plus the allowance, which with the default
// allowance of one is twice the servers, coherent with the stretch of two.
// The gateway already applies the learned ceiling at the dispatch, and the
// static one exists so that the queue does not hold nodes without room.
func riskCapCap(c int, allowance float64) int {
	if c < 1 {
		c = 1
	}
	if allowance < 0 {
		allowance = 0
	}
	return int(float64(c) * (1 + allowance))
}
