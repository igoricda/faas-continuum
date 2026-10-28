package gateway

import (
	"github.com/igoricda/faas-continuum/internal/arbitro/monitor"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// ingestCapacity is the ingestion of the piggyback together with the
// optimistic marking of the capacity. It is called by the record of the
// attempt for every attempt against an edge or fog node. A nil monitor
// makes it do nothing, which is the case of a normal successful
// invocation without gating. The capacity map was already parsed by the
// caller and decoded once, so nothing is decoded again here, and a nil map
// means nothing to merge. The clock of the gateway gives the instant of the
// reception at the arbiter. With no error, which is a successful answer,
// every entry present in the map is merged through the update of the
// capacity, with the keys already concrete, since the producer reports by
// the name of the function. A parsed capacity rejection marks the resolved
// concrete variant as exhausted with the same key as the producer, which
// covers the silence of a piggyback that only exists on success under
// exhaustion. An empty concrete name, in a legacy call or a test, falls
// back to the requested name. Any other error does nothing.
func (g *Gateway) ingestCapacity(target nodes.Node, requested, concrete string, caps map[string]telemetry.FunctionCapacity, err error) {
	if g.mon == nil {
		return
	}
	recvNow := g.now()
	if err == nil {
		var hosted []string
		var ceilings map[string]int
		if g.nodes != nil {
			if rec, ok := g.nodes.Get(target.NodeID); ok {
				hosted, ceilings = rec.Functions, rec.FunctionCapacity
			}
		}
		for fn, cap := range caps {
			if clean, ok := monitor.SanitizeForNode(hosted, ceilings, fn, cap); ok {
				g.mon.UpdateCapacity(target.NodeID, fn, clean, recvNow)
			}
		}
		return
	}
	if IsCapacityRejection(err) {
		if concrete == "" {
			concrete = requested
		}
		g.mon.MarkExhausted(target.NodeID, concrete, recvNow)
	}
}
