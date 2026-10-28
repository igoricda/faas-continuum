// RiskSelector: selection by calibrated risk, the only canonical selector of
// the arbiter. There is a single statistical predictor in place of the bucket,
// the inheritance, the curves, the contention factor, the cold factor, the
// adaptive factor, the veto by rate and the absolution.
//
// The correct choice on the first attempt is the primary mechanism, and the
// fan-out is the safety net. The guiding metric is the rate of correct first
// choices.
package selection

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// RiskConfig parameterizes the risk selector. There are few hyperparameters,
// all of them with a derivation or a calibration. The risk epsilon is the
// target of miscoverage of the adaptive conformal inference, and in the
// configuration of the learner the corresponding field is named simply risk,
// in the vocabulary of the statistical engine and without the prefix of the
// selector.
type RiskConfig struct {
	HalfLife       time.Duration
	RiskEpsilon    float64
	ACIGamma       float64
	NMin           float64
	Explore        float64
	Stretch        float64
	StretchSLO     float64
	Band           float64
	QueueAllowance float64
	DefaultServers int
	MinSuccess     float64
	// There is no reservation: an atomic reservation would only be needed if
	// the concurrency test failed without it, and it passed, so there is no
	// dead flag.
}

// DefaultRiskConfig returns the defaults, fine-tuned in the simulation.
func DefaultRiskConfig() RiskConfig {
	return RiskConfig{
		HalfLife: 10 * time.Minute, RiskEpsilon: 0.05, ACIGamma: 0.01, NMin: 3,
		Explore: 0.05, Stretch: 2.0, StretchSLO: 0, Band: 0.10,
		QueueAllowance: 1.0, DefaultServers: 1, MinSuccess: 0.5,
	}
}

// Validate applies the domains of the hyperparameters and fails fast in the caller.
func (c RiskConfig) Validate() error {
	if c.HalfLife <= 0 {
		return fmt.Errorf("risk: halflife must be > 0")
	}
	if c.RiskEpsilon <= 0 || c.RiskEpsilon > 0.5 {
		return fmt.Errorf("risk: risk must be in (0, 0.5]")
	}
	if c.ACIGamma <= 0 || c.ACIGamma > 0.1 {
		return fmt.Errorf("risk: aci-gamma must be in (0, 0.1]")
	}
	if c.NMin < 2 {
		return fmt.Errorf("risk: nmin must be >= 2")
	}
	if c.Explore < 0 || c.Explore > 0.5 {
		return fmt.Errorf("risk: explore must be in [0, 0.5]")
	}
	if c.Stretch < 1 {
		return fmt.Errorf("risk: stretch must be >= 1")
	}
	if c.StretchSLO != 0 && c.StretchSLO < 1 {
		return fmt.Errorf("risk: stretch-slo must be 0 or >= 1")
	}
	if c.Band < 0 || c.Band >= 1 {
		return fmt.Errorf("risk: band must be in [0, 1)")
	}
	if c.QueueAllowance < 0 {
		return fmt.Errorf("risk: queue-allowance must be >= 0")
	}
	if c.DefaultServers < 1 {
		return fmt.Errorf("risk: default-servers must be >= 1")
	}
	if c.MinSuccess < 0 || c.MinSuccess >= 1 {
		return fmt.Errorf("risk: min-success must be in [0, 1)")
	}
	return nil
}

// LearnConfig derives the configuration of the learner, with the half-life, the
// risk, the adaptive conformal inference and the minimum number of samples.
func (c RiskConfig) LearnConfig() learn.Config {
	return learn.Config{HalfLife: c.HalfLife, Risk: c.RiskEpsilon, ACIGamma: c.ACIGamma, NMin: c.NMin}
}

// effectiveCriteriaOf resolves the criteria, falling back to the defaults when
// they are nil, while an explicitly empty slice disables the criteria.
func effectiveCriteriaOf(c []SLOCriterion) []SLOCriterion {
	if c != nil {
		return c
	}
	return DefaultCriteria
}

// effectivePSIVetoOf resolves the thresholds of the full pressure veto, where
// a zeroed structure means the defaults.
func effectivePSIVetoOf(t PSIVetoThresholds) PSIVetoThresholds {
	if t == (PSIVetoThresholds{}) {
		return DefaultPSIVetoThresholds()
	}
	return t
}

// RiskSelector predicts a calibrated upper bound per candidate and orders by
// the cold start, the layer and the ratio of the bound to the success
// probability. The receiver is a pointer, since the observational state is
// shared, with the statistics and the random source under the mutex, and the
// signature is identical to the selector interface.
type RiskSelector struct {
	// Dependencies of the deterministic shield: the same as the queue selector,
	// the base, plus the readers of the requests in flight and of the
	// projection of the gateway.
	Base               QueueSelector
	Hist               HistorySource
	Clock              func() time.Time
	InflightOf         func(nodeID string) int
	InflightOfFn       func(nodeID, function string) int
	ProjectedCostSince func(nodeID string, since time.Time) float64
	PSIVetoThresholds  PSIVetoThresholds
	Criteria           []SLOCriterion
	ThroughputWindow   time.Duration
	Learner            LearnerPort
	Cfg                RiskConfig
	// The random source is injectable for the tests of the band and of the
	// exploration, and nil makes the execution deterministic, with the band
	// taking the first candidate and the exploration disabled. The access is
	// under the mutex, since the generator is not safe for concurrent use and
	// the handler is concurrent.
	Rand  *rand.Rand
	Stats *RiskStats
	mu    sync.Mutex
}

// clockOrNow returns the time of the injected clock or the current time.
func (s *RiskSelector) clockOrNow() time.Time {
	if s.Clock != nil {
		return s.Clock()
	}
	return time.Now()
}

// bump increments a counter of the statistics when they are wired, and a nil receiver does nothing.
func (s *RiskSelector) bump(f func(*RiskStats)) {
	if s.Stats != nil {
		f(s.Stats)
	}
}

// drawFloat returns a value in the unit interval under the mutex. Without a
// random source it returns one, which disables the exploration and is ignored
// by the deterministic band.
func (s *RiskSelector) drawFloat() float64 {
	if s.Rand == nil {
		return 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Rand.Float64()
}

// riskThroughputKeeps is the additional filter of a declared throughput, which
// reuses the throughput criterion without reimplementing it. Without statistics
// of the rate in the risk selector the evaluation fails open and keeps the
// candidate, and it is the extension point for when the selector tracks the
// rate.
func riskThroughputKeeps(criteria []SLOCriterion, slo *SLO) bool {
	if slo == nil || slo.MinThroughputRPS <= 0 {
		return true
	}
	for _, c := range criteria {
		if c == nil || !c.Declared(slo) {
			continue
		}
		if _, ok := c.(ThroughputCriterion); !ok {
			continue
		}
		if apt, dataOk := c.Segment(slo, SegmentStats{}); dataOk && !apt {
			return false
		}
	}
	return true
}

// riskScored carries a candidate, its prediction and its live state.
type riskScored struct {
	n    nodes.Node
	p    learn.Prediction
	live learn.LiveState
}

// riskLiveInfo carries the live state and the cold start flag per node.
type riskLiveInfo struct {
	live learn.LiveState
	cold bool
	c    int
}

// riskCandidate carries the decision state of a viable candidate.
type riskCandidate struct {
	node  nodes.Node
	live  learn.LiveState
	cold  bool
	c     int
	ratio float64 // B/P_succ
	b     float64 // B
}

// Select implements the selector: the deterministic shield, the prediction, the
// effective target, the viable candidates and the ordering by cold start, layer
// and ratio of the bound to the success probability, with a random band and the
// probation at the end. Without an objective the target is relative to the best
// node, through the stretch over the best bound, which is what prevents sending
// everything to the edge.
func (s *RiskSelector) Select(candidates []nodes.Node, functionName string, slo *SLO) []nodes.Node {
	cfg := s.Cfg
	now := s.clockOrNow()
	group := ResolveRequestGroup(functionName, candidates)
	hosted := hostingFilter(candidates, functionName, group)
	hosted, reqs, qualityDeclared := applyQualityPartition(hosted, functionName, slo)
	capacityNameByNode := capacityNamesByNode(hosted, functionName, reqs, qualityDeclared)
	latest := latestSnapshot(s.Base.Mon, hosted)
	limits := s.Base.Cfg
	criteria := effectiveCriteriaOf(s.Criteria)

	// Deterministic shield, in the same order of the helpers as the previous selector.
	projectedByNode := make(map[string]float64, len(hosted))
	aptList := make([]nodes.Node, 0, len(hosted))
	psiVetoed := make([]nodes.Node, 0, len(hosted))
	projectedDropped := 0
	for _, n := range hosted {
		_, _, _, projected, _ := projectedLoad(s.Base.Mon, s.Hist, s.ProjectedCostSince, group, n.NodeID, s.ThroughputWindow, now)
		projectedByNode[n.NodeID] = projected
		threshold := limits.For(n.NodeID).MaxCPUPct
		if projected > threshold {
			log.Printf("arbitro: selection node=%s result=PROJECTED_OVERLOAD projected=%.2f threshold=%.2f", n.NodeID, projected, threshold)
			s.bump(func(st *RiskStats) { st.ProjectedExcluded.Add(1) })
			projectedDropped++
			continue
		}
		if th := effectivePSIVetoOf(s.PSIVetoThresholds); func() bool {
			if s.Base.Mon == nil {
				return false
			}
			pkg, _, ok := s.Base.Mon.LatestAt(n.NodeID)
			return PSIVetoFullFromPackage(pkg, ok, th)
		}() {
			psiVetoed = append(psiVetoed, n)
			continue
		}
		aptList = append(aptList, n)
	}
	// Fail-open of the pressure veto: it restores the set only when the veto
	// emptied it.
	if len(aptList) == 0 && projectedDropped == 0 && len(psiVetoed) > 0 {
		aptList = append(aptList, psiVetoed...)
	}
	variantsByNode := capacityVariantsByNode(aptList, functionName)
	aptList, caps := filterExhaustedVariantsFromMonitor(s.Base.Mon, aptList, capacityNameByNode, variantsByNode, now)
	aptList = filterProjectedOverloaded(aptList, projectedByNode, limits)
	aptList = FilterOverloaded(aptList, latest, limits)

	// Live state and static ceiling.
	liveByNode := make(map[string]riskLiveInfo, len(aptList))
	kept := aptList[:0]
	for _, n := range aptList {
		kFn, kNode := 0, 0
		if s.InflightOfFn != nil {
			kFn = s.InflightOfFn(n.NodeID, functionName)
		}
		if s.InflightOf != nil {
			kNode = s.InflightOf(n.NodeID)
		}
		live, cold, c := liveStateForNode(caps[n.NodeID], kFn, kNode, cfg.DefaultServers)
		if kFn+1 > riskCapCap(c, cfg.QueueAllowance) {
			s.bump(func(st *RiskStats) { st.CapExcluded.Add(1) })
			continue
		}
		liveByNode[n.NodeID] = riskLiveInfo{live: live, cold: cold, c: c}
		kept = append(kept, n)
	}
	aptList = kept
	if len(aptList) == 0 {
		s.bump(func(st *RiskStats) { st.EmptyQueue.Add(1) })
		s.logDecision(group, functionName, slo, 0, 0, nil, nil, "empty")
		return nil
	}
	if s.Learner == nil {
		s.bump(func(st *RiskStats) { st.EmptyQueue.Add(1) })
		s.logDecision(group, functionName, slo, 0, 0, nil, nil, "no-learner")
		return nil
	}

	// Prediction.
	scoredList := make([]riskScored, 0, len(aptList))
	for _, n := range aptList {
		li := liveByNode[n.NodeID]
		p := s.Learner.Predict(now, group, n.NodeID, li.live)
		scoredList = append(scoredList, riskScored{n: n, p: p, live: li.live})
	}

	// Bootstrap: no candidate outside the probation, ordered by layer, by the requests in flight and by the processor usage.
	var viable []riskScored
	var probation []riskScored
	for _, sc := range scoredList {
		if sc.p.Probation {
			probation = append(probation, sc)
		} else {
			viable = append(viable, sc)
		}
	}
	s.bump(func(st *RiskStats) { st.Probation.Add(uint64(len(probation))) })
	if len(viable) == 0 {
		s.bump(func(st *RiskStats) { st.Bootstrap.Add(1) })
		out := orderBootstrap(probation, latest)
		s.bumpColdChosen(out, liveByNode)
		s.logDecision(group, functionName, slo, 0, 0, out, scoredByID(scoredList), "bootstrap")
		return out
	}

	// Effective target.
	bBest := viable[0].p.UpperMS
	for _, sc := range viable[1:] {
		if sc.p.UpperMS < bBest {
			bBest = sc.p.UpperMS
		}
	}
	hasSLO := slo != nil && slo.MaxLatencyMS > 0
	var tEff float64
	switch {
	case !hasSLO:
		tEff = cfg.Stretch * bBest
	case cfg.StretchSLO > 0:
		tEff = min(cfg.StretchSLO*bBest, float64(slo.MaxLatencyMS))
	default:
		tEff = float64(slo.MaxLatencyMS)
	}

	// Viable candidates.
	cands := make([]riskCandidate, 0, len(viable))
	for _, sc := range viable {
		if sc.p.UpperMS > tEff*(1+1e-9) {
			s.bump(func(st *RiskStats) {
				if hasSLO {
					st.SLOExcluded.Add(1)
				} else {
					st.StretchExcluded.Add(1)
				}
			})
			continue
		}
		if !(sc.p.NeffBeta < 5 || sc.p.PSucc >= cfg.MinSuccess) {
			s.bump(func(st *RiskStats) {
				if hasSLO {
					st.SLOExcluded.Add(1)
				} else {
					st.StretchExcluded.Add(1)
				}
			})
			continue
		}
		ratio := sc.p.UpperMS / sc.p.PSucc
		cands = append(cands, riskCandidate{node: sc.n, live: sc.live, cold: sc.p.Cold, c: liveByNode[sc.n.NodeID].c, ratio: ratio, b: sc.p.UpperMS})
	}
	if hasSLO && len(cands) == 0 && len(probation) == 0 {
		// Parity with the previous criterion of the predicted load over the
		// objective, which sends an empty queue to the cloud: it applies only
		// when there is no probation in sight, and with a probation it stays at
		// the end, which is the bootstrap of the previous selector that the
		// fail-open filter preserves.
		s.bump(func(st *RiskStats) { st.EmptyQueue.Add(1) })
		s.logDecision(group, functionName, slo, tEff, bBest, nil, scoredByID(scoredList), "slo-empty")
		return nil
	}
	if !riskThroughputKeeps(criteria, slo) {
		s.bump(func(st *RiskStats) { st.EmptyQueue.Add(1) })
		s.logDecision(group, functionName, slo, tEff, bBest, nil, scoredByID(scoredList), "throughput-empty")
		return nil
	}
	s.bump(func(st *RiskStats) { st.Feasible.Add(uint64(len(cands))) })

	// Ordering by cold start, layer and ratio of the bound to the success probability, with a band for the ties.
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].cold != cands[j].cold {
			return !cands[i].cold
		}
		ri, rj := layerRank(cands[i].node.Layer), layerRank(cands[j].node.Layer)
		if ri != rj {
			return ri < rj
		}
		return cands[i].ratio < cands[j].ratio
	})
	ordered := make([]nodes.Node, 0, len(cands)+len(probation))
	for i := 0; i < len(cands); {
		j := i + 1
		for j < len(cands) &&
			cands[j].cold == cands[i].cold &&
			layerRank(cands[j].node.Layer) == layerRank(cands[i].node.Layer) &&
			cands[j].ratio <= (1+cfg.Band)*cands[i].ratio {
			j++
		}
		ordered = append(ordered, s.pickBandFirst(cands[i:j])...)
		i = j
	}
	// The probation goes after every viable candidate.
	ordered = append(ordered, orderBootstrap(probation, latest)...)

	// Exploration ahead of the queue: with the exploration probability, an idle
	// probation, without requests in flight in the node or in the function, is
	// promoted to the first place, only without an objective or with a target at
	// least three times the best bound, and only with a random source wired.
	if cfg.Explore > 0 && s.Rand != nil && len(probation) > 0 &&
		(!hasSLO || float64(slo.MaxLatencyMS) >= 3*bBest) &&
		s.drawFloat() < cfg.Explore {
		for _, sc := range probation {
			if sc.live.KFn == 0 && sc.live.KNode == 0 {
				front := sc.n
				rest := make([]nodes.Node, 0, len(ordered))
				for _, n := range ordered {
					if n.NodeID != front.NodeID {
						rest = append(rest, n)
					}
				}
				ordered = append([]nodes.Node{front}, rest...)
				s.bump(func(st *RiskStats) { st.ExploreFront.Add(1) })
				break
			}
		}
	}

	s.bumpColdChosen(ordered, liveByNode)
	s.logDecision(group, functionName, slo, tEff, bBest, ordered, scoredByID(scoredList), "ok")
	return ordered
}

// pickBandFirst draws the first candidate of the band with a weight inversely
// proportional to the ratio of the bound to the success probability and returns
// it followed by the others in order. A unitary band, or the absence of a
// random source, takes the first candidate.
func (s *RiskSelector) pickBandFirst(band []riskCandidate) []nodes.Node {
	out := make([]nodes.Node, 0, len(band))
	if len(band) == 0 {
		return out
	}
	idx := 0
	if len(band) > 1 && s.Rand != nil {
		s.mu.Lock()
		total := 0.0
		for _, c := range band {
			total += 1 / c.ratio
		}
		r := s.Rand.Float64() * total
		for i, c := range band {
			r -= 1 / c.ratio
			if r <= 0 {
				idx = i
				break
			}
		}
		s.mu.Unlock()
		if len(band) > 1 {
			s.bump(func(st *RiskStats) { st.BandRandom.Add(1) })
		}
	}
	out = append(out, band[idx].node)
	for i, c := range band {
		if i != idx {
			out = append(out, c.node)
		}
	}
	return out
}

// orderBootstrap orders by layer, by the requests in flight and by the
// processor usage, which is the bootstrap of the risk selector and the order of
// the probation.
func orderBootstrap(list []riskScored, latest map[string]telemetry.Package) []nodes.Node {
	cp := make([]riskScored, len(list))
	copy(cp, list)
	sort.SliceStable(cp, func(i, j int) bool {
		ri, rj := layerRank(cp[i].n.Layer), layerRank(cp[j].n.Layer)
		if ri != rj {
			return ri < rj
		}
		if cp[i].live.KFn != cp[j].live.KFn {
			return cp[i].live.KFn < cp[j].live.KFn
		}
		return cpuUsage(cp[i].n.NodeID, latest) < cpuUsage(cp[j].n.NodeID, latest)
	})
	out := make([]nodes.Node, 0, len(cp))
	for _, sc := range cp {
		out = append(out, sc.n)
	}
	return out
}

func (s *RiskSelector) bumpColdChosen(out []nodes.Node, liveByNode map[string]riskLiveInfo) {
	if len(out) == 0 {
		return
	}
	if li, ok := liveByNode[out[0].NodeID]; ok && li.cold {
		s.bump(func(st *RiskStats) { st.ColdChosen.Add(1) })
	}
}

// scoredByID indexes the bound and the success probability per node for the decision log.
func scoredByID(list []riskScored) map[string][2]float64 {
	out := make(map[string][2]float64, len(list))
	for _, sc := range list {
		out[sc.n.NodeID] = [2]float64{sc.p.UpperMS, sc.p.PSucc}
	}
	return out
}

// logDecision emits one line per decision.
func (s *RiskSelector) logDecision(group, function string, slo *SLO, tEff, bBest float64, out []nodes.Node, bp map[string][2]float64, reason string) {
	sloStr := "none"
	if slo != nil && slo.MaxLatencyMS > 0 {
		sloStr = fmt.Sprintf("%d", slo.MaxLatencyMS)
	}
	var sb strings.Builder
	sb.WriteByte('[')
	for i, n := range out {
		if i > 0 {
			sb.WriteByte(',')
		}
		if v, ok := bp[n.NodeID]; ok {
			fmt.Fprintf(&sb, "%s(B=%.0f,P=%.2f)", n.NodeID, v[0], v[1])
		} else {
			fmt.Fprintf(&sb, "%s", n.NodeID)
		}
	}
	sb.WriteByte(']')
	log.Printf("arbitro: selection risk group=%s fn=%s slo=%s t_eff=%.0f best=%.0f order=%s reason=%s",
		group, function, sloStr, tEff, bBest, sb.String(), reason)
}

// serviceUpperMS is the service time without the wait: the gateway sizes the
// slice and multiplies it by the rank at the gate, so returning the bound would
// count the queue twice. Without requests in flight and with one server the
// wait is zero and the upper bound is the service time. The result is absent
// without a learner or in probation, which makes the gateway fail open.
func (s *RiskSelector) serviceUpperMS(group string, n nodes.Node) (int64, bool) {
	if s.Learner == nil {
		return 0, false
	}
	k := 0
	if s.InflightOf != nil {
		k = s.InflightOf(n.NodeID)
	}
	p := s.Learner.Predict(s.clockOrNow(), group, n.NodeID, liveStateSolo(k))
	if p.Probation || p.UpperMS <= 0 {
		return 0, false
	}
	ms := int64(p.UpperMS + 0.5)
	if ms <= 0 {
		return 0, false
	}
	return ms, true
}

// PredictLatencyMS implements the latency predictor of the gateway, with a
// structural assertion: the service time, without the wait.
func (s *RiskSelector) PredictLatencyMS(group string, n nodes.Node, slo *SLO) (int64, bool) {
	return s.serviceUpperMS(group, n)
}

// PredictLatencyMSNewRequest implements the predictor of a new request: the
// service time, without the wait, since the gate already multiplies by the rank.
func (s *RiskSelector) PredictLatencyMSNewRequest(group string, n nodes.Node, slo *SLO) (int64, bool) {
	return s.serviceUpperMS(group, n)
}

// PredictMaxQueueK implements the capacity predictor of the gateway, with the
// same logic of the shared free function. It feeds the queue unviability of the
// gateway, while the selection uses the static ceiling.
func (s *RiskSelector) PredictMaxQueueK(group string, n nodes.Node, slo *SLO) (int, bool) {
	return maxQueueKForNode(s.Hist, group, n, s.clockOrNow())
}
