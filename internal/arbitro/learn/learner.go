// Learner keeps one model per (group, node) pair in a sharded map.
//
// Observe, Predict and Snapshot take constant time per model. The map uses a
// mutex per shard and each model has its own mutex; the lock order is always
// shard then model, and the shard lock is released before a model is
// touched.
package learn

import (
	"hash/fnv"
	"sync"
	"time"
)

const learnerShards = 16

type modelKey struct {
	group, node string
}

type learnerShard struct {
	mu sync.Mutex
	m  map[modelKey]*Model
}

// Learner holds the models indexed by (group, node). The zero value is not
// usable: use NewLearner.
type Learner struct {
	cfg    Config
	shards [learnerShards]learnerShard
}

// NewLearner returns a learner with the given configuration, clamped to its
// valid domains. The caller is responsible for validating it.
func NewLearner(cfg Config) *Learner {
	l := &Learner{cfg: cfg.withDefaults()}
	for i := range l.shards {
		l.shards[i].m = make(map[modelKey]*Model)
	}
	return l
}

func (l *Learner) shardFor(group, node string) *learnerShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(group))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(node))
	return &l.shards[h.Sum32()%learnerShards]
}

// modelFor returns the existing model or creates one with the resolved
// prior. The caller must not hold a shard lock, because creating a model
// calls PriorFunc.
func (l *Learner) modelFor(group, node string) *Model {
	sh := l.shardFor(group, node)
	key := modelKey{group, node}
	sh.mu.Lock()
	if m, ok := sh.m[key]; ok {
		sh.mu.Unlock()
		return m
	}
	sh.mu.Unlock()
	m := NewModel(l.warmPriorFor(group, node), l.coldPriorFor(group, node), l.cfg.Risk)
	sh.mu.Lock()
	if prev, ok := sh.m[key]; ok {
		sh.mu.Unlock()
		return prev
	}
	sh.m[key] = m
	sh.mu.Unlock()
	return m
}

func (l *Learner) warmPriorFor(group, node string) NIGPrior {
	if l.cfg.Prior != nil {
		if p, ok := l.cfg.Prior(group, node); ok && p.ServiceMS > 0 {
			return DefaultWarmPrior(p.ServiceMS, p.Sigma)
		}
	}
	return WeakWarmPrior()
}

func (l *Learner) coldPriorFor(group, node string) NIGPrior {
	if l.cfg.Prior != nil {
		if p, ok := l.cfg.Prior(group, node); ok && (p.ColdMS > 0 || p.ServiceMS > 0) {
			return ColdPrior(p.ServiceMS, p.Sigma, p.ColdMS)
		}
	}
	return ColdPrior(0, 0, 0)
}

// Observe folds the observation into the model of (group, node).
func (l *Learner) Observe(group, node string, o Obs) {
	l.modelFor(group, node).Observe(o, l.cfg)
}

// Predict forecasts the outcome for (group, node) given the live state.
func (l *Learner) Predict(now time.Time, group, node string, s LiveState) Prediction {
	return l.modelFor(group, node).Predict(now, s, l.cfg)
}

// ModelDump is the snapshot of one (group, node) pair reported by the node
// listing endpoint. The median and upper bound are the warm values for a
// single request, and nu is twice the warm shape parameter.
type ModelDump struct {
	Group    string  `json:"group"`
	Node     string  `json:"node"`
	MedianMS float64 `json:"median_ms"`
	UpperMS  float64 `json:"upper_ms"`
	Nu       float64 `json:"nu"`
	Neff     float64 `json:"n_eff"`
	Alpha    float64 `json:"alpha_t"`
	PSucc    float64 `json:"p_succ"`
	NeffBeta float64 `json:"n_eff_beta"`
	ColdNeff float64 `json:"cold_n_eff"`
}

// Snapshot returns the snapshot of every model.
func (l *Learner) Snapshot(now time.Time) []ModelDump {
	solo := LiveState{KFn: 0, KNode: 0, Servers: 1, Alive: 1}
	var out []ModelDump
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		models := make([]*Model, 0, len(sh.m))
		keys := make([]modelKey, 0, len(sh.m))
		for k, m := range sh.m {
			keys = append(keys, k)
			models = append(models, m)
		}
		sh.mu.Unlock()
		for j, m := range models {
			p := m.Predict(now, solo, l.cfg)
			m.mu.Lock()
			_, a, _, _ := m.warm.Posterior(now, l.cfg.HalfLife)
			coldNeff := m.cold.Neff()
			m.mu.Unlock()
			out = append(out, ModelDump{
				Group: keys[j].group, Node: keys[j].node,
				MedianMS: p.MedianMS, UpperMS: p.UpperMS,
				Nu: 2 * a, Neff: p.Neff,
				Alpha: m.Alpha(l.cfg.Risk),
				PSucc: p.PSucc, NeffBeta: p.NeffBeta,
				ColdNeff: coldNeff,
			})
		}
	}
	return out
}
