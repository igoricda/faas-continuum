// Portas do gateway, validação de config e priors (SELETOR-V2 §9, §10).
package selection

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/learn"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
)

// TestRiskPorts: S_up sem espera (G2); probation => fail-open; queuecap
// delega à lógica compartilhada maxQueueKForNode (queuecap.go).
func TestRiskPorts(t *testing.T) {
	learner := learn.NewLearner(DefaultRiskConfig().LearnConfig())
	// Modelo aquecido: 5 sucessos de 800ms.
	for i := 0; i < 5; i++ {
		learner.Observe("fn", "n1", learn.Obs{At: fixtureTime, Outcome: learn.OutcomeSuccess, LatencyMS: 800, UseRegr: true, M: 1})
	}
	sel := &RiskSelector{
		Clock:   func() time.Time { return fixtureTime },
		Learner: learner,
		Cfg:     DefaultRiskConfig(),
	}
	n := nodes.Node{NodeID: "n1", Layer: nodes.LayerBorda, Host: "h", Functions: []string{"fn"}}
	ms, ok := sel.PredictLatencyMS("fn", n, nil)
	if !ok || ms < 800 || ms > 2000 {
		t.Fatalf("PredictLatencyMS = (%d, %v), want S_up ~800-2000", ms, ok)
	}
	ms2, ok := sel.PredictLatencyMSNewRequest("fn", n, nil)
	if !ok || ms2 != ms {
		t.Fatalf("NewRequest = (%d, %v), want igual a %d", ms2, ok, ms)
	}
	// S_up exclui a espera: com k alto, a porta não cresce com a fila
	// (a fila vive só no ranking do seletor, G2).
	busy := &RiskSelector{
		Clock:      func() time.Time { return fixtureTime },
		InflightOf: func(string) int { return 7 },
		Learner:    learner,
		Cfg:        DefaultRiskConfig(),
	}
	msBusy, ok := busy.PredictLatencyMS("fn", n, nil)
	if !ok {
		t.Fatalf("busy: ok=false")
	}
	full := learner.Predict(fixtureTime, "fn", "n1", learn.LiveState{KFn: 8, KNode: 7, Servers: 1, Alive: 1})
	if !(float64(msBusy) < full.UpperMS) {
		t.Fatalf("porta=%d, B com fila=%v: a porta deve excluir W", msBusy, full.UpperMS)
	}
	// Probation => fail-open (gateway usa a fatia sem previsão).
	fresh := &RiskSelector{Clock: func() time.Time { return fixtureTime }, Learner: learn.NewLearner(DefaultRiskConfig().LearnConfig()), Cfg: DefaultRiskConfig()}
	if _, ok := fresh.PredictLatencyMS("fn", n, nil); ok {
		t.Fatalf("probation: ok=true want false")
	}
	// Sem learner => fail-open.
	if _, ok := (&RiskSelector{}).PredictLatencyMS("fn", n, nil); ok {
		t.Fatalf("sem learner: ok=true want false")
	}
	// QueueCap sem histórico => fail-open (teto aprendido, queuecap.go).
	if _, ok := sel.PredictMaxQueueK("fn", n, nil); ok {
		t.Fatalf("queuecap sem hist: ok=true want false")
	}
}

// TestRiskConfigValidate cobre os domínios da §10.
func TestRiskConfigValidate(t *testing.T) {
	if err := DefaultRiskConfig().Validate(); err != nil {
		t.Fatalf("defaults: %v", err)
	}
	bad := func(mut func(*RiskConfig)) RiskConfig {
		c := DefaultRiskConfig()
		mut(&c)
		return c
	}
	cases := []RiskConfig{
		bad(func(c *RiskConfig) { c.HalfLife = 0 }),
		bad(func(c *RiskConfig) { c.RiskEpsilon = 0 }),
		bad(func(c *RiskConfig) { c.RiskEpsilon = 0.6 }),
		bad(func(c *RiskConfig) { c.ACIGamma = 0 }),
		bad(func(c *RiskConfig) { c.NMin = 1 }),
		bad(func(c *RiskConfig) { c.Explore = 0.6 }),
		bad(func(c *RiskConfig) { c.Stretch = 0.9 }),
		bad(func(c *RiskConfig) { c.StretchSLO = 0.5 }),
		bad(func(c *RiskConfig) { c.Band = 1 }),
		bad(func(c *RiskConfig) { c.QueueAllowance = -1 }),
		bad(func(c *RiskConfig) { c.DefaultServers = 0 }),
		bad(func(c *RiskConfig) { c.MinSuccess = 1 }),
	}
	for i, c := range cases {
		if err := c.Validate(); err == nil {
			t.Fatalf("caso %d: erro=nil want fail", i)
		}
	}
}

// TestLoadRiskPriors: exato, fallback por camada, vazio e inválido.
func TestLoadRiskPriors(t *testing.T) {
	if p, err := LoadRiskPriors(""); err != nil || len(p) != 0 {
		t.Fatalf("vazio: p=%v err=%v", p, err)
	}
	path := filepath.Join(t.TempDir(), "priors.json")
	raw := `{"fn": {"n1": {"service_ms": 900, "sigma": 0.4}, "borda": {"service_ms": 700, "cold_ms": 8000}}}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := LoadRiskPriors(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	layerOf := func(id string) string {
		if id == "n9" {
			return "borda"
		}
		return ""
	}
	fn := p.PriorFunc(layerOf)
	pr, ok := fn("fn", "n1")
	if !ok || pr.ServiceMS != 900 || pr.Sigma != 0.4 {
		t.Fatalf("exato: %+v ok=%v", pr, ok)
	}
	pr, ok = fn("fn", "n9")
	if !ok || pr.ServiceMS != 700 || pr.ColdMS != 8000 {
		t.Fatalf("camada: %+v ok=%v", pr, ok)
	}
	if _, ok := fn("outro", "n1"); ok {
		t.Fatalf("grupo ausente: ok=true")
	}
	if _, ok := fn("fn", "zz"); ok {
		t.Fatalf("sem camada: ok=true")
	}
	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badPath, []byte(`{oops`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRiskPriors(badPath); err == nil {
		t.Fatalf("json inválido: erro=nil")
	}
	if _, err := LoadRiskPriors(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatalf("ausente: erro=nil")
	}
}
