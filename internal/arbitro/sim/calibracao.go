// Package sim builds an offline bench of the arbiter to validate the model
// before the lab: the edge, fog and cloud nodes are mocked HTTP servers whose
// latencies, budget censoring, cold start and saturation come from real
// session data, while the gateway, the selectors, the history and the monitor
// are the production code and only the wire of the nodes is mocked.
package sim

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/igoricda/faas-continuum/internal/arbitro/history"
)

// Amostras aggregates the observed latencies of a pair of node and category
// per class of concurrency. The censored values are the exceeded budgets,
// where the kill of the arbiter interrupted the execution, so the value is a
// lower bound of the real duration and the bench uses that floor plus a
// random extension to undo the censoring.
type Amostras struct {
	Fast       map[string][]int64 `json:"fast"`
	Censuradas map[string][]int64 `json:"censuradas"`
	Cold       []int64            `json:"cold"`
}

// Calibracao is the set of real samples indexed by node and category.
type Calibracao struct {
	PorNo map[string]map[string]*Amostras `json:"por_no"`
	Cloud []int64                         `json:"cloud"`

	// Severity scales the probability of a slow execution above the observed
	// budget in each class: one is faithful to the data, and a value above one
	// stresses the edge.
	Severidade float64 `json:"-"`
}

type linhaInvLog struct {
	Timestamp         time.Time       `json:"timestamp"`
	FunctionName      string          `json:"function_name"`
	NodeID            string          `json:"node_id"`
	Outcome           string          `json:"outcome"`
	Layer             string          `json:"layer"`
	ObservedLatencyMS int64           `json:"observed_latency_ms"`
	Inflight          int             `json:"inflight"`
	ColdStart         bool            `json:"cold_start"`
	ScenarioContext   json.RawMessage `json:"scenario_context"`
}

// LoadCalibracao reads the real invocation log of the session and aggregates
// the samples per node, category and class of concurrency. Lines without a
// node or a category, from the absence of a node or from the cloud, are
// handled apart, and the cloud becomes the distribution of the fallback.
func LoadCalibracao(path string, severidade float64) (*Calibracao, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sim: abrir calibração %q: %w", path, err)
	}
	defer f.Close()

	cal := &Calibracao{PorNo: map[string]map[string]*Amostras{}, Severidade: severidade}
	if cal.Severidade <= 0 {
		cal.Severidade = 1
	}
	get := func(no, cat string) *Amostras {
		if cal.PorNo[no] == nil {
			cal.PorNo[no] = map[string]*Amostras{}
		}
		a := cal.PorNo[no][cat]
		if a == nil {
			a = &Amostras{Fast: map[string][]int64{}, Censuradas: map[string][]int64{}}
			cal.PorNo[no][cat] = a
		}
		return a
	}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for sc.Scan() {
		ln := strings.TrimSpace(sc.Text())
		if ln == "" {
			continue
		}
		var r linhaInvLog
		if err := json.Unmarshal([]byte(ln), &r); err != nil {
			continue // partial or legacy line: the calibration is best-effort
		}
		switch r.NodeID {
		case "cloud-fallback":
			if r.Outcome == history.OutcomeSucesso && r.ObservedLatencyMS > 0 {
				cal.Cloud = append(cal.Cloud, r.ObservedLatencyMS)
			}
			continue
		case "", "no-node-available":
			continue
		}
		cat := categoriaDoLog(r.NodeID, r.FunctionName)
		if cat == "" {
			continue
		}
		a := get(r.NodeID, cat)
		if r.ColdStart {
			if r.ObservedLatencyMS > 0 {
				a.Cold = append(a.Cold, r.ObservedLatencyMS)
			}
			continue
		}
		b := bucketInflight(r.Inflight)
		switch r.Outcome {
		case history.OutcomeSucesso:
			if r.ObservedLatencyMS > 0 {
				a.Fast[b] = append(a.Fast[b], r.ObservedLatencyMS)
			}
		case history.OutcomeOrcamentoExcedido:
			a.Censuradas[b] = append(a.Censuradas[b], r.ObservedLatencyMS)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("sim: ler calibração: %w", err)
	}
	cal.sortTudo()
	return cal, nil
}

// categoriaDoLog normalizes the names of the wire, either the requested
// category or the resolved concrete variant, into the two categories of the
// experiment. An unknown node gives an empty category.
func categoriaDoLog(no, fn string) string {
	switch {
	case no == "xp4":
		switch fn {
		case "lpr", "alpr", "alpr-ncnn":
			return "lpr"
		case "ocr", "ocr-tvbox":
			return "ocr"
		}
	case strings.HasPrefix(no, "ccsc"):
		switch fn {
		case "lpr", "alpr", "alpr-cpu", "alpr-gpu":
			return "lpr"
		case "ocr", "ocr-server":
			return "ocr"
		}
	}
	return ""
}

func bucketInflight(k int) string {
	switch {
	case k <= 1:
		return "1"
	case k == 2:
		return "2"
	case k <= 4:
		return "3-4"
	default:
		return "5+"
	}
}

var buckets = []string{"1", "2", "3-4", "5+"}

func (c *Calibracao) sortTudo() {
	for _, porCat := range c.PorNo {
		for _, a := range porCat {
			for _, v := range a.Fast {
				sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
			}
			for _, v := range a.Censuradas {
				sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
			}
			sort.Slice(a.Cold, func(i, j int) bool { return a.Cold[i] < a.Cold[j] })
		}
	}
	sort.Slice(c.Cloud, func(i, j int) bool { return c.Cloud[i] < c.Cloud[j] })
}

// Servico samples the duration of a single execution in the pair of node and
// category with a given number of executions in flight. The calibrated model:
//
//   - the class of concurrency is the range of the depth, with one, two, three
//     to four, and five or more;
//   - the probability of a slow execution is the ratio of the censored to the
//     total of the class, scaled by the severity and bounded by 0.95, which
//     reflects the real fraction of executions that exceeded the budget in
//     that regime;
//   - a normal execution samples uniformly from the success latencies of the
//     class, with a fallback to the first class under a factor of contention
//     of one plus 0.15 times the depth minus one when the class has no
//     success;
//   - a slow execution uses the censored value of the class, which is the kill
//     of the arbiter, as the floor, plus a uniform extension that undoes the
//     censoring, of up to four seconds at the edge and 2.5 seconds at the fog.
func (c *Calibracao) Servico(no, cat string, k int, rng *rand.Rand) time.Duration {
	a := c.amostras(no, cat)
	if a == nil {
		return time.Duration(250+rng.Intn(250)) * time.Millisecond
	}
	b := bucketInflight(k)
	fast := a.Fast[b]
	cens := a.Censuradas[b]
	pSlow := 0.0
	if n := len(fast) + len(cens); n > 0 {
		pSlow = float64(len(cens)) / float64(n)
	}
	pSlow *= c.Severidade
	if pSlow > 0.95 {
		pSlow = 0.95
	}
	if k <= 1 && pSlow > 0.05 {
		pSlow = 0.05 // an isolated first execution rarely exceeds the budget, since the batch is primed
	}
	if rng.Float64() < pSlow && len(cens) > 0 {
		piso := cens[rng.Intn(len(cens))]
		span := int64(4000)
		if strings.HasPrefix(no, "ccsc") {
			span = 2500
		}
		return time.Duration(piso+rng.Int63n(span)) * time.Millisecond
	}
	if len(fast) > 0 {
		v := fast[rng.Intn(len(fast))]
		return time.Duration(v) * time.Millisecond
	}
	base := c.bucketBase(no, cat)
	fator := 1 + 0.15*float64(k-1)
	return time.Duration(float64(base) * fator * 1e6)
}

// Cold returns the duration of a cold start, with an empty pool: it uses the
// real cold samples of the pair and, without a sample, a conservative
// default.
func (c *Calibracao) Cold(no, cat string, rng *rand.Rand) time.Duration {
	a := c.amostras(no, cat)
	if a != nil && len(a.Cold) > 0 {
		return time.Duration(a.Cold[rng.Intn(len(a.Cold))]) * time.Millisecond
	}
	if no == "xp4" {
		if cat == "ocr" {
			// Without a real sample, the range observed for the pair in the session.
			return time.Duration(19+rng.Intn(20)) * time.Second
		}
		// The pair of the edge and the recognition had no cold measured in the
		// session, so the conservative default applies.
		return time.Duration(5+rng.Intn(4)) * time.Second
	}
	return time.Duration(800+rng.Intn(600)) * time.Millisecond
}

// CloudServico samples the latency of the cloud uniformly over the real
// successes, with a warm fallback in the session. A positive probability of a
// cold execution injects the slow tail to stress, and the default is zero
// because the five-second timeouts of the real log were the censoring of the
// budget bug, where the cloud was killed at the objective, and not a
// measurement of a slow cloud.
func (c *Calibracao) CloudServico(rng *rand.Rand, frioP float64) time.Duration {
	if len(c.Cloud) > 0 && (frioP <= 0 || rng.Float64() >= frioP) {
		return time.Duration(c.Cloud[rng.Intn(len(c.Cloud))]) * time.Millisecond
	}
	return time.Duration(5000+rng.Intn(9000)) * time.Millisecond
}

func (c *Calibracao) amostras(no, cat string) *Amostras {
	if c.PorNo[no] == nil {
		return nil
	}
	return c.PorNo[no][cat]
}

// bucketBase returns the median of the first class, or of the nearest
// available sample, for the fallback without a success in the target class.
func (c *Calibracao) bucketBase(no, cat string) float64 {
	a := c.amostras(no, cat)
	if a == nil {
		return 300
	}
	for _, b := range buckets {
		if v := a.Fast[b]; len(v) > 0 {
			return float64(v[len(v)/2])
		}
	}
	return 300
}

// Resumo returns readable lines of the loaded calibration, for the report.
func (c *Calibracao) Resumo() []string {
	var out []string
	for no, porCat := range c.PorNo {
		for cat, a := range porCat {
			nFast, nCens, nCold := 0, 0, len(a.Cold)
			for _, v := range a.Fast {
				nFast += len(v)
			}
			for _, v := range a.Censuradas {
				nCens += len(v)
			}
			out = append(out, fmt.Sprintf("calibracao %s/%s: sucessos=%d censuradas=%d colds=%d",
				no, cat, nFast, nCens, nCold))
		}
	}
	sort.Strings(out)
	out = append(out, fmt.Sprintf("calibracao cloud: sucessos=%d", len(c.Cloud)))
	return out
}
