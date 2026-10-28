package sim

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// workloadClient drives the load of the runner, with the two stages of the
// recognition and of the OCR, against the real gateway and aggregates the
// metrics of the client. There is one client per stage, and the priming uses
// its own discarded instance, so the metrics and the latency of the measured
// regime do not carry the warm-up stage.
type workloadClient struct {
	url   string
	sloMS int
	http  *http.Client

	invocos, ok, erros, sloOk, sloTotal atomic.Int64
	mu                                  sync.Mutex
	latencias                           []int64
	porCodigo                           map[string]int
}

func newWorkloadClient(url string, sloMS int) *workloadClient {
	return &workloadClient{
		url:       url,
		sloMS:     sloMS,
		http:      &http.Client{Timeout: 120 * time.Second},
		porCodigo: map[string]int{},
	}
}

// invoke makes a single call of a stage to the invocation endpoint of the gateway.
func (c *workloadClient) invoke(fn string) {
	corpo := fmt.Sprintf(`{"function":%q,"input":{"image":"sim.jpg"}`, fn)
	if c.sloMS > 0 {
		corpo += fmt.Sprintf(`,"slo":{"max_latency_ms":%d}`, c.sloMS)
	}
	corpo += "}"
	req, err := http.NewRequest(http.MethodPost, c.url+"/invoke", strings.NewReader(corpo))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	t0 := time.Now()
	resp, err := c.http.Do(req)
	lat := time.Since(t0)
	c.invocos.Add(1)
	if err != nil {
		c.erros.Add(1)
		c.mu.Lock()
		c.porCodigo["CLIENT_"+err.Error()]++
		c.mu.Unlock()
		return
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	c.mu.Lock()
	c.latencias = append(c.latencias, lat.Milliseconds())
	c.mu.Unlock()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		c.ok.Add(1)
		if c.sloMS > 0 {
			c.sloTotal.Add(1)
			if lat.Milliseconds() <= int64(c.sloMS) {
				c.sloOk.Add(1)
			}
		}
		return
	}
	c.erros.Add(1)
	var er struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(body, &er)
	if er.Code == "" {
		er.Code = fmt.Sprintf("HTTP_%d", resp.StatusCode)
	}
	c.mu.Lock()
	c.porCodigo[er.Code]++
	c.mu.Unlock()
}

// disparo replicates the two-stage request of the runner, first the
// recognition and then the OCR.
func (c *workloadClient) disparo() { c.invoke("lpr"); c.invoke("ocr") }

// burst runs a number of workers in a loop until the deadline, as the probes
// of six and ten workers.
func (c *workloadClient) burst(workers int, d time.Duration) {
	if workers <= 0 {
		workers = 1
	}
	var wg sync.WaitGroup
	deadline := time.Now().Add(d)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				c.disparo()
			}
		}()
	}
	wg.Wait()
}

// cadence replicates the adaptive run: requests at every interval, with a
// thread per request, so that latencies above the interval overlap, and a
// warm-up plus repetitions per image and interval.
func (c *workloadClient) cadence(intervals []float64, warmup, reps, imagens int) {
	if imagens <= 0 {
		imagens = 1
	}
	var wg sync.WaitGroup
	for _, interval := range intervals {
		for img := 0; img < imagens; img++ {
			for i := 0; i < warmup+reps; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); c.disparo() }()
				time.Sleep(time.Duration(interval * float64(time.Second)))
			}
		}
	}
	wg.Wait()
}

// fill copies the metrics of the client into the result.
func (c *workloadClient) fill(res *Resultado) {
	res.Invocos = int(c.invocos.Load())
	res.OK = int(c.ok.Load())
	res.Erros = int(c.erros.Load())
	res.SLOOk = int(c.sloOk.Load())
	res.SLOTotal = int(c.sloTotal.Load())
	c.mu.Lock()
	if len(c.latencias) > 0 {
		lats := make([]int64, len(c.latencias))
		copy(lats, c.latencias)
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		res.P50MS = pct(lats, .50)
		res.P95MS = pct(lats, .95)
		res.P99MS = pct(lats, .99)
	}
	res.PorCodigo = make(map[string]int, len(c.porCodigo))
	for k, v := range c.porCodigo {
		res.PorCodigo[k] = v
	}
	c.mu.Unlock()
}
