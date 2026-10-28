// This file implements the autonomous arm of the hybrid propagation model:
// besides the piggyback, the node sends on its own the package with the
// most recent metrics, at a fixed period, tolerating windows without
// invocations.
package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

// Reporter sends the telemetry package periodically to a monitoring URL.
// Failures are logged and never interrupt the loop, because the monitor
// may be temporarily unavailable and a problem of the monitor must never
// bring the agent down.
type Reporter struct {
	target    string
	interval  time.Duration
	collector *Collector
	client    *http.Client
	header    http.Header
}

func NewReporter(target string, interval time.Duration, c *Collector, client *http.Client, header http.Header) *Reporter {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	var h http.Header
	if header != nil {
		h = header.Clone()
	}
	return &Reporter{target: target, interval: interval, collector: c, client: client, header: h}
}

func (r *Reporter) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pkg, err := r.collector.Collect(SourcePeriodic)
			if err != nil {
				log.Printf("telemetry: envio periódico descartado por falha de coleta: %v", err)
				continue
			}
			data, err := json.Marshal(pkg)
			if err != nil {
				log.Printf("telemetry: envio periódico descartado por falha de serialização: %v", err)
				continue
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.target, bytes.NewReader(data))
			if err != nil {
				log.Printf("telemetry: falha ao criar requisição para %q: %v", r.target, err)
				continue
			}
			for k, vals := range r.header {
				for _, v := range vals {
					req.Header.Add(k, v)
				}
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := r.client.Do(req)
			if err != nil {
				log.Printf("telemetry: falha ao enviar pacote para %q (monitor possivelmente fora do ar; segue tentando): %v", r.target, err)
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode > 299 {
				log.Printf("telemetry: monitor respondeu %d ao pacote periódico", resp.StatusCode)
			}
		}
	}
}
