package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"time"
)

// Load returns Defaults overlaid with the JSONC/JSON file at path when it
// exists, then with environment variables, then with flagOverrides (only the
// flags explicitly set by the caller). Precedence: flags, then environment,
// then file, then defaults.
//
// The file is read as JSONC: // and /* */ comments and trailing commas are
// removed before decoding, so a file can be documented inline while plain JSON
// keeps working. Unknown fields are rejected.
func Load(path string, flagOverrides map[string]string) (Config, error) {
	cfg := Defaults()

	// 1. File overlay (if exists).
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return Config{}, fmt.Errorf("config: failed to open config file %q: %w", path, err)
			}
		} else {
			stripped := stripJSONC(data)
			dec := json.NewDecoder(bytes.NewReader(stripped))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&cfg); err != nil {
				return Config{}, fmt.Errorf("config: failed to decode config file %q: %w", path, err)
			}
		}
	}

	// 2. Env overlay (only when non-empty).
	for _, b := range bindings {
		var v string
		for _, env := range b.envs {
			if val := os.Getenv(env); val != "" {
				v = val
				break
			}
		}
		if v != "" {
			if err := b.set(&cfg, v); err != nil {
				return Config{}, err
			}
		}
	}

	// 3. Flag overlay (only explicitly-set flags).
	if flagOverrides != nil {
		for _, b := range bindings {
			if b.flag == "" {
				continue
			}
			if v, ok := flagOverrides[b.flag]; ok {
				if err := b.set(&cfg, v); err != nil {
					return Config{}, fmt.Errorf("config: invalid flag %q value %q: %w", b.flag, v, err)
				}
			}
		}
	}

	return cfg, nil
}

// Validate applies the fail-fast rules. It does not require absolute paths.
func (c Config) Validate() error {
	if time.Duration(c.Arbitro.StaleAfter) <= 0 {
		return fmt.Errorf("config: stale_after must be positive")
	}
	if time.Duration(c.NodeAPI.DefaultTimeout) <= 0 {
		return fmt.Errorf("config: default_timeout must be positive")
	}
	if time.Duration(c.NodeAPI.ReconcileTimeout) <= 0 {
		return fmt.Errorf("config: reconcile_timeout must be positive")
	}
	if time.Duration(c.Telemetry.InvocationSampleInterval) <= 0 {
		return fmt.Errorf("config: invocation_sample_interval must be positive")
	}
	if time.Duration(c.NodeAPI.ReadHeaderTimeout) <= 0 {
		return fmt.Errorf("config: read_header_timeout must be positive")
	}
	if time.Duration(c.NodeAPI.IdleTimeout) <= 0 {
		return fmt.Errorf("config: idle_timeout must be positive")
	}
	if c.NodeAPI.MaxManagementBodyBytes <= 0 {
		return fmt.Errorf("config: max_management_body_bytes must be positive")
	}
	if c.NodeAPI.MaxFunctionOutputBytes <= 0 {
		return fmt.Errorf("config: max_function_output_bytes must be positive")
	}
	if c.NodeAPI.MaxInvokeBodyBytes <= 0 {
		return fmt.Errorf("config: max_invoke_body_bytes must be positive")
	}
	if c.NodeAPI.MonitorURL != "" && time.Duration(c.Telemetry.ReportInterval) <= 0 {
		return fmt.Errorf("config: report-interval must be positive when monitor-url is set")
	}
	if time.Duration(c.Scaling.DefaultScaleDownTimeout) <= 0 {
		return fmt.Errorf("config: default_scale_down_timeout must be positive")
	}
	if c.Arbitro.HistoryCap <= 0 {
		return fmt.Errorf("config: history_cap must be positive")
	}
	if c.Arbitro.HistoryRetention < 0 {
		return fmt.Errorf("config: history_retention must be >= 0 (0 disables age trimming)")
	}
	if c.Arbitro.HistoryRetention > 0 && c.Arbitro.HistoryRetention < c.Arbitro.ThroughputWindow {
		return fmt.Errorf("config: history_retention (%s) must cover throughput_window (%s) or be 0",
			time.Duration(c.Arbitro.HistoryRetention), time.Duration(c.Arbitro.ThroughputWindow))
	}
	if c.Arbitro.MinSamples <= 0 {
		return fmt.Errorf("config: min_samples must be positive")
	}
	if time.Duration(c.Arbitro.ThroughputWindow) <= 0 {
		return fmt.Errorf("config: throughput_window must be positive")
	}
	if c.Arbitro.HistorySync != "" && c.Arbitro.HistorySync != "batched" && c.Arbitro.HistorySync != "strict" {
		return fmt.Errorf("config: invalid history_sync %q, must be 'batched' or 'strict'", c.Arbitro.HistorySync)
	}
	// Selector: only "risk" (default) or "hl" (plain queue selector). Any other
	// value, including "", aborts startup.
	if c.Arbitro.Selector != "risk" && c.Arbitro.Selector != "hl" {
		return fmt.Errorf("config: invalid selector %q, must be 'risk' or 'hl'", c.Arbitro.Selector)
	}
	if c.Arbitro.MaxRaceFanout <= 0 {
		return fmt.Errorf("config: max_race_fanout must be positive")
	}
	if c.NodeAPI.LateQueueCap <= 0 {
		return fmt.Errorf("config: late_queue_cap must be positive")
	}
	if c.NodeAPI.LateCapMS <= 0 {
		return fmt.Errorf("config: late_cap_ms must be positive")
	}
	// Gateway hard limits follow the same fail-fast rule, with no silent
	// fallback.
	if c.Arbitro.MaxCPUPct <= 0 {
		return fmt.Errorf("config: max_cpu_pct must be positive")
	}
	if c.Arbitro.MaxMemPct <= 0 {
		return fmt.Errorf("config: max_mem_pct must be positive")
	}
	if c.Arbitro.MaxPSIAvg10 <= 0 {
		return fmt.Errorf("config: max_psi_avg10 must be positive")
	}
	// Per-node hard limits: each entry is partial, where zero means "inherit the
	// global value", hence the >= 0 rule rather than > 0. Negative, NaN and Inf
	// are rejected: NaN and Inf pass a <= 0 check and would disable the limit
	// for that node, since comparisons in Overloaded would always be false and
	// the node would never be considered overloaded. An empty node_id
	// identifies no node and would be a phantom override.
	for nodeID, nl := range c.Arbitro.NodeLimits {
		if strings.TrimSpace(nodeID) == "" {
			return fmt.Errorf("config: node_limits com node_id vazio")
		}
		for name, v := range map[string]float64{"max_cpu_pct": nl.MaxCPUPct, "max_mem_pct": nl.MaxMemPct, "max_psi_avg10": nl.MaxPSIAvg10} {
			if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
				return fmt.Errorf("config: node_limits[%s].%s deve ser >= 0 (0 herda o global), got %v", nodeID, name, v)
			}
		}
	}
	// Full PSI veto: the threshold must be positive at both layers
	// (configuration and selection); zero is not a valid threshold.
	if c.Arbitro.PSIVetoCPUSomeAvg10 <= 0 {
		return fmt.Errorf("config: psi_veto_cpu_some_avg10 must be positive")
	}
	if c.Arbitro.PSIVetoMemFullAvg10 <= 0 {
		return fmt.Errorf("config: psi_veto_mem_full_avg10 must be positive")
	}
	if time.Duration(c.Arbitro.InvokeTimeout) <= 0 {
		return fmt.Errorf("config: invoke_timeout must be positive")
	}
	if c.Arbitro.CloudFallbackPath == "" {
		return fmt.Errorf("config: cloud_fallback_path must not be empty")
	}
	// The fine-grained ranges of the risk knobs live in
	// selection.RiskConfig.Validate, which the gateway applies at startup.
	return nil
}
