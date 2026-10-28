package config

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// binding describes how a single field maps across JSON / flag / env.
type binding struct {
	jsonKey string
	flag    string
	envs    []string
	set     func(cfg *Config, value string) error
}

func stringBinding(jsonKey, flag string, envs []string, fn func(*Config, string)) binding {
	return binding{jsonKey: jsonKey, flag: flag, envs: envs, set: func(cfg *Config, v string) error { fn(cfg, v); return nil }}
}

func durationBinding(jsonKey, flag string, envs []string, fn func(*Config, Duration)) binding {
	return binding{jsonKey: jsonKey, flag: flag, envs: envs, set: func(cfg *Config, v string) error {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("config: invalid duration for %s %q: %w", jsonKey, v, err)
		}
		fn(cfg, Duration(d))
		return nil
	}}
}

// intBinding returns a binding for an integer field. flag is the flag name used
// for flag overrides and may be empty when the field has no flag; envs lists the
// environment variables to check in order, first non-empty wins.
func intBinding(jsonKey, flag string, envs []string, fn func(*Config, int)) binding {
	return binding{jsonKey: jsonKey, flag: flag, envs: envs, set: func(cfg *Config, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("config: invalid int for %s %q: %w", jsonKey, v, err)
		}
		fn(cfg, n)
		return nil
	}}
}

func boolBinding(jsonKey, flag string, envs []string, fn func(*Config, bool)) binding {
	return binding{jsonKey: jsonKey, flag: flag, envs: envs, set: func(cfg *Config, v string) error {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("config: invalid bool for %s %q: %w", jsonKey, v, err)
		}
		fn(cfg, b)
		return nil
	}}
}

// floatBinding covers floating point limits such as percentages.
func floatBinding(jsonKey, flag string, envs []string, fn func(*Config, float64)) binding {
	return binding{jsonKey: jsonKey, flag: flag, envs: envs, set: func(cfg *Config, v string) error {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("config: invalid float for %s %q: %w", jsonKey, v, err)
		}
		fn(cfg, f)
		return nil
	}}
}

// bindings is the single internal table that drives the env and flag overlays.
var bindings = []binding{
	// runtime
	stringBinding("runtime.socket", "socket", []string{"FAAS_SOCKET"}, func(c *Config, v string) { c.Runtime.Socket = v }),
	stringBinding("runtime.namespace", "namespace", []string{"FAAS_NAMESPACE"}, func(c *Config, v string) { c.Runtime.Namespace = v }),
	stringBinding("runtime.socket_root", "socket-root", []string{"FAAS_SOCKET_ROOT"}, func(c *Config, v string) { c.Runtime.SocketRoot = v }),
	stringBinding("registry.path", "registry", []string{"FAAS_REGISTRY"}, func(c *Config, v string) { c.Registry.Path = v }),

	// telemetry
	boolBinding("telemetry.enabled", "", []string{"FAAS_TELEMETRY_ENABLED"}, func(c *Config, v bool) { c.Telemetry.Enabled = v }),
	stringBinding("telemetry.cgroup_root", "cgroup-root", []string{"FAAS_CGROUP_ROOT"}, func(c *Config, v string) { c.Telemetry.CgroupRoot = v }),
	stringBinding("telemetry.proc_stat_path", "", []string{"FAAS_TELEMETRY_PROC_STAT_PATH"}, func(c *Config, v string) { c.Telemetry.ProcStatPath = v }),
	stringBinding("telemetry.meminfo_path", "", []string{"FAAS_TELEMETRY_MEMINFO_PATH"}, func(c *Config, v string) { c.Telemetry.MeminfoPath = v }),
	stringBinding("telemetry.powercap_root", "", []string{"FAAS_TELEMETRY_POWERCAP_ROOT"}, func(c *Config, v string) { c.Telemetry.PowercapRoot = v }),
	stringBinding("telemetry.power_supply_root", "", []string{"FAAS_TELEMETRY_POWER_SUPPLY_ROOT"}, func(c *Config, v string) { c.Telemetry.PowerSupplyRoot = v }),
	durationBinding("telemetry.report_interval", "report-interval", []string{"FAAS_REPORT_INTERVAL"}, func(c *Config, v Duration) { c.Telemetry.ReportInterval = v }),
	durationBinding("telemetry.invocation_sample_interval", "", []string{"FAAS_TELEMETRY_INVOCATION_SAMPLE_INTERVAL"}, func(c *Config, v Duration) { c.Telemetry.InvocationSampleInterval = v }),

	// nodeapi
	stringBinding("nodeapi.listen", "listen", []string{"FAAS_LISTEN"}, func(c *Config, v string) { c.NodeAPI.Listen = v }),
	stringBinding("nodeapi.host", "host", []string{"FAAS_HOST"}, func(c *Config, v string) { c.NodeAPI.Host = v }),
	stringBinding("nodeapi.node_id", "node-id", []string{"FAAS_NODE_ID"}, func(c *Config, v string) { c.NodeAPI.NodeID = v }),
	stringBinding("nodeapi.monitor_url", "monitor-url", []string{"FAAS_MONITOR_URL"}, func(c *Config, v string) { c.NodeAPI.MonitorURL = v }),
	durationBinding("nodeapi.default_timeout", "default-timeout", []string{"FAAS_DEFAULT_TIMEOUT"}, func(c *Config, v Duration) { c.NodeAPI.DefaultTimeout = v }),
	durationBinding("nodeapi.reconcile_timeout", "reconcile-timeout", []string{"FAAS_RECONCILE_TIMEOUT"}, func(c *Config, v Duration) { c.NodeAPI.ReconcileTimeout = v }),
	intBinding("nodeapi.max_management_body_bytes", "", []string{"FAAS_NODEAPI_MAX_MANAGEMENT_BODY_BYTES"}, func(c *Config, v int) { c.NodeAPI.MaxManagementBodyBytes = v }),
	intBinding("nodeapi.max_function_output_bytes", "", []string{"FAAS_NODEAPI_MAX_FUNCTION_OUTPUT_BYTES"}, func(c *Config, v int) { c.NodeAPI.MaxFunctionOutputBytes = v }),
	intBinding("nodeapi.max_invoke_body_bytes", "", []string{"FAAS_NODEAPI_MAX_INVOKE_BODY_BYTES"}, func(c *Config, v int) { c.NodeAPI.MaxInvokeBodyBytes = v }),
	durationBinding("nodeapi.read_header_timeout", "", []string{"FAAS_NODEAPI_READ_HEADER_TIMEOUT"}, func(c *Config, v Duration) { c.NodeAPI.ReadHeaderTimeout = v }),
	durationBinding("nodeapi.idle_timeout", "", []string{"FAAS_NODEAPI_IDLE_TIMEOUT"}, func(c *Config, v Duration) { c.NodeAPI.IdleTimeout = v }),
	intBinding("nodeapi.late_queue_cap", "late-queue-cap", []string{"FAAS_NODEAPI_LATE_QUEUE_CAP", "FAAS_LATE_QUEUE_CAP"}, func(c *Config, v int) { c.NodeAPI.LateQueueCap = v }),
	intBinding("nodeapi.late_cap_ms", "late-cap-ms", []string{"FAAS_NODEAPI_LATE_CAP_MS", "FAAS_LATE_CAP_MS"}, func(c *Config, v int) { c.NodeAPI.LateCapMS = v }),

	// scaling
	durationBinding("scaling.default_scale_down_timeout", "default-scale-down-timeout", []string{"FAAS_DEFAULT_SCALE_DOWN_TIMEOUT"}, func(c *Config, v Duration) { c.Scaling.DefaultScaleDownTimeout = v }),

	// arbitro
	stringBinding("arbitro.url", "arbitro-url", []string{"FAAS_ARBITRO_URL"}, func(c *Config, v string) { c.Arbitro.URL = v }),
	stringBinding("arbitro.token_file", "arbitro-token-file", []string{"FAAS_ARBITRO_TOKEN_FILE"}, func(c *Config, v string) { c.Arbitro.TokenFile = v }),
	stringBinding("arbitro.token", "arbitro-token", []string{"FAAS_ARBITRO_TOKEN"}, func(c *Config, v string) { c.Arbitro.Token = v }),
	stringBinding("arbitro.layer", "layer", []string{"FAAS_LAYER"}, func(c *Config, v string) { c.Arbitro.Layer = v }),
	intBinding("arbitro.min_instances", "node-min-instances", []string{"FAAS_NODE_MIN_INSTANCES"}, func(c *Config, v int) { c.Arbitro.MinInstances = v }),

	// arbitro listen split: FAAS_LISTEN feeds nodeapi only; arbitro uses FAAS_ARBITRO_LISTEN
	stringBinding("arbitro.listen", "listen", []string{"FAAS_ARBITRO_LISTEN"}, func(c *Config, v string) { c.Arbitro.Listen = v }),

	stringBinding("arbitro.nodes_path", "nodes", []string{"FAAS_ARBITRO_NODES_PATH"}, func(c *Config, v string) { c.Arbitro.NodesPath = v }),
	stringBinding("arbitro.credential_path", "credential", []string{"FAAS_ARBITRO_CREDENTIAL"}, func(c *Config, v string) { c.Arbitro.CredentialPath = v }),
	stringBinding("arbitro.node_tokens_path", "node-tokens", []string{"FAAS_ARBITRO_NODE_TOKENS_PATH"}, func(c *Config, v string) { c.Arbitro.NodeTokensPath = v }),
	durationBinding("arbitro.stale_after", "stale-after", []string{"FAAS_STALE_AFTER"}, func(c *Config, v Duration) { c.Arbitro.StaleAfter = v }),

	// arbitro history with explicit alias lists (first non-empty wins in listed order;
	// order preserves the legacy last-wins winner for each key)
	stringBinding("arbitro.history_path", "history", []string{"ARBITRO_HISTORY_PATH", "FAAS_ARBITRO_HISTORY_PATH"}, func(c *Config, v string) { c.Arbitro.HistoryPath = v }),
	intBinding("arbitro.history_cap", "history-cap", []string{"ARBITRO_HISTORY_CAP", "FAAS_ARBITRO_HISTORY_CAP"}, func(c *Config, v int) { c.Arbitro.HistoryCap = v }),
	durationBinding("arbitro.history_retention", "history-retention", []string{"ARBITRO_HISTORY_RETENTION", "FAAS_ARBITRO_HISTORY_RETENTION"}, func(c *Config, v Duration) { c.Arbitro.HistoryRetention = v }),
	intBinding("arbitro.min_samples", "selflearning-min-samples", []string{"FAAS_ARBITRO_HISTORY_MIN_SAMPLES", "FAAS_ARBITRO_MIN_SAMPLES", "ARBITRO_MIN_SAMPLES"}, func(c *Config, v int) { c.Arbitro.MinSamples = v }),
	durationBinding("arbitro.throughput_window", "selflearning-throughput-window", []string{"FAAS_ARBITRO_SELFLEARNING_THROUGHPUT_WINDOW", "FAAS_ARBITRO_THROUGHPUT_WINDOW", "ARBITRO_THROUGHPUT_WINDOW"}, func(c *Config, v Duration) { c.Arbitro.ThroughputWindow = v }),
	stringBinding("arbitro.history_sync", "history-sync", []string{"ARBITRO_HISTORY_SYNC", "FAAS_ARBITRO_HISTORY_SYNC"}, func(c *Config, v string) { c.Arbitro.HistorySync = v }),
	stringBinding("arbitro.selector", "selector", []string{"ARBITRO_SELECTOR", "FAAS_ARBITRO_SELECTOR"}, func(c *Config, v string) { c.Arbitro.Selector = v }),
	stringBinding("arbitro.invocation_log_path", "invocation-log", []string{"ARBITRO_INVOCATION_LOG_PATH", "FAAS_ARBITRO_INVOCATION_LOG_PATH"}, func(c *Config, v string) { c.Arbitro.InvocationLogPath = v }),
	intBinding("arbitro.max_race_fanout", "max-race-fanout", []string{"ARBITRO_MAX_RACE_FANOUT", "FAAS_ARBITRO_MAX_RACE_FANOUT"}, func(c *Config, v int) { c.Arbitro.MaxRaceFanout = v }),
	boolBinding("arbitro.observe_armed", "gateway-observe-armed", []string{"ARBITRO_OBSERVE_ARMED", "FAAS_ARBITRO_OBSERVE_ARMED"}, func(c *Config, v bool) { c.Arbitro.ObserveArmed = v }),
	durationBinding("arbitro.risk_half_life", "risk-halflife", []string{"ARBITRO_RISK_HALFLIFE", "FAAS_ARBITRO_RISK_HALFLIFE"}, func(c *Config, v Duration) { c.Arbitro.RiskHalfLife = v }),
	floatBinding("arbitro.risk_epsilon", "risk-epsilon", []string{"ARBITRO_RISK_EPSILON", "FAAS_ARBITRO_RISK_EPSILON"}, func(c *Config, v float64) { c.Arbitro.RiskEpsilon = v }),
	floatBinding("arbitro.risk_aci_gamma", "risk-aci-gamma", []string{"ARBITRO_RISK_ACI_GAMMA", "FAAS_ARBITRO_RISK_ACI_GAMMA"}, func(c *Config, v float64) { c.Arbitro.RiskACIGamma = v }),
	floatBinding("arbitro.risk_nmin", "risk-nmin", []string{"ARBITRO_RISK_NMIN", "FAAS_ARBITRO_RISK_NMIN"}, func(c *Config, v float64) { c.Arbitro.RiskNMin = v }),
	floatBinding("arbitro.risk_explore", "risk-explore", []string{"ARBITRO_RISK_EXPLORE", "FAAS_ARBITRO_RISK_EXPLORE"}, func(c *Config, v float64) { c.Arbitro.RiskExplore = v }),
	floatBinding("arbitro.risk_stretch", "risk-stretch", []string{"ARBITRO_RISK_STRETCH", "FAAS_ARBITRO_RISK_STRETCH"}, func(c *Config, v float64) { c.Arbitro.RiskStretch = v }),
	floatBinding("arbitro.risk_stretch_slo", "risk-stretch-slo", []string{"ARBITRO_RISK_STRETCH_SLO", "FAAS_ARBITRO_RISK_STRETCH_SLO"}, func(c *Config, v float64) { c.Arbitro.RiskStretchSLO = v }),
	floatBinding("arbitro.risk_band", "risk-band", []string{"ARBITRO_RISK_BAND", "FAAS_ARBITRO_RISK_BAND"}, func(c *Config, v float64) { c.Arbitro.RiskBand = v }),
	floatBinding("arbitro.risk_queue_allowance", "risk-queue-allowance", []string{"ARBITRO_RISK_QUEUE_ALLOWANCE", "FAAS_ARBITRO_RISK_QUEUE_ALLOWANCE"}, func(c *Config, v float64) { c.Arbitro.RiskQueueAllowance = v }),
	intBinding("arbitro.risk_default_servers", "risk-default-servers", []string{"ARBITRO_RISK_DEFAULT_SERVERS", "FAAS_ARBITRO_RISK_DEFAULT_SERVERS"}, func(c *Config, v int) { c.Arbitro.RiskDefaultServers = v }),
	floatBinding("arbitro.risk_min_success", "risk-min-success", []string{"ARBITRO_RISK_MIN_SUCCESS", "FAAS_ARBITRO_RISK_MIN_SUCCESS"}, func(c *Config, v float64) { c.Arbitro.RiskMinSuccess = v }),
	stringBinding("arbitro.risk_priors", "risk-priors", []string{"ARBITRO_RISK_PRIORS", "FAAS_ARBITRO_RISK_PRIORS"}, func(c *Config, v string) { c.Arbitro.RiskPriorsPath = v }),

	// Gateway hard limits. Precedence is flags, then env, then file, then
	// defaults, following the same pattern as the history bindings. Envs are
	// accepted with and without the FAAS_ prefix.
	floatBinding("arbitro.max_cpu_pct", "max-cpu-pct", []string{"FAAS_ARBITRO_MAX_CPU_PCT", "ARBITRO_MAX_CPU_PCT"}, func(c *Config, v float64) { c.Arbitro.MaxCPUPct = v }),
	floatBinding("arbitro.max_mem_pct", "max-mem-pct", []string{"FAAS_ARBITRO_MAX_MEM_PCT", "ARBITRO_MAX_MEM_PCT"}, func(c *Config, v float64) { c.Arbitro.MaxMemPct = v }),
	floatBinding("arbitro.max_psi_avg10", "max-psi-avg10", []string{"FAAS_ARBITRO_MAX_PSI_AVG10", "ARBITRO_MAX_PSI_AVG10"}, func(c *Config, v float64) { c.Arbitro.MaxPSIAvg10 = v }),
	floatBinding("arbitro.psi_veto_cpu_some_avg10", "psi-veto-cpu", []string{"FAAS_ARBITRO_PSI_VETO_CPU", "ARBITRO_PSI_VETO_CPU"}, func(c *Config, v float64) { c.Arbitro.PSIVetoCPUSomeAvg10 = v }),
	floatBinding("arbitro.psi_veto_mem_full_avg10", "psi-veto-mem-full", []string{"FAAS_ARBITRO_PSI_VETO_MEM_FULL", "ARBITRO_PSI_VETO_MEM_FULL"}, func(c *Config, v float64) { c.Arbitro.PSIVetoMemFullAvg10 = v }),
	durationBinding("arbitro.invoke_timeout", "invoke-timeout", []string{"FAAS_ARBITRO_INVOKE_TIMEOUT", "ARBITRO_INVOKE_TIMEOUT"}, func(c *Config, v Duration) { c.Arbitro.InvokeTimeout = v }),
	stringBinding("arbitro.cloud_fallback_path", "cloud-fallback", []string{"FAAS_ARBITRO_CLOUD_FALLBACK", "ARBITRO_CLOUD_FALLBACK"}, func(c *Config, v string) { c.Arbitro.CloudFallbackPath = v }),
	stringBinding("arbitro.category_tokens_path", "category-tokens", []string{"FAAS_ARBITRO_CATEGORY_TOKENS", "ARBITRO_CATEGORY_TOKENS"}, func(c *Config, v string) { c.Arbitro.CategoryTokensPath = v }),

	// Explicit variant-to-category map as a JSON object string, for example
	// '{"tiny":"grupo"}'. Empty or unset means nil, so the key is omitted from
	// the POST /nodes payload. There is no automatic derivation.
	binding{jsonKey: "arbitro.function_categories", flag: "function-categories", envs: []string{"FAAS_FUNCTION_CATEGORIES"}, set: func(cfg *Config, v string) error {
		if strings.TrimSpace(v) == "" {
			cfg.Arbitro.FunctionCategories = nil
			return nil
		}
		var m map[string]string
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			return fmt.Errorf("config: invalid JSON object for function_categories %q: %w", v, err)
		}
		cfg.Arbitro.FunctionCategories = m
		return nil
	}},
}
