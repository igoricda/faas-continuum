// Package config defines the typed configuration shared by the node agent and
// the gateway, and loads it from a JSONC file, environment variables and
// command line flags.
//
// Environment variables only override when non-empty, and flag overrides are
// applied only for flags the caller passed explicitly.
package config

import (
	"time"
)

// NodeHardLimits is the partial set of hard limits declared for a node under
// `arbitro.node_limits`. A zero field inherits the matching global limit, so a
// node that only needs a tighter CPU limit declares `{"max_cpu_pct": 60}` and
// keeps the global memory and PSI limits. The merge with the global values
// happens at gateway startup, never during selection (see
// cmd/arbitro/node_limits.go).
type NodeHardLimits struct {
	MaxCPUPct   float64 `json:"max_cpu_pct"`
	MaxMemPct   float64 `json:"max_mem_pct"`
	MaxPSIAvg10 float64 `json:"max_psi_avg10"`
}

// Arbitro holds the settings of the central gateway (see cmd/arbitro/main.go for
// the flags that populate these fields). Token is kept in memory only
// (show-once) and is never persisted (json:"-", see auth.LoadOrGenerate in
// internal/auth/auth.go).
type Arbitro struct {
	URL            string   `json:"url"`
	TokenFile      string   `json:"token_file"`
	Token          string   `json:"-"`
	Layer          string   `json:"layer"`
	MinInstances   int      `json:"min_instances"`
	Listen         string   `json:"listen"`
	NodesPath      string   `json:"nodes_path"`
	StaleAfter     Duration `json:"stale_after"`
	CredentialPath string   `json:"credential_path"`
	NodeTokensPath string   `json:"node_tokens_path"`
	HistoryPath    string   `json:"history_path"`
	HistorySync    string   `json:"history_sync"`
	HistoryCap     int      `json:"history_cap"`
	// HistoryRetention is the age-based retention window of the history;
	// entries older than it are pruned on load and periodically. Zero disables
	// age trimming, leaving the per-segment cap as the only bound; a positive
	// value must cover the largest read window (ThroughputWindow), and Validate
	// rejects a retention shorter than the window.
	HistoryRetention Duration `json:"history_retention"`
	// InvocationLogPath enables the append-only invocation logger (JSONL, no
	// size limit). Empty (the default) disables it.
	InvocationLogPath string   `json:"invocation_log_path"`
	MinSamples        int      `json:"min_samples"`
	ThroughputWindow  Duration `json:"throughput_window"`
	// MaxRaceFanout caps the attempts per invocation; a value <= 0 means no cap
	// (the whole queue). Defaults to 3.
	MaxRaceFanout int `json:"max_race_fanout"`
	// ObserveArmed enables observing the invocations armed during the current
	// scheduling slice. Defaults to false.
	ObserveArmed bool `json:"observe_armed"`
	// Tuning knobs of the risk-based selector. Their valid ranges are checked
	// by RiskConfig.Validate at gateway startup.
	RiskHalfLife       Duration `json:"risk_half_life"`
	RiskEpsilon        float64  `json:"risk_epsilon"`
	RiskACIGamma       float64  `json:"risk_aci_gamma"`
	RiskNMin           float64  `json:"risk_nmin"`
	RiskExplore        float64  `json:"risk_explore"`
	RiskStretch        float64  `json:"risk_stretch"`
	RiskStretchSLO     float64  `json:"risk_stretch_slo"`
	RiskBand           float64  `json:"risk_band"`
	RiskQueueAllowance float64  `json:"risk_queue_allowance"`
	RiskDefaultServers int      `json:"risk_default_servers"`
	RiskMinSuccess     float64  `json:"risk_min_success"`
	RiskPriorsPath     string   `json:"risk_priors"`
	// Gateway hard limits. They follow the same precedence as the rest of the
	// struct: flags, then environment, then file, then defaults (see
	// bindings.go).
	MaxCPUPct   float64 `json:"max_cpu_pct"`
	MaxMemPct   float64 `json:"max_mem_pct"`
	MaxPSIAvg10 float64 `json:"max_psi_avg10"`
	// NodeLimits overrides the hard limits per node (key = node_id, the same
	// identifier used by the node registry). Each entry is partial: a zero or
	// absent field inherits the matching global limit
	// (max_cpu_pct/max_mem_pct/max_psi_avg10), which keeps the global triple as
	// the default and avoids repeating three numbers per node. Negative, NaN
	// and Inf values are rejected by Validate. Nil or empty means every node
	// uses the global triple.
	//
	// The map is resolved once at gateway startup into a complete triple per
	// node, and selection only reads the result (selection.LimitsConfig.For).
	// There is no flag or environment variable for it, since a map does not map
	// onto a scalar value.
	//
	// It affects only the hard-limit stage (Overloaded, FilterOverloaded,
	// projected overload and overload status). No statistic, prediction,
	// ordering or veto of the self-learning path is affected.
	NodeLimits map[string]NodeHardLimits `json:"node_limits,omitempty"`
	// Selector chooses the selection algorithm: "risk" (the risk-based
	// selector, default) or "hl" (plain queue selector, i.e. hard limits only,
	// without learning). Any other value, including "", aborts startup in
	// Validate. Set with the -selector flag or the ARBITRO_SELECTOR /
	// FAAS_ARBITRO_SELECTOR environment variables.
	Selector string `json:"selector"`
	// PSIVetoCPUSomeAvg10 and PSIVetoMemFullAvg10 calibrate the full PSI veto
	// of the selector (see PSIVetoThresholds in selection/psi_veto.go). The
	// defaults are 28 and 12, matching DefaultPSIVetoThresholds. Validate
	// requires both to be positive; there is no zero-means-default case at the
	// configuration layer.
	PSIVetoCPUSomeAvg10 float64 `json:"psi_veto_cpu_some_avg10"`
	PSIVetoMemFullAvg10 float64 `json:"psi_veto_mem_full_avg10"`
	// InvokeTimeout is the default invocation budget at the gateway. It must be
	// positive, so a negative value cannot become a deadline in the past.
	InvokeTimeout Duration `json:"invoke_timeout"`
	// CloudFallbackPath is the file that persists the cloud fallback
	// configuration.
	CloudFallbackPath string `json:"cloud_fallback_path"`
	// CategoryTokensPath is the file that persists the per-category tokens and
	// the downstream map (see cattokens). Empty disables it.
	CategoryTokensPath string `json:"category_tokens_path"`
	// FunctionCategories is the explicit variant-to-category mapping sent in
	// the POST /nodes payload. Nil omits the key. It is not derived from the
	// local registry.
	FunctionCategories map[string]string `json:"function_categories,omitempty"`
}

// Config is the typed configuration shared by the node agent and the gateway.
// See Defaults below and bindings.go for the flag, environment and file
// mappings.
type Config struct {
	Runtime   Runtime   `json:"runtime"`
	Registry  Registry  `json:"registry"`
	Telemetry Telemetry `json:"telemetry"`
	NodeAPI   NodeAPI   `json:"nodeapi"`
	Scaling   Scaling   `json:"scaling"`
	Arbitro   Arbitro   `json:"arbitro"`
}

// Runtime holds the container runtime settings. Socket and SocketRoot are used
// by internal/containerruntime.
type Runtime struct {
	Socket     string `json:"socket"`
	Namespace  string `json:"namespace"`
	SocketRoot string `json:"socket_root"`
}

// Registry holds the persistence settings of the function registry. See
// NewPersistentRegistry in internal/registry/persistence.go.
type Registry struct {
	Path              string   `json:"path"`
	AllowedRegistries []string `json:"allowed_registries,omitempty"`
}

// Telemetry holds the telemetry collection and reporting settings. See
// Collector in internal/telemetry/telemetry.go and Reporter in reporter.go.
type Telemetry struct {
	Enabled                  bool     `json:"enabled"`
	CgroupRoot               string   `json:"cgroup_root"`
	ProcStatPath             string   `json:"proc_stat_path"`
	MeminfoPath              string   `json:"meminfo_path"`
	PowercapRoot             string   `json:"powercap_root"`
	PowerSupplyRoot          string   `json:"power_supply_root"`
	ReportInterval           Duration `json:"report_interval"`
	InvocationSampleInterval Duration `json:"invocation_sample_interval"`
}

// NodeAPI holds the settings of the node HTTP API. See Server in
// internal/nodeapi/server.go.
type NodeAPI struct {
	Listen                 string   `json:"listen"`
	Host                   string   `json:"host"`
	NodeID                 string   `json:"node_id"`
	MonitorURL             string   `json:"monitor_url"`
	DefaultTimeout         Duration `json:"default_timeout"`
	ReconcileTimeout       Duration `json:"reconcile_timeout"`
	MaxManagementBodyBytes int      `json:"max_management_body_bytes"`
	MaxFunctionOutputBytes int      `json:"max_function_output_bytes"`
	MaxInvokeBodyBytes     int      `json:"max_invoke_body_bytes"`
	ReadHeaderTimeout      Duration `json:"read_header_timeout"`
	IdleTimeout            Duration `json:"idle_timeout"`
	// LateQueueCap bounds the queue of the late-report sender; reports beyond
	// the cap are dropped and counted. Defaults to 1024.
	LateQueueCap int `json:"late_queue_cap"`
	// LateCapMS bounds the detached execution of an opted-in invocation, in
	// milliseconds. The effective late cap is min(nodeTimeout, lateCap).
	LateCapMS int `json:"late_cap_ms"`
}

// Scaling holds the autoscaling settings. See EnsureMinInstances and the reaper
// in internal/pool; DefaultScaleDownTimeout feeds the idle timeout.
type Scaling struct {
	DefaultScaleDownTimeout Duration `json:"default_scale_down_timeout"`
}

// Defaults returns the built-in configuration.
func Defaults() Config {
	return Config{
		Runtime:   Runtime{Socket: "/run/containerd/containerd.sock", Namespace: "faas-continuum", SocketRoot: "/run/faas-continuum/sockets"},
		Registry:  Registry{Path: "./faas-continuum-functions.json"},
		Telemetry: Telemetry{Enabled: true, CgroupRoot: "", ProcStatPath: "/proc/stat", MeminfoPath: "/proc/meminfo", PowercapRoot: "/sys/class/powercap", PowerSupplyRoot: "/sys/class/power_supply", ReportInterval: Duration(10 * time.Second), InvocationSampleInterval: Duration(200 * time.Millisecond)},
		NodeAPI:   NodeAPI{Listen: ":8080", Host: "", NodeID: "", MonitorURL: "", DefaultTimeout: Duration(90 * time.Second), ReconcileTimeout: Duration(60 * time.Second), MaxManagementBodyBytes: 64 << 10, MaxFunctionOutputBytes: 8 << 20, MaxInvokeBodyBytes: 8 << 20, ReadHeaderTimeout: Duration(10 * time.Second), IdleTimeout: Duration(120 * time.Second), LateQueueCap: 1024, LateCapMS: 90000},
		Scaling:   Scaling{DefaultScaleDownTimeout: Duration(15 * time.Minute)},
		Arbitro:   Arbitro{URL: "", TokenFile: "", Token: "", Layer: "borda", MinInstances: 0, Listen: ":8090", NodesPath: "./faas-continuum-nodes.json", StaleAfter: Duration(90 * time.Second), CredentialPath: "./faas-continuum-arbitro-credential.json", NodeTokensPath: "./faas-continuum-node-tokens.json", HistoryPath: "./faas-continuum-history.json", HistorySync: "batched", HistoryCap: 1000, HistoryRetention: Duration(time.Hour), MinSamples: 20, ThroughputWindow: Duration(60 * time.Second), MaxRaceFanout: 3, MaxCPUPct: 85, MaxMemPct: 90, MaxPSIAvg10: 25, PSIVetoCPUSomeAvg10: 28, PSIVetoMemFullAvg10: 12, InvokeTimeout: Duration(90 * time.Second), CloudFallbackPath: "./faas-continuum-cloud-fallback.json", CategoryTokensPath: "./faas-continuum-category-tokens.json", RiskHalfLife: Duration(10 * time.Minute), RiskEpsilon: 0.05, RiskACIGamma: 0.01, RiskNMin: 3, RiskExplore: 0.05, RiskStretch: 2.0, RiskStretchSLO: 0, RiskBand: 0.10, RiskQueueAllowance: 1.0, RiskDefaultServers: 1, RiskMinSuccess: 0.5, RiskPriorsPath: "", Selector: "risk"},
	}
}
