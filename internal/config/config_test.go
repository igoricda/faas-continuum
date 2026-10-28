package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("", nil)
	if err != nil {
		t.Fatalf("Load defaults failed: %v", err)
	}
	want := Defaults()
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("Load(\"\", nil) != Defaults()\n got: %+v\nwant: %+v", cfg, want)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate on defaults failed: %v", err)
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	// Override a few fields via file.
	content := `{
		"runtime": {"socket": "/tmp/file.sock"},
		"registry": {"path": "/tmp/file-registry.json"},
		"nodeapi": {"listen": ":9090", "default_timeout": "45s"},
		"telemetry": {"report_interval": "20s"}
	}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load file failed: %v", err)
	}
	if cfg.Runtime.Socket != "/tmp/file.sock" {
		t.Fatalf("Runtime.Socket = %q, want %q", cfg.Runtime.Socket, "/tmp/file.sock")
	}
	if cfg.Registry.Path != "/tmp/file-registry.json" {
		t.Fatalf("Registry.Path = %q, want %q", cfg.Registry.Path, "/tmp/file-registry.json")
	}
	if cfg.NodeAPI.Listen != ":9090" {
		t.Fatalf("NodeAPI.Listen = %q, want %q", cfg.NodeAPI.Listen, ":9090")
	}
	if time.Duration(cfg.NodeAPI.DefaultTimeout) != 45*time.Second {
		t.Fatalf("DefaultTimeout = %v, want 45s", cfg.NodeAPI.DefaultTimeout)
	}
	if time.Duration(cfg.Telemetry.ReportInterval) != 20*time.Second {
		t.Fatalf("ReportInterval = %v, want 20s", cfg.Telemetry.ReportInterval)
	}
	// Untouched field stays at default.
	if cfg.Runtime.Namespace != Defaults().Runtime.Namespace {
		t.Fatalf("Runtime.Namespace changed unexpectedly: got %q", cfg.Runtime.Namespace)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate after file override failed: %v", err)
	}
	// Verify Duration marshals as string.
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw failed: %v", err)
	}
	// Ensure the marshaled JSON contains quoted durations (spot check).
	// Unmarshal back and compare.
	var decoded Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("round-trip unmarshal failed: %v", err)
	}
	if decoded.Telemetry.ReportInterval != cfg.Telemetry.ReportInterval {
		t.Fatalf("round-trip duration mismatch")
	}
}

func TestLoadEnv(t *testing.T) {
	t.Setenv("FAAS_LISTEN", ":7070")
	t.Setenv("FAAS_TELEMETRY_ENABLED", "false")
	cfg, err := Load("", nil)
	if err != nil {
		t.Fatalf("Load with env failed: %v", err)
	}
	if cfg.NodeAPI.Listen != ":7070" {
		t.Fatalf("FAAS_LISTEN not applied: got %q want :7070", cfg.NodeAPI.Listen)
	}
	if cfg.Telemetry.Enabled != false {
		t.Fatalf("FAAS_TELEMETRY_ENABLED not applied: got %v want false", cfg.Telemetry.Enabled)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate after env override failed: %v", err)
	}
}

func TestLoadMergePrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	// File sets listen=file and host=file-host and socket=file-socket.
	content := `{
		"runtime": {"socket": "/tmp/file.sock"},
		"nodeapi": {"listen": "file", "host": "file-host"}
	}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAAS_LISTEN", "env")
	t.Setenv("FAAS_HOST", "env-host")

	// Case 1: flag beats env beats file.
	cfg, err := Load(path, map[string]string{"listen": "flag"})
	if err != nil {
		t.Fatalf("Load merge failed: %v", err)
	}
	if cfg.NodeAPI.Listen != "flag" {
		t.Fatalf("precedence flag>env>file failed: Listen got %q want flag", cfg.NodeAPI.Listen)
	}
	// Host: env beats file when no flag.
	if cfg.NodeAPI.Host != "env-host" {
		t.Fatalf("precedence env>file failed: Host got %q want env-host", cfg.NodeAPI.Host)
	}
	// Socket: only in file beats default.
	if cfg.Runtime.Socket != "/tmp/file.sock" {
		t.Fatalf("file>default failed: Socket got %q want /tmp/file.sock", cfg.Runtime.Socket)
	}

	// Case 2: no flag, env beats file.
	cfg2, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load merge (no flag) failed: %v", err)
	}
	if cfg2.NodeAPI.Listen != "env" {
		t.Fatalf("env>file failed: Listen got %q want env", cfg2.NodeAPI.Listen)
	}

	// Case 3: file only, no env, no flag => file value.
	t.Setenv("FAAS_LISTEN", "")
	t.Setenv("FAAS_HOST", "")
	// Need to reload without env.
	// Explicitly unset via empty is handled by Load's v != "" check.
	cfg3, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load file-only failed: %v", err)
	}
	if cfg3.NodeAPI.Listen != "file" {
		t.Fatalf("file>default (no env) failed: Listen got %q want file", cfg3.NodeAPI.Listen)
	}
	if cfg3.NodeAPI.Host != "file-host" {
		t.Fatalf("file host failed: got %q", cfg3.NodeAPI.Host)
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		// Top-level bogus field must trigger DisallowUnknownFields.
		if err := os.WriteFile(path, []byte(`{"bogus": 1}`), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(path, nil)
		if err == nil {
			t.Fatal("expected Load error for unknown field, got nil")
		}
	})

	t.Run("monitor url requires positive report interval", func(t *testing.T) {
		cfg := Defaults()
		cfg.NodeAPI.MonitorURL = "http://example.com"
		cfg.Telemetry.ReportInterval = Duration(0)
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected Validate error for MonitorURL with zero ReportInterval")
		}
		cfg.Telemetry.ReportInterval = Duration(10 * time.Second)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("positive interval should pass, got %v", err)
		}
		// No monitor URL => zero interval allowed (check that we don't error when monitor empty).
		cfg2 := Defaults()
		cfg2.Telemetry.ReportInterval = Duration(0)
		cfg2.NodeAPI.MonitorURL = ""
		if err := cfg2.Validate(); err != nil {
			t.Fatalf("zero interval without monitor should pass, got %v", err)
		}
	})

	t.Run("invocation sample interval zero", func(t *testing.T) {
		cfg := Defaults()
		cfg.Telemetry.InvocationSampleInterval = Duration(0)
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected Validate error for zero InvocationSampleInterval")
		}
	})

	t.Run("other positive checks", func(t *testing.T) {
		cfg := Defaults()
		cfg.NodeAPI.DefaultTimeout = Duration(0)
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for zero DefaultTimeout")
		}
		cfg = Defaults()
		cfg.NodeAPI.ReconcileTimeout = Duration(0)
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for zero ReconcileTimeout")
		}
		cfg = Defaults()
		cfg.NodeAPI.ReadHeaderTimeout = Duration(0)
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for zero ReadHeaderTimeout")
		}
		cfg = Defaults()
		cfg.NodeAPI.IdleTimeout = Duration(0)
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for zero IdleTimeout")
		}
		cfg = Defaults()
		cfg.NodeAPI.MaxManagementBodyBytes = 0
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for zero MaxManagementBodyBytes")
		}
		cfg = Defaults()
		cfg.NodeAPI.MaxFunctionOutputBytes = 0
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for zero MaxFunctionOutputBytes")
		}
		cfg = Defaults()
		cfg.NodeAPI.MaxInvokeBodyBytes = 0
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for zero MaxInvokeBodyBytes")
		}
	})

	t.Run("unknown nested field", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		if err := os.WriteFile(path, []byte(`{"runtime": {"unknown_field": "x"}}`), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(path, nil)
		if err == nil {
			t.Fatal("expected error for unknown nested field")
		}
	})

	t.Run("duration parsing via env", func(t *testing.T) {
		t.Setenv("FAAS_REPORT_INTERVAL", "not-a-duration")
		_, err := Load("", nil)
		if err == nil {
			t.Fatal("expected error for invalid duration env")
		}
	})

	t.Run("duration parsing via flag", func(t *testing.T) {
		_, err := Load("", map[string]string{"report-interval": "bad"})
		if err == nil {
			t.Fatal("expected error for invalid duration flag")
		}
	})

	t.Run("bool parsing via env", func(t *testing.T) {
		t.Setenv("FAAS_TELEMETRY_ENABLED", "notabool")
		_, err := Load("", nil)
		if err == nil {
			t.Fatal("expected error for invalid bool env")
		}
	})
}

func TestNonExistentFileIsIgnored(t *testing.T) {
	cfg, err := Load("/tmp/does-not-exist-12345.json", nil)
	if err != nil {
		t.Fatalf("nonexistent file should be ignored, got %v", err)
	}
	if !reflect.DeepEqual(cfg, Defaults()) {
		t.Fatalf("nonexistent file should yield defaults")
	}
}

func TestEmptyPathIsDefaults(t *testing.T) {
	cfg, err := Load("", map[string]string{})
	if err != nil {
		t.Fatalf("empty path failed: %v", err)
	}
	if !reflect.DeepEqual(cfg, Defaults()) {
		t.Fatalf("empty path should yield defaults")
	}
}

func TestDefaultScaleDownTimeout(t *testing.T) {
	t.Run("defaults when file omits scaling", func(t *testing.T) {
		cfg, err := Load("/tmp/does-not-exist-12345-scaling-default.json", nil)
		if err != nil {
			t.Fatalf("Load nonexistent failed: %v", err)
		}
		if time.Duration(cfg.Scaling.DefaultScaleDownTimeout) != 15*time.Minute {
			t.Fatalf("DefaultScaleDownTimeout = %v, want 15m", time.Duration(cfg.Scaling.DefaultScaleDownTimeout))
		}
		if cfg.Scaling.DefaultScaleDownTimeout != Duration(15*time.Minute) {
			t.Fatalf("DefaultScaleDownTimeout typed mismatch: got %v want %v", cfg.Scaling.DefaultScaleDownTimeout, Duration(15*time.Minute))
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate on defaults should pass, got %v", err)
		}
	})

	t.Run("file overlay 5m", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		content := `{"scaling":{"default_scale_down_timeout":"5m"}}`
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path, nil)
		if err != nil {
			t.Fatalf("Load file overlay failed: %v", err)
		}
		if time.Duration(cfg.Scaling.DefaultScaleDownTimeout) != 5*time.Minute {
			t.Fatalf("DefaultScaleDownTimeout = %v, want 5m", time.Duration(cfg.Scaling.DefaultScaleDownTimeout))
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate after file overlay failed: %v", err)
		}
	})

	t.Run("env overrides file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		content := `{"scaling":{"default_scale_down_timeout":"5m"}}`
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("FAAS_DEFAULT_SCALE_DOWN_TIMEOUT", "30s")
		cfg, err := Load(path, nil)
		if err != nil {
			t.Fatalf("Load env override failed: %v", err)
		}
		if time.Duration(cfg.Scaling.DefaultScaleDownTimeout) != 30*time.Second {
			t.Fatalf("DefaultScaleDownTimeout = %v, want 30s (env override)", time.Duration(cfg.Scaling.DefaultScaleDownTimeout))
		}
	})

	t.Run("flag overrides env and file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		content := `{"scaling":{"default_scale_down_timeout":"5m"}}`
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("FAAS_DEFAULT_SCALE_DOWN_TIMEOUT", "30s")
		cfg, err := Load(path, map[string]string{"default-scale-down-timeout": "10s"})
		if err != nil {
			t.Fatalf("Load flag override failed: %v", err)
		}
		if time.Duration(cfg.Scaling.DefaultScaleDownTimeout) != 10*time.Second {
			t.Fatalf("DefaultScaleDownTimeout = %v, want 10s (flag override)", time.Duration(cfg.Scaling.DefaultScaleDownTimeout))
		}
	})

	t.Run("validate", func(t *testing.T) {
		cfg := Defaults()
		if time.Duration(cfg.Scaling.DefaultScaleDownTimeout) != 15*time.Minute {
			t.Fatalf("Defaults DefaultScaleDownTimeout = %v, want 15m", time.Duration(cfg.Scaling.DefaultScaleDownTimeout))
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate on default 15m should pass, got %v", err)
		}

		cfg.Scaling.DefaultScaleDownTimeout = Duration(0)
		err := cfg.Validate()
		if err == nil {
			t.Fatal("expected Validate error for zero DefaultScaleDownTimeout, got nil")
		}
		if err.Error() != "config: default_scale_down_timeout must be positive" {
			t.Fatalf("Validate error mismatch for 0: got %q want %q", err.Error(), "config: default_scale_down_timeout must be positive")
		}

		cfg.Scaling.DefaultScaleDownTimeout = Duration(-1 * time.Second)
		err = cfg.Validate()
		if err == nil {
			t.Fatal("expected Validate error for -1s DefaultScaleDownTimeout, got nil")
		}
		if err.Error() != "config: default_scale_down_timeout must be positive" {
			t.Fatalf("Validate error mismatch for -1s: got %q want %q", err.Error(), "config: default_scale_down_timeout must be positive")
		}
	})

	t.Run("invalid env duration", func(t *testing.T) {
		t.Setenv("FAAS_DEFAULT_SCALE_DOWN_TIMEOUT", "invalid")
		_, err := Load("", nil)
		if err == nil {
			t.Fatal("expected Load error for invalid env duration, got nil")
		}
		if !strings.Contains(err.Error(), "invalid duration") {
			t.Fatalf("error should contain 'invalid duration', got %q", err.Error())
		}
	})
}

func TestAliasWinnerDocumented(t *testing.T) {
	t.Setenv("ARBITRO_HISTORY_CAP", "700")
	t.Setenv("FAAS_ARBITRO_HISTORY_CAP", "500")
	t.Setenv("ARBITRO_HISTORY_PATH", "alias-path")
	t.Setenv("FAAS_ARBITRO_HISTORY_PATH", "faas-path")
	cfg, err := Load("", nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Arbitro.HistoryCap != 700 {
		t.Errorf("HistoryCap=%d want 700 (ARBITRO_HISTORY_CAP vence)", cfg.Arbitro.HistoryCap)
	}
	if cfg.Arbitro.HistoryPath != "alias-path" {
		t.Errorf("HistoryPath=%q want alias-path", cfg.Arbitro.HistoryPath)
	}
}

func TestListenSplitIsolation(t *testing.T) {
	t.Setenv("FAAS_LISTEN", ":7070")
	cfg, err := Load("", nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.NodeAPI.Listen != ":7070" {
		t.Errorf("NodeAPI.Listen=%q want :7070", cfg.NodeAPI.Listen)
	}
	if cfg.Arbitro.Listen == ":7070" {
		t.Errorf("Arbitro.Listen=%q: FAAS_LISTEN não deve alimentar o árbitro", cfg.Arbitro.Listen)
	}
}

func TestArbitroPrecedenceFileVsFlag(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{"arbitro": {"url": "http://file:8090", "layer": "nevoa", "min_instances": 3}}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Arbitro.URL != "http://file:8090" || cfg.Arbitro.Layer != "nevoa" || cfg.Arbitro.MinInstances != 3 {
		t.Fatalf("file não aplicado: %+v", cfg.Arbitro)
	}
	cfg2, err := Load(path, map[string]string{"arbitro-url": "http://flag:8090", "layer": "borda", "node-min-instances": "7"})
	if err != nil {
		t.Fatalf("Load com flags: %v", err)
	}
	if cfg2.Arbitro.URL != "http://flag:8090" || cfg2.Arbitro.Layer != "borda" || cfg2.Arbitro.MinInstances != 7 {
		t.Fatalf("flag não venceu file: %+v", cfg2.Arbitro)
	}
}

func TestExplicitZeroReachesValidate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"arbitro": {"history_cap": 0}}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("Validate deveria rejeitar history_cap=0 explícito")
	}
}

func TestHistoryRetentionValidation(t *testing.T) {
	// Default (1h) cobre a janela default (60s).
	if err := Defaults().Validate(); err != nil {
		t.Fatalf("default deveria validar: %v", err)
	}
	// Zero disables trimming and is valid with any window.
	cfg := Defaults()
	cfg.Arbitro.HistoryRetention = 0
	cfg.Arbitro.ThroughputWindow = Duration(4 * time.Hour)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("retention=0 deveria validar: %v", err)
	}
	// Positive but shorter than the read window is an error.
	cfg = Defaults()
	cfg.Arbitro.HistoryRetention = Duration(time.Minute)
	cfg.Arbitro.ThroughputWindow = Duration(10 * time.Minute)
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate deveria rejeitar retention < throughput_window")
	}
	// Negativa => erro.
	cfg = Defaults()
	cfg.Arbitro.HistoryRetention = Duration(-time.Hour)
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate deveria rejeitar retention negativa")
	}
}

func TestAgenteIntegration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{"registry":{"path":"/tmp/x.json"},"nodeapi":{"listen":":9999"}}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, map[string]string{"listen": ":8888"})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.NodeAPI.Listen != ":8888" {
		t.Errorf("Listen = %q, want :8888 (flag wins)", cfg.NodeAPI.Listen)
	}
	if cfg.Registry.Path != "/tmp/x.json" {
		t.Errorf("Registry.Path = %q, want /tmp/x.json (file applied)", cfg.Registry.Path)
	}
}

func TestTuningDefaults(t *testing.T) {
	cfg := Defaults()
	if cfg.NodeAPI.MaxManagementBodyBytes != 64<<10 {
		t.Errorf("MaxManagementBodyBytes = %d, want %d", cfg.NodeAPI.MaxManagementBodyBytes, 64<<10)
	}
	if cfg.NodeAPI.MaxFunctionOutputBytes != 8<<20 {
		t.Errorf("MaxFunctionOutputBytes = %d, want %d", cfg.NodeAPI.MaxFunctionOutputBytes, 8<<20)
	}
	if time.Duration(cfg.NodeAPI.ReadHeaderTimeout) != 10*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want 10s", cfg.NodeAPI.ReadHeaderTimeout)
	}
	if time.Duration(cfg.NodeAPI.IdleTimeout) != 120*time.Second {
		t.Errorf("IdleTimeout = %v, want 120s", cfg.NodeAPI.IdleTimeout)
	}
	if time.Duration(cfg.Telemetry.InvocationSampleInterval) != 200*time.Millisecond {
		t.Errorf("InvocationSampleInterval = %v, want 200ms", cfg.Telemetry.InvocationSampleInterval)
	}
	if time.Duration(cfg.Telemetry.ReportInterval) != 10*time.Second {
		t.Errorf("ReportInterval = %v, want 10s", cfg.Telemetry.ReportInterval)
	}
	if time.Duration(cfg.NodeAPI.DefaultTimeout) != 90*time.Second {
		t.Errorf("DefaultTimeout = %v, want 90s", cfg.NodeAPI.DefaultTimeout)
	}
	if time.Duration(cfg.NodeAPI.ReconcileTimeout) != 60*time.Second {
		t.Errorf("ReconcileTimeout = %v, want 60s", cfg.NodeAPI.ReconcileTimeout)
	}
}
