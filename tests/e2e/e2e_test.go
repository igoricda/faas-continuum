// Package e2e_test provides black-box regression/integration flows across packages.
//
// e2e = regression/integration flows exercised via public APIs only;
// unit tests stay in-package (white-box). This suite is the only external
// package (package e2e_test) and stays hardware-free: no containerd daemon,
// no /proc, no RAPL, no network.
package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/igoricda/faas-continuum/internal/arbitro/gateway"
	"github.com/igoricda/faas-continuum/internal/arbitro/nodes"
	"github.com/igoricda/faas-continuum/internal/arbitro/selection"
	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// fakeProvisioner is a memory provisioner for pool black-box tests.
type fakeProvisioner struct {
	provisioned []string
}

func (f *fakeProvisioner) Provision(_ context.Context, instanceID, _ string) error {
	f.provisioned = append(f.provisioned, instanceID)
	return nil
}

func (f *fakeProvisioner) Terminate(_ context.Context, instanceID string) error {
	for i, id := range f.provisioned {
		if id == instanceID {
			f.provisioned = append(f.provisioned[:i], f.provisioned[i+1:]...)
			break
		}
	}
	return nil
}

// TestE2E_Registry covers registry register→get→list→unregister round-trip
// with persistence dir via public constructors only.
func TestE2E_Registry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")

	reg, err := registry.NewPersistentRegistry(path)
	if err != nil {
		t.Fatalf("NewPersistentRegistry: %v", err)
	}

	const (
		host  = "localhost:8080"
		name  = "hello"
		image = "docker.io/library/alpine:latest"
	)

	fn, token, err := reg.Register(host, name, image, registry.RegisterOptions{MinInstances: 1})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if token == "" {
		t.Fatal("Register returned empty token")
	}
	if fn.Name != name {
		t.Fatalf("Register Name = %q want %q", fn.Name, name)
	}

	// Persistence: reload from same file and verify visibility.
	reg2, err := registry.NewPersistentRegistry(path)
	if err != nil {
		t.Fatalf("NewPersistentRegistry reload: %v", err)
	}
	got, ok := reg2.Get(name)
	if !ok {
		t.Fatal("Get after reload: not found")
	}
	if got.ResourceURL != host+"/function/"+name {
		t.Fatalf("ResourceURL = %q want %q", got.ResourceURL, host+"/function/"+name)
	}

	list := reg2.List()
	if len(list) != 1 {
		t.Fatalf("List len = %d want 1", len(list))
	}

	if err := reg2.Unregister(name); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	if _, ok := reg2.Get(name); ok {
		t.Fatal("Get after Unregister: should be missing")
	}
	if got := len(reg2.List()); got != 0 {
		t.Fatalf("List after Unregister len = %d want 0", got)
	}
}

// TestE2E_Pool covers pool EnsureMinInstances→Acquire→Release via fake
// provisioner through public constructors.
func TestE2E_Pool(t *testing.T) {
	fp := &fakeProvisioner{}
	mgr := pool.NewManagerWithQueueCap(fp, nil)
	ctx := context.Background()

	const fn = "e2e-func"
	const image = "docker.io/library/alpine:latest"

	if err := mgr.EnsureMinInstances(ctx, fn, image, 1); err != nil {
		t.Fatalf("EnsureMinInstances: %v", err)
	}
	if got := mgr.Count(fn); got != 1 {
		t.Fatalf("Count after EnsureMinInstances = %d want 1", got)
	}

	id, err := mgr.Acquire(ctx, fn, image, 2)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if id == "" {
		t.Fatal("Acquire returned empty id")
	}

	mgr.Release(fn, id)

	// Re-acquire should reuse the released instance.
	id2, err := mgr.Acquire(ctx, fn, image, 2)
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	if id2 != id {
		t.Fatalf("second Acquire = %q want reuse %q", id2, id)
	}
	mgr.Release(fn, id2)
}

// TestE2E_Config covers config Load defaults + file precedence smoke
// via exported Defaults and Load. Uses t.TempDir for file state.
func TestE2E_Config(t *testing.T) {
	// Defaults smoke.
	def := config.Defaults()
	if def.NodeAPI.Listen == "" {
		t.Fatal("Defaults NodeAPI.Listen empty")
	}
	if def.Registry.Path == "" {
		t.Fatal("Defaults Registry.Path empty")
	}
	if err := def.Validate(); err != nil {
		t.Fatalf("Defaults Validate: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
		"runtime": {"socket": "/tmp/e2e.sock"},
		"nodeapi": {"listen": ":9090"},
		"registry": {"path": "/tmp/e2e-registry.json"}
	}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	loaded, err := config.Load(path, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.NodeAPI.Listen != ":9090" {
		t.Fatalf("Load file precedence NodeAPI.Listen = %q want :9090", loaded.NodeAPI.Listen)
	}
	if loaded.Runtime.Socket != "/tmp/e2e.sock" {
		t.Fatalf("Load file precedence Runtime.Socket = %q want /tmp/e2e.sock", loaded.Runtime.Socket)
	}
	// Untouched field stays at default.
	if loaded.Runtime.Namespace != def.Runtime.Namespace {
		t.Fatalf("untouched field changed: Namespace = %q want %q", loaded.Runtime.Namespace, def.Runtime.Namespace)
	}

	// Flag overrides file.
	flagged, err := config.Load(path, map[string]string{"listen": ":7070"})
	if err != nil {
		t.Fatalf("Load with flag: %v", err)
	}
	if flagged.NodeAPI.Listen != ":7070" {
		t.Fatalf("flag precedence Listen = %q want :7070", flagged.NodeAPI.Listen)
	}
}

// TestE2E_Selection covers arbitro selection RankNodes pure-function smoke
// via exported selection.RankNodes and gateway public constructor.
func TestE2E_Selection(t *testing.T) {
	cfg := selection.LimitsConfig{
		MaxCPUPct:   85,
		MaxMemPct:   90,
		MaxPSIAvg10: 25,
	}

	candidates := []nodes.Node{
		{NodeID: "borda-1", Layer: nodes.LayerBorda, Host: "10.0.0.1:8080"},
		{NodeID: "nevoa-1", Layer: nodes.LayerNevoa, Host: "10.0.0.2:8080"},
		{NodeID: "borda-2", Layer: nodes.LayerBorda, Host: "10.0.0.3:8080"},
	}
	latest := map[string]telemetry.Package{
		"borda-1": {CPUUsagePct: 10, MemoryUsagePct: 10},
		"nevoa-1": {CPUUsagePct: 10, MemoryUsagePct: 10},
		"borda-2": {CPUUsagePct: 90, MemoryUsagePct: 10}, // overloaded (CPU >85)
	}

	// Without a ceiling per layer the objective does not filter, and only the
	// overload of the second edge leaves the queue.
	slo := &selection.SLO{MaxLatencyMS: 500}
	ranked := selection.RankNodes(candidates, latest, slo, cfg)
	if len(ranked) != 2 || ranked[0].NodeID != "borda-1" || ranked[1].NodeID != "nevoa-1" {
		t.Fatalf("RankNodes sobrecarga want [borda-1 nevoa-1] got %v", nodeIDs(ranked))
	}

	// Without SLO and without overloaded, ordering is borda before nevoa, CPU asc.
	candidates2 := []nodes.Node{
		{NodeID: "nevoa-1", Layer: nodes.LayerNevoa, Host: "10.0.0.2:8080"},
		{NodeID: "borda-1", Layer: nodes.LayerBorda, Host: "10.0.0.1:8080"},
	}
	latest2 := map[string]telemetry.Package{
		"nevoa-1": {CPUUsagePct: 5},
		"borda-1": {CPUUsagePct: 50},
	}
	ranked2 := selection.RankNodes(candidates2, latest2, nil, cfg)
	if len(ranked2) != 2 || ranked2[0].NodeID != "borda-1" {
		t.Fatalf("RankNodes order want borda first got %v", nodeIDs(ranked2))
	}

	// Also smoke gateway public constructor (alternative path when RankNodes
	// were not exported): NewGatewayWithHistory must not panic with nil deps.
	_ = gateway.NewGatewayWithHistory(gateway.GatewayConfig{
		Selector: selection.QueueSelector{Cfg: cfg},
	})
}

func nodeIDs(ns []nodes.Node) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.NodeID
	}
	return out
}
