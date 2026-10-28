package telemetry

import (
	"encoding/json"
	"strings"
	"testing"
)

// The periodic report fills the complete map through the snapshot, while
// the piggyback never does, since it only carries the invoked function.
func TestCapacityPeriodicFillsFullMap(t *testing.T) {
	f := newFixture(t)
	c := newTestCollector(t, f)
	c.SetCapacitySnapshot(func() map[string]FunctionCapacity {
		return map[string]FunctionCapacity{
			"sleep": {MaxInstances: 1, InstancesAvailable: 1, QueueDepth: 2},
			"alpr":  {MaxInstances: 4, InstancesAvailable: 3},
		}
	})

	per, err := c.Collect(SourcePeriodic)
	if err != nil {
		t.Fatalf("Collect(periodic): %v", err)
	}
	if len(per.Capacity) != 2 {
		t.Fatalf("capacity periódica tem %d entradas, esperado 2", len(per.Capacity))
	}
	sl := per.Capacity["sleep"]
	if sl.MaxInstances != 1 || sl.InstancesAvailable != 1 || sl.QueueDepth != 2 {
		t.Errorf("sleep = %+v, esperado {1 1 2}", sl)
	}

	pb, err := c.Collect(SourcePiggyback)
	if err != nil {
		t.Fatalf("Collect(piggyback): %v", err)
	}
	if pb.Capacity != nil {
		t.Errorf("piggyback deveria sair sem capacity (nodeapi preenche), obteve %+v", pb.Capacity)
	}
}

// Without an installed snapshot, no channel carries the capacity.
func TestCapacityAbsentWithoutSnapshot(t *testing.T) {
	f := newFixture(t)
	c := newTestCollector(t, f)
	for _, src := range []string{SourcePeriodic, SourcePiggyback} {
		pkg, err := c.Collect(src)
		if err != nil {
			t.Fatalf("Collect(%s): %v", src, err)
		}
		if pkg.Capacity != nil {
			t.Errorf("Collect(%s) sem snapshot: capacity = %+v, esperado nil", src, pkg.Capacity)
		}
		data, err := json.Marshal(pkg)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), `"capacity"`) {
			t.Errorf("Collect(%s) sem snapshot: JSON contém capacity: %s", src, data)
		}
	}
}

// Older readers decode a package without the key, with a nil map.
func TestCapacityMissingFieldTolerance(t *testing.T) {
	raw := `{"node_id":"n","timestamp":"2026-09-16T00:00:00Z","cpu_usage_pct":1.5,"memory_usage_pct":2.5}`
	var pkg Package
	if err := json.Unmarshal([]byte(raw), &pkg); err != nil {
		t.Fatalf("decode sem capacity: %v", err)
	}
	if pkg.Capacity != nil {
		t.Errorf("capacity = %+v, esperado nil (desconhecido, sem gating)", pkg.Capacity)
	}
	if pkg.NodeID != "n" || pkg.CPUUsagePct != 1.5 {
		t.Errorf("campos vizinhos corrompidos: %+v", pkg)
	}
}

// The budget of the JSON is around 100 bytes per function with a capacity.
func TestCapacityJSONDeltaPerFunction(t *testing.T) {
	base := Package{NodeID: "n"}
	baseRaw, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	with := Package{NodeID: "n", Capacity: map[string]FunctionCapacity{
		"sleep": {MaxInstances: 1, InstancesAvailable: 1, QueueDepth: 2},
	}}
	withRaw, err := json.Marshal(with)
	if err != nil {
		t.Fatal(err)
	}
	delta := len(withRaw) - len(baseRaw)
	t.Logf("JSON capacity: base=%dB com-1-fn=%dB delta=%dB", len(baseRaw), len(withRaw), delta)
	if delta > 100 {
		t.Errorf("delta por função = %dB, acima do orçamento de ~100B: %s", delta, withRaw)
	}
	for _, chave := range []string{`"capacity"`, `"max_instances"`, `"instances_available"`, `"queue_depth"`} {
		if !strings.Contains(string(withRaw), chave) {
			t.Errorf("JSON deveria conter %s: %s", chave, withRaw)
		}
	}
}
