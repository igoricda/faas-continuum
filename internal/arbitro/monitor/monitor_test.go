package monitor

import (
	"sync"
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func makePackage(nodeID string, cpu float64) telemetry.Package {
	return telemetry.Package{
		NodeID:         nodeID,
		Timestamp:      time.Now().UTC(),
		CPUUsagePct:    cpu,
		MemoryUsagePct: 42.5,
		Source:         telemetry.SourcePeriodic,
	}
}

func TestMonitor_UpdateGet(t *testing.T) {
	m := NewMonitor()
	if m == nil {
		t.Fatalf("NewMonitor retornou nil")
	}
	pkg := makePackage("n1", 12.3)
	m.Update(pkg)

	got, ok := m.Latest("n1")
	if !ok {
		t.Fatalf("Latest n1: não encontrado após Update")
	}
	if got.NodeID != pkg.NodeID || got.CPUUsagePct != pkg.CPUUsagePct || got.MemoryUsagePct != pkg.MemoryUsagePct {
		t.Fatalf("Latest mismatch: got %+v want %+v", got, pkg)
	}
	if !got.Timestamp.Equal(pkg.Timestamp) {
		t.Fatalf("Timestamp mismatch: got %v want %v", got.Timestamp, pkg.Timestamp)
	}
}

func TestMonitor_LatestInexistente(t *testing.T) {
	m := NewMonitor()
	_, ok := m.Latest("inexistente")
	if ok {
		t.Fatalf("Latest de nó inexistente deveria retornar false")
	}

	// After inserting a node, another one that does not exist is still
	// unknown.
	m.Update(makePackage("n1", 10))
	_, ok = m.Latest("n2")
	if ok {
		t.Fatalf("Latest n2 inexistente deveria retornar false mesmo com n1 existente")
	}
}

func TestMonitor_UpdateSobrescreve(t *testing.T) {
	m := NewMonitor()
	p1 := makePackage("n1", 10.0)
	m.Update(p1)

	// Overwrite with different values.
	p2 := makePackage("n1", 99.9)
	p2.MemoryUsagePct = 88.8
	m.Update(p2)

	got, ok := m.Latest("n1")
	if !ok {
		t.Fatalf("Latest n1 não encontrado após segunda Update")
	}
	if got.CPUUsagePct != 99.9 {
		t.Fatalf("Update não sobrescreveu CPUUsagePct: got %v want 99.9", got.CPUUsagePct)
	}
	if got.MemoryUsagePct != 88.8 {
		t.Fatalf("Update não sobrescreveu MemoryUsagePct: got %v want 88.8", got.MemoryUsagePct)
	}
	if !got.Timestamp.Equal(p2.Timestamp) {
		t.Fatalf("Timestamp não sobrescrito: got %v want %v", got.Timestamp, p2.Timestamp)
	}

	// The other nodes are not affected.
	p3 := makePackage("n2", 5.5)
	m.Update(p3)
	got1, _ := m.Latest("n1")
	if got1.CPUUsagePct != 99.9 {
		t.Fatalf("n1 foi afetado por Update de n2: got %v", got1.CPUUsagePct)
	}
	got2, ok := m.Latest("n2")
	if !ok || got2.CPUUsagePct != 5.5 {
		t.Fatalf("n2 não encontrado ou valor errado: ok=%v pkg=%+v", ok, got2)
	}
}

func TestMonitor_LatestRetornaCopia(t *testing.T) {
	m := NewMonitor()
	pkg := makePackage("n1", 42.0)
	m.Update(pkg)

	got, _ := m.Latest("n1")
	// Mutating the returned copy must not affect the stored one.
	got.CPUUsagePct = 999

	got2, _ := m.Latest("n1")
	if got2.CPUUsagePct != 42.0 {
		t.Fatalf("Latest retornou referência compartilhada: mutação externa afetou Monitor (got %v want 42.0)", got2.CPUUsagePct)
	}
}

func TestMonitor_Concorrencia(t *testing.T) {
	m := NewMonitor()
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			pkg := makePackage("n-conc", float64(i))
			m.Update(pkg)
		}()
		go func() {
			defer wg.Done()
			_, _ = m.Latest("n-conc")
		}()
	}
	wg.Wait()

	// After the concurrent writes, the lookup returns one of the written
	// values.
	got, ok := m.Latest("n-conc")
	if !ok {
		t.Fatalf("Latest n-conc não encontrado após concorrência")
	}
	if got.NodeID != "n-conc" {
		t.Fatalf("NodeID incorreto após concorrência: %q", got.NodeID)
	}

	// Concurrency with several distinct nodes: all of them exist and no
	// race is reported.
	m2 := NewMonitor()
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			id := string(rune('a' + i%10))
			pkg := makePackage(id, float64(i))
			m2.Update(pkg)
			_, _ = m2.Latest(id)
		}()
	}
	wg.Wait()
}

func TestMonitor_NewMonitorInicializaMap(t *testing.T) {
	m := NewMonitor()
	if m.latest == nil {
		t.Fatalf("NewMonitor não inicializou map latest (nil)")
	}
	// Updating a monitor that was just created must not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Update em Monitor novo causou panic: %v", r)
		}
	}()
	m.Update(makePackage("n1", 1.0))
}

// TestMonitor_UpdateDeleteLatest checks that the removal drops the package
// and that the lookup then reports the node as unknown.
func TestMonitor_UpdateDeleteLatest(t *testing.T) {
	m := NewMonitor()
	pkg := makePackage("n1", 12.3)
	m.Update(pkg)

	got, ok := m.Latest("n1")
	if !ok {
		t.Fatalf("Latest n1 antes do Delete: não encontrado")
	}
	if got.CPUUsagePct != 12.3 {
		t.Fatalf("CPU mismatch antes do Delete: got %v want 12.3", got.CPUUsagePct)
	}

	m.Delete("n1")

	_, ok = m.Latest("n1")
	if ok {
		t.Fatalf("Latest n1 depois do Delete deveria retornar false")
	}
}

func TestMonitor_DeleteThenReAddConfirmaPacoteFresco(t *testing.T) {
	m := NewMonitor()
	pkg1 := makePackage("n1", 10.0)
	m.Update(pkg1)

	m.Delete("n1")

	_, ok := m.Latest("n1")
	if ok {
		t.Fatalf("Latest n1 após delete deveria retornar false")
	}

	pkg2 := makePackage("n1", 99.9)
	m.Update(pkg2)

	got, ok := m.Latest("n1")
	if !ok {
		t.Fatalf("Latest n1 após re-add: não encontrado")
	}
	if got.CPUUsagePct != 99.9 {
		t.Fatalf("CPU após re-add: got %v want 99.9", got.CPUUsagePct)
	}
}

func TestMonitor_DeleteMissingNoOp(t *testing.T) {
	m := NewMonitor()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Delete de nó inexistente causou panic: %v", r)
		}
	}()
	m.Delete("inexistente")
}
