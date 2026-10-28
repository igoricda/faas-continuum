package monitor

import (
	"testing"
	"time"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

// TestMonitorLatestAtRecvTime checks that the receive time on the gateway
// is returned and that the timestamp of the node package is ignored, which
// is the deterministic basis of the double counting guard of the load
// projection.
func TestMonitorLatestAtRecvTime(t *testing.T) {
	m := NewMonitor()
	recvNow := time.Unix(1_700_000_000, 0)
	nodeClock := recvNow.Add(-42 * time.Second)
	pkg := telemetry.Package{NodeID: "n1", Timestamp: nodeClock, CPUUsagePct: 12}
	m.UpdateAt(pkg, recvNow)

	got, recvAt, ok := m.LatestAt("n1")
	if !ok {
		t.Fatal("LatestAt ok=false após UpdateAt")
	}
	if !recvAt.Equal(recvNow) {
		t.Fatalf("recvAt=%v, want %v (relógio do Árbitro)", recvAt, recvNow)
	}
	if recvAt.Equal(nodeClock) {
		t.Fatal("recvAt não pode ser o Package.Timestamp do nó")
	}
	if got.NodeID != "n1" || got.CPUUsagePct != 12 {
		t.Fatalf("pkg=%+v, want pkg original", got)
	}
}

func TestMonitorLatestAtMissingAndDelete(t *testing.T) {
	m := NewMonitor()
	if _, _, ok := m.LatestAt("n1"); ok {
		t.Fatal("LatestAt de nó ausente deveria ok=false")
	}
	m.Update(makePackage("n1", 10))
	if _, _, ok := m.LatestAt("n1"); !ok {
		t.Fatal("LatestAt deveria ok=true após Update")
	}
	m.Delete("n1")
	if _, recvAt, ok := m.LatestAt("n1"); ok {
		t.Fatalf("LatestAt após Delete ok=true (recvAt=%v sobreviveu)", recvAt)
	}
}

// TestMonitorUpdateSetsRecvAt checks that the plain update keeps working
// and stamps the receive time with the wall clock.
func TestMonitorUpdateSetsRecvAt(t *testing.T) {
	m := NewMonitor()
	before := time.Now()
	m.Update(makePackage("n1", 10))
	_, recvAt, ok := m.LatestAt("n1")
	if !ok {
		t.Fatal("LatestAt ok=false após Update")
	}
	if recvAt.Before(before) || time.Since(recvAt) > time.Minute {
		t.Fatalf("recvAt=%v, want ~agora", recvAt)
	}
}
