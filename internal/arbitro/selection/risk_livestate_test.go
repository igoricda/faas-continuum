// Estado vivo e teto estático (SELETOR-V2 §6.5, §7.4).
package selection

import (
	"testing"

	"github.com/igoricda/faas-continuum/internal/telemetry"
)

func TestLiveStateForNode(t *testing.T) {
	fc := telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 4, QueueDepth: 0}
	// Warm ocioso: c=4, k direto do gauge.
	live, cold, c := liveStateForNode(capacityRead{fc: fc, ok: true}, 1, 1, 1)
	if c != 4 || cold || live.KFn != 1 || live.KNode != 1 || live.Servers != 4 || live.Alive != 4 {
		t.Fatalf("ocioso: %+v cold=%v c=%d", live, cold, c)
	}
	// Fila telemetrada: k_fn = max(k, c+queue).
	fcQ := telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 4, QueueDepth: 3}
	live, _, _ = liveStateForNode(capacityRead{fc: fcQ, ok: true}, 1, 1, 1)
	if live.KFn != 7 {
		t.Fatalf("fila: KFn=%d want 7 (c+queue)", live.KFn)
	}
	// Marca de exaustão: warm no teto (zero NÃO é cold), k_fn >= c.
	fcZero := telemetry.FunctionCapacity{MaxInstances: 4, InstancesAvailable: 0}
	live, cold, _ = liveStateForNode(capacityRead{fc: fcZero, mark: true, ok: true}, 0, 0, 1)
	if cold || live.KFn != 4 {
		t.Fatalf("marca: cold=%v KFn=%d want false/4", cold, live.KFn)
	}
	// Genuine-cold: zero vivo sem marca.
	live, cold, _ = liveStateForNode(capacityRead{fc: fcZero, ok: true}, 0, 0, 1)
	if !cold {
		t.Fatalf("cold: cold=false want true")
	}
	// Sem leitura: default_servers (conservador), sem cold.
	live, cold, c = liveStateForNode(capacityRead{}, 0, 0, 1)
	if c != 1 || cold {
		t.Fatalf("desconhecido: %+v cold=%v c=%d", live, cold, c)
	}
	if live.TelFresh {
		t.Fatalf("desconhecido: TelFresh=true want false")
	}
	// Pool aberto (max 0): c = max(vivas, default).
	fcOpen := telemetry.FunctionCapacity{MaxInstances: 0, InstancesAvailable: 3}
	_, _, c = liveStateForNode(capacityRead{fc: fcOpen, ok: true}, 0, 0, 1)
	if c != 3 {
		t.Fatalf("aberto: c=%d want 3", c)
	}
}

func TestRiskCapCap(t *testing.T) {
	if got := riskCapCap(2, 1.0); got != 4 {
		t.Fatalf("cap(2,1.0)=%d want 4", got)
	}
	if got := riskCapCap(4, 1.0); got != 8 {
		t.Fatalf("cap(4,1.0)=%d want 8", got)
	}
	if got := riskCapCap(0, 1.0); got != 2 {
		t.Fatalf("cap(0,1.0)=%d want 2 (c<1 => 1)", got)
	}
}
