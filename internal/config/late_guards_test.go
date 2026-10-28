package config

import (
	"testing"
)

func TestLateGuards(t *testing.T) {
	cfg := Defaults()
	if cfg.Arbitro.MaxRaceFanout <= 0 {
		t.Fatalf("default max_race_fanout = %d, esperado >0", cfg.Arbitro.MaxRaceFanout)
	}
	if cfg.NodeAPI.LateQueueCap <= 0 {
		t.Fatalf("default late_queue_cap = %d, esperado >0", cfg.NodeAPI.LateQueueCap)
	}
	if cfg.NodeAPI.LateCapMS <= 0 {
		t.Fatalf("default late_cap_ms = %d, esperado >0", cfg.NodeAPI.LateCapMS)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Defaults rejeitado no Validate: %v", err)
	}

	for _, tc := range []struct {
		name string
		mut  func(*Config)
	}{
		{"max_race_fanout=0", func(c *Config) { c.Arbitro.MaxRaceFanout = 0 }},
		{"late_queue_cap=0", func(c *Config) { c.NodeAPI.LateQueueCap = 0 }},
		{"late_cap_ms=0", func(c *Config) { c.NodeAPI.LateCapMS = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := Defaults()
			tc.mut(&bad)
			if err := bad.Validate(); err == nil {
				t.Fatalf("Validate aceitou %s, esperado erro", tc.name)
			}
		})
	}
}
