// Package scaler periodically reaps idle instances down to the minimum of
// each function.
//
// Every function has an idle timeout of its own, or the global default. The
// scaler runs on a fixed ticker: when the time since the last use of an
// instance reaches the effective timeout, the excess instances are reaped
// newest first.
package scaler

import (
	"context"
	"log"
	"time"

	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// DefaultInterval is the period of the scaler ticker.
const DefaultInterval = 30 * time.Second

// Manager runs the periodic reaping loop.
type Manager struct {
	pool           *pool.Manager
	defaultTimeout time.Duration
	interval       time.Duration
	clock          func() time.Time

	// list is the single source of functions for the reaping loop. When it
	// is nil, the manager calls the registry through the captured
	// function.
	list func() []registry.Function
}

// New returns a Manager. A zero interval selects DefaultInterval and a nil
// clock selects time.Now. It starts no goroutine: the caller runs
// mgr.Run(ctx).
func New(pl *pool.Manager, reg *registry.Registry, defaultTimeout, interval time.Duration) *Manager {
	if interval == 0 {
		interval = DefaultInterval
	}
	var list func() []registry.Function
	if reg != nil {
		list = reg.List
	} else {
		list = func() []registry.Function {
			return nil
		}
	}
	m := &Manager{
		pool:           pl,
		defaultTimeout: defaultTimeout,
		interval:       interval,
		clock:          time.Now,
		list:           list,
	}
	return m
}

// WithClock sets the clock used by the manager.
func (m *Manager) WithClock(fn func() time.Time) *Manager {
	if fn != nil {
		m.clock = fn
	} else {
		m.clock = time.Now
	}
	return m
}

// reapOnce runs one reaping iteration: it lists the functions and calls
// pool.ReapIdle for each. Run calls it on every tick.
func (m *Manager) reapOnce(ctx context.Context) {
	now := m.clock()
	funcs := m.list()
	if m.pool == nil {
		return
	}
	n, err := m.pool.ReapIdle(ctx, now, funcs, m.defaultTimeout)
	if err != nil {
		log.Printf("scaler: failed to reap idle instances: %v", err)
	}
	if n > 0 {
		log.Printf("scaler: reaped %d idle instances", n)
	}
}

// Run blocks until the context is cancelled, calling reapOnce on every
// tick.
func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reapOnce(ctx)
		}
	}
}
