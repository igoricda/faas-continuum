package pool

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/igoricda/faas-continuum/internal/config"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// EffectiveTimeout returns the effective timeout of the function.
func EffectiveTimeout(fn registry.Function, globalDefault time.Duration) time.Duration {
	if d := time.Duration(fn.IdleTimeout); d > 0 {
		return d
	}
	if globalDefault > 0 {
		return globalDefault
	}
	return time.Duration(config.Defaults().Scaling.DefaultScaleDownTimeout)
}

func selectVictims(idle []instance, excess int) []string {
	sort.Slice(idle, func(i, j int) bool {
		_, idxI, okI := ParseInstanceID(idle[i].id)
		_, idxJ, okJ := ParseInstanceID(idle[j].id)
		if okI && okJ {
			return idxI > idxJ
		}
		if okI != okJ {
			return okI
		}
		return idle[i].id > idle[j].id
	})
	victims := make([]string, 0, excess)
	for i := 0; i < excess && i < len(idle); i++ {
		victims = append(victims, idle[i].id)
	}
	return victims
}

func (m *Manager) reapVictims(ctx context.Context, functionName string, victims []string) (int, []error) {
	s := m.shardFor(functionName)
	var errs []error
	reaped := 0
	for _, vid := range victims {
		if err := m.provisioner.Terminate(ctx, vid); err != nil {
			errs = append(errs, fmt.Errorf("instância %q: %w", vid, err))
			continue
		}
		s.mu.Lock()
		m.removeInstanceLockedOnShard(s, functionName, vid)
		s.mu.Unlock()
		m.globalMu.Lock()
		delete(m.instanceByID, vid)
		m.globalMu.Unlock()
		reaped++
	}
	return reaped, errs
}

func (m *Manager) collectIdleVictimsLockedOnShard(s *shard, functionName string, min int) []string {
	list := s.instances[functionName]
	if len(list) <= min {
		return nil
	}
	var idle []instance
	for _, inst := range list {
		if !inst.busy {
			idle = append(idle, inst)
		}
	}
	excess := len(list) - min
	if len(idle) == 0 {
		return nil
	}
	if excess > len(idle) {
		excess = len(idle)
	}
	return selectVictims(idle, excess)
}

func (m *Manager) ScaleDownToMin(ctx context.Context, functionName string, min int) (int, error) {
	if min < 0 {
		return 0, fmt.Errorf("pool: min não pode ser negativo, recebido %d", min)
	}
	s := m.shardFor(functionName)
	s.mu.Lock()
	victims := m.collectIdleVictimsLockedOnShard(s, functionName, min)
	s.mu.Unlock()
	if len(victims) == 0 {
		return 0, nil
	}

	reaped, errs := m.reapVictims(ctx, functionName, victims)
	if len(errs) > 0 {
		return reaped, fmt.Errorf("pool: falha ao escalar para baixo %q: %w", functionName, errors.Join(errs...))
	}
	return reaped, nil
}

func (m *Manager) ReapIdle(ctx context.Context, now time.Time, funcs []registry.Function, globalDefault time.Duration) (int, error) {
	if len(funcs) == 0 {
		return 0, nil
	}
	total := 0
	var errs []error
	for _, fn := range funcs {
		eff := EffectiveTimeout(fn, globalDefault)
		s := m.shardFor(fn.Name)
		s.mu.Lock()
		lu := s.lastUsed[fn.Name]
		if now.Sub(lu) < eff {
			s.mu.Unlock()
			continue
		}
		victims := m.collectIdleVictimsLockedOnShard(s, fn.Name, fn.MinInstances)
		s.mu.Unlock()
		if len(victims) == 0 {
			continue
		}

		reaped, victimErrs := m.reapVictims(ctx, fn.Name, victims)
		total += reaped
		errs = append(errs, victimErrs...)
	}
	if len(errs) > 0 {
		return total, fmt.Errorf("pool: falha ao reap idle: %w", errors.Join(errs...))
	}
	return total, nil
}

func (m *Manager) RemoveFunction(ctx context.Context, functionName string) error {
	// The teardown error carries the package prefix, so the waiters woken
	// with it fail fast instead of timing out slowly.
	teardownErr := fmt.Errorf("pool: função %q removida durante a espera (teardown)", functionName)
	s := m.shardFor(functionName)
	s.mu.Lock()
	list := s.instances[functionName]
	delete(s.instances, functionName)
	if s.pendingID != nil {
		delete(s.pendingID, functionName)
	}
	delete(s.lastUsed, functionName)
	// Drain the queue by waking every waiter with the teardown error, which
	// mirrors the wakeup of a failed provisioning in AcquireWaitCold.
	// Waking only one would rely on the cascade of the error branch,
	// and that cascade stops as soon as a waiter in the middle
	// cancels. No I/O happens under the lock.
	for len(s.waiters[functionName]) > 0 {
		m.wakeOneLockedOnShard(s, functionName, acquireResult{err: teardownErr})
	}
	s.mu.Unlock()

	// The identifier leaves the global index only after a successful
	// termination, one instance at a time, which mirrors the reaping of
	// victims: an instance whose termination failed keeps its identifier
	// and the next index skips ahead, so the next acquisition cannot
	// collide with it. The snapshot is taken and the lock released
	// before the slow call.
	var errs []error
	for _, inst := range list {
		if err := m.provisioner.Terminate(ctx, inst.id); err != nil {
			errs = append(errs, fmt.Errorf("pool: instância %q: %w", inst.id, err))
			continue
		}
		m.globalMu.Lock()
		delete(m.instanceByID, inst.id)
		m.globalMu.Unlock()
	}
	if len(errs) > 0 {
		return fmt.Errorf("pool: falha ao remover %d de %d instância(s) de %q: %w", len(errs), len(list), functionName, errors.Join(errs...))
	}
	return nil
}

func (m *Manager) Adopt(functionName, instanceID string) {
	s := m.shardFor(functionName)
	s.mu.Lock()
	for _, inst := range s.instances[functionName] {
		if inst.id == instanceID {
			s.mu.Unlock()
			return
		}
	}
	s.mu.Unlock()

	m.globalMu.RLock()
	_, indexed := m.instanceByID[instanceID]
	m.globalMu.RUnlock()
	if indexed {
		s.mu.Lock()
		for _, cur := range s.instances[functionName] {
			if cur.id == instanceID {
				s.mu.Unlock()
				return
			}
		}
		if s.instances == nil {
			s.instances = make(map[string][]instance)
		}
		// An adopted instance starts free: the shard slice owns the busy
		// flag.
		s.instances[functionName] = append(s.instances[functionName], instance{id: instanceID})
		s.mu.Unlock()
		return
	}
	inst := instance{id: instanceID}
	s.mu.Lock()
	if s.instances == nil {
		s.instances = make(map[string][]instance)
	}
	// Check again after taking the lock.
	for _, cur := range s.instances[functionName] {
		if cur.id == instanceID {
			s.mu.Unlock()
			return
		}
	}
	s.instances[functionName] = append(s.instances[functionName], inst)
	s.mu.Unlock()
	m.globalMu.Lock()
	if m.instanceByID == nil {
		m.instanceByID = make(map[string]struct{})
	}
	m.instanceByID[instanceID] = struct{}{}
	m.globalMu.Unlock()
}
