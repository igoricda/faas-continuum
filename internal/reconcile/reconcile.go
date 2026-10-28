// Package reconcile rebuilds the state of the instance pool after the agent
// restarts.
//
// Persisting the function metadata is only half of the problem: the pool
// itself is not persisted, and should not be, because the containers are
// managed by the container runtime, which survives the death of the agent
// process. Ignoring the surviving containers would waste work already done
// and break the promise of a pre-allocated floor exactly in the most
// sensitive window, right after a crash or a deployment.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/igoricda/faas-continuum/internal/containerruntime"
	"github.com/igoricda/faas-continuum/internal/pool"
	"github.com/igoricda/faas-continuum/internal/registry"
)

// ContainerRuntime is what the reconciliation needs from the container
// runtime, expressed in terms of instance identifiers instead of the types
// of the runtime SDK, so that it can be tested without a real daemon. It is
// implemented by the pool adapter.
type ContainerRuntime interface {
	// ListInstanceIDs returns the identifiers of all the containers present
	// in the namespace of the framework, including those that survived a
	// previous run of the agent.
	ListInstanceIDs(ctx context.Context) ([]string, error)
	// AdoptExisting inspects an existing container and, when its task is
	// running, records the handle in the accounting of the adapter. An
	// error meaning that the container exists but does not run is not a
	// failure. Every other error is returned.
	AdoptExisting(ctx context.Context, instanceID string) error
	// StopAndRemoveExisting stops and removes an orphan or unhealthy
	// container.
	StopAndRemoveExisting(ctx context.Context, instanceID string) error
}

// PoolManager is the subset of the pool manager used by the reconciliation.
// It is declared as an interface to make the contract explicit, and the
// concrete pool manager satisfies it structurally.
type PoolManager interface {
	Adopt(functionName, instanceID string)
	EnsureMinInstances(ctx context.Context, functionName, imageRef string, minInstances int) error
	ScaleDownToMin(ctx context.Context, functionName string, minInstances int) (int, error)
}

// Run performs the full reconciliation:
//
//  1. It lists the containers that exist in the container runtime.
//  2. For each one, it extracts the function name from the instance
//     identifier. Without a matching persisted function the container is an
//     orphan and is removed. A container whose task does not run is
//     unhealthy and is removed, and it does not count towards the minimum
//     of instances: the health signal available here is the state of the
//     task, not the state of the application. A healthy container is
//     always adopted as a free instance, because no request survives an
//     agent restart: the original connection with the client is already
//     gone.
//  3. For each registered function, it fills the gap between the adopted
//     instances and the minimum. A success clears a previous degraded
//     state, while a failure marks the function as degraded instead of
//     taking the whole agent down.
//
// The reconciliation is best-effort: a problem with one function is
// recorded in the log and in its state instead of interrupting the
// reconciliation of the others. The aggregated error is returned so the
// caller can log a warning at startup, and it must not be treated as
// fatal, since a degraded function can still be recovered through the node
// API.
func Run(ctx context.Context, rt ContainerRuntime, reg *registry.Registry, pl PoolManager) error {
	if err := adoptOrRemoveExisting(ctx, rt, reg, pl); err != nil {
		return err
	}
	return restoreMinInstances(ctx, reg, pl)
}

func adoptOrRemoveExisting(ctx context.Context, rt ContainerRuntime, reg *registry.Registry, pl PoolManager) error {
	instanceIDs, err := rt.ListInstanceIDs(ctx)
	if err != nil {
		return fmt.Errorf("reconcile: falha ao listar contêineres existentes: %w", err)
	}

	for _, instanceID := range instanceIDs {
		// Outside the naming convention, so the container belongs to
		// someone else: it is ignored and never removed.
		if _, _, parses := pool.ParseInstanceID(instanceID); !parses {
			log.Printf("reconcile: contêiner %q não segue a convenção de nomenclatura esperada — ignorado", instanceID)
			continue
		}
		// The owner is resolved explicitly, because the parser and the
		// registry decide together. An identifier without a registered
		// owner is an orphan of the framework, left by a function that was
		// unregistered after the restart, or a foreign object in the
		// namespace. It is removed and never adopted by the wrong
		// function: an identifier of the form "fn-1" is not a container
		// of the function "fn-1", whose identifiers carry a numeric
		// suffix.
		functionName, ok := pool.ResolveOwner(instanceID, func(name string) bool {
			_, exists := reg.Get(name)
			return exists
		})
		if !ok {
			// The identifier matches the instance naming convention, so it is
			// an orphan of the framework or a foreign object in the
			// dedicated namespace. The removal is best-effort and only
			// affects the observability.
			discard(ctx, rt, instanceID, "reconcile: removendo contêiner que casa o padrão de instanceID %q sem dono registrado", instanceID)
			continue
		}

		err := rt.AdoptExisting(ctx, instanceID)
		if errors.Is(err, containerruntime.ErrNotRunning) {
			discard(ctx, rt, instanceID, "reconcile: contêiner %q não está em execução — removendo", instanceID)
			continue
		}
		if err != nil {
			discard(ctx, rt, instanceID, "reconcile: falha ao inspecionar contêiner %q: %v — tratando como não saudável", instanceID, err)
			continue
		}

		pl.Adopt(functionName, instanceID)
		log.Printf("reconcile: instância %q adotada para a função %q", instanceID, functionName)
	}
	return nil
}

// discard records the reason and removes the container on a best-effort
// basis, never aborting the loop: a failed removal is logged as well.
func discard(ctx context.Context, rt ContainerRuntime, instanceID string, format string, args ...any) {
	log.Printf(format, args...)
	if err := rt.StopAndRemoveExisting(ctx, instanceID); err != nil {
		log.Printf("reconcile: falha ao remover contêiner %q: %v", instanceID, err)
	}
}

func restoreMinInstances(ctx context.Context, reg *registry.Registry, pl PoolManager) error {
	var errs []error
	for _, fn := range reg.List() {
		reaped, err := pl.ScaleDownToMin(ctx, fn.Name, fn.MinInstances)
		if err != nil {
			log.Printf("reconcile: falha ao escalar para baixo %q: %v", fn.Name, err)
		}
		if reaped > 0 {
			log.Printf("reconcile: scaled down %q (%d instances removed)", fn.Name, reaped)
		}

		if err := pl.EnsureMinInstances(ctx, fn.Name, fn.ImageRef, fn.MinInstances); err != nil {
			errMsg := fmt.Sprintf("falha ao restaurar min_instances de %q (imagem %q): %v", fn.Name, fn.ImageRef, err)
			log.Printf("reconcile: %s", errMsg)
			if markErr := reg.MarkDegraded(fn.Name, errMsg); markErr != nil {
				log.Printf("reconcile: falha adicional ao marcar %q como degraded: %v", fn.Name, markErr)
			}
			errs = append(errs, fmt.Errorf("%s: %w", fn.Name, err))
			continue
		}

		if fn.Degraded() {
			if err := reg.ClearDegraded(fn.Name); err != nil {
				log.Printf("reconcile: falha ao limpar estado degraded de %q: %v", fn.Name, err)
			} else {
				log.Printf("reconcile: função %q não está mais degraded", fn.Name)
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("reconcile: %d função(ões) não puderam restaurar min_instances: %w", len(errs), errors.Join(errs...))
	}
	return nil
}
