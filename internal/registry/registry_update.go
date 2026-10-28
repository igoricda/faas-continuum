package registry

import (
	"errors"
	"fmt"
	"time"
)

// Update replaces the configuration of a function that is already
// registered, which is required: Update never creates a function implicitly.
// It preserves the creation time, updates the other fields and the update
// time, and clears the degraded state, since a successful update is the
// normal way to repair a function that had become degraded. It applies the
// same validation of the instance bounds as Register.
//
// Update does not touch the instance pool: that belongs to the caller,
// which decides how to migrate the running instances to the new
// configuration.
//
// With rotate set, the invoke token is regenerated and its plaintext is
// returned once; otherwise the existing hash is preserved.
func (r *Registry) Update(host, name, imageRef string, opts RegisterOptions, rotate bool) (Function, string, error) {
	minInstances, maxInstances, normalizedRef, err := r.validateRegisterOpts(name, imageRef, opts)
	if err != nil {
		return Function{}, "", err
	}

	plainToken := ""
	sumHex := ""
	if rotate {
		var genErr error
		plainToken, sumHex, genErr = generateInvokeToken()
		if genErr != nil {
			return Function{}, "", genErr
		}
	}

	base := Function{
		Name:            name,
		ResourceURL:     fmt.Sprintf("%s/function/%s", host, name),
		ImageRef:        normalizedRef,
		MinInstances:    minInstances,
		MaxInstances:    maxInstances,
		QueueMax:        opts.QueueMax,
		TimeoutMS:       opts.TimeoutMS,
		IdleTimeout:     opts.IdleTimeout,
		ExpectedMetrics: opts.ExpectedMetrics,
		LateReportable:  opts.LateReportable,
		GPU:             opts.GPU,
		UpdatedAt:       time.Now().UTC(),
		LastError:       "",
	}

	var updated Function
	err = r.apply(func(functions map[string]Function) (undo func(), err error) {
		prev, exists := functions[name]
		if !exists {
			return nil, fmt.Errorf("%w: %q", ErrFunctionNotFound, name)
		}
		updated = base
		updated.CreatedAt = prev.CreatedAt // preservado
		if rotate {
			updated.InvokeTokenSHA256 = sumHex
			updated.setInvokeTokenCache(sumHex)
		} else {
			updated.InvokeTokenSHA256 = prev.InvokeTokenSHA256 // preservado
			updated.invokeTokenSum, updated.invokeTokenSumValid = prev.invokeTokenSum, prev.invokeTokenSumValid
		}
		functions[name] = updated
		return func() { functions[name] = prev }, nil // rollback em memória
	})
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return Function{}, "", err
		}
		return Function{}, "", fmt.Errorf("registry: falha ao persistir atualização de %q: %w", name, err)
	}
	return updated, plainToken, nil
}

// MarkDegraded marks a function as registered but without usable instances,
// recording the reason of the last error. It is used when the provisioning
// after a configuration change fails after the old instances were already
// torn down, so they can no longer be restored.
func (r *Registry) MarkDegraded(name, lastError string) error {
	err := r.apply(func(functions map[string]Function) (undo func(), err error) {
		prev, exists := functions[name]
		if !exists {
			return nil, fmt.Errorf("%w: %q", ErrFunctionNotFound, name)
		}
		fn := functions[name]
		fn.LastError = lastError
		functions[name] = fn
		// The degraded state is derived: a non-empty last error means the
		// function is degraded.
		return func() { functions[name] = prev }, nil // rollback
	})
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return err
		}
		return fmt.Errorf("registry: falha ao persistir estado degradado de %q: %w", name, err)
	}
	return nil
}

// ClearDegraded clears the degraded state of a function, which the restart
// reconciliation uses when it manages to restore the minimum instances of a
// function that had become degraded. Calling it on a function that is not
// degraded is not an error.
func (r *Registry) ClearDegraded(name string) error {
	err := r.apply(func(functions map[string]Function) (undo func(), err error) {
		prev, exists := functions[name]
		if !exists {
			return nil, fmt.Errorf("%w: %q", ErrFunctionNotFound, name)
		}
		if prev.LastError == "" {
			return nil, nil // idempotent: nothing changed, no save
		}
		fn := functions[name]
		fn.LastError = ""
		functions[name] = fn
		// The degraded state is derived: an empty last error means the
		// function is not degraded.
		return func() { functions[name] = prev }, nil // rollback
	})
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return err
		}
		return fmt.Errorf("registry: falha ao persistir limpeza de estado degradado de %q: %w", name, err)
	}
	return nil
}

// Unregister removes a function from the registry and frees its name for a
// new registration. It returns ErrFunctionNotFound when the function does
// not exist. It is used both to undo a registration whose provisioning
// failed, which avoids leaving a zombie function behind, and to remove a
// function explicitly.
func (r *Registry) Unregister(name string) error {
	err := r.apply(func(functions map[string]Function) (undo func(), err error) {
		prev, exists := functions[name]
		if !exists {
			return nil, fmt.Errorf("%w: %q", ErrFunctionNotFound, name)
		}
		delete(functions, name)
		return func() { functions[name] = prev }, nil // rollback em memória
	})
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return err
		}
		return fmt.Errorf("registry: falha ao persistir remoção de %q: %w", name, err)
	}
	return nil
}
