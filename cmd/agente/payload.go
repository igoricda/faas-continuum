package main

import (
	"encoding/json"
	"time"

	"github.com/igoricda/faas-continuum/internal/registry"
)

type registrationPayload struct {
	NodeID             string                                        `json:"node_id"`
	Layer              string                                        `json:"layer"`
	Host               string                                        `json:"host"`
	MinInstances       int                                           `json:"min_instances"`
	PSISupported       bool                                          `json:"psi_supported"`
	Functions          []string                                      `json:"functions,omitempty"`
	FunctionCategories map[string]string                             `json:"function_categories,omitempty"`
	FunctionExpected   map[string]map[string]registry.ExpectedMetric `json:"function_expected_metrics,omitempty"`
	FunctionCapacity   map[string]int                                `json:"max_instances,omitempty"`
	// The effective ceiling of execution per function, in milliseconds, which
	// the arbiter uses as the hard deadline of the best-effort delivery. It is
	// built by the agent from the effective timeouts of the functions.
	FunctionTimeoutMS map[string]int `json:"function_timeout_ms,omitempty"`
}

func buildRegistrationPayload(nodeID, layer, host string, psiSupported bool, minInstances int, funcs []string, categories map[string]string, getExpected func() map[string]map[string]registry.ExpectedMetric, capacity map[string]int, getTimeouts func() map[string]int) ([]byte, error) {
	var expected map[string]map[string]registry.ExpectedMetric
	if getExpected != nil {
		expected = getExpected()
	}
	var timeouts map[string]int
	if getTimeouts != nil {
		timeouts = getTimeouts()
	}
	p := registrationPayload{
		NodeID:             nodeID,
		Layer:              layer,
		Host:               host,
		MinInstances:       minInstances,
		PSISupported:       psiSupported,
		Functions:          funcs,
		FunctionCategories: categories,
		FunctionExpected:   expected,
		FunctionCapacity:   capacity,
		FunctionTimeoutMS:  timeouts,
	}
	return json.Marshal(p)
}

// effectiveFunctionTimeouts builds the effective ceiling of execution per
// function, in milliseconds, which is the single source of the hard deadline:
// the timeout of the function when it is positive and the default of the node
// otherwise, and for a function that supports late reports the minimum between
// the ceiling and the late ceiling, which is exactly the value in which the
// node answers. A non-positive late ceiling means no extra ceiling. It is pure,
// and the result is absent when there are no functions, which omits the key
// from the registration.
func effectiveFunctionTimeouts(funcs []registry.Function, defaultTimeout, lateCap time.Duration) map[string]int {
	if len(funcs) == 0 {
		return nil
	}
	out := make(map[string]int, len(funcs))
	for _, fn := range funcs {
		t := defaultTimeout
		if fn.TimeoutMS > 0 {
			t = time.Duration(fn.TimeoutMS) * time.Millisecond
		}
		if fn.LateReportable && lateCap > 0 && t > lateCap {
			t = lateCap
		}
		out[fn.Name] = int(t.Milliseconds())
	}
	return out
}
