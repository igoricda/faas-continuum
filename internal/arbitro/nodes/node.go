package nodes

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/igoricda/faas-continuum/internal/registry"
)

// Layer values available for a node.
const (
	LayerBorda = "borda"
	LayerNevoa = "nevoa"
)

// Node is a registered node.
//
// Functions lists the function names hosted by the node.
//
// FunctionCategories maps a hosted function name to its routing category.
//
// FunctionExpected maps a hosted function to its declared quality metrics,
// copied from the function definition in the agent registry when the node
// registers. Every outer key must be a member of Functions; metric names must
// be non-empty and values finite. A nil or empty inner map declares nothing
// and is valid. The field is omitted when empty, so files written without it
// load with a nil map, and a reader that does not know the field drops it
// silently.
//
// FunctionCapacity is the declared per-function ceiling (max_instances),
// copied from the function definition when the node registers. It is
// declarative, not a proactive gate: capacity filtering relies on live
// occupancy from the monitor. The declared ceiling is reported to clients and
// used to cap ingested availability. Values must be >= 0; 0 or absent means
// no ceiling. When Functions is non-empty every key must be a member of it;
// an empty Functions list makes the node eligible for every function and the
// ceiling is ignored. The field is omitted when empty.
//
// FunctionTimeoutMS is the effective execution ceiling per function in
// milliseconds: the function timeout when positive, otherwise the node
// default. The gateway uses it as the hard deadline for best-effort delivery.
// Keys must be a subset of Functions and values must be in
// 1..registry.MaxTimeoutMS. The field is omitted when empty.
//
// Fields are ordered widest first for alignment.
type Node struct {
	LastSeen           time.Time                                     `json:"last_seen"`
	NodeID             string                                        `json:"node_id"`
	Layer              string                                        `json:"layer"`
	Host               string                                        `json:"host"`
	Functions          []string                                      `json:"functions,omitempty"`
	FunctionCategories map[string]string                             `json:"function_categories,omitempty"`
	FunctionExpected   map[string]map[string]registry.ExpectedMetric `json:"function_expected_metrics,omitempty"`
	FunctionCapacity   map[string]int                                `json:"max_instances,omitempty"`
	FunctionTimeoutMS  map[string]int                                `json:"function_timeout_ms,omitempty"`
	MinInstances       int                                           `json:"min_instances"` // informational only
	PSISupported       bool                                          `json:"psi_supported"`
}

// Status values derived from node state: ativo means recently seen and not
// overloaded; indisponivel means stale (now-lastSeen > staleAfter);
// sobrecarregado means ativo with telemetry above the hard limits.
const (
	StatusAtivo          = "ativo"
	StatusIndisponivel   = "indisponivel"
	StatusSobrecarregado = "sobrecarregado"
)

// MaxNodes is the maximum number of distinct nodes the registry accepts.
const MaxNodes = 256

// Sentinel errors returned by the node and token stores. Messages carry the
// "arbitro:" prefix for traceability.
var (
	ErrInvalidLayer              = errors.New("arbitro: layer inválido")
	ErrInvalidNodeID             = errors.New("arbitro: node_id não pode ser vazio")
	ErrInvalidHost               = errors.New("arbitro: host não pode ser vazio")
	ErrInvalidFunctionCategories = errors.New("arbitro: function_categories inválido")
	ErrInvalidFunctionExpected   = errors.New("arbitro: function_expected_metrics inválido")
	ErrInvalidFunctionCapacity   = errors.New("arbitro: max_instances inválido")
	ErrInvalidFunctionTimeout    = errors.New("arbitro: function_timeout_ms inválido")
	ErrTooManyNodes              = errors.New("arbitro: número máximo de nós atingido")
	ErrNodeNotFound              = errors.New("arbitro: nó não encontrado")
)

// DerivedStatus returns "ativo" or "indisponivel" based on staleness. It is a
// pure function; a node is indisponivel when now.Sub(lastSeen) > staleAfter and
// remains ativo at the exact boundary.
func DerivedStatus(n Node, staleAfter time.Duration, now time.Time) string {
	if now.Sub(n.LastSeen) > staleAfter {
		return StatusIndisponivel
	}
	return StatusAtivo
}

func validateNode(n Node) error {
	if n.NodeID == "" {
		return ErrInvalidNodeID
	}
	if n.Host == "" {
		return ErrInvalidHost
	}
	if n.Layer != LayerBorda && n.Layer != LayerNevoa {
		return ErrInvalidLayer
	}
	if len(n.FunctionCategories) > 0 {
		hosted := make(map[string]struct{}, len(n.Functions))
		for _, f := range n.Functions {
			hosted[f] = struct{}{}
		}
		for k, v := range n.FunctionCategories {
			if k == "" {
				return fmt.Errorf("%w: function_categories com chave vazia", ErrInvalidFunctionCategories)
			}
			if v == "" {
				return fmt.Errorf("%w: function_categories[%q] com valor vazio", ErrInvalidFunctionCategories, k)
			}
			if _, ok := hosted[k]; !ok {
				return fmt.Errorf("%w: function_categories[%q] não é membro de functions", ErrInvalidFunctionCategories, k)
			}
		}
	}
	if len(n.FunctionExpected) > 0 {
		hosted := make(map[string]struct{}, len(n.Functions))
		for _, f := range n.Functions {
			hosted[f] = struct{}{}
		}
		for variant, metrics := range n.FunctionExpected {
			if variant == "" {
				return fmt.Errorf("%w: function_expected_metrics com chave externa vazia", ErrInvalidFunctionExpected)
			}
			if _, ok := hosted[variant]; !ok {
				return fmt.Errorf("%w: function_expected_metrics[%q] não é membro de functions", ErrInvalidFunctionExpected, variant)
			}
			// Inner rules mirror validateExpectedMetrics in the registry
			// package, which is the source of truth: a non-empty metric name
			// and a finite value. A nil or empty inner map declares nothing
			// and is valid.
			for name, metric := range metrics {
				if name == "" {
					return fmt.Errorf("%w: function_expected_metrics[%q] com nome de métrica vazio", ErrInvalidFunctionExpected, variant)
				}
				if math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) {
					return fmt.Errorf("%w: function_expected_metrics[%q][%q] com valor não-finito (%v)", ErrInvalidFunctionExpected, variant, name, metric.Value)
				}
			}
		}
	}
	if len(n.FunctionCapacity) > 0 {
		hosted := make(map[string]struct{}, len(n.Functions))
		for _, f := range n.Functions {
			hosted[f] = struct{}{}
		}
		for fn, max := range n.FunctionCapacity {
			if max < 0 {
				return fmt.Errorf("%w: max_instances[%q] negativo (%d)", ErrInvalidFunctionCapacity, fn, max)
			}
			if len(n.Functions) > 0 {
				if _, ok := hosted[fn]; !ok {
					return fmt.Errorf("%w: max_instances[%q] não é membro de functions", ErrInvalidFunctionCapacity, fn)
				}
			}
		}
	}
	if len(n.FunctionTimeoutMS) > 0 {
		hosted := make(map[string]struct{}, len(n.Functions))
		for _, f := range n.Functions {
			hosted[f] = struct{}{}
		}
		for fn, ms := range n.FunctionTimeoutMS {
			if ms < 1 || ms > registry.MaxTimeoutMS {
				return fmt.Errorf("%w: function_timeout_ms[%q] fora de 1..%d (%d)", ErrInvalidFunctionTimeout, fn, registry.MaxTimeoutMS, ms)
			}
			if len(n.Functions) > 0 {
				if _, ok := hosted[fn]; !ok {
					return fmt.Errorf("%w: function_timeout_ms[%q] não é membro de functions", ErrInvalidFunctionTimeout, fn)
				}
			}
		}
	}
	return nil
}
