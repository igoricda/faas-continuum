// Package nodes implements the arbitration node registry: node registration,
// derived liveness status, and per-node bearer tokens. Node ranking lives in
// the selection package.
//
// The registry is persisted as compact JSON using an atomic
// temporary-file-and-rename write with mode 0600 and fsync.
package nodes

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/igoricda/faas-continuum/internal/fsutil"
)

// NodeStore holds the node registry and optionally persists it to disk. It is
// safe for concurrent use.
type NodeStore struct {
	mu    sync.RWMutex
	nodes map[string]Node // key: node_id
	path  string          // "" means no persistence (tests)
}

// NewNodeStore returns a store backed by path. An empty path keeps the store
// in memory. An existing file is loaded, a missing file starts an empty store,
// and a corrupt file returns an error instead of silently discarding data.
func NewNodeStore(path string) (*NodeStore, error) {
	s := &NodeStore{
		nodes: make(map[string]Node, 16),
		path:  path,
	}
	if path == "" {
		return s, nil
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *NodeStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("arbitro: falha ao ler %q: %w", s.path, err)
	}
	var nodes map[string]Node
	if err := json.Unmarshal(data, &nodes); err != nil {
		return fmt.Errorf("arbitro: arquivo de persistência %q corrompido: %w", s.path, err)
	}
	// Validate on load exactly as Upsert does, so a hand-edited file with an
	// invalid layer or a negative ceiling fails at startup instead of being
	// accepted silently.
	for id, n := range nodes {
		if err := validateNode(n); err != nil {
			return fmt.Errorf("arbitro: arquivo de persistência %q com nó %q inválido: %w", s.path, id, err)
		}
	}
	if nodes != nil {
		s.nodes = nodes
	}
	return nil
}

// Upsert inserts or updates a node atomically and reports whether it created a
// new entry, so the caller can pick a response status without a separate Get.
// The layer must be borda or nevoa and node_id and host must be non-empty.
// ErrTooManyNodes is returned only when adding a new node while at MaxNodes;
// an existing node can still be updated at the cap. LastSeen is always set to
// the current UTC time. On a persistence failure the in-memory change is
// rolled back.
func (s *NodeStore) Upsert(n Node) (bool, error) {
	if err := validateNode(n); err != nil {
		return false, err
	}

	s.mu.Lock()
	_, existed := s.nodes[n.NodeID]
	if !existed && len(s.nodes) >= MaxNodes {
		s.mu.Unlock()
		return false, ErrTooManyNodes
	}
	var old Node
	var hadOld bool
	if existed {
		old = s.nodes[n.NodeID]
		hadOld = true
	}
	n.LastSeen = time.Now().UTC()
	s.nodes[n.NodeID] = n
	s.mu.Unlock()

	if err := s.save(); err != nil {
		s.mu.Lock()
		if hadOld {
			s.nodes[n.NodeID] = old
		} else {
			delete(s.nodes, n.NodeID)
		}
		s.mu.Unlock()
		return false, fmt.Errorf("arbitro: falha ao persistir registro de %q: %w", n.NodeID, err)
	}
	return !existed, nil
}

// Get returns the node with the given node_id.
func (s *NodeStore) Get(nodeID string) (Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[nodeID]
	return n, ok
}

// List returns all registered nodes as a snapshot under RLock, sorted by
// NodeID. The order is deterministic because map iteration order is random and
// would otherwise shuffle ties in the gateway queue and in ranking.
func (s *NodeStore) List() []Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n)
	}
	slices.SortFunc(out, func(a, b Node) int {
		switch {
		case a.NodeID < b.NodeID:
			return -1
		case a.NodeID > b.NodeID:
			return 1
		default:
			return 0
		}
	})
	return out
}

// Touch updates the node's LastSeen to the current UTC time. It does not take
// a time argument and does not call save: liveness is telemetry, exempt from
// fsync, which matters on devices with limited persistent storage. It returns
// ErrNodeNotFound if the node does not exist.
//
// Upsert and Delete mutate in memory, save outside the lock, and roll back on
// failure. A concurrent Touch between such a mutation and a failed save is
// therefore lost, because the rollback restores the previous value. The window
// is narrow and self-limiting: liveness is re-derived on the next Touch or
// telemetry update.
func (s *NodeStore) Touch(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNodeNotFound, nodeID)
	}
	n.LastSeen = time.Now().UTC()
	s.nodes[nodeID] = n
	return nil
}

// Delete removes a node from the registry and returns ErrNodeNotFound if it
// does not exist. The change is persisted, and rolled back in memory on
// failure.
func (s *NodeStore) Delete(nodeID string) error {
	s.mu.Lock()
	n, ok := s.nodes[nodeID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNodeNotFound, nodeID)
	}
	delete(s.nodes, nodeID)
	s.mu.Unlock()

	if err := s.save(); err != nil {
		s.mu.Lock()
		s.nodes[nodeID] = n
		s.mu.Unlock()
		return fmt.Errorf("arbitro: falha ao persistir remoção de %q: %w", nodeID, err)
	}
	return nil
}

func (s *NodeStore) save() error {
	if s.path == "" {
		return nil
	}
	// Snapshot under RLock to keep the lock out of Marshal and I/O.
	s.mu.RLock()
	snapshot := make(map[string]Node, len(s.nodes))
	for k, v := range s.nodes {
		snapshot[k] = v
	}
	s.mu.RUnlock()

	// Compact output limits flash wear.
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("arbitro: falha ao serializar: %w", err)
	}
	data = append(data, '\n')
	if err := fsutil.WriteFileDurable(s.path+".tmp", data); err != nil {
		return fmt.Errorf("arbitro: falha ao gravar %q: %w", s.path+".tmp", err)
	}
	if err := os.Rename(s.path+".tmp", s.path); err != nil {
		return fmt.Errorf("arbitro: falha ao renomear %q para %q: %w", s.path+".tmp", s.path, err)
	}
	return nil
}
