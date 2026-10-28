// Per-node bearer tokens. Each node owns a token bound to its node_id, so a
// compromised node cannot forge registration, telemetry, or liveness for
// another node.
//
// Tokens are stored as unsalted SHA-256 hashes. That is sufficient because the
// plaintext carries 256 bits of crypto/rand entropy, so offline brute force is
// infeasible; hashing protects the file at rest and is not a stretching step.
// Verify runs on the request path and must cost microseconds, so a
// password-hashing KDF is not used here. Do not reuse this store for
// low-entropy secrets.
//
// Persistence matches NodeStore: snapshot under the lock, compact marshal,
// then tmp+rename+0600+fsync outside the lock, with an in-memory rollback on
// failure. fsync on Issue is acceptable because issuance is rare operator work;
// Verify never performs I/O.
package nodes

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/igoricda/faas-continuum/internal/fsutil"
)

// MaxPresentedNodeTokenBytes caps the presented token length accepted before
// hashing.
const MaxPresentedNodeTokenBytes = 4096

// nodeTokenBytes is the plaintext token entropy in bytes (256 bits).
const nodeTokenBytes = 32

// NodeTokenStore holds the SHA-256 hash of each node token by node_id and
// optionally persists them to disk. It is safe for concurrent use: Verify only
// reads under RLock, while Issue and Revoke snapshot and perform I/O outside
// the lock with rollback on failure.
type NodeTokenStore struct {
	mu     sync.RWMutex
	tokens map[string][32]byte // node_id -> sha256(plaintext token)
	path   string              // "" means no persistence (tests)
}

// NewNodeTokenStore returns a store backed by path. An empty path keeps the
// store in memory. An existing file is loaded, a missing file starts an empty
// store, and a corrupt file returns an error instead of failing open.
func NewNodeTokenStore(path string) (*NodeTokenStore, error) {
	s := &NodeTokenStore{
		tokens: make(map[string][32]byte),
		path:   path,
	}
	if path == "" {
		return s, nil
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *NodeTokenStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("arbitro: falha ao ler %q: %w", s.path, err)
	}
	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("arbitro: arquivo de tokens %q corrompido: %w", s.path, err)
	}
	for id, hexSum := range raw {
		sum, err := hex.DecodeString(hexSum)
		if err != nil || len(sum) != sha256.Size {
			return fmt.Errorf("arbitro: arquivo de tokens %q corrompido: entrada inválida para %q", s.path, id)
		}
		var arr [32]byte
		copy(arr[:], sum)
		s.tokens[id] = arr
	}
	return nil
}

// Issue generates a new token for nodeID and persists only its hash. The id
// does not need to be registered yet, which allows an operator to issue a token
// before the first registration. The plaintext is returned once and must never
// be logged or persisted.
func (s *NodeTokenStore) Issue(nodeID string) (string, error) {
	if nodeID == "" {
		return "", ErrInvalidNodeID
	}
	buf := make([]byte, nodeTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("arbitro: falha ao gerar token para %q: %w", nodeID, err)
	}
	plain := hex.EncodeToString(buf)
	sum := sha256.Sum256([]byte(plain))

	s.mu.Lock()
	_, existed := s.tokens[nodeID]
	if !existed && len(s.tokens) >= MaxNodes {
		s.mu.Unlock()
		return "", ErrTooManyNodes
	}
	var old [32]byte
	var hadOld bool
	if existed {
		old, hadOld = s.tokens[nodeID], true
	}
	s.tokens[nodeID] = sum
	s.mu.Unlock()

	if err := s.save(); err != nil {
		s.mu.Lock()
		if hadOld {
			s.tokens[nodeID] = old
		} else {
			delete(s.tokens, nodeID)
		}
		s.mu.Unlock()
		return "", fmt.Errorf("arbitro: falha ao persistir token de %q: %w", nodeID, err)
	}
	return plain, nil
}

// Verify reports whether presented is the token of nodeID. It is total and
// purely in-memory: malformed input returns false without panicking, and an
// unknown id returns false without revealing whether the id exists.
func (s *NodeTokenStore) Verify(nodeID, presented string) bool {
	if nodeID == "" || presented == "" || len(presented) > MaxPresentedNodeTokenBytes {
		return false
	}
	s.mu.RLock()
	stored, ok := s.tokens[nodeID]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	sum := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(sum[:], stored[:]) == 1
}

// Revoke removes the token of nodeID. Revoking an unknown id is a no-op. The
// change is persisted, and rolled back in memory on failure.
func (s *NodeTokenStore) Revoke(nodeID string) error {
	s.mu.Lock()
	old, existed := s.tokens[nodeID]
	if !existed {
		s.mu.Unlock()
		return nil
	}
	delete(s.tokens, nodeID)
	s.mu.Unlock()

	if err := s.save(); err != nil {
		s.mu.Lock()
		s.tokens[nodeID] = old
		s.mu.Unlock()
		return fmt.Errorf("arbitro: falha ao persistir revogação de %q: %w", nodeID, err)
	}
	return nil
}

func (s *NodeTokenStore) save() error {
	if s.path == "" {
		return nil
	}
	// Snapshot under RLock to keep the lock out of Marshal and I/O.
	s.mu.RLock()
	snapshot := make(map[string]string, len(s.tokens))
	for k, v := range s.tokens {
		snapshot[k] = hex.EncodeToString(v[:])
	}
	s.mu.RUnlock()

	// Compact output limits flash wear.
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("arbitro: falha ao serializar tokens: %w", err)
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
