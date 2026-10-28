// Tokens scoped by category. The package solves the limitation of forwarding
// the credential verbatim: each function in each node has its own invoke
// token, so a request of a category that moved from one node to another used
// to die with an unauthorized answer and without failover. Here the arbiter
// issues one token per category, which is the resolved group, and translates
// it, per attempt, into the invocation token that the destination node
// expects in the resolved concrete variant, which restores the failover
// between nodes.
//
// Trust model: the downstream map, from the category to the node to the
// concrete variant to the plaintext token of the node, is provisioned by the
// operator through the management interface and persisted in a file with
// restrictive permissions, and the plaintext is necessary because the arbiter
// needs to present it to the node. The token of the category, in turn, is
// only persisted as a hash, since it has 256 bits of entropy from the secure
// random source, is verified in constant time and needs no expensive
// re-hash in the hot path. This package must never be used for secrets with
// low entropy.
//
// Compatibility: a nil store, with the category tokens disabled, preserves
// the legacy behavior verbatim in every path, since the verification and the
// lookup of the downstream token are nil safe and return false without any
// input or output.
package cattokens

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/igoricda/faas-continuum/internal/fsutil"
)

// MaxPresentedTokenBytes bounds the presented token before any hash, which is
// fail-closed, in the same spirit as the equivalent limits of the other
// credential stores.
const MaxPresentedTokenBytes = 4096

// MaxNameBytes bounds the category, the node and the concrete variant against
// an abuse of cardinality in the file, since external input has a ceiling.
const MaxNameBytes = 128

// tokenBytes is the entropy of the plaintext token of a category, of 256 bits.
const tokenBytes = 32

var (
	// ErrInvalidCategory indicates an empty category, one that is too long or
	// one with a null byte, which would collide with the separator of the
	// flat and segment keys.
	ErrInvalidCategory = errors.New("cattokens: categoria inválida")
	// ErrInvalidDownstream indicates a malformed node, concrete variant or token in the map.
	ErrInvalidDownstream = errors.New("cattokens: downstream inválido")
)

// downKey composes the flat key of the map, with the same anti-collision
// separator as the segment keys of the history, and no component contains
// the separator, which the validation of names guarantees.
func downKey(category, nodeID, concrete string) string {
	return category + "\x00" + nodeID + "\x00" + concrete
}

// Store keeps the hashes of the tokens per category and the downstream map
// of translation. It is safe for concurrent use: the verification and the
// downstream lookup are read only under the read lock, and the issuance and
// the downstream update take a snapshot and perform the input and output
// outside the lock, with a rollback on failure, the same pattern as the node
// token store.
type Store struct {
	mu         sync.RWMutex
	tokens     map[string][32]byte // categoria -> sha256(token em texto plano)
	downstream map[string]string   // downKey -> token do nó em texto plano
	path       string              // empty means no persistence, which is the case of the tests
}

// NewStore creates a store. When the path is not empty and the file exists,
// it loads the saved state, when the file does not exist it starts empty, and
// when the file exists but is corrupted it fails loudly, which is
// fail-closed.
func NewStore(path string) (*Store, error) {
	s := &Store{
		tokens:     make(map[string][32]byte),
		downstream: make(map[string]string),
		path:       path,
	}
	if path == "" {
		return s, nil
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func validName(v string) bool {
	return v != "" && len(v) <= MaxNameBytes && !strings.ContainsRune(v, '\x00')
}

func (s *Store) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cattokens: falha ao ler %q: %w", s.path, err)
	}
	var raw struct {
		Tokens     map[string]string `json:"tokens"`
		Downstream map[string]string `json:"downstream"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("cattokens: arquivo %q corrompido: %w", s.path, err)
	}
	tokens := make(map[string][32]byte, len(raw.Tokens))
	for cat, hexSum := range raw.Tokens {
		if !validName(cat) {
			return fmt.Errorf("cattokens: arquivo %q corrompido: categoria inválida %q", s.path, cat)
		}
		sum, err := hex.DecodeString(hexSum)
		if err != nil || len(sum) != sha256.Size {
			return fmt.Errorf("cattokens: arquivo %q corrompido: hash inválido para %q", s.path, cat)
		}
		var arr [32]byte
		copy(arr[:], sum)
		tokens[cat] = arr
	}
	down := make(map[string]string, len(raw.Downstream))
	for key, tok := range raw.Downstream {
		parts := strings.Split(key, "\x00")
		if len(parts) != 3 || !validName(parts[0]) || !validName(parts[1]) || !validName(parts[2]) ||
			tok == "" || len(tok) > MaxPresentedTokenBytes {
			return fmt.Errorf("cattokens: arquivo %q corrompido: downstream inválido %q", s.path, key)
		}
		down[key] = tok
	}
	s.tokens = tokens
	s.downstream = down
	return nil
}

// Issue generates a new token for the category and persists it, as a hash
// only. Reissuing revokes the previous token. The plaintext is returned once
// and is never logged nor persisted.
func (s *Store) Issue(category string) (string, error) {
	if s == nil {
		return "", ErrInvalidCategory
	}
	if !validName(category) {
		return "", ErrInvalidCategory
	}
	buf := make([]byte, tokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("cattokens: falha ao gerar token para %q: %w", category, err)
	}
	plain := hex.EncodeToString(buf)
	sum := sha256.Sum256([]byte(plain))

	s.mu.Lock()
	old, hadOld := s.tokens[category]
	s.tokens[category] = sum
	s.mu.Unlock()

	if err := s.save(); err != nil {
		s.mu.Lock()
		if hadOld {
			s.tokens[category] = old
		} else {
			delete(s.tokens, category)
		}
		s.mu.Unlock()
		return "", fmt.Errorf("cattokens: falha ao persistir token de %q: %w", category, err)
	}
	return plain, nil
}

// Verify reports whether the presented token is the token of the category. It
// is nil safe, so an absent store returns false and is the legacy path. It is
// total and pure in memory: a malformed entry returns false without a panic,
// and an unknown category returns false without distinguishing itself from a
// wrong token, so the categories are not enumerated.
func (s *Store) Verify(category, presented string) bool {
	if s == nil || category == "" || presented == "" || len(presented) > MaxPresentedTokenBytes {
		return false
	}
	s.mu.RLock()
	stored, ok := s.tokens[category]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	sum := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(sum[:], stored[:]) == 1
}

// SetDownstream records or overwrites the token that the node expects in the
// concrete variant, for the translation. An empty token removes the entry,
// without a dedicated deletion route. It persists to the disk with a rollback
// on failure.
func (s *Store) SetDownstream(category, nodeID, concrete, token string) error {
	if s == nil {
		return ErrInvalidCategory
	}
	if !validName(category) {
		return ErrInvalidCategory
	}
	if !validName(nodeID) || !validName(concrete) || len(token) > MaxPresentedTokenBytes {
		return ErrInvalidDownstream
	}
	key := downKey(category, nodeID, concrete)
	s.mu.Lock()
	old, hadOld := s.downstream[key]
	if token == "" {
		delete(s.downstream, key)
	} else {
		s.downstream[key] = token
	}
	s.mu.Unlock()

	if err := s.save(); err != nil {
		s.mu.Lock()
		if hadOld {
			s.downstream[key] = old
		} else {
			delete(s.downstream, key)
		}
		s.mu.Unlock()
		return fmt.Errorf("cattokens: falha ao persistir downstream de %q: %w", category, err)
	}
	return nil
}

// DownstreamFor returns the token to present to the node in the concrete
// variant, when it is mapped. It is nil safe, so an absent store returns
// false and preserves the legacy path verbatim.
func (s *Store) DownstreamFor(category, nodeID, concrete string) (string, bool) {
	if s == nil || category == "" || nodeID == "" || concrete == "" {
		return "", false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	tok, ok := s.downstream[downKey(category, nodeID, concrete)]
	return tok, ok
}

// Snapshot returns the issued categories and, per category, the mapped pairs
// of node and concrete variant, without any plaintext, which is safe for an
// operator query.
func (s *Store) Snapshot() map[string]map[string][]string {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]map[string][]string, len(s.tokens))
	for cat := range s.tokens {
		out[cat] = make(map[string][]string)
	}
	byCat := make(map[string]map[string][]string)
	for key := range s.downstream {
		parts := strings.SplitN(key, "\x00", 3)
		if len(parts) != 3 {
			continue
		}
		m := byCat[parts[0]]
		if m == nil {
			m = make(map[string][]string)
			byCat[parts[0]] = m
		}
		m[parts[1]] = append(m[parts[1]], parts[2])
	}
	for cat, m := range byCat {
		dst := out[cat]
		if dst == nil {
			dst = make(map[string][]string)
			out[cat] = dst
		}
		for node, concretes := range m {
			dst[node] = concretes
		}
	}
	return out
}

func (s *Store) save() error {
	if s.path == "" {
		return nil
	}
	s.mu.RLock()
	snapshot := struct {
		Tokens     map[string]string `json:"tokens"`
		Downstream map[string]string `json:"downstream"`
	}{
		Tokens:     make(map[string]string, len(s.tokens)),
		Downstream: make(map[string]string, len(s.downstream)),
	}
	for k, v := range s.tokens {
		snapshot.Tokens[k] = hex.EncodeToString(v[:])
	}
	for k, v := range s.downstream {
		snapshot.Downstream[k] = v
	}
	s.mu.RUnlock()

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("cattokens: falha ao serializar: %w", err)
	}
	data = append(data, '\n')
	if err := fsutil.WriteFileDurable(s.path+".tmp", data); err != nil {
		return fmt.Errorf("cattokens: falha ao gravar %q: %w", s.path+".tmp", err)
	}
	if err := os.Rename(s.path+".tmp", s.path); err != nil {
		return fmt.Errorf("cattokens: falha ao renomear %q para %q: %w", s.path+".tmp", s.path, err)
	}
	return nil
}
