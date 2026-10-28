package nodes

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustNode(id, layer, host string) Node {
	return Node{
		NodeID:       id,
		Layer:        layer,
		Host:         host,
		MinInstances: 1,
		PSISupported: true,
		Functions:    []string{"hello"},
	}
}

// ---------------------------------------------------------------------------
// Validation and errors
// ---------------------------------------------------------------------------

func TestNodeStore_Validacao(t *testing.T) {
	t.Helper()
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}

	cases := []struct {
		name string
		node Node
		want error
	}{
		{"node_id vazio", Node{NodeID: "", Layer: LayerBorda, Host: "10.0.0.1:8080"}, ErrInvalidNodeID},
		{"host vazio", Node{NodeID: "n1", Layer: LayerBorda, Host: ""}, ErrInvalidHost},
		{"layer inválido", Node{NodeID: "n1", Layer: "cloud", Host: "10.0.0.1:8080"}, ErrInvalidLayer},
		{"layer vazio", Node{NodeID: "n1", Layer: "", Host: "10.0.0.1:8080"}, ErrInvalidLayer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Upsert(tc.node)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Upsert(%q) err=%v, want %v", tc.name, err, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// A repeated registration updates the fields and the last seen time
// ---------------------------------------------------------------------------

func TestNodeStore_UpsertIdempotente(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}

	n1 := mustNode("n1", LayerBorda, "10.0.0.1:8080")
	isNew, err := s.Upsert(n1)
	if err != nil {
		t.Fatalf("Upsert 1: %v", err)
	}
	if !isNew {
		t.Fatalf("primeiro Upsert deve retornar isNew=true")
	}
	got, ok := s.Get("n1")
	if !ok {
		t.Fatalf("Get após Upsert: não encontrado")
	}
	firstSeen := got.LastSeen
	if firstSeen.IsZero() {
		t.Fatalf("LastSeen não preenchido")
	}

	time.Sleep(10 * time.Millisecond)

	n2 := Node{NodeID: "n1", Layer: LayerNevoa, Host: "10.0.0.2:9090", MinInstances: 5, PSISupported: false, Functions: []string{"a", "b"}}
	isNew, err = s.Upsert(n2)
	if err != nil {
		t.Fatalf("Upsert 2: %v", err)
	}
	if isNew {
		t.Fatalf("segundo Upsert mesmo node_id deve retornar isNew=false")
	}
	got, ok = s.Get("n1")
	if !ok {
		t.Fatalf("Get após segundo Upsert: não encontrado")
	}
	if got.Layer != LayerNevoa || got.Host != "10.0.0.2:9090" || got.MinInstances != 5 || got.PSISupported != false {
		t.Fatalf("campos não atualizados: %+v", got)
	}
	if len(got.Functions) != 2 {
		t.Fatalf("functions não atualizado: %v", got.Functions)
	}
	if !got.LastSeen.After(firstSeen) {
		t.Fatalf("LastSeen não avançou: first=%v second=%v", firstSeen, got.LastSeen)
	}
	if n := len(s.List()); n != 1 {
		t.Fatalf("List len=%d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// The maximum number of nodes
// ---------------------------------------------------------------------------

func TestNodeStore_TetoMaxNodes(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	// Fill the store up to the maximum.
	for i := 0; i < MaxNodes; i++ {
		id := fmt.Sprintf("node-%03d", i)
		n := mustNode(id, LayerBorda, fmt.Sprintf("10.0.0.%d:8080", i%254+1))
		isNew, err := s.Upsert(n)
		if err != nil {
			t.Fatalf("Upsert %s: %v", id, err)
		}
		if !isNew {
			t.Fatalf("Upsert %s esperado isNew=true", id)
		}
	}
	if n := len(s.List()); n != MaxNodes {
		t.Fatalf("List len=%d, want %d", n, MaxNodes)
	}

	// A new identifier above the limit must fail.
	_, err = s.Upsert(mustNode("overflow", LayerBorda, "10.0.0.99:8080"))
	if !errors.Is(err, ErrTooManyNodes) {
		t.Fatalf("Upsert overflow err=%v, want ErrTooManyNodes", err)
	}

	// An existing node can still be updated while the store is full.
	isNew, err := s.Upsert(mustNode("node-000", LayerNevoa, "10.0.0.99:9999"))
	if err != nil {
		t.Fatalf("Upsert existente no teto: %v", err)
	}
	if isNew {
		t.Fatalf("Upsert existente no teto deve retornar isNew=false")
	}
	got, _ := s.Get("node-000")
	if got.Layer != LayerNevoa || got.Host != "10.0.0.99:9999" {
		t.Fatalf("atualização no teto não aplicou: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// The result of a concurrent registration is correct
// ---------------------------------------------------------------------------

func TestNodeStore_UpsertConcorrenteIsNew(t *testing.T) {
	s, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}

	var wg sync.WaitGroup
	results := make([]bool, 2)
	errs := make([]error, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer wg.Done()
			isNew, err := s.Upsert(mustNode("concurrent", LayerBorda, "10.0.0.1:8080"))
			results[i] = isNew
			errs[i] = err
		}()
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Fatalf("goroutine %d err=%v", i, e)
		}
	}
	trueCount := 0
	for _, v := range results {
		if v {
			trueCount++
		}
	}
	if trueCount != 1 {
		t.Fatalf("exatamente um isNew=true esperado, got %d (results=%v)", trueCount, results)
	}
	if n := len(s.List()); n != 1 {
		t.Fatalf("List len=%d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Persistence round trip
// ---------------------------------------------------------------------------

func TestNodeStore_PersistenciaRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes.json")
	defer os.Remove(path)

	s1, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s1: %v", err)
	}
	nodes := []Node{
		mustNode("a", LayerBorda, "10.0.0.1:8080"),
		mustNode("b", LayerNevoa, "10.0.0.2:8080"),
	}
	for _, n := range nodes {
		if _, err := s1.Upsert(n); err != nil {
			t.Fatalf("Upsert %s: %v", n.NodeID, err)
		}
	}

	// The file exists and holds compact JSON without indentation.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(raw) != 2 {
		t.Fatalf("arquivo len=%d, want 2", len(raw))
	}
	// Compact: the file must not contain the indentation pattern.
	if containsIndent(data) {
		t.Fatalf("arquivo contém MarshalIndent, esperado Marshal compacto")
	}

	s2, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s2: %v", err)
	}
	for _, want := range nodes {
		got, ok := s2.Get(want.NodeID)
		if !ok {
			t.Fatalf("Get %s após reabrir: não encontrado", want.NodeID)
		}
		if got.NodeID != want.NodeID || got.Layer != want.Layer || got.Host != want.Host {
			t.Fatalf("Get %s mismatch: got %+v want %+v", want.NodeID, got, want)
		}
		if got.LastSeen.IsZero() {
			t.Fatalf("LastSeen zero após round-trip para %s", want.NodeID)
		}
	}

	// A corrupt file must fail loudly.
	corruptPath := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(corruptPath, []byte("{corrompido"), 0o600); err != nil {
		t.Fatalf("WriteFile corrupt: %v", err)
	}
	defer os.Remove(corruptPath)
	_, err = NewNodeStore(corruptPath)
	if err == nil {
		t.Fatalf("NewNodeStore com arquivo corrompido deveria falhar")
	}

	// An empty path disables the persistence.
	s3, err := NewNodeStore("")
	if err != nil {
		t.Fatalf("NewNodeStore \"\": %v", err)
	}
	if _, err := s3.Upsert(mustNode("x", LayerBorda, "10.0.0.1:8080")); err != nil {
		t.Fatalf("Upsert sem persistência: %v", err)
	}
	if len(s3.List()) != 1 {
		t.Fatalf("List sem persistência len=%d, want 1", len(s3.List()))
	}
}

func containsIndent(data []byte) bool {
	for i := 0; i+2 < len(data); i++ {
		if data[i] == '\n' && data[i+1] == ' ' && data[i+2] == ' ' {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Touch does not rewrite the file on disk
// ---------------------------------------------------------------------------

func TestNodeStore_TouchNoPersist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes-touch.json")
	defer os.Remove(path)

	s1, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s1: %v", err)
	}
	n := mustNode("n1", LayerBorda, "10.0.0.1:8080")
	if _, err := s1.Upsert(n); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, _ := s1.Get("n1")
	persistedLastSeen := got.LastSeen

	// Reopen the store and touch the node, which only changes memory.
	s2, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s2: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := s2.Touch("n1"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	touched, _ := s2.Get("n1")
	if !touched.LastSeen.After(persistedLastSeen) {
		t.Fatalf("Touch não avançou LastSeen em memória")
	}

	// Reopen the store again and check that the last seen time on disk is
	// the one of the registration, not the one of the touch.
	s3, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s3: %v", err)
	}
	diskNode, ok := s3.Get("n1")
	if !ok {
		t.Fatalf("Get s3: não encontrado")
	}
	if !diskNode.LastSeen.Equal(persistedLastSeen) {
		t.Fatalf("last_seen em disco mudou após Touch: disco=%v persistido=%v touched=%v", diskNode.LastSeen, persistedLastSeen, touched.LastSeen)
	}

	// Touching a node that does not exist must fail.
	if err := s2.Touch("inexistente"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("Touch inexistente err=%v, want ErrNodeNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// Removal is persisted and visible to the lookup and the listing
// ---------------------------------------------------------------------------

func TestNodeStore_Delete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes-del.json")
	defer os.Remove(path)

	s, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := s.Upsert(mustNode("n1", LayerBorda, "10.0.0.1:8080")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if _, err := s.Upsert(mustNode("n2", LayerBorda, "10.0.0.2:8080")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if err := s.Delete("n1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := s.Get("n1"); ok {
		t.Fatalf("Get após Delete: deveria não existir")
	}
	if n := len(s.List()); n != 1 {
		t.Fatalf("List após Delete len=%d, want 1", n)
	}

	// Reopen the store and confirm that the removal was persisted.
	s2, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore s2: %v", err)
	}
	if _, ok := s2.Get("n1"); ok {
		t.Fatalf("n1 ressuscitou após reabrir")
	}
	if _, ok := s2.Get("n2"); !ok {
		t.Fatalf("n2 deveria existir após reabrir")
	}

	// Removing a node that does not exist must fail.
	if err := s.Delete("inexistente"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("Delete inexistente err=%v, want ErrNodeNotFound", err)
	}
	if err := s.Delete("n1"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("Delete já removido err=%v, want ErrNodeNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// Boundaries of the derived status
// ---------------------------------------------------------------------------

func TestNodeStore_DerivedStatus(t *testing.T) {
	staleAfter := 90 * time.Second
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name     string
		lastSeen time.Time
		want     string
	}{
		{"exatamente staleAfter => ativo", now.Add(-staleAfter), StatusAtivo},
		{"1ns além => indisponivel", now.Add(-staleAfter).Add(-time.Nanosecond), StatusIndisponivel},
		{"bem recente => ativo", now.Add(-10 * time.Second), StatusAtivo},
		{"bem antigo => indisponivel", now.Add(-200 * time.Second), StatusIndisponivel},
		{"agora => ativo", now, StatusAtivo},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := Node{NodeID: "n1", LastSeen: tc.lastSeen}
			got := DerivedStatus(n, staleAfter, now)
			if got != tc.want {
				t.Fatalf("DerivedStatus lastSeen=%v staleAfter=%v now=%v got=%q want=%q", tc.lastSeen, staleAfter, now, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// File permissions and compact encoding
// ---------------------------------------------------------------------------

func TestNodeStore_FilePerm(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes-perm.json")
	defer os.Remove(path)

	s, err := NewNodeStore(path)
	if err != nil {
		t.Fatalf("NewNodeStore: %v", err)
	}
	if _, err := s.Upsert(mustNode("n1", LayerBorda, "10.0.0.1:8080")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("perm=%o, want 0600", perm)
	}
}

// ---------------------------------------------------------------------------
// Node tokens
// ---------------------------------------------------------------------------

func TestNodeTokenIssueVerify(t *testing.T) {
	s, err := NewNodeTokenStore("")
	if err != nil {
		t.Fatal(err)
	}
	plainA, err := s.Issue("no-a")
	if err != nil {
		t.Fatal(err)
	}
	if plainA == "" {
		t.Fatal("Issue() devolveu plaintext vazio")
	}
	if !s.Verify("no-a", plainA) {
		t.Error("Verify() = false para o token recém-emitido")
	}
	if _, err := s.Issue(""); !errors.Is(err, ErrInvalidNodeID) {
		t.Errorf("Issue(\"\") = %v, esperado ErrInvalidNodeID", err)
	}

	// The token is bound to the node: the token of one node does not
	// work for another.
	if _, err := s.Issue("no-b"); err != nil {
		t.Fatal(err)
	}
	if s.Verify("no-b", plainA) {
		t.Error("Verify(no-b, token-de-no-a) = true; token cruzado foi aceito")
	}
	// In total, a malformed token returns false without panicking.
	for _, tc := range []struct{ id, tok string }{
		{"no-a", ""},
		{"", plainA},
		{"no-a", strings.Repeat("x", MaxPresentedNodeTokenBytes+1)},
		{"inexistente", plainA},
	} {
		if s.Verify(tc.id, tc.tok) {
			t.Errorf("Verify(id=%q, len(tok)=%d) = true, esperado false", tc.id, len(tc.tok))
		}
	}
}

func TestNodeTokenPersistReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-tokens.json")
	s, err := NewNodeTokenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := s.Issue("no-a")
	if err != nil {
		t.Fatal(err)
	}
	if plain == "" {
		t.Fatal("Issue() devolveu plaintext vazio")
	}

	// The plaintext never reaches the disk, only the hash.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), plain) {
		t.Error("plaintext do token encontrado no arquivo persistido")
	}

	reloaded, err := NewNodeTokenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reloaded.Verify("no-a", plain) {
		t.Error("token não sobreviveu ao reload do arquivo")
	}

	// The secret file has restrictive permissions.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("permissão = %o, esperado 600", fi.Mode().Perm())
	}
}

func TestNodeTokenRevoke(t *testing.T) {
	s, err := NewNodeTokenStore("")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := s.Issue("no-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Revoke("no-a"); err != nil {
		t.Fatal(err)
	}
	if s.Verify("no-a", plain) {
		t.Error("token revogado continua válido")
	}
	if err := s.Revoke("inexistente"); err != nil {
		t.Errorf("Revoke de ID inexistente = %v, esperado nil (no-op)", err)
	}
}

func TestNodeTokenCorruptFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-tokens.json")
	if err := os.WriteFile(path, []byte("{json inválido"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewNodeTokenStore(path); err == nil {
		t.Error("arquivo corrompido aceito; esperado fail-closed")
	}
}

func TestNodeTokenConcurrentVerify(t *testing.T) {
	s, err := NewNodeTokenStore("")
	if err != nil {
		t.Fatal(err)
	}
	// The token is issued first and verified afterwards under the read
	// lock. Issuing the same identifier concurrently would invalidate
	// the plaintext and make the result flaky by construction, and the
	// race between two concurrent writes is already covered by the race
	// detector in the separate stages.
	plains := make(map[string]string, 8)
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("no-%d", i)
		plain, err := s.Issue(id)
		if err != nil {
			t.Fatal(err)
		}
		plains[id] = plain
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("no-%d", i%8)
			if !s.Verify(id, plains[id]) {
				t.Error("Verify concorrente = false")
			}
			if s.Verify(id, "token-errado") {
				t.Error("Verify concorrente aceitou token errado")
			}
		}(i)
	}
	wg.Wait()
}
