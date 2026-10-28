package cattokens

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestIssueVerifyRoundTrip(t *testing.T) {
	s, err := NewStore("")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := s.Issue("lpr")
	if err != nil {
		t.Fatal(err)
	}
	if plain == "" {
		t.Fatal("Issue devolveu plaintext vazio")
	}
	if !s.Verify("lpr", plain) {
		t.Fatal("Verify negou o token recém-emitido")
	}
	if s.Verify("lpr", plain+"x") {
		t.Fatal("Verify aceitou token adulterado")
	}
	if s.Verify("ocr", plain) {
		t.Fatal("Verify aceitou token de outra categoria")
	}
	if s.Verify("lpr", "") || s.Verify("", plain) {
		t.Fatal("Verify aceitou entrada vazia")
	}
	// Re-emitir revoga o anterior.
	plain2, err := s.Issue("lpr")
	if err != nil {
		t.Fatal(err)
	}
	if plain2 == plain {
		t.Fatal("re-emissão devolveu o mesmo token")
	}
	if s.Verify("lpr", plain) || !s.Verify("lpr", plain2) {
		t.Fatal("re-emissão não revogou o anterior")
	}
	if _, err := s.Issue(""); !errors.Is(err, ErrInvalidCategory) {
		t.Fatalf("Issue vazia err=%v, want ErrInvalidCategory", err)
	}
}

func TestDownstreamSetGetRemove(t *testing.T) {
	s, _ := NewStore("")
	if _, ok := s.DownstreamFor("lpr", "xp4", "alpr-ncnn"); ok {
		t.Fatal("DownstreamFor achou entrada inexistente")
	}
	if err := s.SetDownstream("lpr", "xp4", "alpr-ncnn", "tok-tv"); err != nil {
		t.Fatal(err)
	}
	got, ok := s.DownstreamFor("lpr", "xp4", "alpr-ncnn")
	if !ok || got != "tok-tv" {
		t.Fatalf("DownstreamFor=%q,%v, want tok-tv,true", got, ok)
	}
	if _, ok := s.DownstreamFor("lpr", "xp4", "ocr"); ok {
		t.Fatal("DownstreamFor casou concreto errado")
	}
	// token vazio remove.
	if err := s.SetDownstream("lpr", "xp4", "alpr-ncnn", ""); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.DownstreamFor("lpr", "xp4", "alpr-ncnn"); ok {
		t.Fatal("remoção não surtiu efeito")
	}
	if err := s.SetDownstream("", "xp4", "alpr-ncnn", "x"); !errors.Is(err, ErrInvalidCategory) {
		t.Fatalf("categoria vazia err=%v", err)
	}
	if err := s.SetDownstream("lpr", "", "alpr-ncnn", "x"); !errors.Is(err, ErrInvalidDownstream) {
		t.Fatalf("nó vazio err=%v", err)
	}
}

func TestNilSafe(t *testing.T) {
	var s *Store
	if s.Verify("lpr", "x") {
		t.Fatal("Verify em nil devolveu true")
	}
	if _, ok := s.DownstreamFor("lpr", "xp4", "alpr-ncnn"); ok {
		t.Fatal("DownstreamFor em nil devolveu true")
	}
	if s.Snapshot() != nil {
		t.Fatal("Snapshot em nil devolveu não-nil")
	}
	if _, err := s.Issue("lpr"); !errors.Is(err, ErrInvalidCategory) {
		t.Fatalf("Issue em nil err=%v", err)
	}
}

func TestPersistRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cat.json")
	s, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := s.Issue("ocr")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetDownstream("ocr", "xp4", "ocr", "tok-ocr-tv"); err != nil {
		t.Fatal(err)
	}
	s2, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if !s2.Verify("ocr", plain) {
		t.Fatal("Verify pós-reload negou")
	}
	got, ok := s2.DownstreamFor("ocr", "xp4", "ocr")
	if !ok || got != "tok-ocr-tv" {
		t.Fatalf("downstream pós-reload=%q,%v", got, ok)
	}
	// The file does not contain the plaintext of the category token.
	raw, _ := os.ReadFile(path)
	if stringContains(string(raw), plain) {
		t.Fatal("plaintext do token de categoria vazou no arquivo")
	}
	snap := s2.Snapshot()
	if _, ok := snap["ocr"]["xp4"]; !ok {
		t.Fatalf("Snapshot sem xp4: %v", snap)
	}
	if len(snap["ocr"]["xp4"]) != 1 || snap["ocr"]["xp4"][0] != "ocr" {
		t.Fatalf("Snapshot concretes=%v", snap["ocr"]["xp4"])
	}
}

func TestLoadCorrupt(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("{invalido"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(bad); err == nil {
		t.Fatal("load de JSON corrompido não falhou")
	}
}

func stringContains(hay, needle string) bool {
	return len(hay) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(hay); i++ {
			if hay[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
