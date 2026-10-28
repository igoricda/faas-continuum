// Package auth manages the local API credential used by the management
// routes. The credential is created on first run and persisted only as an
// Argon2id PHC hash; the plaintext token is returned once at provisioning and
// never written to disk or logs. Invalid credential files are rejected
// fail-closed with instructions to reprovision.
package auth

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/igoricda/faas-continuum/internal/fsutil"
)

// DefaultHeaderName is the HTTP header that carries the credential.
const DefaultHeaderName = "Authorization"

// plainTokenBytes is the size of the plaintext token generated at
// provisioning: 256 bits of entropy, which is why the low Argon2id cost is
// sufficient.
const plainTokenBytes = 32

// errFormatoInvalido signals a credential file in an unrecognized or corrupt
// format. The recovery is always the same: the operator removes the file and
// reprovisions the node.
var errFormatoInvalido = errors.New("credencial em formato não reconhecido ou corrompida; remova o arquivo para reprovisionar")

// Credential is the local credential persisted at provisioning. The plaintext
// token is never part of it: only the Argon2id PHC hash reaches disk.
type Credential struct {
	HeaderName string `json:"header_name"`
	TokenHash  string `json:"token_hash"`
}

// Generated is the result of provisioning: the credential to persist and the
// plaintext token, which the caller must show to the operator once. PlainToken
// must never be logged or persisted.
type Generated struct {
	Cred       Credential
	PlainToken string
}

// Generate creates a new local credential with a cryptographically random
// token and its Argon2id hash. The plaintext token is returned once in
// Generated.PlainToken.
func Generate() (Generated, error) {
	buf := make([]byte, plainTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return Generated{}, fmt.Errorf("auth: falha ao gerar credencial: %w", err)
	}
	plain := hex.EncodeToString(buf)

	hash, err := HashToken(plain)
	if err != nil {
		return Generated{}, fmt.Errorf("auth: falha ao hashear credencial recém-gerada: %w", err)
	}
	return Generated{
		Cred: Credential{
			HeaderName: DefaultHeaderName,
			TokenHash:  hash,
		},
		PlainToken: plain,
	}, nil
}

// SaveToFile persists the credential atomically and durably (temp file,
// fsync, rename): if the process dies mid-write, either the previous file
// survives intact or the new one is complete. The file is owner-only (0600).
// A credential without TokenHash is refused, since the hash is the only value
// this file exists to store.
func SaveToFile(path string, cred Credential) error {
	if cred.TokenHash == "" {
		return fmt.Errorf("auth: recusa ao salvar credencial sem token_hash em %q", path)
	}

	data, err := json.Marshal(cred)
	if err != nil {
		return fmt.Errorf("auth: falha ao serializar credencial: %w", err)
	}
	if err := fsutil.WriteFileDurable(path+".tmp", data); err != nil {
		return fmt.Errorf("auth: falha ao gravar credencial em %q: %w", path, err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return fmt.Errorf("auth: falha ao renomear %q para %q: %w", path+".tmp", path, err)
	}
	return nil
}

// LoadFromFile reads a credential previously written by SaveToFile. Parsing
// is strict: the JSON object must contain exactly header_name and token_hash,
// with a non-empty token_hash. Unknown keys (such as a plaintext "token"),
// corrupt JSON, and missing keys are rejected with the reprovisioning error.
func LoadFromFile(path string) (Credential, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Credential{}, fmt.Errorf("auth: falha ao ler credencial de %q: %w", path, err)
	}

	type credentialLoad struct {
		HeaderName string `json:"header_name"`
		TokenHash  string `json:"token_hash"`
	}

	var cred credentialLoad
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cred); err != nil {
		return Credential{}, fmt.Errorf("auth: credencial em %q: %w", path, errFormatoInvalido)
	}

	if cred.HeaderName == "" || cred.TokenHash == "" {
		return Credential{}, fmt.Errorf("auth: credencial em %q: %w", path, errFormatoInvalido)
	}
	return Credential{HeaderName: cred.HeaderName, TokenHash: cred.TokenHash}, nil
}

// LoadOrGenerate loads the credential stored at path. If the file does not
// exist yet, it generates a credential, persists it, and returns the
// plaintext token for one-time display; the token is empty when the credential
// came from disk. Errors other than a missing file are returned unchanged so
// the caller can abort.
func LoadOrGenerate(path string) (Credential, string, error) {
	cred, err := LoadFromFile(path)
	if err == nil {
		return cred, "", nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Credential{}, "", err
	}

	g, err := Generate()
	if err != nil {
		return Credential{}, "", err
	}
	if err := SaveToFile(path, g.Cred); err != nil {
		return Credential{}, "", err
	}
	return g.Cred, g.PlainToken, nil
}
