package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters for the local credential. The protected secret is a
// random 256-bit token, so the hash guards the on-disk file against direct
// reads rather than compensating for weak entropy; t=1 keeps the cost low on
// constrained nodes. The PHC string records the parameters it was hashed
// with, so raising them later needs no schema change.
//
// Wire format: $argon2id$v=19$m=<KiB>,t=<pass>,p=<threads>$<salt>$<hash>,
// with salt and hash in base64 RawStdEncoding.
const (
	// DefaultMemoryKiB is the Argon2id memory cost in KiB (19 MiB).
	DefaultMemoryKiB = 19456
	// DefaultTimeCost is the number of Argon2id passes.
	DefaultTimeCost = 1
	// DefaultParallelism is the Argon2id thread count.
	DefaultParallelism = 1
	// SaltLengthBytes is the random salt size in bytes.
	SaltLengthBytes = 16
	// KeyLengthBytes is the derived key size in bytes.
	KeyLengthBytes = 32
	// MaxPresentedTokenBytes bounds the size of a presented token; longer
	// values are rejected before any hashing work.
	MaxPresentedTokenBytes = 4096
)

// Fixed PHC fields of the format used here.
const (
	phcAlg     = "argon2id"
	phcVersion = 19 // argon2.Version (0x13) from golang.org/x/crypto
)

// errTokenVazio is the only error condition of HashToken.
var errTokenVazio = errors.New("token vazio")

// HashToken derives the Argon2id hash of token and returns it as a
// self-describing PHC string. It draws a random salt of SaltLengthBytes from
// crypto/rand and uses the default parameters and a KeyLengthBytes key. It
// fails only for an empty token or a failing random source.
func HashToken(token string) (string, error) {
	if token == "" {
		return "", fmt.Errorf("auth: falha ao hashear token: %w", errTokenVazio)
	}

	salt := make([]byte, SaltLengthBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("auth: falha ao gerar salt do hash: %w", err)
	}

	chave := argon2.IDKey([]byte(token), salt, DefaultTimeCost, DefaultMemoryKiB, DefaultParallelism, KeyLengthBytes)

	return fmt.Sprintf("$%s$v=%d$m=%d,t=%d,p=%d$%s$%s",
		phcAlg, phcVersion,
		DefaultMemoryKiB, DefaultTimeCost, DefaultParallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(chave),
	), nil
}

// VerifyToken reports whether token matches the stored PHC hash. It
// re-derives the key with the parameters recorded in the PHC string, not the
// package defaults, and compares in constant time. It never panics: malformed
// input (empty, truncated, wrong algorithm or version, invalid parameters,
// undecodable fields) returns false. Empty tokens and tokens longer than
// MaxPresentedTokenBytes are rejected before any hashing work.
func VerifyToken(token, phc string) bool {
	if token == "" || len(token) > MaxPresentedTokenBytes {
		return false
	}

	partes := strings.Split(phc, "$")
	if len(partes) != 6 { // "", alg, version, params, salt, hash
		return false
	}
	if partes[0] != "" || partes[1] != phcAlg || partes[2] != fmt.Sprintf("v=%d", phcVersion) {
		return false
	}

	memoria, custo, paralelismo, ok := parseParametrosPHC(partes[3])
	if !ok {
		return false
	}

	salt, err := base64.RawStdEncoding.DecodeString(partes[4])
	if err != nil {
		return false
	}
	hashArmazenado, err := base64.RawStdEncoding.DecodeString(partes[5])
	if err != nil {
		return false
	}

	recomputado := argon2.IDKey([]byte(token), salt, custo, memoria, uint8(paralelismo), KeyLengthBytes)
	return subtle.ConstantTimeCompare(recomputado, hashArmazenado) == 1
}

// parseParametrosPHC parses the "m=...,t=...,p=..." field of a PHC string
// strictly: exactly three subfields in that order, uint32 values, and
// m/t/p >= 1. Validation runs before any argon2.IDKey call, which panics on
// time < 1 or threads < 1. Parallelism is capped at 255 because IDKey takes
// threads as uint8; larger values would truncate to 0 and panic later.
func parseParametrosPHC(campo string) (memoria, custo, paralelismo uint32, ok bool) {
	subcampos := strings.Split(campo, ",")
	if len(subcampos) != 3 {
		return 0, 0, 0, false
	}

	prefixos := [3]string{"m=", "t=", "p="}
	destinos := [3]*uint32{&memoria, &custo, &paralelismo}
	for i, subcampo := range subcampos {
		if !strings.HasPrefix(subcampo, prefixos[i]) {
			return 0, 0, 0, false
		}
		valor, err := strconv.ParseUint(subcampo[len(prefixos[i]):], 10, 32)
		if err != nil {
			return 0, 0, 0, false
		}
		*destinos[i] = uint32(valor)
	}

	if memoria < 1 || custo < 1 || paralelismo < 1 || paralelismo > 255 {
		return 0, 0, 0, false
	}
	return memoria, custo, paralelismo, true
}
