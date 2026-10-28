package registry

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
)

// invokeTokenBytes is the plaintext token entropy in bytes (256 bits).
const invokeTokenBytes = 32

// MaxPresentedInvokeTokenBytes bounds the presented token before any hashing
// (fail-closed).
const MaxPresentedInvokeTokenBytes = 4096

// generateInvokeToken creates a random token and returns the plaintext, which
// is shown to the caller once, plus the hex SHA-256 to persist.
//
// SHA-256 without a salt is safe here because the input has 256 bits of
// entropy from crypto/rand, so offline brute force is infeasible; the hash
// protects the registry at rest and is not used for key stretching. The
// management credential uses Argon2id instead.
//
// The plaintext must never be logged or persisted.
func generateInvokeToken() (plain, sumHex string, err error) {
	buf := make([]byte, invokeTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("registry: falha ao gerar token de invocação: %w", err)
	}
	plain = hex.EncodeToString(buf)
	sum := sha256.Sum256([]byte(plain))
	return plain, hex.EncodeToString(sum[:]), nil
}

// decodeInvokeTokenSum decodes storedHex into 32 bytes for the hot-path cache.
// Invalid or empty input returns ok=false, and the caller keeps the
// fail-closed semantics.
func decodeInvokeTokenSum(storedHex string) (sum [32]byte, ok bool) {
	raw, err := hex.DecodeString(storedHex)
	if err != nil || len(raw) != sha256.Size {
		return sum, false
	}
	copy(sum[:], raw)
	return sum, true
}

// setInvokeTokenCache fills the decoded hash cache from storedHex. It is
// called by Register, Update and load; an invalid hex value leaves the cache
// invalid, so verification denies the token.
func (fn *Function) setInvokeTokenCache(storedHex string) {
	sum, ok := decodeInvokeTokenSum(storedHex)
	fn.invokeTokenSum, fn.invokeTokenSumValid = sum, ok
}

// VerifyPresentedToken reports whether presented matches the cached hash,
// with no hex decoding per request: only sha256.Sum256 and
// ConstantTimeCompare. Functions without a valid cache (zero value or
// hand-built literals) fall back to VerifyInvokeToken, so behavior stays
// total; persisted entries always carry the cache.
func (fn Function) VerifyPresentedToken(presented string) bool {
	if presented == "" || len(presented) > MaxPresentedInvokeTokenBytes {
		return false
	}
	if fn.invokeTokenSumValid {
		sum := sha256.Sum256([]byte(presented))
		return subtle.ConstantTimeCompare(sum[:], fn.invokeTokenSum[:]) == 1
	}
	return VerifyInvokeToken(presented, fn.InvokeTokenSHA256)
}

// VerifyInvokeToken reports whether presented is the token whose hex hash is
// storedHex. It is total: malformed input or an empty hash returns false
// without panicking. An empty hash, as found in functions registered before
// invoke tokens existed, denies access; those functions need an Update that
// rotates the token.
func VerifyInvokeToken(presented, storedHex string) bool {
	if presented == "" || len(presented) > MaxPresentedInvokeTokenBytes {
		return false
	}
	stored, err := hex.DecodeString(storedHex)
	if err != nil || len(stored) != sha256.Size {
		return false
	}
	sum := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(sum[:], stored) == 1
}
