package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"

	"github.com/oklog/ulid/v2"
)

func NewID() string {
	return ulid.Make().String()
}

// NewAPIKey returns the raw key (shown once) and its stored hash.
func NewAPIKey() (raw, hash string) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	raw = "rlyd_" + hex.EncodeToString(b)
	return raw, HashKey(raw)
}

func HashKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
