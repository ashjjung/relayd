package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"

	"github.com/oklog/ulid/v2"
)

func newID() string {
	return ulid.Make().String()
}

// newAPIKey returns the raw key (shown once) and its stored hash.
func newAPIKey() (raw, hash string) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	raw = "rlyd_" + hex.EncodeToString(b)
	return raw, hashKey(raw)
}

func hashKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
