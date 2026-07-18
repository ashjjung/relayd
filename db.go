package main

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/oklog/ulid/v2"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS tenants (
	id         TEXT PRIMARY KEY,
	name       TEXT NOT NULL,
	created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys (
	key_hash   TEXT PRIMARY KEY,
	tenant_id  TEXT NOT NULL REFERENCES tenants(id),
	created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS endpoints (
	id              TEXT PRIMARY KEY,
	tenant_id       TEXT NOT NULL REFERENCES tenants(id),
	name            TEXT NOT NULL,
	destination_url TEXT NOT NULL,
	created_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
	id              TEXT PRIMARY KEY,
	tenant_id       TEXT NOT NULL REFERENCES tenants(id),
	endpoint_id     TEXT NOT NULL REFERENCES endpoints(id),
	payload         BLOB NOT NULL,
	headers         TEXT NOT NULL,
	status          TEXT NOT NULL DEFAULT 'pending', -- pending | delivering | delivered | dead
	attempt_count   INTEGER NOT NULL DEFAULT 0,
	next_attempt_at INTEGER NOT NULL,
	created_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_due ON events(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_events_tenant ON events(tenant_id, created_at);

CREATE TABLE IF NOT EXISTS attempts (
	id            TEXT PRIMARY KEY,
	event_id      TEXT NOT NULL REFERENCES events(id),
	status_code   INTEGER NOT NULL,
	response_body TEXT NOT NULL,
	duration_ms   INTEGER NOT NULL,
	error         TEXT NOT NULL,
	attempted_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id, attempted_at);
`

func openDB(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// modernc.org/sqlite serializes writes; a single conn avoids SQLITE_BUSY entirely.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return db, nil
}

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
