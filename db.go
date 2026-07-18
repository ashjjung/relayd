package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oklog/ulid/v2"
)

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS tenants (
	id         TEXT PRIMARY KEY,
	name       TEXT NOT NULL,
	created_at BIGINT NOT NULL
)`,
	`CREATE TABLE IF NOT EXISTS api_keys (
	key_hash   TEXT PRIMARY KEY,
	tenant_id  TEXT NOT NULL REFERENCES tenants(id),
	created_at BIGINT NOT NULL
)`,
	`CREATE TABLE IF NOT EXISTS endpoints (
	id              TEXT PRIMARY KEY,
	tenant_id       TEXT NOT NULL REFERENCES tenants(id),
	name            TEXT NOT NULL,
	destination_url TEXT NOT NULL,
	created_at      BIGINT NOT NULL
)`,
	`CREATE TABLE IF NOT EXISTS events (
	id              TEXT PRIMARY KEY,
	tenant_id       TEXT NOT NULL REFERENCES tenants(id),
	endpoint_id     TEXT NOT NULL REFERENCES endpoints(id),
	payload         BYTEA NOT NULL,
	headers         TEXT NOT NULL,
	status          TEXT NOT NULL DEFAULT 'pending', -- pending | delivering | delivered | dead
	attempt_count   INTEGER NOT NULL DEFAULT 0,
	next_attempt_at BIGINT NOT NULL,
	created_at      BIGINT NOT NULL
)`,
	`CREATE INDEX IF NOT EXISTS idx_events_due ON events(status, next_attempt_at)`,
	`CREATE INDEX IF NOT EXISTS idx_events_tenant ON events(tenant_id, created_at)`,
	`CREATE TABLE IF NOT EXISTS attempts (
	id            TEXT PRIMARY KEY,
	event_id      TEXT NOT NULL REFERENCES events(id),
	status_code   INTEGER NOT NULL,
	response_body TEXT NOT NULL,
	duration_ms   BIGINT NOT NULL,
	error         TEXT NOT NULL,
	attempted_at  BIGINT NOT NULL
)`,
	`CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id, attempted_at)`,
}

func openDB(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("begin schema: %w", err)
	}
	defer tx.Rollback()
	for _, statement := range schemaStatements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply schema: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		db.Close()
		return nil, fmt.Errorf("commit schema: %w", err)
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
