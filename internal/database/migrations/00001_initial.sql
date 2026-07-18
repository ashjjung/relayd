-- +goose Up
CREATE TABLE IF NOT EXISTS tenants (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys (
    key_hash   TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL REFERENCES tenants(id),
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS endpoints (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    name            TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    created_at      BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    endpoint_id     TEXT NOT NULL REFERENCES endpoints(id),
    payload         BYTEA NOT NULL,
    headers         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    next_attempt_at BIGINT NOT NULL,
    created_at      BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_due ON events(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_events_tenant ON events(tenant_id, created_at);

CREATE TABLE IF NOT EXISTS attempts (
    id            TEXT PRIMARY KEY,
    event_id      TEXT NOT NULL REFERENCES events(id),
    status_code   INTEGER NOT NULL,
    response_body TEXT NOT NULL,
    duration_ms   BIGINT NOT NULL,
    error         TEXT NOT NULL,
    attempted_at  BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id, attempted_at);

-- +goose Down
DROP TABLE IF EXISTS attempts;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS endpoints;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS tenants;
