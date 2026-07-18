# relayd

A small webhook reliability daemon. Point your webhook senders at relayd; it persists every event before acknowledging, delivers to your destination with exponential-backoff retries, dead-letters what can't be delivered, and lets you replay anything. One Go binary backed by PostgreSQL.

The contract: **once relayd returns 200, the event is committed and will be delivered at least once** — through destination downtime, relayd restarts, or crashes mid-flight.

## How it works

```
sender ──POST──▶ /in/{endpoint} ──▶ PostgreSQL ──▶ delivery worker ──▶ destination
                     │ 200 fast                              │ retries w/ backoff
                                                             ▼
                                                        dead letter ──▶ replay
```

- **Ack after persist.** The ingest handler writes to PostgreSQL before returning 200.
- **At-least-once delivery.** A worker claims due events, POSTs them to the destination, and records every attempt. Non-2xx or transport errors reschedule with backoff (5s, 30s, 2m, 10m, 30m); after 5 failed attempts the event goes to `dead`.
- **Byte-for-byte payloads.** Bodies are stored and forwarded as raw bytes with the original headers, so sender signatures (`Stripe-Signature`, `X-Hub-Signature-256`, …) still verify at your destination.
- **Crash recovery.** On boot, any event stuck in `delivering` is requeued.
- **Nothing is deleted.** Dead events sit in the dead letter until you replay them.

## Quickstart

```sh
export DATABASE_URL='postgresql://user:password@host/relayd?sslmode=require'

go build -o relayd ./cmd/relayd

# create a tenant — prints your API key once
./relayd create-tenant "me"

./relayd serve   # :8080 by default
```

Register a destination and take webhooks:

```sh
export KEY=rlyd_...   # from create-tenant

# register where events should be delivered
curl -X POST localhost:8080/endpoints \
  -H "Authorization: Bearer $KEY" \
  -d '{"name":"my-app","destination_url":"https://myapp.example.com/webhooks"}'
# → {"id":"01J...","ingest_path":"/in/01J..."}

# point your webhook sender (Stripe, GitHub, ...) at the ingest path
curl -X POST localhost:8080/in/01J... -d '{"hello":"world"}'
# → {"event_id":"01J..."}
```

Inspect and replay:

```sh
curl localhost:8080/events?status=dead -H "Authorization: Bearer $KEY"
curl localhost:8080/events/{id}        -H "Authorization: Bearer $KEY"   # full attempt history
curl -X POST localhost:8080/events/{id}/replay -H "Authorization: Bearer $KEY"
```

## Watch it retry

A deliberately unreliable receiver lives in `cmd/flaky` — it fails the first N requests, then succeeds:

```sh
go run ./cmd/flaky -fail 2 &                      # receiver on :9090
RELAYD_BACKOFF=2,4 ./relayd serve &               # fast retries for demo

# register http://localhost:9090/hook as a destination, send an event,
# then watch GET /events/{id}: 500 → 500 → 200, status "delivered".
```

Kill relayd mid-retry and restart it — delivery resumes where it left off.

## Deploy on Railway with Neon

1. Push this repository to GitHub.
2. In Railway, create a project with **Deploy from GitHub repo** and select this repository.
3. Add your Neon connection string as a sealed service variable named `DATABASE_URL`.
4. Deploy. Railway builds the included `Dockerfile` and supplies `PORT` automatically.
5. Under **Settings → Networking**, generate a public domain.

Create your first tenant locally using the same Neon connection string. The command prints the API key only once:

```sh
go run ./cmd/relayd create-tenant "me"
```

Never commit `DATABASE_URL` or the generated API key.

## Project layout

```text
cmd/
  relayd/                application entry point
  flaky/                 local retry test receiver
internal/
  api/                   HTTP routes and handlers
  database/              PostgreSQL connection and migrations
    migrations/          versioned Goose SQL migrations
  identity/              ULIDs and API-key hashing
  worker/                delivery and retry loop
```

## Database migrations

SQL migrations are embedded into the binary and applied by Goose when `relayd` starts. To change the schema, add the next sequential file under `internal/database/migrations`, such as `00002_add_event_expiry.sql`, with `-- +goose Up` and `-- +goose Down` sections.

## API

| Route | Auth | Purpose |
|---|---|---|
| `POST /in/{endpoint_id}` | none (unguessable ID is the credential) | ingest an event |
| `POST /endpoints` | Bearer | register a destination URL |
| `GET /endpoints` | Bearer | list endpoints |
| `GET /events?status=` | Bearer | list events (`pending`, `delivering`, `delivered`, `dead`) |
| `GET /events/{id}` | Bearer | event + full attempt history |
| `POST /events/{id}/replay` | Bearer | requeue a delivered/dead event |

Auth is per-tenant Bearer keys (`rlyd_...`), stored as SHA-256 hashes. All management queries are tenant-scoped.

## Configuration

| Env var | Default | |
|---|---|---|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `PORT` | `8080` | platform-provided port |
| `RELAYD_ADDR` | `:$PORT` | optional full listen-address override |
| `RELAYD_BACKOFF` | `5,30,120,600,1800` | retry delays in seconds |

## Non-goals (for now)

Outbound HMAC signing, payload transformations, fan-out to multiple destinations, rate limiting, a web UI. The point is the reliability core.
