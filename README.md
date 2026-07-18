# relayd

A small webhook reliability daemon. Point your webhook senders at relayd; it persists every event before acknowledging, delivers to your destination with exponential-backoff retries, dead-letters what can't be delivered, and lets you replay anything. One Go binary, one SQLite file, no other infrastructure.

The contract: **once relayd returns 200, the event is on disk and will be delivered at least once** — through destination downtime, relayd restarts, or crashes mid-flight.

## How it works

```
sender ──POST──▶ /in/{endpoint} ──▶ SQLite (durable) ──▶ delivery worker ──▶ destination
                     │ 200 fast                              │ retries w/ backoff
                                                             ▼
                                                        dead letter ──▶ replay
```

- **Ack after persist.** The ingest handler writes to SQLite before returning 200.
- **At-least-once delivery.** A worker claims due events, POSTs them to the destination, and records every attempt. Non-2xx or transport errors reschedule with backoff (5s, 30s, 2m, 10m, 30m); after 5 failed attempts the event goes to `dead`.
- **Byte-for-byte payloads.** Bodies are stored and forwarded as raw bytes with the original headers, so sender signatures (`Stripe-Signature`, `X-Hub-Signature-256`, …) still verify at your destination.
- **Crash recovery.** On boot, any event stuck in `delivering` is requeued.
- **Nothing is deleted.** Dead events sit in the dead letter until you replay them.

## Quickstart

```sh
go build -o relayd .

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
| `RELAYD_DB` | `relayd.db` | SQLite path |
| `RELAYD_ADDR` | `:8080` | listen address |
| `RELAYD_BACKOFF` | `5,30,120,600,1800` | retry delays in seconds |

## Non-goals (for now)

Outbound HMAC signing, payload transformations, fan-out to multiple destinations, rate limiting, a web UI. The point is the reliability core.
