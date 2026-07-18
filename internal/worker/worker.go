package worker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"relayd/internal/identity"
)

const (
	maxAttempts      = 5
	claimBatchSize   = 10
	deliveryTimeout  = 10 * time.Second
	maxResponseBytes = 4 << 10 // keep at most 4KiB of the destination's response
)

// Backoff after attempt N fails (1-indexed). Override for demos with e.g.
// RELAYD_BACKOFF=2,4,8 (seconds).
var backoff = []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute}

func init() {
	if raw := os.Getenv("RELAYD_BACKOFF"); raw != "" {
		var custom []time.Duration
		for _, part := range strings.Split(raw, ",") {
			secs, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil {
				log.Fatalf("bad RELAYD_BACKOFF %q: %v", raw, err)
			}
			custom = append(custom, time.Duration(secs)*time.Second)
		}
		backoff = custom
	}
}

type Worker struct {
	db     *sql.DB
	client *http.Client
}

func New(db *sql.DB) *Worker {
	return &Worker{db: db, client: &http.Client{Timeout: deliveryTimeout}}
}

// RecoverStale requeues events left in 'delivering' by a crash. Run once at boot,
// before the worker starts: at-least-once delivery survives any restart.
func (w *Worker) RecoverStale() {
	res, err := w.db.Exec(`UPDATE events SET status = 'pending' WHERE status = 'delivering'`)
	if err != nil {
		log.Fatalf("recover stale events: %v", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Printf("recovered %d in-flight event(s) from previous run", n)
	}
}

// Run processes due events until the context is canceled.
func (w *Worker) Run(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.deliverDue()
		}
	}
}

type claimedEvent struct {
	id, endpointID string
	payload        []byte
	headers        string
	attemptCount   int
	destinationURL string
}

func (w *Worker) deliverDue() {
	events, err := w.claim()
	if err != nil {
		log.Printf("claim: %v", err)
		return
	}
	for _, ev := range events {
		w.deliver(ev)
	}
}

func (w *Worker) claim() ([]claimedEvent, error) {
	rows, err := w.db.Query(`
		WITH due AS (
			SELECT id
			FROM events
			WHERE status = 'pending' AND next_attempt_at <= $1
			ORDER BY next_attempt_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE events AS e
		SET status = 'delivering'
		FROM due, endpoints AS ep
		WHERE e.id = due.id AND ep.id = e.endpoint_id
		RETURNING e.id, e.endpoint_id, e.payload, e.headers, e.attempt_count, ep.destination_url`,
		time.Now().Unix(), claimBatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []claimedEvent
	for rows.Next() {
		var ev claimedEvent
		if err := rows.Scan(&ev.id, &ev.endpointID, &ev.payload, &ev.headers, &ev.attemptCount, &ev.destinationURL); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// hop-by-hop or transport-owned headers we never replay to the destination
var skipHeaders = map[string]bool{
	"Host": true, "Content-Length": true, "Connection": true,
	"Accept-Encoding": true, "Transfer-Encoding": true, "Upgrade": true,
}

func (w *Worker) deliver(ev claimedEvent) {
	attemptNum := ev.attemptCount + 1

	req, err := http.NewRequest(http.MethodPost, ev.destinationURL, bytes.NewReader(ev.payload))
	if err != nil {
		w.recordOutcome(ev, attemptNum, 0, "", 0, "build request: "+err.Error())
		return
	}

	// Replay original headers byte-for-byte so sender signatures (Stripe-Signature
	// etc.) still verify at the destination.
	var orig map[string][]string
	if err := json.Unmarshal([]byte(ev.headers), &orig); err == nil {
		for k, vals := range orig {
			if skipHeaders[http.CanonicalHeaderKey(k)] {
				continue
			}
			for _, v := range vals {
				req.Header.Add(k, v)
			}
		}
	}
	req.Header.Set("X-Relayd-Event-Id", ev.id)
	req.Header.Set("X-Relayd-Attempt", strconv.Itoa(attemptNum))

	start := time.Now()
	resp, err := w.client.Do(req)
	durationMS := time.Since(start).Milliseconds()
	if err != nil {
		w.recordOutcome(ev, attemptNum, 0, "", durationMS, err.Error())
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	w.recordOutcome(ev, attemptNum, resp.StatusCode, string(body), durationMS, "")
}

func (w *Worker) recordOutcome(ev claimedEvent, attemptNum, statusCode int, respBody string, durationMS int64, errMsg string) {
	now := time.Now().Unix()

	tx, err := w.db.Begin()
	if err != nil {
		log.Printf("record outcome begin: %v", err)
		return
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`
		INSERT INTO attempts (id, event_id, status_code, response_body, duration_ms, error, attempted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		identity.NewID(), ev.id, statusCode, respBody, durationMS, errMsg, now); err != nil {
		log.Printf("record attempt: %v", err)
		return
	}

	success := statusCode >= 200 && statusCode < 300
	var q string
	args := []any{}
	switch {
	case success:
		q = `UPDATE events SET status = 'delivered', attempt_count = $1 WHERE id = $2`
		args = append(args, attemptNum, ev.id)
		log.Printf("event %s delivered (attempt %d, %d, %dms)", ev.id, attemptNum, statusCode, durationMS)
	case attemptNum >= maxAttempts:
		q = `UPDATE events SET status = 'dead', attempt_count = $1 WHERE id = $2`
		args = append(args, attemptNum, ev.id)
		log.Printf("event %s DEAD after %d attempts", ev.id, attemptNum)
	default:
		delay := backoff[min(attemptNum-1, len(backoff)-1)]
		q = `UPDATE events SET status = 'pending', attempt_count = $1, next_attempt_at = $2 WHERE id = $3`
		args = append(args, attemptNum, now+int64(delay.Seconds()), ev.id)
		log.Printf("event %s attempt %d failed (code=%d err=%q), retry in %s", ev.id, attemptNum, statusCode, errMsg, delay)
	}
	if _, err := tx.Exec(q, args...); err != nil {
		log.Printf("update event: %v", err)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Printf("record outcome commit: %v", err)
	}
}
