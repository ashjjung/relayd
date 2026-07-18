package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const maxPayloadBytes = 1 << 20 // 1 MiB

type ctxKey int

const ctxTenantID ctxKey = 0

type server struct {
	db *sql.DB
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()

	// Ingest: unauthenticated by design — the unguessable endpoint ID is the
	// credential, since webhook senders can't set custom auth headers.
	mux.HandleFunc("POST /in/{endpoint}", s.handleIngest)

	// Management API: Bearer key, tenant-scoped.
	mux.Handle("POST /endpoints", s.auth(s.handleCreateEndpoint))
	mux.Handle("GET /endpoints", s.auth(s.handleListEndpoints))
	mux.Handle("GET /events", s.auth(s.handleListEvents))
	mux.Handle("GET /events/{id}", s.auth(s.handleGetEvent))
	mux.Handle("POST /events/{id}/replay", s.auth(s.handleReplay))

	return mux
}

func (s *server) auth(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || raw == "" {
			jsonError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		var tenantID string
		err := s.db.QueryRow(`SELECT tenant_id FROM api_keys WHERE key_hash = $1`, hashKey(raw)).Scan(&tenantID)
		if errors.Is(err, sql.ErrNoRows) {
			jsonError(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		if err != nil {
			jsonError(w, http.StatusInternalServerError, "auth lookup failed")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), ctxTenantID, tenantID)))
	})
}

func tenantID(r *http.Request) string {
	return r.Context().Value(ctxTenantID).(string)
}

// --- ingest ---

func (s *server) handleIngest(w http.ResponseWriter, r *http.Request) {
	endpointID := r.PathValue("endpoint")

	var epTenant string
	err := s.db.QueryRow(`SELECT tenant_id FROM endpoints WHERE id = $1`, endpointID).Scan(&epTenant)
	if errors.Is(err, sql.ErrNoRows) {
		jsonError(w, http.StatusNotFound, "unknown endpoint")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "lookup failed")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxPayloadBytes+1))
	if err != nil {
		jsonError(w, http.StatusBadRequest, "read body failed")
		return
	}
	if len(body) > maxPayloadBytes {
		jsonError(w, http.StatusRequestEntityTooLarge, "payload exceeds 1MiB")
		return
	}

	headers, _ := json.Marshal(r.Header)
	id := newID()
	now := time.Now().Unix()

	// The contract: 200 means the event is on disk. Persist before responding.
	_, err = s.db.Exec(`
		INSERT INTO events (id, tenant_id, endpoint_id, payload, headers, status, next_attempt_at, created_at)
		VALUES ($1, $2, $3, $4, $5, 'pending', $6, $7)`,
		id, epTenant, endpointID, body, string(headers), now, now)
	if err != nil {
		log.Printf("ingest persist failed: %v", err)
		jsonError(w, http.StatusInternalServerError, "persist failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"event_id": id})
}

// --- management ---

func (s *server) handleCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name           string `json:"name"`
		DestinationURL string `json:"destination_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.DestinationURL == "" {
		jsonError(w, http.StatusBadRequest, "need json body with name and destination_url")
		return
	}
	if !strings.HasPrefix(req.DestinationURL, "http://") && !strings.HasPrefix(req.DestinationURL, "https://") {
		jsonError(w, http.StatusBadRequest, "destination_url must be http(s)")
		return
	}

	id := newID()
	_, err := s.db.Exec(`INSERT INTO endpoints (id, tenant_id, name, destination_url, created_at) VALUES ($1, $2, $3, $4, $5)`,
		id, tenantID(r), req.Name, req.DestinationURL, time.Now().Unix())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "create failed")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{
		"id":              id,
		"name":            req.Name,
		"destination_url": req.DestinationURL,
		"ingest_path":     "/in/" + id,
	})
}

func (s *server) handleListEndpoints(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(`SELECT id, name, destination_url, created_at FROM endpoints WHERE tenant_id = $1 ORDER BY created_at DESC`, tenantID(r))
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "query failed")
		return
	}
	defer rows.Close()

	type endpoint struct {
		ID             string `json:"id"`
		Name           string `json:"name"`
		DestinationURL string `json:"destination_url"`
		IngestPath     string `json:"ingest_path"`
		CreatedAt      int64  `json:"created_at"`
	}
	out := []endpoint{}
	for rows.Next() {
		var e endpoint
		if err := rows.Scan(&e.ID, &e.Name, &e.DestinationURL, &e.CreatedAt); err != nil {
			jsonError(w, http.StatusInternalServerError, "scan failed")
			return
		}
		e.IngestPath = "/in/" + e.ID
		out = append(out, e)
	}
	writeJSON(w, http.StatusOK, out)
}

type eventJSON struct {
	ID            string `json:"id"`
	EndpointID    string `json:"endpoint_id"`
	Status        string `json:"status"`
	AttemptCount  int    `json:"attempt_count"`
	NextAttemptAt int64  `json:"next_attempt_at"`
	CreatedAt     int64  `json:"created_at"`
	Payload       string `json:"payload,omitempty"`
}

func (s *server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	q := `SELECT id, endpoint_id, status, attempt_count, next_attempt_at, created_at FROM events WHERE tenant_id = $1`
	args := []any{tenantID(r)}
	if st := r.URL.Query().Get("status"); st != "" {
		q += ` AND status = $2`
		args = append(args, st)
	}
	q += ` ORDER BY created_at DESC LIMIT 100`

	rows, err := s.db.Query(q, args...)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "query failed")
		return
	}
	defer rows.Close()

	out := []eventJSON{}
	for rows.Next() {
		var e eventJSON
		if err := rows.Scan(&e.ID, &e.EndpointID, &e.Status, &e.AttemptCount, &e.NextAttemptAt, &e.CreatedAt); err != nil {
			jsonError(w, http.StatusInternalServerError, "scan failed")
			return
		}
		out = append(out, e)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	var e eventJSON
	var payload []byte
	err := s.db.QueryRow(`
		SELECT id, endpoint_id, status, attempt_count, next_attempt_at, created_at, payload
		FROM events WHERE id = $1 AND tenant_id = $2`, r.PathValue("id"), tenantID(r)).
		Scan(&e.ID, &e.EndpointID, &e.Status, &e.AttemptCount, &e.NextAttemptAt, &e.CreatedAt, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		jsonError(w, http.StatusNotFound, "no such event")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "query failed")
		return
	}
	e.Payload = string(payload)

	rows, err := s.db.Query(`
		SELECT status_code, response_body, duration_ms, error, attempted_at
		FROM attempts WHERE event_id = $1 ORDER BY attempted_at ASC`, e.ID)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "query failed")
		return
	}
	defer rows.Close()

	type attempt struct {
		StatusCode   int    `json:"status_code"`
		ResponseBody string `json:"response_body"`
		DurationMS   int64  `json:"duration_ms"`
		Error        string `json:"error,omitempty"`
		AttemptedAt  int64  `json:"attempted_at"`
	}
	attempts := []attempt{}
	for rows.Next() {
		var a attempt
		if err := rows.Scan(&a.StatusCode, &a.ResponseBody, &a.DurationMS, &a.Error, &a.AttemptedAt); err != nil {
			jsonError(w, http.StatusInternalServerError, "scan failed")
			return
		}
		attempts = append(attempts, a)
	}
	writeJSON(w, http.StatusOK, map[string]any{"event": e, "attempts": attempts})
}

func (s *server) handleReplay(w http.ResponseWriter, r *http.Request) {
	res, err := s.db.Exec(`
		UPDATE events SET status = 'pending', attempt_count = 0, next_attempt_at = $1
		WHERE id = $2 AND tenant_id = $3 AND status IN ('delivered', 'dead')`,
		time.Now().Unix(), r.PathValue("id"), tenantID(r))
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "replay failed")
		return
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		jsonError(w, http.StatusConflict, "event not found or not in a replayable state (delivered/dead)")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "pending"})
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
