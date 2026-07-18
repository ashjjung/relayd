package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"relayd/internal/database"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("relayd ")

	if len(os.Args) < 2 {
		usage()
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := database.Open(ctx, databaseURL)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	switch os.Args[1] {
	case "serve":
		serve(db)
	case "create-tenant":
		if len(os.Args) < 3 {
			log.Fatal("usage: relayd create-tenant <name>")
		}
		createTenant(db, os.Args[2])
	default:
		usage()
	}
}

func serve(db *sql.DB) {
	addr := os.Getenv("RELAYD_ADDR")
	if addr == "" {
		if port := os.Getenv("PORT"); port != "" {
			addr = ":" + port
		} else {
			addr = ":8080"
		}
	}

	wrk := newWorker(db)
	wrk.recoverStale()
	go wrk.run(context.Background())

	srv := &http.Server{
		Addr:              addr,
		Handler:           (&server{db: db}).routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func createTenant(db *sql.DB, name string) {
	id := newID()
	raw, hash := newAPIKey()
	now := time.Now().Unix()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO tenants (id, name, created_at) VALUES ($1, $2, $3)`, id, name, now); err != nil {
		log.Fatalf("create tenant: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO api_keys (key_hash, tenant_id, created_at) VALUES ($1, $2, $3)`, hash, id, now); err != nil {
		log.Fatalf("create api key: %v", err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("tenant %q created (id %s)\n\napi key (shown once, store it now):\n\n  %s\n", name, id, raw)
}

func usage() {
	fmt.Fprintln(os.Stderr, `relayd — a small webhook reliability daemon

usage:
  relayd serve                  start the ingest API + delivery worker
  relayd create-tenant <name>   create a tenant, print its API key (shown once)

env:
  DATABASE_URL     PostgreSQL connection string (required)
  RELAYD_ADDR     listen address (default: PORT, then :8080)
  RELAYD_BACKOFF  retry delays in seconds, e.g. "2,4,8" (default: 5,30,120,600,1800)`)
	os.Exit(2)
}
