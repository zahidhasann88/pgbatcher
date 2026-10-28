package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/zahidhasann88/pgbatcher"
)

type event struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Payload string `json:"payload"`
}

func main() {
	dsn := env("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/events?sslmode=disable")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	for i := 0; i < 30; i++ {
		if db.Ping() == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS events(id text PRIMARY KEY,kind text NOT NULL,payload text NOT NULL,created_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		log.Fatal(err)
	}
	b := pgbatcher.New(db, pgbatcher.WithWindow(500*time.Microsecond), pgbatcher.WithBatchSize(100))
	defer b.Close(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var x event
		if err := json.NewDecoder(r.Body).Decode(&x); err != nil || x.ID == "" {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		query := `INSERT INTO events(id,kind,payload) VALUES($1,$2,$3)`
		var result sql.Result
		var err error
		if env("BATCHING", "true") == "false" {
			result, err = db.ExecContext(ctx, query, x.ID, x.Kind, x.Payload)
		} else {
			result, err = b.ExecContext(pgbatcher.WithBatch(ctx), query, x.ID, x.Kind, x.Payload)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		n, _ := result.RowsAffected()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true,"rows":` + jsonNumber(n) + `}`))
	})
	addr := env("LISTEN_ADDR", ":8080")
	log.Printf("listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func jsonNumber(n int64) string { b, _ := json.Marshal(n); return string(b) }
