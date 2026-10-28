package pgbatcher_test

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/lib/pq"
	"github.com/zahidhasann88/pgbatcher"
)

func ExampleNew() {
	db, err := sql.Open("postgres", "postgres://localhost/app?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	b := pgbatcher.New(db, pgbatcher.WithWindow(500*time.Microsecond), pgbatcher.WithBatchSize(100))
	defer b.Close(context.Background())

	ctx := pgbatcher.WithBatch(context.Background())
	if _, err := b.ExecContext(ctx, `INSERT INTO events (id, body) VALUES ($1, $2)`, 42, "hello"); err != nil {
		log.Fatal(err)
	}
}

func ExampleWrap() {
	connector, err := pq.NewConnector("postgres://localhost/app?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	wrapped := pgbatcher.Wrap(connector)
	db := sql.OpenDB(wrapped)
	defer func() {
		db.Close()
		wrapped.(interface{ Close(context.Context) error }).Close(context.Background())
	}()

	ctx := pgbatcher.WithBatch(context.Background())
	if _, err := db.ExecContext(ctx, `INSERT INTO events (id) VALUES ($1)`, 1); err != nil {
		log.Fatal(err)
	}
}
