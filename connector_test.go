package pgbatcher

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/lib/pq"
)

func TestWrappedConnectorAndTransactionBypass(t *testing.T) {
	dsn := os.Getenv("PG_BATCHER_TEST_DSN")
	if dsn == "" {
		t.Skip("set PG_BATCHER_TEST_DSN to run PostgreSQL integration tests")
	}
	raw, e := pq.NewConnector(dsn)
	if e != nil {
		t.Fatal(e)
	}
	wrapped := Wrap(raw, WithWindow(time.Millisecond), WithBatchSize(3))
	db := sql.OpenDB(wrapped)
	defer func() {
		_ = db.Close()
		_ = wrapped.(interface{ Close(context.Context) error }).Close(context.Background())
	}()
	if _, e = db.Exec(`DROP TABLE IF EXISTS pgbatcher_wrap; CREATE TABLE pgbatcher_wrap(id int primary key)`); e != nil {
		t.Fatal(e)
	}
	for i := 1; i <= 3; i++ {
		if _, e = db.ExecContext(WithBatch(context.Background()), `INSERT INTO pgbatcher_wrap(id) VALUES($1)`, i); e != nil {
			t.Fatal(e)
		}
	}
	tx, e := db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tx.ExecContext(WithBatch(context.Background()), `INSERT INTO pgbatcher_wrap(id) VALUES($1)`, 4); e != nil {
		t.Fatal(e)
	}
	if e = tx.Rollback(); e != nil {
		t.Fatal(e)
	}
	var n int
	if e = db.QueryRow(`SELECT count(*) FROM pgbatcher_wrap`).Scan(&n); e != nil || n != 3 {
		t.Fatalf("count=%d err=%v", n, e)
	}
}
