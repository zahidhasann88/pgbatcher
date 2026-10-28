package pgbatcher

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PG_BATCHER_TEST_DSN")
	if dsn == "" {
		t.Skip("set PG_BATCHER_TEST_DSN to run PostgreSQL integration tests (Docker is not required)")
	}
	db, e := sql.Open("postgres", dsn)
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(10)
	if e = db.Ping(); e != nil {
		db.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
func TestPostgresConcurrentAndIsolation(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	_, e := db.Exec(`DROP TABLE IF EXISTS pgbatcher_items; CREATE TABLE pgbatcher_items(id int PRIMARY KEY, value text UNIQUE)`)
	if e != nil {
		t.Fatal(e)
	}
	m := &Counters{}
	b := New(db, WithWindow(500*time.Microsecond), WithBatchSize(100), WithMetrics(m))
	defer b.Close(ctx)
	const n = 1000
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = b.ExecContext(WithBatch(ctx), `INSERT INTO pgbatcher_items(id,value) VALUES($1,$2)`, i, fmt.Sprintf("v-%d", i))
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Fatalf("insert %d: %v", i, e)
		}
	}
	var got int
	if e := db.QueryRow(`SELECT count(*) FROM pgbatcher_items`).Scan(&got); e != nil || got != n {
		t.Fatalf("count=%d err=%v", got, e)
	}
	if m.Errors.Load() != 0 || m.Batches.Load() >= n {
		t.Fatalf("batches=%d errors=%d: inserts were not coalesced", m.Batches.Load(), m.Errors.Load())
	}
	_, _ = db.Exec(`INSERT INTO pgbatcher_items VALUES(1001,'duplicate')`)
	out := make([]error, 20)
	for i := range out {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, out[i] = b.ExecContext(WithBatch(ctx), `INSERT INTO pgbatcher_items(id,value) VALUES($1,$2)`, 2000+i, func() string {
				if i == 7 {
					return "duplicate"
				}
				return fmt.Sprintf("safe-%d", i)
			}())
		}(i)
	}
	wg.Wait()
	for i, e := range out {
		if (i == 7) != (e != nil) {
			t.Fatalf("isolation index %d err %v", i, e)
		}
	}
}
func TestPostgresNativeConflictAndReturningPassthrough(t *testing.T) {
	db := testDB(t)
	_, e := db.Exec(`DROP TABLE IF EXISTS pgbatcher_native; CREATE TABLE pgbatcher_native(id int PRIMARY KEY, value text)`)
	if e != nil {
		t.Fatal(e)
	}
	b := New(db)
	defer b.Close(context.Background())
	ctx := WithBatch(context.Background())
	r, e := b.ExecContext(ctx, `INSERT INTO pgbatcher_native(id,value) VALUES($1,$2) ON CONFLICT(id) DO NOTHING`, 1, "a")
	if e != nil {
		t.Fatal(e)
	}
	if n, _ := r.RowsAffected(); n != 1 {
		t.Fatalf("first affected=%d", n)
	}
	r, e = b.ExecContext(ctx, `INSERT INTO pgbatcher_native(id,value) VALUES($1,$2) ON CONFLICT(id) DO NOTHING`, 1, "b")
	if e != nil {
		t.Fatal(e)
	}
	if n, _ := r.RowsAffected(); n != 0 {
		t.Fatalf("do nothing affected=%d", n)
	}
	_, e = b.ExecContext(ctx, `INSERT INTO pgbatcher_native(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value`, 1, "c")
	if e != nil {
		t.Fatal(e)
	}
	var value string
	if e = db.QueryRow(`INSERT INTO pgbatcher_native(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value RETURNING value`, 1, "d").Scan(&value); e != nil || value != "d" {
		t.Fatalf("returning=%q err=%v", value, e)
	}
}
func TestPostgresTransactionNotRouted(t *testing.T) {
	db := testDB(t)
	tx, e := db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	_, e = tx.ExecContext(WithBatch(context.Background()), `CREATE TEMP TABLE tx_test(x int)`)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tx.ExecContext(WithBatch(context.Background()), `INSERT INTO tx_test VALUES(1)`); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
}
