# pgbatcher

pgbatcher merges concurrent single-row PostgreSQL `INSERT`s into one multi-row `INSERT`.
Each caller still gets its own result or error. Batching is opt-in per call, and all other SQL runs unchanged.

> **Status: experimental (v0).** In the [benchmark](docs/BENCHMARKS.md) with 128+ concurrent goroutines and 10 connections,
> throughput was 6–50× higher and Postgres CPU per row 20–28× lower. With one goroutine it was slower. Measure your own workload.

## Install

```sh
go get github.com/zahidhasann88/pgbatcher
```

Requires Go 1.22+. Works with `database/sql` (tested with `lib/pq`).

## Quick start

```go
db, err := sql.Open("postgres", "postgres://localhost/app?sslmode=disable")
if err != nil {
	log.Fatal(err)
}
defer db.Close()

b := pgbatcher.New(db, pgbatcher.WithWindow(500*time.Microsecond), pgbatcher.WithBatchSize(100))
defer b.Close(context.Background())

// Only calls with WithBatch(ctx) are batched.
ctx := pgbatcher.WithBatch(context.Background())
res, err := b.ExecContext(ctx, `INSERT INTO events (id, body) VALUES ($1, $2)`, 42, "hello")
```

`res.RowsAffected()` returns 1 for a successful row. `LastInsertId()` returns an error, as with PostgreSQL in general.

**Connector wrapper.** You can also wrap a driver connector and keep using `*sql.DB` directly:

```go
connector, _ := pq.NewConnector(dsn)
wrapped := pgbatcher.Wrap(connector, pgbatcher.WithWindow(time.Millisecond))
db := sql.OpenDB(wrapped)

db.ExecContext(pgbatcher.WithBatch(ctx), `INSERT INTO t (id) VALUES ($1)`, 1)

// Shutdown: close the DB first, then the connector's queue.
db.Close()
wrapped.(interface{ Close(context.Context) error }).Close(context.Background())
```

Statements inside a transaction (`*sql.Tx`) are never batched. Only `Exec` calls are intercepted; queries and prepared statements pass through.

## What gets batched

Only this shape, with placeholders numbered `$1..$n` in order:

```sql
INSERT INTO [schema.]table (col1, col2, ...) VALUES ($1, $2, ...)
```

Comments, quoted identifiers, casing, and a trailing `;` are handled. Everything else runs individually and unchanged,
including `RETURNING`, `ON CONFLICT`, `SELECT`, CTEs, literals in `VALUES`, multi-row inserts, and multiple statements.

## Behavior

- **Flushing:** a group flushes when the first of these is reached: the time window, the batch size, or the byte limit.
  Calls are grouped by statement shape.
- **Error isolation:** if a batch fails, it is split in half and retried until the failing row runs alone.
  Only that caller gets the error. Errors not caused by a single row (e.g. a lost connection) may be retried and reach several callers.
- **Cancellation:** a caller cancelled before the flush is removed from the batch. After the flush starts, its row may still be
  inserted, but the caller gets `ctx.Err()`; other callers are not affected. With `Wrap`, such a caller returns only after
  the batch finishes, because the batch may be running on its connection.
- **Shutdown:** `Close(ctx)` rejects new calls, flushes pending batches, and waits for in-flight work.

## Configuration
| Option | Default | Description |
|---|---|---|
| `WithWindow(d)` | 500µs | Maximum wait before a flush. Adds up to this much latency per call. |
| `WithBatchSize(n)` | 100 | Maximum rows per batch. |
| `WithMaxBytes(n)` | 1 MiB | Approximate queued-bytes limit. |
| `WithMetrics(m)` | none | Callbacks for flushes, fallbacks, and errors. `Counters` is a ready-made atomic implementation. |

Zero or negative values are ignored and the default is kept.

## When not to use it

- Low concurrency or latency-sensitive paths: every batched call waits up to one window.
- Bulk loads where you already have all rows: use `COPY` or `pgx.Batch`.
- Inserts that need `RETURNING`, `ON CONFLICT`, or triggers/rules that change row counts.

## More

- [docs/DESIGN.md](docs/DESIGN.md): architecture, design trade-offs, and known limitations.
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md): benchmark setup and full results (`go run ./bench`).
- [examples/http-ingest](examples/http-ingest): HTTP service with Docker Compose and a load script.
- Tests: `go test -race ./...`. Set `PG_BATCHER_TEST_DSN` to run the PostgreSQL integration tests.

## License

[MIT](LICENSE)
