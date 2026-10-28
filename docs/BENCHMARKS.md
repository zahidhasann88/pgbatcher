# Benchmarks

Measured on 2026-10-28. Rerun on your own hardware before relying on these numbers.

```sh
PG_BATCHER_TEST_DSN='host=127.0.0.1 user=postgres password=postgres dbname=pgbatcher_test sslmode=disable' go run ./bench -runs 3
```

## Setup

- Laptop: Intel Core i7-8665U (4 cores / 8 threads), Windows 10, Docker Desktop on WSL2 (kernel 6.18).
- PostgreSQL 16.15 (`postgres:16` image, default config) in Docker. The benchmark (Go 1.24, `lib/pq`) runs in a
  second container that shares Postgres' network and PID namespace, so traffic goes over loopback.
- Table `pgbatcher_bench(id bigint, payload text)`, no indexes. `MaxOpenConns=10`, window 500µs, batch size 100.
- Each case runs for 3 seconds, 3 times. Throughput shows the median and the min–max across runs.
  Latency, average batch size, and pool waits come from the last run (pool waits are averaged across runs).
- **PG CPU per 1k rows** is the user+system CPU of all Postgres processes, read from `/proc`, divided by the number of rows inserted.
- The machine is a laptop, so results are noisy. Some cases vary by 2–3× between runs.

## Results: default settings (`synchronous_commit=on`)

Each plain INSERT is its own transaction and waits for a WAL flush. Batching shares one flush across many rows.

| Goroutines | Mode | rows/s (median) | rows/s (min–max) | p50 | p95 | p99 | Avg batch | Pool waits | PG CPU / 1k rows |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | plain | 451 | 443–480 | 2.06ms | 2.61ms | 5.2ms | – | 0 | 301ms |
| 1 | pgbatcher | 257 | 252–257 | 3.6ms | 6.52ms | 9.35ms | 1.0 | 0 | 461ms |
| 16 | plain | 1,921 | 1,821–2,173 | 6.42ms | 16.79ms | 57.26ms | – | 5,919 | 390ms |
| 16 | pgbatcher | 3,983 | 3,763–4,082 | 3.57ms | 5.97ms | 10.63ms | 8.4 | 0 | 57ms |
| 128 | plain | 1,860 | 1,760–2,158 | 48.95ms | 196.82ms | 302.53ms | – | 5,871 | 390ms |
| 128 | pgbatcher | 18,190 | 15,381–19,073 | 5.39ms | 21.03ms | 49.97ms | 44.4 | 0 | 20ms |
| 1024 | plain | 787 | 203–1,104 | 625ms | 3.33s | 4.00s | – | 3,104 | 484ms |
| 1024 | pgbatcher | 39,526 | 18,137–49,409 | 18.69ms | 69.52ms | 134.77ms | 72.9 | 1,003 | 17ms |

## Results: `synchronous_commit=off`

This removes most of the per-commit disk cost, so it isolates the round-trip and per-statement savings.

| Goroutines | Mode | rows/s (median) | rows/s (min–max) | p50 | p95 | p99 | Avg batch | Pool waits | PG CPU / 1k rows |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | plain | 865 | 854–891 | 0.70ms | 3.76ms | 12.6ms | – | 0 | 723ms |
| 1 | pgbatcher | 466 | 419–485 | 2.08ms | 4.29ms | 8.79ms | 1.0 | 0 | 516ms |
| 16 | plain | 5,874 | 4,013–6,178 | 1.5ms | 9.85ms | 20.57ms | – | 16,142 | 675ms |
| 16 | pgbatcher | 7,525 | 7,142–7,611 | 2.14ms | 2.56ms | 2.98ms | 15.3 | 0 | 38ms |
| 128 | plain | 7,263 | 7,216–7,956 | 11.59ms | 54.8ms | 89.91ms | – | 22,528 | 448ms |
| 128 | pgbatcher | 42,454 | 34,997–42,496 | 2.49ms | 5.99ms | 13.47ms | 46.8 | 0 | 21ms |
| 1024 | plain | 8,282 | 7,080–8,964 | 80.64ms | 368.55ms | 570.23ms | – | 25,438 | 404ms |
| 1024 | pgbatcher | 113,461 | 97,078–120,778 | 7.54ms | 27.62ms | 50.02ms | 76.6 | 323 | 16ms |

## Takeaways

- **One goroutine: pgbatcher is slower.** There is nothing to batch, so every call pays the window plus scheduling
  overhead. In this environment that was about 1.5ms extra at p50.
- **16 goroutines:** about 1.3–2× throughput, lower and more stable tail latency, and 7–18× less Postgres CPU per row.
- **128+ goroutines:** about 6–50× throughput, p99 in tens of milliseconds instead of hundreds, and 20–28× less Postgres CPU per row.
- The gain is largest when each statement is expensive relative to a row (commit flushes, network round-trips, a small connection pool).

## Enqueue microbenchmark

```text
BenchmarkEnqueue-8   21789   133505 ns/op   6368 B/op   20 allocs/op
```

`go test -run='^$' -bench=BenchmarkEnqueue -benchtime=2s -benchmem .` with a fake in-memory executor and a 100µs window.
ns/op is dominated by the window. The enqueue path copies arguments and is not allocation-free.

## Note on earlier results

An earlier version of this file reported pgbatcher as slower at every concurrency level. That run was affected by a bug:
the rewritten multi-row INSERT was malformed (`VALUES (($1,$2),...`), so every batch failed and fell back to one
statement per row through the error-isolation path. The bug is fixed, and `TestCompose` and the PostgreSQL integration
test now check that batches really execute.
