# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-10-28

First release.

### Added

- `New`, `Batcher.ExecContext`, and `Close`: batch opted-in single-row INSERTs through a `*sql.DB`.
- `Wrap`: batch at the `database/sql` driver level; statements inside transactions are never batched.
- `WithBatch` per-call opt-in, and the `WithWindow`, `WithBatchSize`, `WithMaxBytes`, and `WithMetrics` options.
- Conservative SQL tokenizer that batches only `INSERT INTO t (cols) VALUES ($1, ...)`; everything else passes through.
- Multi-row `VALUES` rewrite, grouped by statement shape, flushed by time window, batch size, or byte limit.
- Error isolation: failed batches are split and retried so only the failing caller gets the error.
- Per-caller context cancellation, and `Metrics` interface with an atomic `Counters` implementation.
- Benchmark harness (`go run ./bench`), HTTP ingest example with Docker Compose, and CI.

### Known limitations

- Keyed SELECT batching, typed-array `unnest`, and batched `RETURNING` / `ON CONFLICT` are not implemented.
- The `/*+ batch */` SQL hint is not supported; use `WithBatch`.
- See [docs/DESIGN.md](docs/DESIGN.md) for details.

[Unreleased]: https://github.com/zahidhasann88/pgbatcher/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/zahidhasann88/pgbatcher/releases/tag/v0.1.0
