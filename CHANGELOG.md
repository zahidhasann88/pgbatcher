# Changelog

## Unreleased

- Initial Go 1.22+ MVP: conservative INSERT parser, explicit opt-in queue, time/size/byte flushes, multi-VALUES rewrite, recursive failure isolation, database/sql helper and connector wrapper, metrics, local-PostgreSQL integration tests, example service, and benchmark harness.
- Keyed SELECT batching, typed-array `unnest`, `RETURNING`, `ON CONFLICT`, and SQL hint opt-in are not implemented; see `docs/DESIGN.md`.

### Fixed

- Batched INSERTs were rewritten as malformed SQL (`VALUES (($1,$2),...`), so every batch failed and fell back to one statement per row. Batches now execute as a single multi-row INSERT.
- With `Wrap`, a caller cancelled mid-flush could return while the batch was still running on its connection, letting `database/sql` reuse that connection concurrently.

### Changed

- Module path is `github.com/zahidhasann88/pgbatcher`.
- `ParseInsert` is now unexported; it returned an unexported type.
- Window flushes use one timer per pending group instead of a background ticker that ran every `window/10`.
- `go run ./bench` supports `-runs`, `-workers`, `-duration`, `-window`, `-size`, and `-maxconns`, and reports errors, average batch size, and Postgres CPU.
