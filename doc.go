// Package pgbatcher merges concurrent single-row PostgreSQL INSERTs into one
// multi-row INSERT while giving each caller its own result or error.
//
// Batching is opt-in per call with [WithBatch]. Use [New] to batch through a
// *sql.DB, or [Wrap] to batch at the database/sql driver level. Only
// "INSERT INTO t (cols...) VALUES ($1, ...)" is batched; all other SQL, and any
// statement inside a transaction, runs unchanged.
//
// If a batch fails, it is split and retried so that only the caller whose row
// caused the error receives it.
package pgbatcher
