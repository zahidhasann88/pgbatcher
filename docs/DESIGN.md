# Design and known limitations

## Architecture / API trade-off

`New(*sql.DB, ...Option)` owns a shared queue and exposes `ExecContext`; this is the smallest robust way for independent calls from many goroutines to share a coordinator while retaining a result per invocation. It is not a transparent `*sql.DB` replacement: callers must route eligible calls through the `Batcher`. Calls through `*sql.Tx` are not routed through `Batcher` and remain ordinary transaction statements.

`Wrap(driver.Connector, ...Option)` wraps `database/sql` driver calls. All wrapped connections share a queue, and each queued call holds its originating connection while waiting; a batch executes on the connection of its first caller. A caller that is cancelled after its row was taken into a batch therefore waits for that batch to finish before returning, so `database/sql` never reuses a connection that a batch is still using. Transactions are tracked per wrapped connection and bypass batching. This adapter requires driver `ExecerContext` support for its batch path. The returned connector can be closed with a type assertion to `interface{ Close(context.Context) error }` after its `sql.DB` is closed. Connector users should prefer `New` if they want explicit ownership. Prepared statements, Query calls, and non-Exec driver paths are not intercepted for batching.

## Opt-in and parsing

`WithBatch(ctx)` is the only opt-in in the current helper API. The parser is a tokenizer (not regex-only), discarding line/block comments and preserving quoted identifiers, ordinary strings, dollar-quoted strings, and positional parameters as token classes. It accepts only a single INSERT with explicit identifiers/columns, one VALUES tuple of positional parameters, and an optional basic ON CONFLICT token sequence at parser level. The runtime currently rejects conflict and RETURNING clauses from batching because aggregate command results cannot reliably be demultiplexed. `/*+ batch */` SQL hint recognition is not implemented. Unsupported SQL is sent to the underlying executor unchanged.

## Rewriting and results

The requested design proposed typed-array `unnest` via catalog lookups/registration, which reduces parameter count and stabilizes query shape. The MVP instead renders `INSERT ... VALUES (...), (...)` with renumbered placeholders. This avoids driver-specific PostgreSQL array encoders but changes SQL text with batch size, can approach PostgreSQL's 65,535 bind limit for sufficiently wide/large groups, and does not use a catalog type cache. This deviation is explicit and not suitable for extreme batch widths.

On success each caller receives a simple result representing one affected row. On failure the combined statement is bisected recursively, then singleton SQL is retried with the original caller context. Constraint failures are isolated to the offending singleton when PostgreSQL can identify them this way. Shared failures can cause repeated attempts; an unavailable connection can make all affected callers fail. Error results can differ from a hypothetical sequential transaction for concurrent unique-key races because statement ordering is batch ordering, not goroutine start ordering.

## Scope gaps and correctness boundaries

- Keyed SELECT + `ANY`, row demultiplexing, duplicate/missing/NULL key behavior, and custom row scanning are not implemented.
- INSERT `RETURNING` and `ON CONFLICT` pass through individually; therefore these preserve native semantics but do not batch.
- Dynamic multi-VALUES is used, not `unnest`; no PostgreSQL catalog type resolution is required.
- No prepared statement batching or transaction batching. A `Batcher` built from `*sql.DB` cannot discover arbitrary `*sql.Tx` use; code must not route transaction statements through the batcher. Direct Tx calls naturally bypass it.
- The byte trigger is an estimate, not a wire-protocol size measurement. Batch result attribution assumes one affected row for each successful basic INSERT; triggers/rules that alter row counts are unsupported.
- Per-call argument copying occurs on enqueue to keep caller mutation from racing with a later flush. The enqueue path is not allocation-free.
- `Metrics.Fallback` fires for every call that is not batched (not opted in or unsupported shape). `Metrics.Error` fires once per failed batch attempt, including bisection retries, so it is not a per-caller error count.
- No adaptive window, Prometheus adapter, pgxpool package, or integration with ORM session semantics.

## Shutdown and concurrency

Queue access and admission are mutex-protected. Each pending group has its own timer that flushes it when the window expires, while size/byte thresholds flush synchronously on the caller that crosses them. No background goroutine runs while the queue is idle. Flush work is tracked so `Close(ctx)` can reject intake, flush pending groups, and await active work. Context cancellation before a flush drops the caller's queued row; once execution starts, cancellation is detached from the shared statement, the row may still be written, and the caller receives `ctx.Err()`. That favors peer isolation, but means cancellation after dispatch does not promise server-side query cancellation.
