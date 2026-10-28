package pgbatcher

import (
	"context"
	"time"
)

type contextKey struct{}

// WithBatch explicitly opts a call into batching.
func WithBatch(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKey{}, true)
}
func opted(ctx context.Context) bool { v, _ := ctx.Value(contextKey{}).(bool); return v }

// Option configures a Batcher or a wrapped connector.
type Option func(*config)
type config struct {
	window         time.Duration
	size, maxBytes int
	metrics        Metrics
}

func defaultConfig() config {
	return config{window: 500 * time.Microsecond, size: 100, maxBytes: 1 << 20}
}

// WithWindow sets the maximum time a call waits before its batch is flushed. Default 500µs.
func WithWindow(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.window = d
		}
	}
}

// WithBatchSize sets the maximum number of rows per batch. Default 100.
func WithBatchSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.size = n
		}
	}
}

// WithMaxBytes sets the approximate queued-bytes limit that triggers a flush. Default 1 MiB.
func WithMaxBytes(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxBytes = n
		}
	}
}

// WithMetrics attaches callbacks; implementations must be concurrency-safe.
func WithMetrics(m Metrics) Option { return func(c *config) { c.metrics = m } }
