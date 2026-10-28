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

type Option func(*config)
type config struct {
	window         time.Duration
	size, maxBytes int
	metrics        Metrics
}

func defaultConfig() config {
	return config{window: 500 * time.Microsecond, size: 100, maxBytes: 1 << 20}
}
func WithWindow(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.window = d
		}
	}
}
func WithBatchSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.size = n
		}
	}
}
func WithMaxBytes(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxBytes = n
		}
	}
}

// WithMetrics attaches callbacks; implementations must be concurrency-safe.
func WithMetrics(m Metrics) Option { return func(c *config) { c.metrics = m } }
