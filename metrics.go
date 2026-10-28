package pgbatcher

import (
	"sync/atomic"
	"time"
)

// Metrics is an optional, allocation-free callback surface. Implementations must be concurrency-safe.
type Metrics interface {
	BatchFlushed(size int, wait time.Duration)
	Fallback()
	Error()
}
type Counters struct {
	Batches   atomic.Uint64
	Rows      atomic.Uint64
	Fallbacks atomic.Uint64
	Errors    atomic.Uint64
}

func (c *Counters) BatchFlushed(n int, _ time.Duration) { c.Batches.Add(1); c.Rows.Add(uint64(n)) }
func (c *Counters) Fallback()                           { c.Fallbacks.Add(1) }
func (c *Counters) Error()                              { c.Errors.Add(1) }
