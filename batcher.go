package pgbatcher

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Execer is the small execution surface used by Batcher. *sql.DB and connector-backed adapters satisfy it.
type Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// Batcher queues opted-in INSERTs and flushes them as multi-row statements. It is safe for concurrent use.
type Batcher struct {
	db              Execer
	cfg             config
	mu              sync.Mutex
	queues          map[string]*pendingGroup
	closing, closed bool
	active          sync.WaitGroup
}
type item struct {
	ctx    context.Context
	sql    string
	args   []any
	born   time.Time
	done   chan outcome
	exec   Execer
	pinned bool
	state  atomic.Int32
}

const (
	queued int32 = iota
	claimed
	abandoned
)

type outcome struct {
	r   sql.Result
	err error
}
type pendingGroup struct {
	shape insertShape
	items []*item
	bytes int
	born  time.Time
	timer *time.Timer
}

// New creates a shared batching coordinator over db. The *sql.DB remains responsible for pooling.
func New(db *sql.DB, opts ...Option) *Batcher { return newBatcher(db, opts...) }
func newBatcher(db Execer, opts ...Option) *Batcher {
	c := defaultConfig()
	for _, o := range opts {
		if o != nil {
			o(&c)
		}
	}
	return &Batcher{db: db, cfg: c, queues: map[string]*pendingGroup{}}
}
func (b *Batcher) flushIfQueued(key string, g *pendingGroup) {
	b.mu.Lock()
	if b.queues[key] != g {
		b.mu.Unlock()
		return
	}
	delete(b.queues, key)
	b.active.Add(1)
	b.mu.Unlock()
	defer b.active.Done()
	b.flush(g)
}

// ExecContext runs q like sql.DB.ExecContext. It is batched only when ctx comes from WithBatch
// and q is a supported single-row INSERT; otherwise it runs directly on the underlying db.
func (b *Batcher) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return b.execOn(ctx, b.db, false, q, args...)
}

// execOn queues a call. pinned means ex is the caller's own driver connection, so the
// caller must not return while a batch may still be executing on it.
func (b *Batcher) execOn(ctx context.Context, ex Execer, pinned bool, q string, args ...any) (sql.Result, error) {
	if b == nil || ex == nil {
		return nil, errors.New("pgbatcher: nil executor")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !opted(ctx) {
		if b.cfg.metrics != nil {
			b.cfg.metrics.Fallback()
		}
		return ex.ExecContext(ctx, q, args...)
	}
	shape, ok := batchableShape(q, args)
	if !ok {
		if b.cfg.metrics != nil {
			b.cfg.metrics.Fallback()
		}
		return ex.ExecContext(ctx, q, args...)
	}
	it := &item{ctx: ctx, sql: q, args: append([]any(nil), args...), born: time.Now(), done: make(chan outcome, 1), exec: ex, pinned: pinned}
	key := normalizeKey(q)
	b.mu.Lock()
	if b.closing || b.closed {
		b.mu.Unlock()
		return nil, errors.New("pgbatcher: closed")
	}
	g := b.queues[key]
	if g == nil {
		g = &pendingGroup{shape: shape, born: it.born}
		b.queues[key] = g
		g.timer = time.AfterFunc(b.cfg.window, func() { b.flushIfQueued(key, g) })
	}
	g.items = append(g.items, it)
	g.bytes += len(q) + len(args)*16
	trigger := len(g.items) >= b.cfg.size || g.bytes >= b.cfg.maxBytes
	if trigger {
		delete(b.queues, key)
		g.timer.Stop()
		b.active.Add(1)
	}
	b.mu.Unlock()
	if trigger {
		b.flush(g)
		b.active.Done()
	}
	select {
	case out := <-it.done:
		return out.r, out.err
	case <-ctx.Done():
		if it.pinned && !it.state.CompareAndSwap(queued, abandoned) {
			<-it.done
		}
		return nil, ctx.Err()
	}
}
func normalizeKey(q string) string {
	ts, e := tokenize(q)
	if e != nil {
		return q
	}
	var b strings.Builder
	for _, t := range ts {
		b.WriteByte(t.kind)
		b.WriteString(strings.ToLower(t.s))
		b.WriteByte(0)
	}
	return b.String()
}
func (b *Batcher) flush(g *pendingGroup) {
	live := g.items[:0]
	for _, it := range g.items {
		if it.ctx.Err() != nil || !it.state.CompareAndSwap(queued, claimed) {
			continue
		}
		live = append(live, it)
	}
	if len(live) == 0 {
		return
	}
	start := time.Now()
	b.executeSplit(g.shape, live)
	if b.cfg.metrics != nil {
		b.cfg.metrics.BatchFlushed(len(live), start.Sub(g.born))
	}
}
func (b *Batcher) executeSplit(s insertShape, items []*item) {
	if len(items) == 0 {
		return
	}
	q, args := compose(s, items)
	// The first live context contributes values but not cancellation: one caller's cancellation must not cancel peers.
	ctx := context.Background()
	for _, it := range items {
		if it.ctx.Err() == nil {
			ctx = context.WithoutCancel(it.ctx)
			break
		}
	}
	res, err := items[0].exec.ExecContext(ctx, q, args...)
	if err == nil {
		ra, _ := res.RowsAffected()
		per := int64(0)
		if len(items) > 0 && ra > 0 {
			per = ra / int64(len(items))
			if per == 0 {
				per = 1
			}
		}
		r := rowResult{affected: per}
		for _, it := range items {
			it.done <- outcome{r: r}
		}
		return
	}
	if b.cfg.metrics != nil {
		b.cfg.metrics.Error()
	}
	if len(items) == 1 {
		it := items[0]
		if err := it.ctx.Err(); err != nil {
			it.done <- outcome{err: err}
			return
		}
		r, e := it.exec.ExecContext(it.ctx, it.sql, it.args...)
		it.done <- outcome{r: r, err: e}
		return
	}
	mid := len(items) / 2
	b.executeSplit(s, items[:mid])
	b.executeSplit(s, items[mid:])
}
func compose(s insertShape, items []*item) (string, []any) {
	var q strings.Builder
	q.Grow(len(s.prefix) + len(s.suffix) + len(items)*32)
	q.WriteString(s.prefix)
	args := make([]any, 0, len(items)*s.placeholderCount)
	argn := 1
	for ri, it := range items {
		if ri > 0 {
			q.WriteByte(',')
		}
		q.WriteByte('(')
		for j := 0; j < s.placeholderCount; j++ {
			if j > 0 {
				q.WriteByte(',')
			}
			q.WriteByte('$')
			q.WriteString(strconv.Itoa(argn))
			argn++
		}
		q.WriteByte(')')
		args = append(args, it.args...)
	}
	q.WriteString(s.suffix)
	return q.String(), args
}

type rowResult struct{ affected int64 }

func (r rowResult) RowsAffected() (int64, error) { return r.affected, nil }
func (rowResult) LastInsertId() (int64, error) {
	return 0, errors.New("pgbatcher: LastInsertId is not supported by PostgreSQL")
}

// Close rejects new work, flushes all queued groups, and waits for in-flight flushes.
func (b *Batcher) Close(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closing = true
	gs := make([]*pendingGroup, 0, len(b.queues))
	for k, g := range b.queues {
		g.timer.Stop()
		gs = append(gs, g)
		delete(b.queues, k)
		b.active.Add(1)
	}
	b.mu.Unlock()
	for _, g := range gs {
		b.flush(g)
		b.active.Done()
	}
	waited := make(chan struct{})
	go func() { b.active.Wait(); close(waited) }()
	select {
	case <-waited:
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wrap creates a database/sql connector with a shared batching coordinator. Calls opt in with WithBatch.
func Wrap(c driver.Connector, opts ...Option) driver.Connector {
	return &wrappedConnector{Connector: c, b: newBatcher(nilExec{}, opts...)}
}

type nilExec struct{}

func (nilExec) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, errors.New("pgbatcher: internal connector executor")
}

type wrappedConnector struct {
	driver.Connector
	b *Batcher
}

func (c *wrappedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, e := c.Connector.Connect(ctx)
	if e != nil {
		return nil, e
	}
	return &wrappedConn{Conn: conn, b: c.b}, nil
}
func (c *wrappedConnector) Driver() driver.Driver { return c.Connector.Driver() }

// Close shuts down this connector's shared queue. Close the sql.DB first.
func (c *wrappedConnector) Close(ctx context.Context) error { return c.b.Close(ctx) }

type execAdapter struct{ c driver.Conn }

func (a execAdapter) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	ex, ok := a.c.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("pgbatcher: wrapped driver lacks ExecerContext")
	}
	nv := make([]driver.NamedValue, len(args))
	for i, v := range args {
		nv[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	r, e := ex.ExecContext(ctx, q, nv)
	if e != nil {
		return nil, e
	}
	return driverResult{r}, nil
}

type driverResult struct{ r driver.Result }

func (d driverResult) RowsAffected() (int64, error) { return d.r.RowsAffected() }
func (d driverResult) LastInsertId() (int64, error) { return d.r.LastInsertId() }

type wrappedConn struct {
	driver.Conn
	b    *Batcher
	mu   sync.Mutex
	inTx bool
}

func (c *wrappedConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	vals := make([]any, len(args))
	for i, v := range args {
		vals[i] = v.Value
	}
	c.mu.Lock()
	tx := c.inTx
	c.mu.Unlock()
	if tx || !opted(ctx) {
		ex, ok := c.Conn.(driver.ExecerContext)
		if !ok {
			return nil, driver.ErrSkip
		}
		return ex.ExecContext(ctx, q, args)
	}
	r, e := c.b.execOn(ctx, execAdapter{c.Conn}, true, q, vals...)
	if e != nil {
		return nil, e
	}
	return driverResult{driverResultAdapter{r}}, nil
}

type driverResultAdapter struct{ sql.Result }

func (d driverResultAdapter) LastInsertId() (int64, error) { return d.Result.LastInsertId() }
func (d driverResultAdapter) RowsAffected() (int64, error) { return d.Result.RowsAffected() }

func (c *wrappedConn) Begin() (driver.Tx, error) {
	//lint:ignore SA1019 The legacy method is required by driver.Conn; this adapter preserves drivers that only implement Begin.
	tx, e := c.Conn.Begin()
	if e == nil {
		c.mu.Lock()
		c.inTx = true
		c.mu.Unlock()
		return &wrappedTx{Tx: tx, c: c}, nil
	}
	return nil, e
}
func (c *wrappedConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	x, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return c.Begin()
	}
	tx, e := x.BeginTx(ctx, o)
	if e == nil {
		c.mu.Lock()
		c.inTx = true
		c.mu.Unlock()
		return &wrappedTx{Tx: tx, c: c}, nil
	}
	return nil, e
}

type wrappedTx struct {
	driver.Tx
	c *wrappedConn
}

func (t *wrappedTx) Commit() error {
	e := t.Tx.Commit()
	t.c.mu.Lock()
	t.c.inTx = false
	t.c.mu.Unlock()
	return e
}
func (t *wrappedTx) Rollback() error {
	e := t.Tx.Rollback()
	t.c.mu.Lock()
	t.c.inTx = false
	t.c.mu.Unlock()
	return e
}
