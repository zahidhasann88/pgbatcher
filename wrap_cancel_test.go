package pgbatcher

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type gateConn struct {
	gate    chan struct{}
	started chan struct{}
	busy    atomic.Bool
	misuse  *atomic.Bool
}

func (c *gateConn) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	if !c.busy.CompareAndSwap(false, true) {
		c.misuse.Store(true)
		return nil, errors.New("concurrent use of driver connection")
	}
	defer c.busy.Store(false)
	if c.gate != nil {
		c.started <- struct{}{}
		<-c.gate
	}
	return driver.RowsAffected(len(args)), nil
}
func (c *gateConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *gateConn) Close() error                        { return nil }
func (c *gateConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

type gateConnector struct {
	mu     sync.Mutex
	n      int
	first  *gateConn
	misuse atomic.Bool
}

func (g *gateConnector) Connect(context.Context) (driver.Conn, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	if g.n == 1 {
		return g.first, nil
	}
	return &gateConn{misuse: &g.misuse}, nil
}
func (g *gateConnector) Driver() driver.Driver { return nil }

func TestWrapCanceledCallerKeepsConnectionUntilBatchEnds(t *testing.T) {
	gc := &gateConnector{}
	gc.first = &gateConn{gate: make(chan struct{}), started: make(chan struct{}, 1), misuse: &gc.misuse}
	wrapped := Wrap(gc, WithWindow(time.Hour), WithBatchSize(2))
	db := sql.OpenDB(wrapped)
	defer func() {
		_ = db.Close()
		_ = wrapped.(interface{ Close(context.Context) error }).Close(context.Background())
	}()
	var openGate sync.Once
	release := func() { openGate.Do(func() { close(gc.first.gate) }) }
	defer release()
	b := wrapped.(*wrappedConnector).b
	const q = `INSERT INTO t(a) VALUES($1)`

	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan error, 1)
	go func() { _, e := db.ExecContext(WithBatch(ctxA), q, 1); doneA <- e }()
	for deadline := time.Now().Add(time.Second); ; {
		b.mu.Lock()
		n := len(b.queues)
		b.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first call was not queued")
		}
		time.Sleep(time.Millisecond)
	}

	doneB := make(chan error, 1)
	go func() { _, e := db.ExecContext(WithBatch(context.Background()), q, 2); doneB <- e }()
	<-gc.first.started
	cancelA()
	select {
	case e := <-doneA:
		t.Fatalf("canceled caller returned (%v) while the batch was still running on its connection", e)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	if e := <-doneA; !errors.Is(e, context.Canceled) {
		t.Fatalf("canceled caller err=%v", e)
	}
	if e := <-doneB; e != nil {
		t.Fatalf("peer err=%v", e)
	}
	if _, e := db.ExecContext(context.Background(), q, 3); e != nil {
		t.Fatal(e)
	}
	if gc.misuse.Load() {
		t.Fatal("driver connection was used concurrently")
	}
}
