package pgbatcher

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCanceledQueuedCallerDoesNotPoisonPeers(t *testing.T) {
	f := &fakeExec{}
	b := newBatcher(f, WithWindow(8*time.Millisecond), WithBatchSize(10))
	defer b.Close(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, e := b.ExecContext(WithBatch(ctx), `INSERT INTO t(a) VALUES($1)`, 1); first <- e }()
	time.Sleep(time.Millisecond)
	cancel()
	if e := <-first; !errors.Is(e, context.Canceled) {
		t.Fatalf("cancel err=%v", e)
	}
	_, e := b.ExecContext(WithBatch(context.Background()), `INSERT INTO t(a) VALUES($1)`, 2)
	if e != nil {
		t.Fatal(e)
	}
}
