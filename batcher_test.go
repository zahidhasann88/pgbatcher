package pgbatcher

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeExec struct {
	mu        sync.Mutex
	calls     []string
	failValue any
}

func (f *fakeExec) ExecContext(_ context.Context, q string, args ...any) (sql.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, q)
	for _, a := range args {
		if a == f.failValue && f.failValue != nil {
			return nil, errors.New("constraint violation")
		}
	}
	return rowResult{affected: int64(len(args) / 2)}, nil
}
func TestCoalescesAndIsolates(t *testing.T) {
	f := &fakeExec{failValue: "bad"}
	b := newBatcher(f, WithWindow(5*time.Millisecond), WithBatchSize(50))
	defer b.Close(context.Background())
	const n = 40
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := fmt.Sprintf("v%d", i)
			if i == 17 {
				v = "bad"
			}
			_, errs[i] = b.ExecContext(WithBatch(context.Background()), "INSERT INTO test_t (id, value) VALUES ($1, $2)", i, v)
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		if i == 17 && e == nil {
			t.Fatal("bad row succeeded")
		}
		if i != 17 && e != nil {
			t.Fatalf("row %d: %v", i, e)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) >= n {
		t.Fatalf("expected coalescing, calls=%d", len(f.calls))
	}
}
func TestOptInAndUnsupportedPassthrough(t *testing.T) {
	f := &fakeExec{}
	b := newBatcher(f, WithWindow(time.Hour))
	ctx := context.Background()
	_, e := b.ExecContext(ctx, "INSERT INTO t(a) VALUES($1)", 1)
	if e != nil {
		t.Fatal(e)
	}
	_, e = b.ExecContext(WithBatch(ctx), "INSERT INTO t(a) VALUES($1) RETURNING a", 2)
	if e != nil {
		t.Fatal(e)
	}
	if len(f.calls) != 2 {
		t.Fatalf("calls=%d", len(f.calls))
	}
	_ = b.Close(context.Background())
}
func TestCloseFlushes(t *testing.T) {
	f := &fakeExec{}
	b := newBatcher(f, WithWindow(time.Hour))
	done := make(chan error, 1)
	go func() {
		_, e := b.ExecContext(WithBatch(context.Background()), "INSERT INTO t(a) VALUES($1)", 1)
		done <- e
	}()
	time.Sleep(2 * time.Millisecond)
	if e := b.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if _, e := b.ExecContext(WithBatch(context.Background()), "INSERT INTO t(a) VALUES($1)", 2); e == nil {
		t.Fatal("expected closed")
	}
}
