package pgbatcher

import (
	"context"
	"testing"
	"time"
)

func BenchmarkEnqueue(b *testing.B) {
	f := &fakeExec{}
	batch := newBatcher(f, WithWindow(100*time.Microsecond), WithBatchSize(64))
	defer batch.Close(context.Background())
	ctx := WithBatch(context.Background())
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, e := batch.ExecContext(ctx, `INSERT INTO bench_table(a,b) VALUES($1,$2)`, 1, "payload"); e != nil {
				b.Error(e)
				return
			}
		}
	})
}
