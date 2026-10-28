// Command bench compares plain single-row INSERTs with pgbatcher.
//
//	PG_BATCHER_TEST_DSN='...' go run ./bench -runs 3
//
// Postgres CPU is reported only when Postgres processes are visible in /proc
// (same Linux host, or a container sharing Postgres' PID namespace).
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
	pb "github.com/zahidhasann88/pgbatcher"
)

type config struct {
	duration time.Duration
	window   time.Duration
	size     int
	maxConns int
}

type result struct {
	ops       int
	errs      uint64
	rate      float64
	p50, p95  time.Duration
	p99       time.Duration
	waitCount int64
	wait      time.Duration
	batches   uint64
	rows      uint64
	pgCPU     time.Duration
	hasPGCPU  bool
}

func pct(v []time.Duration, p float64) time.Duration {
	if len(v) == 0 {
		return 0
	}
	return v[int(float64(len(v)-1)*p)]
}

func run(db *sql.DB, cfg config, workers int, batch bool) result {
	before := db.Stats()
	cpuBefore, cpuOK := postgresCPU()
	metrics := &pb.Counters{}
	var b *pb.Batcher
	if batch {
		b = pb.New(db, pb.WithWindow(cfg.window), pb.WithBatchSize(cfg.size), pb.WithMetrics(metrics))
	}
	const q = "INSERT INTO pgbatcher_bench(id,payload) VALUES($1,$2)"
	stopAt := time.Now().Add(cfg.duration)
	var id atomic.Int64
	var errs atomic.Uint64
	var mu sync.Mutex
	var wg sync.WaitGroup
	lat := make([]time.Duration, 0, workers*1000)
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]time.Duration, 0, 1000)
			for time.Now().Before(stopAt) {
				n := id.Add(1)
				t := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				var e error
				if batch {
					_, e = b.ExecContext(pb.WithBatch(ctx), q, n, "x")
				} else {
					_, e = db.ExecContext(ctx, q, n, "x")
				}
				cancel()
				if e != nil {
					errs.Add(1)
					continue
				}
				local = append(local, time.Since(t))
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if b != nil {
		_ = b.Close(context.Background())
	}
	elapsed := time.Since(start)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	after := db.Stats()
	r := result{
		ops: len(lat), errs: errs.Load(), rate: float64(len(lat)) / elapsed.Seconds(),
		p50: pct(lat, .50), p95: pct(lat, .95), p99: pct(lat, .99),
		waitCount: after.WaitCount - before.WaitCount, wait: after.WaitDuration - before.WaitDuration,
		batches: metrics.Batches.Load(), rows: metrics.Rows.Load(),
	}
	if cpuAfter, ok := postgresCPU(); ok && cpuOK {
		r.pgCPU, r.hasPGCPU = cpuAfter-cpuBefore, true
	}
	return r
}

// postgresCPU sums user+system CPU of live postgres processes plus the CPU that the
// postmaster has reaped from exited backends. It assumes the usual 100 clock ticks per second.
func postgresCPU() (time.Duration, bool) {
	stats, _ := filepath.Glob("/proc/[0-9]*/stat")
	var ticks int64
	found := false
	for _, p := range stats {
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		s := string(raw)
		open, end := strings.IndexByte(s, '('), strings.LastIndexByte(s, ')')
		if open < 0 || end < open || s[open+1:end] != "postgres" {
			continue
		}
		f := strings.Fields(s[end+2:])
		if len(f) < 15 {
			continue
		}
		found = true
		for _, i := range []int{11, 12, 13, 14} {
			v, _ := strconv.ParseInt(f[i], 10, 64)
			ticks += v
		}
	}
	return time.Duration(ticks) * 10 * time.Millisecond, found
}

func median(v []float64) float64 {
	sort.Float64s(v)
	return v[len(v)/2]
}

func main() {
	cfg := config{}
	flag.DurationVar(&cfg.duration, "duration", 3*time.Second, "time per case")
	flag.DurationVar(&cfg.window, "window", 500*time.Microsecond, "pgbatcher window")
	flag.IntVar(&cfg.size, "size", 100, "pgbatcher batch size")
	flag.IntVar(&cfg.maxConns, "maxconns", 10, "sql.DB MaxOpenConns")
	runs := flag.Int("runs", 3, "repetitions per case")
	workerList := flag.String("workers", "1,16,128,1024", "comma-separated goroutine counts")
	flag.Parse()

	dsn := os.Getenv("PG_BATCHER_TEST_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "set PG_BATCHER_TEST_DSN")
		os.Exit(2)
	}
	db, e := sql.Open("postgres", dsn)
	if e != nil {
		panic(e)
	}
	defer db.Close()
	db.SetMaxOpenConns(cfg.maxConns)
	db.SetMaxIdleConns(cfg.maxConns)
	if e = db.Ping(); e != nil {
		panic(e)
	}
	if _, e = db.Exec(`DROP TABLE IF EXISTS pgbatcher_bench; CREATE TABLE pgbatcher_bench(id bigint,payload text)`); e != nil {
		panic(e)
	}
	fmt.Printf("duration=%s window=%s size=%d maxconns=%d runs=%d\n", cfg.duration, cfg.window, cfg.size, cfg.maxConns, *runs)
	fmt.Println("| Goroutines | Mode | rows/s (median) | rows/s (min-max) | p50 | p95 | p99 | Avg batch | Pool waits | PG CPU per 1k rows | Errors |")
	fmt.Println("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
	for _, ws := range strings.Split(*workerList, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(ws))
		if err != nil || n <= 0 {
			panic(fmt.Sprintf("bad worker count %q", ws))
		}
		for _, batched := range []bool{false, true} {
			var rates []float64
			var last result
			var errs uint64
			var waits int64
			var cpu time.Duration
			var ops int
			cpuOK := true
			for i := 0; i < *runs; i++ {
				if _, e = db.Exec(`TRUNCATE pgbatcher_bench`); e != nil {
					panic(e)
				}
				r := run(db, cfg, n, batched)
				rates = append(rates, r.rate)
				errs += r.errs
				waits += r.waitCount
				cpu += r.pgCPU
				ops += r.ops
				cpuOK = cpuOK && r.hasPGCPU
				last = r
			}
			mode, avg := "naive", "-"
			if batched {
				mode = "pgbatcher"
				if last.batches > 0 {
					avg = fmt.Sprintf("%.1f", float64(last.rows)/float64(last.batches))
				}
			}
			cpuCol := "n/a"
			if cpuOK && ops > 0 {
				cpuCol = (cpu * 1000 / time.Duration(ops)).Round(time.Microsecond).String()
			}
			lo, hi := rates[0], rates[0]
			for _, r := range rates {
				lo, hi = min(lo, r), max(hi, r)
			}
			fmt.Printf("| %d | %s | %.0f | %.0f-%.0f | %s | %s | %s | %s | %d | %s | %d |\n", n, mode, median(append([]float64(nil), rates...)), lo, hi,
				last.p50.Round(10*time.Microsecond), last.p95.Round(10*time.Microsecond), last.p99.Round(10*time.Microsecond), avg, waits/int64(*runs), cpuCol, errs)
		}
	}
}
