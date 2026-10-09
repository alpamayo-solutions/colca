//go:build boundary

package historian

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// BenchmarkLateWriteIngest exercises the real partitioned sink, including
// transaction/marking/offset work. It only accepts a disposable benchmark DB:
// each case resets its own schema, so never point it at an existing dataset.
func BenchmarkLateWriteIngest(b *testing.B) {
	dsn := os.Getenv("HISTORIAN_BENCH_DSN")
	if dsn == "" {
		b.Skip("HISTORIAN_BENCH_DSN not set")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasPrefix(config.ConnConfig.Database, "sigstats_ingest_bench_") {
		b.Fatal("benchmark requires a disposable sigstats_ingest_bench_ database")
	}
	ctx := context.Background()
	pool, err := Open(ctx, dsn, 4)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	var database string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil || database != config.ConnConfig.Database {
		b.Fatalf("connected database mismatch: database=%q err=%v", database, err)
	}
	for _, scenario := range []struct {
		name    string
		wanted  int // -1: no marking relations
		current bool
	}{
		{"no-relations", -1, false}, {"empty-wanted", 0, false},
		{"300-wanted", 300, false}, {"14000-wanted", 14000, false},
		{"14000-current", 14000, true},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS timescaledb;
DROP TABLE IF EXISTS historian_metric, colca_applied_offset, historian_late_write, historian_late_write_signal CASCADE;
CREATE TABLE historian_metric (
 id bigserial, timestamp timestamptz NOT NULL, value_json jsonb,
 value_number double precision, value_text text, value_bool boolean,
 colca_node_id text, signal_id text NOT NULL,
 UNIQUE(signal_id,timestamp)
);
SELECT create_hypertable('historian_metric','timestamp',chunk_time_interval=>INTERVAL '1 day');`); err != nil {
				b.Fatal(err)
			}
			sink := &Sink{Pool: pool, Writers: 4, Strict: true}
			if err := sink.EnsureSchema(ctx, 0); err != nil {
				b.Fatal(err)
			}
			if scenario.wanted >= 0 {
				if _, err := pool.Exec(ctx, `CREATE TABLE historian_late_write (
 id bigserial PRIMARY KEY, signal_id text NOT NULL, hour timestamptz NOT NULL, UNIQUE(signal_id,hour));
CREATE TABLE historian_late_write_signal (signal_id text PRIMARY KEY);`); err != nil {
					b.Fatal(err)
				}
				if _, err := pool.Exec(ctx, `INSERT INTO historian_late_write_signal SELECT 'signal-' || i FROM generate_series(0,$1::int-1) i`, scenario.wanted); err != nil {
					b.Fatal(err)
				}
			}
			if err := sink.WatchLateWrites(ctx, nil); err != nil {
				b.Fatal(err)
			}
			const pageSize = 1250
			rows := make([]Row, pageSize)
			at := time.Now().UTC().Truncate(time.Hour).Add(-24 * time.Hour)
			if scenario.current {
				at = time.Now().UTC()
			}
			value := 1.0
			b.ResetTimer()
			for page := 0; page < b.N; page++ {
				for i := range rows {
					ordinal := page*pageSize + i
					rows[i] = Row{SignalID: fmt.Sprintf("signal-%d", ordinal%14000), NodeID: "bench", Timestamp: at.Add(time.Duration(ordinal/14000) * time.Millisecond), Number: &value}
				}
				if rejected, err := sink.Apply(ctx, rows, "bench:metrics", int64(page+1), ""); err != nil || len(rejected) != 0 {
					b.Fatalf("rejected=%v err=%v", rejected, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N*pageSize)/b.Elapsed().Seconds(), "rows/s")
			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM historian_metric`).Scan(&count); err != nil || count != b.N*pageSize {
				b.Fatalf("stored=%d want=%d err=%v", count, b.N*pageSize, err)
			}
			if scenario.wanted >= 0 {
				want := min(scenario.wanted, b.N*pageSize)
				if scenario.current {
					want = 0
				}
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM historian_late_write`).Scan(&count); err != nil || count != want {
					b.Fatalf("marks=%d want=%d err=%v", count, want, err)
				}
			}
		})
	}
}
