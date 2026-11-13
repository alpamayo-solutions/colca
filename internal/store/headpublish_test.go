package store

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// A reader does not wait for an append's synced apply: the state lock is taken
// only to publish the head afterwards. Until then the reader sees the old head
// and none of the records being written, and after it sees them all.
func TestReadersDoNotWaitForAnAppendsSync(t *testing.T) {
	s := mustOpen(t)
	if _, _, err := s.Append("metrics", []Record{metricRecord("a", 1)}); err != nil {
		t.Fatal(err)
	}
	applied, release := make(chan struct{}), make(chan struct{})
	apply := s.appendApply
	s.appendApply = func(b *pebble.Batch, o *pebble.WriteOptions) error {
		err := apply(b, o) // the records are in Pebble, the sync "takes long"
		close(applied)
		<-release
		return err
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := s.Append("metrics", []Record{metricRecord("a", 2), metricRecord("a", 3)})
		done <- err
	}()
	<-applied

	read := make(chan []StoredRecord, 1)
	go func() {
		recs, _, _ := s.ReadRecords("metrics", 1, 10, nil)
		read <- recs
	}()
	select {
	case recs := <-read:
		if len(recs) != 1 {
			t.Fatalf("a reader saw %d records while the head was unpublished, want the 1 below it", len(recs))
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("a reader waited for an append's sync")
	}
	if head := s.NextOffset("metrics"); head != 2 {
		t.Fatalf("head %d during the sync, want 2", head)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if recs, _, _ := s.ReadRecords("metrics", 1, 10, nil); len(recs) != 3 || s.NextOffset("metrics") != 4 {
		t.Fatalf("after the publish: %d records, head %d; want 3 and 4", len(recs), s.NextOffset("metrics"))
	}
}

// BenchmarkReadsDuringAppends is a reader asking for the head and a page of a
// stream while another goroutine appends synced batches as fast as it can.
// Set COLCA_BENCH_DIR to a directory on a real disk.
//
//	COLCA_BENCH_DIR=$HOME/.cache/colca-bench go test ./internal/store -run '^$' -bench ReadsDuringAppends
func BenchmarkReadsDuringAppends(b *testing.B) {
	dir := b.TempDir()
	if base := os.Getenv("COLCA_BENCH_DIR"); base != "" {
		var err error
		if dir, err = os.MkdirTemp(base, "reads"); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { os.RemoveAll(dir) })
	}
	s, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	var stop atomic.Bool
	writer := make(chan struct{})
	go func() {
		defer close(writer)
		for i := int64(0); !stop.Load(); i++ {
			if _, _, err := s.Append("metrics", []Record{metricRecord("a", i)}); err != nil {
				return
			}
		}
	}()
	var worst time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		head := s.NextOffset("metrics")
		if head > 1 {
			if _, _, err := s.ReadRecordsBounded(context.Background(), "metrics", head-1, 1, 1, 0, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
		worst = max(worst, time.Since(start))
	}
	b.StopTimer()
	stop.Store(true)
	<-writer
	b.ReportMetric(float64(worst.Microseconds()), "worst-µs")
}
