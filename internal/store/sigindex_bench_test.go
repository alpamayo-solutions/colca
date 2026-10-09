package store

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// prunedSignalStore builds a metrics stream of `signals` signals that has been
// pruned `prunes` times, each prune flushed into its own table, the way a
// parent's hourly retention leaves it: every prune writes one range tombstone
// per signal on the signal index, so the tables that cover si/ carry range
// deletion blocks that grow with signals x prunes. It returns the signal ids.
//
// A production store keeps those tombstones because they sit in the middle
// levels of gigabytes of data and reach the bottom slowly. A store this small
// compacts straight into the last level and drops them at once, so a snapshot
// held open for the life of the store stands in for the depth: Pebble keeps a
// tombstone an open snapshot may still need.
func prunedSignalStore(tb testing.TB, s *Store, signals, prunes, perPrune int) []string {
	tb.Helper()
	ids := make([]string, signals)
	for i := range ids {
		ids[i] = fmt.Sprintf("01HZZZZZZZZZZZZZZZZZZZ%04d", i)
	}
	ts := int64(0)
	batch := func() {
		recs := make([]Record, 0, signals*perPrune)
		for range perPrune {
			for _, id := range ids {
				ts++
				recs = append(recs, metricRecord(id, ts))
			}
		}
		if _, _, err := s.Append("metrics", recs); err != nil {
			tb.Fatal(err)
		}
	}
	batch()
	snap := s.db.NewSnapshot()
	tb.Cleanup(func() { _ = snap.Close() })
	for range prunes {
		batch()
		// Everything but the newest batch goes, as retention leaves a short tail.
		if _, err := s.Prune("metrics", s.NextOffset("metrics")-uint64(signals*perPrune), nil, nil); err != nil {
			tb.Fatal(err)
		}
		if err := s.db.Flush(); err != nil {
			tb.Fatal(err)
		}
	}
	return ids
}

// peakHeap runs fn and returns the highest heap in use it saw above the heap
// before fn, sampled every 200 microseconds, and the bytes fn allocated.
// Pebble's block allocations are on the Go heap only without cgo, as colca is
// built: run this with CGO_ENABLED=0.
func peakHeap(fn func()) (peak, allocated uint64) {
	runtime.GC()
	var before, m runtime.MemStats
	runtime.ReadMemStats(&before)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			runtime.ReadMemStats(&m)
			if m.HeapInuse > before.HeapInuse && m.HeapInuse-before.HeapInuse > peak {
				peak = m.HeapInuse - before.HeapInuse
			}
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Microsecond):
			}
		}
	})
	fn()
	close(stop)
	wg.Wait()
	runtime.ReadMemStats(&m)
	return peak, m.TotalAlloc - before.TotalAlloc
}

// BenchmarkReadSignalsAfterPrunes is the filtered fetch of a consumer that
// follows many signals (a dataops ingest, the maintenance app) on a stream
// retention has pruned many times. peak-heap-MB is what one such read holds at
// once; a parent serves several concurrently.
//
//	CGO_ENABLED=0 go test ./internal/store -run '^$' -bench ReadSignalsAfterPrunes -benchtime 5x
func BenchmarkReadSignalsAfterPrunes(b *testing.B) {
	for _, tc := range []struct{ signals, prunes int }{{100, 100}, {1000, 100}} {
		b.Run(fmt.Sprintf("signals=%d/prunes=%d", tc.signals, tc.prunes), func(b *testing.B) {
			s, err := Open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			ids := prunedSignalStore(b, s, tc.signals, tc.prunes, 2)
			from := s.LWM("metrics")
			for _, at := range []struct {
				name string
				from uint64
			}{{"backlog", from}, {"head", s.NextOffset("metrics") - 1}} {
				b.Run(at.name, func(b *testing.B) {
					var peak, allocated uint64
					b.ResetTimer()
					for range b.N {
						p, a := peakHeap(func() {
							if _, _, err := s.ReadSignals(context.Background(), "metrics", at.from, 1000, 20000, ids, nil); err != nil {
								b.Fatal(err)
							}
						})
						peak, allocated = max(peak, p), allocated+a
					}
					b.ReportMetric(float64(peak)/(1<<20), "peak-heap-MB")
					b.ReportMetric(float64(allocated)/float64(b.N)/(1<<20), "alloc-MB/op")
				})
			}
		})
	}
}
