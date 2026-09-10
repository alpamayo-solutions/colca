package store

import (
	"encoding/json"
	"fmt"
	"testing"
)

// benchPayload is a realistic _Metric payload (~60 bytes on the wire).
func benchPayload(i int) []byte {
	b, _ := json.Marshal(map[string]any{"v": float64(i) * 1.5, "ts": int64(1700000000000 + i)})
	return b
}

// BenchmarkAppendBatch measures one synced batch per iteration at several batch
// sizes. The per-record rate shows fsync coalescing: batch=1 is one fsync per
// record, like MQTT ingest; batch=200 is the replication apply path.
func BenchmarkAppendBatch(b *testing.B) {
	for _, size := range []int{1, 10, 100, 200, 1000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			s, err := Open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			recs := make([]Record, size)
			for i := range recs {
				recs[i] = Record{
					Topic:   fmt.Sprintf("colca/v1/_Metric/m1/m1/sig%d", i%16),
					Payload: benchPayload(i),
					TS:      int64(i),
					KVPath:  fmt.Sprintf("m1/sig%d", i%16),
					KVNode:  "m1",
				}
			}
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, _, err := s.Append("metrics", recs); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size*b.N)/b.Elapsed().Seconds(), "rec/s")
		})
	}
}

// BenchmarkKVScan measures a full projection scan at growing path cardinality,
// the storage cost behind /kv.
func BenchmarkKVScan(b *testing.B) {
	for _, paths := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("paths=%d", paths), func(b *testing.B) {
			s, err := Open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			const batch = 500
			for done := 0; done < paths; done += batch {
				n := min(batch, paths-done)
				recs := make([]Record, n)
				for i := range recs {
					p := done + i
					recs[i] = Record{
						Topic:   fmt.Sprintf("colca/v1/_Metric/m1/m1/line%d/sig%d", p/100, p%100),
						Payload: benchPayload(p),
						TS:      int64(p),
						KVPath:  fmt.Sprintf("m1/line%d/sig%d", p/100, p%100),
						KVNode:  "m1",
					}
				}
				if _, _, err := s.Append("metrics", recs); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				entries, err := s.KVScan("")
				if err != nil {
					b.Fatal(err)
				}
				if got := len(entries); got != paths {
					b.Fatalf("scan returned %d entries, want %d", got, paths)
				}
			}
			b.ReportMetric(float64(paths)/(b.Elapsed().Seconds()/float64(b.N)), "entries/s")
		})
	}
}

// BenchmarkReadSequential measures cursor-style reads in replBatch-sized pages,
// the uplink's read path.
func BenchmarkReadSequential(b *testing.B) {
	s, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	const total, page = 50_000, 200
	for done := 0; done < total; done += 500 {
		recs := make([]Record, 500)
		for i := range recs {
			recs[i] = Record{Topic: "colca/v1/_Metric/m1/m1/temp", Payload: benchPayload(done + i), TS: int64(done + i)}
		}
		if _, _, err := s.Append("metrics", recs); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var from uint64 = 1
		count := 0
		for count < total {
			recs, next, err := s.Read("metrics", from, page, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(recs) == 0 {
				break
			}
			count += len(recs)
			from = next
		}
		if count != total {
			b.Fatalf("read %d records, want %d", count, total)
		}
	}
	b.ReportMetric(float64(total)/(b.Elapsed().Seconds()/float64(b.N)), "rec/s")
}
