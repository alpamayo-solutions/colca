package store

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// putHeadOnly appends, as the stream's next record, a value whose head
// (topic, payload, timestamp) reads plainly but whose remainder does not
// decode. A scan that decodes it fails; one that reads only the head of the
// records it does not keep passes over it.
func putHeadOnly(t *testing.T, s *Store, stream, topic string, ts int64) uint64 {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	off := s.next[stream]
	val := []byte(`{"t":"` + topic + `","p":"eA==","ts":` + strconv.FormatInt(ts, 10) + `,"wb":not json}`)
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(streamKey(stream, off), val, nil); err != nil {
		t.Fatal(err)
	}
	if err := setHead(b, stream, off+1); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	s.state.Lock()
	s.next[stream] = off + 1
	s.state.Unlock()
	return off
}

// The passes over a whole stream (eviction, compaction, retention's policy
// scan and prune, the time seek, a filtered walk) read each record's head and
// decode only what they keep or return.
func TestStreamPassesDecideOnTheHeadWithoutDecoding(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	t.Run("eviction", func(t *testing.T) {
		s := mustOpen(t)
		putHeadOnly(t, s, "entities", "colca/v1/_Signal/n/kept", 1)
		if _, err := s.EvictRecords("entities", 1, 100, func(string) bool { return false }); err != nil {
			t.Fatalf("eviction decoded a record it keeps: %v", err)
		}
	})
	t.Run("compaction", func(t *testing.T) {
		s := mustOpen(t)
		putHeadOnly(t, s, "definitions", "colca/v1/_Group/n/g", 1)
		putHeadOnly(t, s, "definitions", "colca/v1/_Group/n/g", 2)
		st, err := s.Compact("definitions")
		if err != nil {
			t.Fatalf("compaction decoded a record: %v", err)
		}
		if st.Superseded != 1 {
			t.Fatalf("superseded = %d, want 1", st.Superseded)
		}
	})
	t.Run("retention", func(t *testing.T) {
		s := mustOpen(t)
		putHeadOnly(t, s, "metrics", "colca/v1/_Metric/n/a", 1)
		putHeadOnly(t, s, "metrics", "colca/v1/_Metric/n/a", now.UnixMilli())
		target, _, _, err := s.PolicyPruneTarget("metrics", 1, 3, now, time.Second, 0, 0, 3, 0)
		if err != nil || target != 2 {
			t.Fatalf("policy target = %d, %v; want 2", target, err)
		}
		if n, err := s.Prune("metrics", 2, nil, nil); err != nil || n != 1 {
			t.Fatalf("prune = %d, %v; want 1 record", n, err)
		}
	})
	t.Run("seek", func(t *testing.T) {
		s := mustOpen(t)
		for ts := int64(1); ts <= 9; ts++ {
			putHeadOnly(t, s, "logs", "colca/v1/_Log/n/svc/INFO", ts)
		}
		if off, err := s.SeekTS("logs", 5); err != nil || off != 5 {
			t.Fatalf("SeekTS = %d, %v; want 5", off, err)
		}
	})
	t.Run("filtered walk", func(t *testing.T) {
		s := mustOpen(t)
		putHeadOnly(t, s, "logs", "colca/v1/_Log/n/other/INFO", 1)
		if _, _, err := s.Append("logs", []Record{{Topic: "colca/v1/_Log/n/svc/INFO", Payload: []byte(`{}`), TS: 2}}); err != nil {
			t.Fatal(err)
		}
		var got []uint64
		err := s.EachRecord("logs", 1, 3, func(_ uint64, topic string, _ int64) (bool, bool) {
			return topic == "colca/v1/_Log/n/svc/INFO", true
		}, func(r StoredRecord) bool {
			got = append(got, r.Offset)
			return true
		})
		if err != nil || len(got) != 1 || got[0] != 2 {
			t.Fatalf("walk = %v, %v; want only record 2, the other never decoded", got, err)
		}
	})
}

// A record appended while compaction scans lies past the scan: the pass goes
// on and removes what it found superseded, instead of giving up.
func TestCompactionIsNotAbortedByAnAppendDuringItsScan(t *testing.T) {
	s := mustOpen(t)
	def := func(v string) Record {
		return Record{Topic: "colca/v1/_Group/n/g", Payload: []byte(`{"v":"` + v + `"}`), TS: 1}
	}
	if _, _, err := s.Append("definitions", []Record{def("1"), def("2")}); err != nil {
		t.Fatal(err)
	}
	s.compactScanned = func() {
		if _, _, err := s.Append("definitions", []Record{def("3")}); err != nil {
			t.Error(err)
		}
	}
	st, err := s.Compact("definitions")
	if err != nil {
		t.Fatalf("compaction gave up on an append during its scan: %v", err)
	}
	if st.Superseded != 1 {
		t.Fatalf("superseded = %d, want 1", st.Superseded)
	}
	recs, _, err := s.ReadRecords("definitions", 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].Offset != 2 || recs[1].Offset != 3 {
		t.Fatalf("left %+v, want records 2 and 3", recs)
	}
}

// BenchmarkStreamPasses is each whole-stream pass over 200k records with
// ~200-byte payloads: the retention policy scan, an eviction pass that keeps
// everything, a compaction pass over records it keeps, and a filtered walk
// that returns nothing.
//
//	go test ./internal/store -run '^$' -bench StreamPasses
func BenchmarkStreamPasses(b *testing.B) {
	const n = 200_000
	s, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	payload := []byte(`{"signal_id":"01HZZSIGNAL0000000000000000","value":12.5,"quality":"good","unit":"degC","note":"` + strings.Repeat("x", 120) + `"}`)
	for _, stream := range []string{"metrics", "entities", "definitions"} {
		recs := make([]Record, 0, 10_000)
		for i := 0; i < n; i++ {
			recs = append(recs, Record{Topic: "colca/v1/_Metric/n/line/s" + strconv.Itoa(i), Payload: payload, TS: int64(i) + 1})
			if len(recs) == cap(recs) {
				if _, _, err := s.Append(stream, recs); err != nil {
					b.Fatal(err)
				}
				recs = recs[:0]
			}
		}
	}
	next := s.NextOffset("metrics")
	b.Run("policy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, _, _, err := s.PolicyPruneTarget("metrics", 1, next, time.UnixMilli(n+10), time.Millisecond, 0, 0, next, 0); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("evict", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := s.EvictRecords("entities", 1, n, func(string) bool { return false }); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("compact", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := s.Compact("definitions"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("walk", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := s.EachRecord("metrics", 1, next, func(uint64, string, int64) (bool, bool) { return false, true }, func(StoredRecord) bool { return true }); err != nil {
				b.Fatal(err)
			}
		}
	})
}
