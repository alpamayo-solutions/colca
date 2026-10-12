package store

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

func TestKVVisitPreservesSnapshotWhileVisitorWrites(t *testing.T) {
	s := mustOpen(t)
	if _, _, err := s.Append("entities", []Record{kvRec("_Resource", "a", `{"v":1}`), kvRec("_Resource", "c", `{"v":1}`)}); err != nil {
		t.Fatal(err)
	}
	var seen []string
	err := s.KVVisit("", func(e KVEntry) {
		seen = append(seen, e.Path+":"+string(e.Payload))
		if e.Path == "a" {
			if _, _, err := s.Append("entities", []Record{kvRec("_Resource", "b", `{"v":2}`), kvRec("_Resource", "c", `{"v":2}`)}); err != nil {
				t.Fatal(err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(seen, ","); got != `a:{"v":1},c:{"v":1}` {
		t.Fatalf("visit lost its initial snapshot: %s", got)
	}
}

func TestKVVisitDoesNotRetainFullDecodedProjection(t *testing.T) {
	s := mustOpen(t)
	payload := `{"value":"` + strings.Repeat("v", 32<<10) + `"}`
	for base := 0; base < 1024; base += 32 {
		var recs []Record
		for i := base; i < base+32; i++ {
			recs = append(recs, kvRec("_Metric", fmt.Sprintf("bulk/%05d", i), payload))
		}
		if _, _, err := s.Append("metrics", recs); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	var before, now runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak uint64
	count := 0
	err := s.KVVisit("", func(KVEntry) {
		count++
		if count%128 == 0 {
			runtime.GC()
			runtime.ReadMemStats(&now)
			if now.HeapAlloc > before.HeapAlloc && now.HeapAlloc-before.HeapAlloc > peak {
				peak = now.HeapAlloc - before.HeapAlloc
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1024 {
		t.Fatalf("visited %d entries, want 1024", count)
	}
	t.Logf("extra live heap while visiting 32 MiB projection: %d", peak)
	if peak > 12<<20 {
		t.Fatalf("visit retained %d extra live bytes; budget 12 MiB", peak)
	}
}
