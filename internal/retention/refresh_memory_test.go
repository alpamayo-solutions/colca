package retention

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func TestRefreshDoesNotKeepUnrelatedKVSnapshotLive(t *testing.T) {
	st, eng := mustParts(t)
	payload := []byte(`{"value":"` + strings.Repeat("v", 32<<10) + `"}`)
	for base := 0; base < 2048; base += 64 {
		var records []store.Record
		for i := base; i < base+64; i++ {
			path := fmt.Sprintf("bulk/%05d", i)
			records = append(records, store.Record{Topic: "colca/v1/_Metric/" + nodeULID + "/" + path, Payload: payload, KVPath: path, KVNode: nodeULID})
		}
		if _, _, err := st.Append("metrics", records); err != nil {
			t.Fatal(err)
		}
	}
	var records []store.Record
	for i := 0; i < 300; i++ {
		path := fmt.Sprintf("wanted/%04d", i)
		records = append(records, store.Record{Topic: "colca/v1/_Signal/" + nodeULID + "/" + path, Payload: []byte(`{"id":"test"}`), KVPath: path, KVNode: nodeULID})
	}
	if _, _, err := st.Append("entities", records); err != nil {
		t.Fatal(err)
	}
	p := newPruner(t, st, eng, config.Retention{})
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	calls := 0
	p.publish = func(topic string, payload []byte, off uint64) (bool, error) {
		if calls == 0 {
			runtime.GC()
			var live runtime.MemStats
			runtime.ReadMemStats(&live)
			t.Logf("refresh heap baseline=%d live=%d", baseline.HeapAlloc, live.HeapAlloc)
			if live.HeapAlloc > baseline.HeapAlloc+16<<20 {
				t.Fatalf("refresh retained %d extra heap bytes beside 64 MiB unrelated KV; budget 16 MiB", live.HeapAlloc-baseline.HeapAlloc)
			}
		}
		calls++
		return true, nil
	}
	if !p.refreshEntities(1, 301) {
		t.Fatal("refresh failed")
	}
	if calls != 300 {
		t.Fatalf("publish calls=%d, want 300", calls)
	}
}
