package engine

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/store"
)

func TestContractScanDoesNotMaterializeUnrelatedLargeValues(t *testing.T) {
	e := newEngine(t)
	payload := []byte(`{"value":"` + strings.Repeat("v", 32<<10) + `"}`)
	for base := 0; base < 2048; base += 64 {
		records := make([]store.Record, 0, 64)
		for i := base; i < base+64; i++ {
			path := fmt.Sprintf("bulk/%05d", i)
			records = append(records, store.Record{Topic: "colca/v1/_Metric/n-edge1/" + path, Payload: payload, KVPath: path, KVNode: "n-edge1"})
		}
		if _, _, err := e.store.Append("metrics", records); err != nil {
			t.Fatal(err)
		}
	}
	var records []store.Record
	for i := 0; i < 300; i++ {
		path := fmt.Sprintf("wanted/%04d", i)
		records = append(records, store.Record{Topic: "colca/v1/_Resource/n-edge1/" + path, Payload: []byte(`{"id":"test"}`), KVPath: path, KVNode: "n-edge1"})
	}
	if _, _, err := e.store.Append("entities", records); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, err := e.ScanContractAll("_Resource")
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 300 {
		t.Fatalf("records=%d, want 300", len(got))
	}
	for i, record := range got {
		if want := fmt.Sprintf("wanted/%04d", i); record.Path != want {
			t.Fatalf("record %d path=%q, want %q", i, record.Path, want)
		}
	}
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("contract scan allocated %d bytes", allocated)
	if allocated > 24<<20 {
		t.Fatalf("contract scan allocated %d bytes for 300 small matching values alongside 64 MiB unrelated values; budget 24 MiB", allocated)
	}
}

// An entity read is one point lookup: the entries under the topic's path (and
// at paths that merely start with it) are never read, however many and large.
func TestEntityKVGetReadsOnlyTheEntryItAsksFor(t *testing.T) {
	e := newEngine(t)
	payload := []byte(`{"value":"` + strings.Repeat("v", 32<<10) + `"}`)
	for base := 0; base < 1024; base += 64 {
		records := make([]store.Record, 0, 64)
		for i := base; i < base+64; i++ {
			path := fmt.Sprintf("plant/l1/%05d", i)
			records = append(records, store.Record{Topic: "colca/v1/_Signal/n-edge1/" + path, Payload: payload, KVPath: path, KVNode: "n-edge1"})
		}
		if _, _, err := e.store.Append("entities", records); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := e.store.Append("entities", []store.Record{
		{Topic: "colca/v1/_SystemElement/n-edge1/plant/l1", Payload: []byte(`{"id":"l1"}`), KVPath: "plant/l1", KVNode: "n-edge1"},
		{Topic: "colca/v1/_SystemElement/n-edge1/plant/l10", Payload: []byte(`{"id":"l10"}`), KVPath: "plant/l10", KVNode: "n-edge1"},
		{Topic: "colca/v1/_SystemElement/n-other/plant/l1", Payload: []byte(`{"id":"other"}`), KVPath: "plant/l1", KVNode: "n-other"},
	}); err != nil {
		t.Fatal(err)
	}
	es := e.EntityStore()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, ok := es.KVGet("colca/v1/_SystemElement/n-edge1/plant/l1")
	runtime.ReadMemStats(&after)
	if !ok || string(got) != `{"id":"l1"}` {
		t.Fatalf("KVGet = %s %v, want the l1 element", got, ok)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("one entity read allocated %d bytes beside 32 MiB of entries under its path; budget 1 MiB", allocated)
	}
	if got, ok := es.KVGet("colca/v1/_SystemElement/n-edge1/plant/l10"); !ok || string(got) != `{"id":"l10"}` {
		t.Fatalf("KVGet l10 = %s %v", got, ok)
	}
	if _, ok := es.KVGet("colca/v1/_SystemElement/n-edge1/plant/l2"); ok {
		t.Fatal("KVGet found an entry that does not exist")
	}
}

// BenchmarkEntityKVGet is one entity read on a node holding 20k signals under
// the same element, as configure paths do for each element they touch.
func BenchmarkEntityKVGet(b *testing.B) {
	e := newEngine(b)
	for base := 0; base < 20_000; base += 1000 {
		records := make([]store.Record, 0, 1000)
		for i := base; i < base+1000; i++ {
			path := fmt.Sprintf("plant/l1/%05d", i)
			records = append(records, store.Record{Topic: "colca/v1/_Signal/n-edge1/" + path, Payload: []byte(`{"id":"x"}`), KVPath: path, KVNode: "n-edge1"})
		}
		if _, _, err := e.store.Append("entities", records); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := e.store.Append("entities", []store.Record{
		{Topic: "colca/v1/_SystemElement/n-edge1/plant/l1", Payload: []byte(`{"id":"l1"}`), KVPath: "plant/l1", KVNode: "n-edge1"},
	}); err != nil {
		b.Fatal(err)
	}
	es := e.EntityStore()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := es.KVGet("colca/v1/_SystemElement/n-edge1/plant/l1"); !ok {
			b.Fatal("not found")
		}
	}
}
