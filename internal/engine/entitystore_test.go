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
