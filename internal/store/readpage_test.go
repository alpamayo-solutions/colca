package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// A record page stops before the record that would take it past its byte
// bound, and next resumes at that record: paging through returns every
// matching record once and in order, through the signal index and without it.
func TestRecordPagesStopAtTheirByteBoundWithoutSkippingARecord(t *testing.T) {
	s := mustOpen(t)
	for i := range 12 {
		rec := metricRecord("s", int64(i+1))
		rec.Payload = fmt.Appendf(nil, `{"signal_id":"s","value":"%s"}`, strings.Repeat("x", 700))
		if _, _, err := s.Append("metrics", []Record{rec}); err != nil {
			t.Fatal(err)
		}
	}
	even := func(r StoredRecord) bool { return r.Offset%2 == 0 }
	reads := map[string]func(from uint64, maxBytes uint64) ([]StoredRecord, uint64, error){
		"stream": func(from, maxBytes uint64) ([]StoredRecord, uint64, error) {
			return s.ReadRecordsBounded(context.Background(), "metrics", from, 100, 0, maxBytes, even)
		},
		"signal index": func(from, maxBytes uint64) ([]StoredRecord, uint64, error) {
			return s.ReadSignals(context.Background(), "metrics", from, 100, 0, maxBytes, []string{"s"}, even)
		},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			// Each record is some 800 stored bytes: two fit in 2200, a third does not.
			var got []uint64
			for from := uint64(1); from < s.NextOffset("metrics"); {
				page, next, err := read(from, 2200)
				if err != nil {
					t.Fatal(err)
				}
				if len(page) > 2 || next <= from {
					t.Fatalf("page from %d: %d records, next %d", from, len(page), next)
				}
				for _, r := range page {
					got = append(got, r.Offset)
				}
				from = next
			}
			if fmt.Sprint(got) != "[2 4 6 8 10 12]" {
				t.Fatalf("paged through %v, want every even offset once", got)
			}
			// A record larger than the whole bound still makes a page of its own.
			page, next, err := read(1, 100)
			if err != nil || len(page) != 1 || page[0].Offset != 2 || next != 4 {
				t.Fatalf("bound below one record: %d records, next %d, err %v; want offset 2 alone, next at 4 (3 is filtered out)", len(page), next, err)
			}
		})
	}
}

// A KV page stops in the same way, and its token resumes at the entry it left
// out: on the plain walk, the contract index and the level walk.
func TestKVPagesStopAtTheirByteBoundWithoutSkippingAnEntry(t *testing.T) {
	s := mustOpen(t)
	payload := `{"value":"` + strings.Repeat("x", 700) + `"}`
	for i := range 9 {
		if _, _, err := s.Append("entities", []Record{kvRec("_Resource", fmt.Sprintf("line/%02d", i), payload)}); err != nil {
			t.Fatal(err)
		}
	}
	pages := map[string]func(after string, maxBytes uint64) ([]KVEntry, string, error){
		"walk": func(after string, maxBytes uint64) ([]KVEntry, string, error) {
			return s.KVScanPageDepth("line/", after, 100, maxBytes, nil, 0)
		},
		"contract index": func(after string, maxBytes uint64) ([]KVEntry, string, error) {
			return s.KVScanPageDepth("line/", after, 100, maxBytes, []string{"_Resource"}, 0)
		},
		"level": func(after string, maxBytes uint64) ([]KVEntry, string, error) {
			entries, _, next, err := s.KVScanLevel("line/", after, 100, maxBytes, nil, 1)
			return entries, next, err
		},
	}
	for name, page := range pages {
		t.Run(name, func(t *testing.T) {
			var got []string
			after := ""
			for range 20 {
				entries, next, err := page(after, 2200)
				if err != nil {
					t.Fatal(err)
				}
				if len(entries) == 0 || len(entries) > 2 {
					t.Fatalf("page after %q: %d entries", after, len(entries))
				}
				for _, e := range entries {
					got = append(got, e.Path)
				}
				if next == "" {
					break
				}
				after = next
			}
			if len(got) != 9 {
				t.Fatalf("paged through %v, want line/00..08 once each", got)
			}
			for i, path := range got {
				if path != fmt.Sprintf("line/%02d", i) {
					t.Fatalf("paged through %v, want line/00..08 in order", got)
				}
			}
			if entries, _, err := page("", 100); err != nil || len(entries) != 1 {
				t.Fatalf("bound below one entry: %d entries, err %v; want one", len(entries), err)
			}
		})
	}
}
