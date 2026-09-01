package historian

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/alpamayo-solutions/colca/door"
)

type fakeDoor struct {
	pages []door.Page
	acked []int64
	fetch []string
}

func (f *fakeDoor) Fetch(_ context.Context, stream, cursor string, _ int) (door.Page, error) {
	f.fetch = append(f.fetch, cursor)
	if len(f.pages) == 0 {
		return door.Page{}, nil
	}
	page := f.pages[0]
	f.pages = f.pages[1:]
	return page, nil
}

func (f *fakeDoor) Ack(_ context.Context, _, _ string, offset int64) (bool, error) {
	f.acked = append(f.acked, offset)
	return true, nil
}

type fakeStore struct {
	applied int64
	batches [][]Row
	fail    error
}

func (s *fakeStore) Applied(context.Context, string) (int64, error) { return s.applied, nil }

func (s *fakeStore) Apply(_ context.Context, rows []Row, _ string, offset int64) error {
	if s.fail != nil {
		return s.fail
	}
	s.batches = append(s.batches, rows)
	s.applied = offset
	return nil
}

func record(offset int64, payload string) door.Record {
	return door.Record{
		Offset:  offset,
		Topic:   "colca/v1/_Metric/m1/press3/temp",
		Payload: json.RawMessage(payload),
		TS:      1755600000000,
	}
}

func page(next int64, records ...door.Record) door.Page {
	return door.Page{Records: records, Next: next}
}

func TestAPageIsWrittenOnceAndAckedAfterwards(t *testing.T) {
	d := &fakeDoor{pages: []door.Page{page(3,
		record(1, `{"signal_id":"s1","value":1}`),
		record(2, `{"signal_id":"s1","value":2}`),
	)}}
	store := &fakeStore{}
	bridge := &Bridge{Door: d, Store: store}

	written, err := bridge.Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if written != 2 {
		t.Fatalf("wrote %d rows, want 2", written)
	}
	if store.applied != 2 {
		t.Fatalf("marker = %d, want 2", store.applied)
	}
	if len(d.acked) != 1 || d.acked[0] != 2 {
		t.Fatalf("acked %v, want [2] — and only after the write committed", d.acked)
	}
}

func TestAFailedWriteAcksNothing(t *testing.T) {
	// The records must come back on the next pass. Acking first loses them,
	// and metrics retention means they would be gone for good.
	d := &fakeDoor{pages: []door.Page{page(2, record(1, `{"signal_id":"s1","value":1}`))}}
	store := &fakeStore{fail: errors.New("timescale is down")}
	bridge := &Bridge{Door: d, Store: store}

	if _, err := bridge.Once(context.Background()); err == nil {
		t.Fatal("a failed write reported success")
	}
	if len(d.acked) != 0 {
		t.Fatalf("acked %v after a failed write", d.acked)
	}
	if store.applied != 0 {
		t.Fatalf("marker moved to %d after a failed write", store.applied)
	}
}

func TestRecordsAtOrBelowTheMarkerAreNotRewritten(t *testing.T) {
	// The replay a crash between commit and ack produces. The rows are already
	// durable; writing them again would be harmless in the database (the unique
	// index) but would hide a broken marker.
	d := &fakeDoor{pages: []door.Page{page(4,
		record(1, `{"signal_id":"s1","value":1}`),
		record(2, `{"signal_id":"s1","value":2}`),
		record(3, `{"signal_id":"s1","value":3}`),
	)}}
	store := &fakeStore{applied: 2}
	bridge := &Bridge{Door: d, Store: store}

	written, err := bridge.Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if written != 1 {
		t.Fatalf("wrote %d rows, want 1 (only offset 3 was new)", written)
	}
}

// The behaviour that differs from the cache projector, and the reason it does.
func TestAGapIsCountedAndTheBridgeKeepsGoing(t *testing.T) {
	d := &fakeDoor{pages: []door.Page{page(4,
		door.Record{Offset: 1, Topic: "colca/v1/_StreamGap/n1/metrics",
			Payload: json.RawMessage(`{}`), TS: 1},
		record(2, `{"signal_id":"s1","value":7}`),
	)}}
	store := &fakeStore{}
	bridge := &Bridge{Door: d, Store: store}

	written, err := bridge.Once(context.Background())
	if err != nil {
		t.Fatalf("a gap stopped the bridge: %v", err)
	}
	if bridge.Gaps() != 1 {
		t.Fatalf("Gaps = %d, want 1 — a pruned range is an incident and must be visible", bridge.Gaps())
	}
	if written != 1 {
		t.Fatalf("wrote %d rows, want 1: the records after the gap still historise", written)
	}
}

func TestATombstoneWritesNoRowButStillMovesTheMarker(t *testing.T) {
	// Otherwise a page of nothing but tombstones is fetched forever.
	d := &fakeDoor{pages: []door.Page{page(2, record(1, `{"signal_id":"s1","deleted":true}`))}}
	store := &fakeStore{}
	bridge := &Bridge{Door: d, Store: store}

	written, err := bridge.Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if written != 0 {
		t.Fatalf("wrote %d rows for a tombstone", written)
	}
	if store.applied != 1 {
		t.Fatalf("marker = %d, want 1", store.applied)
	}
}

func TestAnUnhistorisableRecordDoesNotWedgeTheStream(t *testing.T) {
	// One malformed record among good ones: the good ones must land, and the
	// marker must pass the bad one, or the bridge re-reads it forever.
	d := &fakeDoor{pages: []door.Page{page(3,
		record(1, `{"value":1}`), // no signal_id
		record(2, `{"signal_id":"s1","value":2}`),
	)}}
	store := &fakeStore{}
	bridge := &Bridge{Door: d, Store: store}

	written, err := bridge.Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if written != 1 {
		t.Fatalf("wrote %d rows, want 1", written)
	}
	if store.applied != 2 {
		t.Fatalf("marker = %d, want 2", store.applied)
	}
}

func TestAnEmptyPageDoesNothing(t *testing.T) {
	d := &fakeDoor{pages: []door.Page{page(0)}}
	store := &fakeStore{}
	bridge := &Bridge{Door: d, Store: store}

	written, err := bridge.Once(context.Background())
	if err != nil || written != 0 {
		t.Fatalf("Once on an empty page: wrote %d, err %v", written, err)
	}
	if len(d.acked) != 0 {
		t.Fatalf("acked %v on an empty page", d.acked)
	}
}

func TestTheCursorIsNamespacedByService(t *testing.T) {
	d := &fakeDoor{pages: []door.Page{page(0)}}
	bridge := &Bridge{Door: d, Store: &fakeStore{}}

	if _, err := bridge.Once(context.Background()); err != nil {
		t.Fatalf("Once: %v", err)
	}
	if len(d.fetch) != 1 || d.fetch[0] != Cursor {
		t.Fatalf("fetched with %v, want [%s]", d.fetch, Cursor)
	}
}
