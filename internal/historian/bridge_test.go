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

func (s *fakeStore) Apply(_ context.Context, rows []Row, _ string, offset int64) ([]Rejection, error) {
	if s.fail != nil {
		return nil, s.fail
	}
	s.batches = append(s.batches, rows)
	s.applied = offset
	return nil, nil
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
	// A replay after a crash between commit and ack. The page starts at offset 2 on
	// purpose: a page starting at 1 is treated as a possibly recreated stream and
	// re-applied (see the next test).
	d := &fakeDoor{pages: []door.Page{page(5,
		record(2, `{"signal_id":"s1","value":1}`),
		record(3, `{"signal_id":"s1","value":2}`),
		record(4, `{"signal_id":"s1","value":3}`),
	)}}
	store := &fakeStore{applied: 3}
	bridge := &Bridge{Door: d, Store: store}

	written, err := bridge.Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if written != 1 {
		t.Fatalf("wrote %d rows, want 1 (only offset 4 was new)", written)
	}
}

// Recreating colcad's data volume restarts offsets at 1 while Timescale keeps
// the marker, and the bridge must not treat the new records as already applied.
// Both cases are covered: a marker past the page's end, and a small marker
// inside the new page's range. The last subtest shows a legitimate marker still
// skips records.
func TestAMarkerFromABeforeTheVolumeWasRecreatedNeverDropsAPage(t *testing.T) {
	newPage := func() door.Page {
		return page(4,
			record(1, `{"signal_id":"s1","value":1}`),
			record(2, `{"signal_id":"s1","value":2}`),
			record(3, `{"signal_id":"s1","value":3}`),
		)
	}

	// The recreated stream after retention pruned its first records: only the
	// marker-past-the-page rule can see it.
	prunedPage := func() door.Page {
		return page(10,
			record(7, `{"signal_id":"s1","value":1}`),
			record(8, `{"signal_id":"s1","value":2}`),
			record(9, `{"signal_id":"s1","value":3}`),
		)
	}

	for _, tc := range []struct {
		name   string
		page   func() door.Page
		marker int64
		last   int64
	}{
		{"a marker far beyond the recreated stream", newPage, 100000, 3},
		{"a marker that lands inside the recreated page", newPage, 2, 3},
		{"a recreated stream whose first offsets were already pruned", prunedPage, 100000, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &fakeDoor{pages: []door.Page{tc.page()}}
			store := &fakeStore{applied: tc.marker}
			bridge := &Bridge{Door: d, Store: store}

			written, err := bridge.Once(context.Background())
			if err != nil {
				t.Fatalf("Once: %v", err)
			}
			if written != 3 {
				t.Fatalf("wrote %d rows, want 3 — a marker counting a stream that no longer "+
					"exists dropped live metrics as already durable", written)
			}
			if store.applied != tc.last {
				t.Fatalf("marker = %d, want %d (this stream's position)", store.applied, tc.last)
			}
		})
	}

	// With the marker at 0 the same page writes the same rows, so the cases above
	// are about a non-zero marker.
	t.Run("a stream nobody has followed yet writes the same page", func(t *testing.T) {
		d := &fakeDoor{pages: []door.Page{newPage()}}
		store := &fakeStore{}
		bridge := &Bridge{Door: d, Store: store}
		written, err := bridge.Once(context.Background())
		if err != nil || written != 3 {
			t.Fatalf("wrote %d rows (err %v), want 3", written, err)
		}
	})
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

func TestCoordinatedHistoryDoesNotAckMalformedRowsOrGaps(t *testing.T) {
	for _, p := range []door.Page{
		page(2, record(1, `{broken`)),
		{Gap: &door.Gap{FromOffset: 1, ToOffset: 10}},
	} {
		d := &fakeDoor{pages: []door.Page{p}}
		s := &fakeStore{}
		b := &Bridge{Door: d, Store: s, Strict: true}
		if _, err := b.Once(context.Background()); err == nil {
			t.Fatal("incomplete coordinated history accepted")
		}
		if len(d.acked) != 0 || s.applied != 0 || b.Drained {
			t.Fatal("failed page acknowledged as complete")
		}
	}
}

func TestRunReportsEachPassToHealth(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail error
		ok   bool
	}{
		{"a pass that applies is healthy", nil, true},
		{"a pass the store refuses is unhealthy with the reason", errors.New("database unreachable"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var gotOK bool
			var gotDetail string
			b := &Bridge{
				Door:  &fakeDoor{pages: []door.Page{page(2, record(1, `{"value":1}`))}},
				Store: &fakeStore{fail: tc.fail},
				Health: func(ok bool, detail string) {
					gotOK, gotDetail = ok, detail
					cancel()
				},
			}
			_ = b.Run(ctx)
			if gotOK != tc.ok {
				t.Fatalf("health ok = %v, want %v", gotOK, tc.ok)
			}
			if !tc.ok && gotDetail != tc.fail.Error() {
				t.Fatalf("health detail = %q, want %q", gotDetail, tc.fail.Error())
			}
		})
	}
}

func TestANullValueReachesTheStoreAsARetraction(t *testing.T) {
	// The value went missing between two readings. The sink decides whether the
	// retraction is new; the bridge must hand it over, or history draws a line
	// across the gap.
	d := &fakeDoor{pages: []door.Page{page(4,
		record(1, `{"signal_id":"s1","value":1}`),
		record(2, `{"signal_id":"s1","value":null}`),
		record(3, `{"signal_id":"s1","value":3}`),
	)}}
	store := &fakeStore{}
	bridge := &Bridge{Door: d, Store: store}

	if _, err := bridge.Once(context.Background()); err != nil {
		t.Fatalf("Once: %v", err)
	}
	if len(store.batches) != 1 || len(store.batches[0]) != 3 {
		t.Fatalf("batches = %+v, want one batch of 3 rows", store.batches)
	}
	if got := store.batches[0]; got[0].Missing() || !got[1].Missing() || got[2].Missing() {
		t.Fatalf("rows = %+v, want value, retraction, value", got)
	}
}
