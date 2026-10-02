package historian

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/door"
)

// fakeSignalDoor serves a snapshot and a queue of entities pages, and records
// the order of the calls so the tests can check the position is captured
// before the snapshot.
type fakeSignalDoor struct {
	head    int64
	kv      []door.KVEntry
	pages   []door.Page
	calls   []string
	acked   []int64
	kvErr   error
	onKV    func()
	fetches []door.FetchOptions
}

func (f *fakeSignalDoor) KV(_ context.Context, _ string, contracts ...string) ([]door.KVEntry, error) {
	f.calls = append(f.calls, "kv")
	if len(contracts) != 1 || contracts[0] != "_Signal" {
		return nil, errors.New("the snapshot must be limited to _Signal")
	}
	if f.onKV != nil {
		f.onKV()
	}
	return f.kv, f.kvErr
}

func (f *fakeSignalDoor) FetchWithOptions(_ context.Context, options door.FetchOptions) (door.Page, error) {
	f.fetches = append(f.fetches, options)
	if options.Tail {
		f.calls = append(f.calls, "tail")
		return door.Page{Next: f.head + 1}, nil
	}
	f.calls = append(f.calls, "fetch")
	if len(f.pages) == 0 {
		return door.Page{Next: f.head + 1, From: f.head + 1}, nil
	}
	p := f.pages[0]
	f.pages = f.pages[1:]
	return p, nil
}

func (f *fakeSignalDoor) Ack(_ context.Context, _, _ string, offset int64) (bool, error) {
	f.acked = append(f.acked, offset)
	return true, nil
}

func signalPayload(id string, logged *bool) json.RawMessage {
	body := map[string]any{"id": id, "name": id}
	if logged != nil {
		body["is_logged"] = *logged
	}
	b, _ := json.Marshal(body)
	return b
}

func ptr(b bool) *bool { return &b }

const (
	topicA = "colca/v1/_Signal/n1/press/current"
	topicB = "colca/v1/_Signal/n1/press/temp"
)

func TestASignalIsHistorisedUnlessItsDefinitionSaysOtherwise(t *testing.T) {
	d := &fakeSignalDoor{head: 10, kv: []door.KVEntry{
		{Topic: topicA, Offset: 3, Payload: signalPayload("s-off", ptr(false))},
		{Topic: topicB, Offset: 4, Payload: signalPayload("s-on", ptr(true))},
		{Topic: "colca/v1/_Signal/n1/press/unset", Offset: 5, Payload: signalPayload("s-unset", nil)},
		{Topic: "colca/v1/_Signal/n1/press/broken", Offset: 6, Payload: json.RawMessage(`{"id":`)},
	}}
	s := &Signals{Door: d}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]bool{"s-off": false, "s-on": true, "s-unset": true, "s-unknown": true} {
		if got := s.Logged(id); got != want {
			t.Errorf("Logged(%s) = %v, want %v", id, got, want)
		}
	}
	if s.NotLogged() != 1 {
		t.Fatalf("NotLogged = %d, want 1", s.NotLogged())
	}
}

func TestThePositionIsCapturedBeforeTheSnapshotAndTheCursorMovesToIt(t *testing.T) {
	d := &fakeSignalDoor{head: 10}
	s := &Signals{Door: d}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(d.calls) < 3 || d.calls[0] != "tail" || d.calls[1] != "kv" || d.calls[2] != "fetch" {
		t.Fatalf("calls = %v, want tail, kv, then fetch", d.calls)
	}
	if len(d.acked) == 0 || d.acked[0] != 10 {
		t.Fatalf("acked %v, want the cursor moved to the captured head 10 first", d.acked)
	}
	for _, f := range d.fetches[1:] {
		if len(f.Contracts) != 1 || f.Contracts[0] != "_Signal" || f.Stream != "entities" || f.Cursor != SignalCursor {
			t.Fatalf("drain fetch %+v is not a _Signal read on the entities cursor", f)
		}
	}
}

func TestAChangeOnTheStreamFlipsTheFlagAndAnOlderRecordCannotRollItBack(t *testing.T) {
	d := &fakeSignalDoor{head: 20, kv: []door.KVEntry{
		// The snapshot already holds the change written at offset 15.
		{Topic: topicA, Offset: 15, Payload: signalPayload("s1", ptr(false))},
	}}
	s := &Signals{Door: d}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Logged("s1") {
		t.Fatal("s1 says is_logged false and is still historised")
	}

	// A record the snapshot already reflects arrives from the stream: ignored.
	d.pages = []door.Page{{From: 14, Next: 15, Records: []door.Record{
		{Offset: 14, Topic: topicA, Payload: signalPayload("s1", ptr(true))},
	}}}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Logged("s1") {
		t.Fatal("an older record rolled the flag back")
	}

	// The operator turns history back on.
	d.pages = []door.Page{{From: 21, Next: 22, Records: []door.Record{
		{Offset: 21, Topic: topicA, Payload: signalPayload("s1", ptr(true))},
	}}}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !s.Logged("s1") {
		t.Fatal("turning is_logged back on did not take effect")
	}
	if s.NotLogged() != 0 {
		t.Fatalf("NotLogged = %d, want 0", s.NotLogged())
	}
	if last := d.acked[len(d.acked)-1]; last != 21 {
		t.Fatalf("last ack %d, want 21", last)
	}
}

func TestARetiredSignalIsHistorisedAgainAndAMoveKeepsTheNewestFlag(t *testing.T) {
	d := &fakeSignalDoor{head: 5, kv: []door.KVEntry{
		{Topic: topicA, Offset: 5, Payload: signalPayload("s1", ptr(false))},
	}}
	s := &Signals{Door: d}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Moved: written at the new topic, then retired at the old one.
	d.pages = []door.Page{{From: 6, Next: 8, Records: []door.Record{
		{Offset: 6, Topic: topicB, Payload: signalPayload("s1", ptr(false))},
		{Offset: 7, Topic: topicA, Payload: nil},
	}}}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Logged("s1") {
		t.Fatal("retiring the old topic of a moved signal dropped its flag")
	}
	// Retired for good.
	d.pages = []door.Page{{From: 8, Next: 9, Records: []door.Record{
		{Offset: 8, Topic: topicB, Payload: nil},
	}}}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !s.Logged("s1") {
		t.Fatal("a retired signal keeps its is_logged false")
	}
}

func TestAnEmptyFilteredPageThatMovedIsNotTheEnd(t *testing.T) {
	d := &fakeSignalDoor{head: 1}
	s := &Signals{Door: d}
	d.pages = []door.Page{
		// Nothing but other contracts: next moved from 2 to 500.
		{From: 2, Next: 500},
		{From: 500, Next: 502, Records: []door.Record{
			{Offset: 501, Topic: topicA, Payload: signalPayload("s1", ptr(false))},
		}},
	}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Logged("s1") {
		t.Fatal("the drain stopped at an empty filtered page that still moved")
	}
}

func TestAGapReloadsTheDefinitions(t *testing.T) {
	d := &fakeSignalDoor{head: 3}
	s := &Signals{Door: d}
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	d.pages = []door.Page{{From: 50, Next: 50, Gap: &door.Gap{}}}
	d.kv = []door.KVEntry{{Topic: topicA, Offset: 40, Payload: signalPayload("s1", ptr(false))}}
	d.head = 60
	if err := s.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Logged("s1") {
		t.Fatal("a gap did not reload the snapshot")
	}
}

func TestTheBridgeWaitsForTheDefinitions(t *testing.T) {
	d := &fakeSignalDoor{head: 1, kvErr: errors.New("node not ready")}
	s := &Signals{Door: d}
	if err := s.Sync(context.Background()); err == nil {
		t.Fatal("a failed snapshot reported success")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	bridge := &Bridge{Door: &fakeDoor{}, Store: &fakeStore{}, Signals: s}
	if _, err := bridge.Once(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Once = %v, want it to wait for the definitions", err)
	}
}

func TestSamplesOfASignalThatIsNotLoggedAreConsumedWithoutARow(t *testing.T) {
	d := &fakeSignalDoor{head: 1, kv: []door.KVEntry{
		{Topic: topicA, Offset: 1, Payload: signalPayload("s-off", ptr(false))},
	}}
	signals := &Signals{Door: d}
	if err := signals.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	metrics := &fakeDoor{pages: []door.Page{page(5,
		record(2, `{"signal_id":"s-off","value":1}`),
		record(3, `{"signal_id":"s-on","value":2}`),
		record(4, `{"signal_id":"s-off","value":null}`),
	)}}
	store := &fakeStore{}
	bridge := &Bridge{Door: metrics, Store: store, Signals: signals}

	written, err := bridge.Once(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if written != 1 || len(store.batches) != 1 || len(store.batches[0]) != 1 || store.batches[0][0].SignalID != "s-on" {
		t.Fatalf("wrote %d rows %+v, want only s-on", written, store.batches)
	}
	if store.applied != 4 || len(metrics.acked) != 1 || metrics.acked[0] != 4 {
		t.Fatalf("marker %d acked %v, want both at 4: skipped samples are consumed", store.applied, metrics.acked)
	}
	if bridge.NotLogged() != 2 {
		t.Fatalf("NotLogged = %d, want 2", bridge.NotLogged())
	}
}

func TestAPageOfOnlyUnloggedSamplesStillMovesTheCursor(t *testing.T) {
	d := &fakeSignalDoor{head: 1, kv: []door.KVEntry{
		{Topic: topicA, Offset: 1, Payload: signalPayload("s-off", ptr(false))},
	}}
	signals := &Signals{Door: d}
	if err := signals.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	metrics := &fakeDoor{pages: []door.Page{page(4,
		record(2, `{"signal_id":"s-off","value":1}`),
		record(3, `{"signal_id":"s-off","value":2}`),
	)}}
	store := &fakeStore{}
	bridge := &Bridge{Door: metrics, Store: store, Signals: signals}
	if _, err := bridge.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.applied != 3 || len(metrics.acked) != 1 || metrics.acked[0] != 3 {
		t.Fatalf("marker %d acked %v, want 3: the page would be fetched forever", store.applied, metrics.acked)
	}
}
