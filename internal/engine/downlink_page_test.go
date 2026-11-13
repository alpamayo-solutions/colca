package engine

import (
	"errors"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// pageEngine is an engine whose local delivery records, per command delivered,
// how many commands its stream held at that moment.
func pageEngine(t *testing.T, ids fakeIDs, maxRecordBytes ...int) (*Engine, *[]int) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if len(maxRecordBytes) > 0 {
		s.SetMaxRecordBytes(uint64(maxRecordBytes[0]))
	}
	var storedAtDelivery []int
	deliver := func(string, []byte, bool) {
		storedAtDelivery = append(storedAtDelivery, int(s.NextOffset("commands"))-1)
	}
	return New(s, &config.Config{ULID: "n-edge1"}, ids, deliver, nil, nil), &storedAtDelivery
}

func page(corrs ...string) []DownlinkCommand {
	cmds := make([]DownlinkCommand, len(corrs))
	for i, c := range corrs {
		cmds[i] = DownlinkCommand{Topic: "colca/v1/_CmdParam/m1/temp/" + c, Payload: cmdPayload(c), TS: int64(i + 1)}
	}
	return cmds
}

// A page is one write: when its first command is delivered locally, all of the
// page is in the stream, and the downlink cursor stands past it. Handed again
// (a lost response, a crash after the write) it is stored and delivered no
// second time.
func TestADownlinkPageIsStoredInOneWrite(t *testing.T) {
	e, storedAtDelivery := pageEngine(t, testIDs())
	at := store.CursorAdvance{Name: "downlink:parent", Stream: "commands-parent", To: 12}

	results, err := e.IngestDownlinkPage(page("propose", "withdraw", "next"), at)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range results {
		if !r.Persisted || r.Offset != uint64(i+1) || r.Err != nil {
			t.Fatalf("result %d = %+v, want persisted at offset %d", i, r, i+1)
		}
	}
	if got := *storedAtDelivery; len(got) != 3 || got[0] != 3 || got[1] != 3 || got[2] != 3 {
		t.Fatalf("commands in the stream at each of the deliveries = %v, want [3 3 3]: the page is stored before any of it is delivered", got)
	}
	if got := e.Store().CursorGet("downlink:parent", "commands-parent"); got != 12 {
		t.Fatalf("downlink cursor = %d, want 12: it moves with the page", got)
	}

	results, err = e.IngestDownlinkPage(page("propose", "withdraw", "next"), at)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range results {
		if !r.Duplicate || r.Persisted {
			t.Fatalf("second delivery, result %d = %+v, want a duplicate", i, r)
		}
	}
	if got := e.Store().NextOffset("commands"); got != 4 {
		t.Fatalf("commands stream head = %d after the page was handed twice, want 4", got)
	}
	if len(*storedAtDelivery) != 3 {
		t.Fatalf("%d local deliveries after the page was handed twice, want 3", len(*storedAtDelivery))
	}
}

// A command this node refuses is left out of the write and reported; the rest
// of the page is stored and the cursor still moves past all of it.
func TestADownlinkPageLeavesOutWhatThisNodeRefuses(t *testing.T) {
	ids := testIDs()
	ids.draining = []string{"hmi"}
	e, _ := pageEngine(t, ids)
	cmds := page("a", "b")
	cmds = append(cmds[:1], append([]DownlinkCommand{
		{Topic: "colca/v1/_CmdParam/m1/hmi/panel", Payload: cmdPayload("draining"), TS: 5},
		{Topic: "not a topic", Payload: cmdPayload("grammar"), TS: 6},
	}, cmds[1:]...)...)

	results, err := e.IngestDownlinkPage(cmds, store.CursorAdvance{Name: "downlink:parent", Stream: "commands-parent", To: 9})
	if err != nil {
		t.Fatal(err)
	}
	var refused *RejectError
	for _, i := range []int{1, 2} {
		if results[i].Persisted || !errors.As(results[i].Err, &refused) {
			t.Fatalf("result %d = %+v, want a refusal", i, results[i])
		}
	}
	if !results[0].Persisted || results[0].Offset != 1 || !results[3].Persisted || results[3].Offset != 2 {
		t.Fatalf("results = %+v, want the first and the last stored at offsets 1 and 2", results)
	}
	if got := e.Store().CursorGet("downlink:parent", "commands-parent"); got != 9 {
		t.Fatalf("downlink cursor = %d, want 9", got)
	}
}

// What cannot be one write is not written at all, so the caller can store it
// command by command: a record that is not a command, and a store failure such
// as a command over max_record_bytes.
func TestADownlinkPageThatCannotBeOneWriteWritesNothing(t *testing.T) {
	e, storedAtDelivery := pageEngine(t, testIDs(), 256)
	at := store.CursorAdvance{Name: "downlink:parent", Stream: "commands-parent", To: 5}

	mixed := append(page("a"), DownlinkCommand{Topic: "colca/v1/_Metric/m1/temp", Payload: []byte(`{"v":1}`), TS: 2})
	if _, err := e.IngestDownlinkPage(mixed, at); !errors.Is(err, ErrNotOnePage) {
		t.Fatalf("a page with a metric: err = %v, want ErrNotOnePage", err)
	}

	big := page("a", "b")
	big[1].Payload = append(append([]byte(`{"correlation_id":"big","expires_at":99999999999999,"pad":"`), make([]byte, 400)...), []byte(`"}`)...)
	for i := range big[1].Payload {
		if big[1].Payload[i] == 0 {
			big[1].Payload[i] = 'x'
		}
	}
	if _, err := e.IngestDownlinkPage(big, at); !errors.Is(err, store.ErrRecordTooLarge) {
		t.Fatalf("a page with a command over max_record_bytes: err = %v, want ErrRecordTooLarge", err)
	}

	if got := e.Store().NextOffset("commands"); got != 1 {
		t.Fatalf("commands stream head = %d, want 1: nothing of either page was written", got)
	}
	if got := e.Store().CursorGet("downlink:parent", "commands-parent"); got != 1 {
		t.Fatalf("downlink cursor = %d, want 1: it does not move without the page", got)
	}
	if len(*storedAtDelivery) != 0 {
		t.Fatalf("%d local deliveries of pages that were not written, want 0", len(*storedAtDelivery))
	}
}

// A person's subscription published at the hub reaches the evaluating child
// with the address their token vouched for: the child's notifications service
// reads it there as actor_email. State a command leads to never keeps it.
func TestADownlinkedCommandKeepsThePersonsVerifiedEmail(t *testing.T) {
	e, _ := pageEngine(t, testIDs())
	topic := "colca/v1/_CmdOperate/m1/m1/01HALARM0000000000000000AA/subscribe"
	anna := Attribution{WrittenBy: "n-hub", ActorID: "kc-anna", ActorLabel: "Anna", ActorKind: "human",
		ActorGroups: []string{"operators"}, ActorEmail: "anna@plant.test", ActorClasses: []string{"admin", "read"}}
	results, err := e.IngestDownlinkPage([]DownlinkCommand{{Topic: topic, Payload: cmdPayload("sub"), TS: 1, Attribution: anna}},
		store.CursorAdvance{Name: "downlink:parent", Stream: "commands-parent", To: 7})
	if err != nil || len(results) != 1 || results[0].Err != nil {
		t.Fatalf("downlink page: %v %+v", err, results)
	}
	recs, _, err := e.Store().Read("commands", 1, 10, func(tp string) bool { return tp == topic })
	if err != nil || len(recs) != 1 {
		t.Fatalf("stored command: %v %+v (results %+v)", err, recs, results)
	}
	if recs[0].ActorEmail != "anna@plant.test" {
		t.Fatalf("downlinked command's actor email = %q, want the person's verified address", recs[0].ActorEmail)
	}
	// The classes the hub witnessed, not recomputed here: this node's fixture
	// grants Anna nothing.
	if got := recs[0].ActorClasses; len(got) != 2 || got[0] != "admin" || got[1] != "read" {
		t.Fatalf("downlinked command's actor classes = %v, want the hub's [admin read]", got)
	}

	state := "colca/v1/_SystemElement/n-edge1/line9"
	if _, err := e.IngestAdminAttributed(state, []byte(`{"id":"el-line9","name":"line9"}`), anna); err != nil {
		t.Fatal(err)
	}
	stateRecs, _, err := e.Store().Read("entities", 1, 100, func(tp string) bool { return tp == state })
	if err != nil || len(stateRecs) != 1 {
		t.Fatalf("stored state: %v %+v", err, stateRecs)
	}
	if stateRecs[0].ActorID != "kc-anna" || stateRecs[0].ActorEmail != "" || len(stateRecs[0].ActorClasses) != 0 {
		t.Fatalf("state record attribution = %+v, want the person without their address", stateRecs[0])
	}
}
