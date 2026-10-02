package cursorwatch

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

type owners map[string]*uns.Entry

func (o owners) Get(ulid string) (*uns.Entry, bool) {
	e, ok := o[ulid]
	return e, ok
}

func (o owners) ByName(name string) (*uns.Entry, bool) {
	for _, e := range o {
		if e.Kind == uns.KindLocal && e.Name == name {
			return e, true
		}
	}
	return nil, false
}

type elements map[string]string

func (e elements) PathOf(id string) (string, bool) {
	p, ok := e[id]
	return p, ok
}

type gauges map[string]float64

func (g gauges) CursorUnreadAge(cursor, stream string, seconds float64) {
	g[stream+":"+cursor] = seconds
}
func (g gauges) ForgetCursorUnreadAge(cursor, stream string) { delete(g, stream+":"+cursor) }

type write struct {
	topic   string
	payload []byte
}

type fixture struct {
	st     *store.Store
	w      *Watchdog
	writes []write
	gauges gauges
	now    time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	f := &fixture{st: st, gauges: gauges{}, now: time.UnixMilli(1_000_000_000)}
	f.w = &Watchdog{
		Store:   st,
		Filters: NewFilters(),
		Owners: owners{
			"01LINE": {ULID: "01LINE", Kind: uns.KindLocal, Name: "dataops-line"},
			"01EXT":  {ULID: "01EXT", Kind: uns.KindExternal, Element: "el-1"},
		},
		Elements: elements{"el-1": "plant/line1"},
		Gauges:   f.gauges,
		NodeID:   "NODE",
		After:    time.Minute,
		Publish: func(topic string, payload []byte) error {
			f.writes = append(f.writes, write{topic, payload})
			return nil
		},
	}
	return f
}

// read makes cursor exist, past one record it already read, fetched without
// a filter since the node started.
func (f *fixture) read(t *testing.T, cursor, stream string) {
	t.Helper()
	f.w.Filters.Remember(cursor, stream, nil)
	last := f.append(t, stream, "colca/v1/_Metric/NODE/read")
	if !f.st.CursorAck(cursor, stream, last+1) {
		t.Fatalf("cursor %s did not move", cursor)
	}
}

// append adds one record per topic, written at the fixture's clock.
func (f *fixture) append(t *testing.T, stream string, topics ...string) uint64 {
	t.Helper()
	recs := make([]store.Record, len(topics))
	for i, topic := range topics {
		recs[i] = store.Record{Topic: topic, Payload: []byte(`{}`), TS: f.now.UnixMilli()}
	}
	_, last, err := f.st.Append(stream, recs)
	if err != nil {
		t.Fatal(err)
	}
	return last
}

const lineTopic = "colca/v1/_Finding/NODE/dataops-line/cursor_lag"

func TestUnreadRecordsOlderThanTheThresholdRaiseOneFindingUntilRead(t *testing.T) {
	f := newFixture(t)
	f.read(t, "c/dataops-line/commands", "commands")
	last := f.append(t, "commands", "colca/v1/_CmdSet/NODE/a", "colca/v1/_CmdSet/NODE/b")

	f.w.Check(f.now.Add(30 * time.Second))
	if len(f.writes) != 0 {
		t.Fatalf("finding written before the threshold: %v", f.writes)
	}
	if got := f.gauges["commands:c/dataops-line/commands"]; got != 30 {
		t.Fatalf("unread age = %v, want 30", got)
	}

	f.w.Check(f.now.Add(61 * time.Second))
	f.w.Check(f.now.Add(70 * time.Second))
	if len(f.writes) != 1 || f.writes[0].topic != lineTopic {
		t.Fatalf("want one finding at %s, got %v", lineTopic, f.writes)
	}
	var finding map[string]any
	if err := json.Unmarshal(f.writes[0].payload, &finding); err != nil {
		t.Fatal(err)
	}
	if finding["reason"] != Reason || finding["suggested_severity"] != "warning" {
		t.Fatalf("finding = %v", finding)
	}
	if !strings.Contains(finding["summary"].(string), "dataops-line") {
		t.Fatalf("summary does not name the service: %v", finding["summary"])
	}

	f.st.CursorAck("c/dataops-line/commands", "commands", last+1)
	f.w.Check(f.now.Add(75 * time.Second))
	if len(f.writes) != 2 || f.writes[1].topic != lineTopic || f.writes[1].payload != nil {
		t.Fatalf("want the finding retired, got %v", f.writes)
	}
	if got := f.gauges["commands:c/dataops-line/commands"]; got != 0 {
		t.Fatalf("unread age after catching up = %v, want 0", got)
	}
}

func TestRecordsOutsideTheFetchFilterDoNotCount(t *testing.T) {
	f := newFixture(t)
	f.read(t, "c/dataops-line/commands", "commands")
	f.w.Filters.Remember("c/dataops-line/commands", "commands", func(r store.StoredRecord) bool {
		return uns.MatchFilter("colca/v1/_CmdSet/NODE/mine/#", r.Topic)
	})
	f.append(t, "commands", "colca/v1/_CmdSet/NODE/other/x", "colca/v1/_CmdSet/NODE/other/y")

	f.w.Check(f.now.Add(10 * time.Minute))
	if len(f.writes) != 0 {
		t.Fatalf("records the consumer does not read raised a finding: %v", f.writes)
	}
	if got := f.gauges["commands:c/dataops-line/commands"]; got != 0 {
		t.Fatalf("unread age = %v, want 0", got)
	}

	f.now = f.now.Add(10 * time.Minute)
	f.append(t, "commands", "colca/v1/_CmdSet/NODE/mine/z")
	f.w.Check(f.now.Add(2 * time.Minute))
	if len(f.writes) != 1 {
		t.Fatalf("a record the consumer reads, 2 min unread, raised no finding: %v", f.writes)
	}
	if got := f.gauges["commands:c/dataops-line/commands"]; got != 120 {
		t.Fatalf("unread age = %v, want 120", got)
	}
}

func TestAPlacedExternalServiceGetsTheFindingAtItsMount(t *testing.T) {
	f := newFixture(t)
	f.read(t, "01EXT/ingest", "metrics")
	f.append(t, "metrics", "colca/v1/_Metric/NODE/plant/line1/x")
	f.w.Check(f.now.Add(2 * time.Minute))
	want := "colca/v1/_Finding/NODE/plant/line1/01EXT/cursor_lag"
	if len(f.writes) != 1 || f.writes[0].topic != want {
		t.Fatalf("want a finding at %s, got %v", want, f.writes)
	}
}

func TestACursorWithoutAnOwnerOnlyGetsTheGauge(t *testing.T) {
	f := newFixture(t)
	f.read(t, "repl/uplink", "metrics")
	f.append(t, "metrics", "colca/v1/_Metric/NODE/x")
	f.w.Check(f.now.Add(5 * time.Minute))
	if len(f.writes) != 0 {
		t.Fatalf("finding for a cursor without an owner: %v", f.writes)
	}
	if got := f.gauges["metrics:repl/uplink"]; got != 300 {
		t.Fatalf("unread age = %v, want 300", got)
	}
}

func TestAFindingLeftByTheLastRunIsRetiredOnceCaughtUp(t *testing.T) {
	f := newFixture(t)
	_, _, err := f.st.Append("entities", []store.Record{{
		Topic: lineTopic, Payload: []byte(`{"reason":"cursor_lag"}`), TS: f.now.UnixMilli(),
		WrittenBy: Author, KVPath: "dataops-line/cursor_lag", KVNode: "NODE",
	}})
	if err != nil {
		t.Fatal(err)
	}
	f.w.Check(f.now)
	if len(f.writes) != 1 || f.writes[0].topic != lineTopic || f.writes[0].payload != nil {
		t.Fatalf("want the old finding retired, got %v", f.writes)
	}
}

func TestACursorNobodyFetchedSinceStartOnlyGetsTheGauge(t *testing.T) {
	f := newFixture(t)
	last := f.append(t, "metrics", "colca/v1/_Metric/NODE/read")
	f.st.CursorAck("c/dataops-line/ingest-OLDGEN", "metrics", last+1) // a previous buffer generation
	f.append(t, "metrics", "colca/v1/_Metric/NODE/x")
	f.w.Check(f.now.Add(5 * time.Minute))
	if len(f.writes) != 0 {
		t.Fatalf("an abandoned cursor raised a finding: %v", f.writes)
	}
	if got := f.gauges["metrics:c/dataops-line/ingest-OLDGEN"]; got != 300 {
		t.Fatalf("unread age = %v, want 300", got)
	}
}

func TestIdleWatchdogHasNoReadDeadline(t *testing.T) {
	f := newFixture(t)
	f.read(t, "c/dataops-line/metrics", "metrics")
	f.w.Check(f.now)
	if got := f.w.nextDelay(f.now); got != -1 {
		t.Fatalf("idle deadline=%v", got)
	}
	f.append(t, "metrics", "colca/v1/_Metric/NODE/unread")
	f.w.Check(f.now)
	if got := f.w.nextDelay(f.now); got != time.Minute {
		t.Fatalf("lag deadline=%v", got)
	}
	f.w.Check(f.now.Add(time.Minute))
	if got := f.w.nextDelay(f.now.Add(time.Minute)); got != -1 {
		t.Fatalf("completed deadline repeated=%v", got)
	}
}

func TestWatchdogWakesOnAppendAndAck(t *testing.T) {
	f := newFixture(t)
	f.now = time.Now().Add(-time.Minute)
	f.read(t, "c/dataops-line/metrics", "metrics")
	writes := make(chan write, 8)
	f.w.Gauges = nil
	f.w.Publish = func(topic string, payload []byte) error { writes <- write{topic, payload}; return nil }
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); f.w.Run(stop) }()
	defer func() { close(stop); <-done }()
	last := f.append(t, "metrics", "colca/v1/_Metric/NODE/new")
	select {
	case got := <-writes:
		if len(got.payload) == 0 {
			t.Fatal("expected finding")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("append missed")
	}
	if !f.st.CursorAck("c/dataops-line/metrics", "metrics", last+1) {
		t.Fatal("ack failed")
	}
	select {
	case got := <-writes:
		if len(got.payload) != 0 {
			t.Fatal("expected retirement")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ack missed")
	}
}

var staleTopic = "colca/v1/_Finding/NODE/" + StaleReason

// stale is the fixture with the stale_cursors finding on and cursor_lag off,
// so each test sees only the finding it is about.
func staleFixture(t *testing.T) *fixture {
	f := newFixture(t)
	f.w.After = 0
	f.w.StaleAfter = 24 * time.Hour
	return f
}

func TestACursorStandingStillWhileItsStreamGrowsIsNamedUntilRetired(t *testing.T) {
	f := staleFixture(t)
	// A removed consumer's cursor: nobody fetches it, its stream grows past it.
	last := f.append(t, "commands", "colca/v1/_CmdSet/NODE/a")
	f.st.CursorAck("c/hygentile-app/reconcile", "commands", last+1)
	f.append(t, "commands", "colca/v1/_CmdSet/NODE/b", "colca/v1/_CmdSet/NODE/c")
	acked := time.Now()

	f.w.Check(acked.Add(23 * time.Hour))
	if len(f.writes) != 0 {
		t.Fatalf("finding before stale_after: %v", f.writes)
	}
	if got := f.w.nextDelay(acked.Add(23 * time.Hour)); got <= 0 || got > time.Hour+time.Minute {
		t.Fatalf("stale deadline = %v, want about 1h", got)
	}

	f.w.Check(acked.Add(25 * time.Hour))
	f.w.Check(acked.Add(26 * time.Hour))
	if len(f.writes) != 1 || f.writes[0].topic != staleTopic {
		t.Fatalf("want one finding at %s, got %v", staleTopic, f.writes)
	}
	var finding struct {
		Reason string `json:"reason"`
		Detail struct {
			Cursors []staleCursor `json:"cursors"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(f.writes[0].payload, &finding); err != nil {
		t.Fatal(err)
	}
	c := finding.Detail.Cursors
	if finding.Reason != StaleReason || len(c) != 1 || c[0].Cursor != "c/hygentile-app/reconcile" ||
		c[0].LagRecords != 2 || c[0].ReadSinceStart {
		t.Fatalf("finding = %+v", finding)
	}

	if err := f.st.CursorDelete("c/hygentile-app/reconcile", "commands"); err != nil {
		t.Fatal(err)
	}
	f.w.Check(acked.Add(27 * time.Hour))
	if len(f.writes) != 2 || f.writes[1].topic != staleTopic || f.writes[1].payload != nil {
		t.Fatalf("want the finding retired once the cursor is gone, got %v", f.writes)
	}
}

func TestACaughtUpCursorOnAQuietStreamIsNotStale(t *testing.T) {
	f := staleFixture(t)
	f.read(t, "c/dataops-line/entities", "entities")
	f.w.Check(time.Now().Add(30 * 24 * time.Hour))
	if len(f.writes) != 0 {
		t.Fatalf("a cursor at the head raised a finding: %v", f.writes)
	}
	if got := f.w.nextDelay(time.Now()); got != -1 {
		t.Fatalf("idle deadline = %v, want none", got)
	}
}

func TestStaleAfterZeroWritesNoStaleFinding(t *testing.T) {
	f := staleFixture(t)
	f.w.StaleAfter = 0
	f.read(t, "c/dataops-line/entities", "entities")
	f.append(t, "entities", "colca/v1/_ServiceDetails/NODE/x")
	f.w.Check(time.Now().Add(30 * 24 * time.Hour))
	if len(f.writes) != 0 {
		t.Fatalf("stale_after 0 wrote a finding: %v", f.writes)
	}
}

// A live consumer that filters out what grows its stream and never acks past
// it is stale too: it holds retention just like a dead one.
func TestAFilteredConsumerThatDoesNotAckPastSkippedRecordsIsStale(t *testing.T) {
	f := staleFixture(t)
	f.read(t, "c/dataops-line/entities", "entities")
	f.w.Filters.Remember("c/dataops-line/entities", "entities", func(r store.StoredRecord) bool {
		return strings.Contains(r.Topic, "/_Signal/")
	})
	f.append(t, "entities", "colca/v1/_ServiceDetails/NODE/x")
	f.w.Check(time.Now().Add(25 * time.Hour))
	if len(f.writes) != 1 || f.writes[0].topic != staleTopic {
		t.Fatalf("want the stale finding, got %v", f.writes)
	}
	if !strings.Contains(string(f.writes[0].payload), `"read_since_start":true`) {
		t.Fatalf("finding does not say the cursor is read: %s", f.writes[0].payload)
	}
}

func TestAStaleFindingLeftByTheLastRunIsRetired(t *testing.T) {
	f := staleFixture(t)
	_, _, err := f.st.Append("entities", []store.Record{{
		Topic: staleTopic, Payload: []byte(`{"reason":"stale_cursors"}`), TS: f.now.UnixMilli(),
		WrittenBy: Author, KVPath: StaleReason, KVNode: "NODE",
	}})
	if err != nil {
		t.Fatal(err)
	}
	f.w.Check(time.Now())
	if len(f.writes) != 1 || f.writes[0].topic != staleTopic || f.writes[0].payload != nil {
		t.Fatalf("want the old finding retired, got %v", f.writes)
	}
}
