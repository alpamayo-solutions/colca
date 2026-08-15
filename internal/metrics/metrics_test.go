package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/alpamayo-solutions/colca/internal/store"
)

func mustStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func seedRecords(t *testing.T, s *store.Store, stream string, n int) {
	t.Helper()
	recs := make([]store.Record, n)
	for i := range recs {
		recs[i] = store.Record{Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1}`), TS: int64(i)}
	}
	if _, _, err := s.Append(stream, recs); err != nil {
		t.Fatalf("seed %s: %v", stream, err)
	}
}

// The collector families carry the STORE's values, derived at scrape time:
// scraping twice around a store mutation must show the new state without any
// metrics call in between. Pinned values: 3 appends → next_offset 4; cursor
// acked to 3 → position 3, lag 1; child HWM applied at 5.
func TestCollectorDerivesGaugesFromStoreAtScrape(t *testing.T) {
	s := mustStore(t)
	seedRecords(t, s, "metrics", 3) // next_offset = 4
	if !s.CursorAck("hub", "metrics", 3) {
		t.Fatal("seed cursor")
	}
	if _, _, err := s.ApplyReplicated("n-child", "entities", []store.ReplRecord{
		{ChildOffset: 2, Topic: "colca/v1/_Entity/m1/a", Payload: []byte("1"), TS: 1},
		{ChildOffset: 5, Topic: "colca/v1/_Entity/m1/b", Payload: []byte("2"), TS: 2},
	}); err != nil {
		t.Fatal(err)
	}

	m := New(s)
	expect := `
# HELP colca_child_hwm Highest child offset already applied, per (child, stream).
# TYPE colca_child_hwm gauge
colca_child_hwm{child="n-child",stream="entities"} 5
# HELP colca_cursor_position Next offset the named cursor will read (derived from the store at scrape time).
# TYPE colca_cursor_position gauge
colca_cursor_position{cursor="hub",stream="metrics"} 3
# HELP colca_cursor_lag_records Records the cursor has not read yet: next_offset - position, floored at 0.
# TYPE colca_cursor_lag_records gauge
colca_cursor_lag_records{cursor="hub",stream="metrics"} 1
# HELP colca_stream_next_offset Next offset the stream will assign (derived from the store at scrape time).
# TYPE colca_stream_next_offset gauge
colca_stream_next_offset{stream="commands"} 1
colca_stream_next_offset{stream="entities"} 3
colca_stream_next_offset{stream="metrics"} 4
`
	if err := testutil.GatherAndCompare(m.reg, strings.NewReader(expect),
		"colca_child_hwm", "colca_cursor_position", "colca_cursor_lag_records",
		"colca_stream_next_offset"); err != nil {
		t.Fatal(err)
	}

	// Mutate the store only — the next scrape must see it (scrape-time
	// derivation, not a snapshot taken in New).
	seedRecords(t, s, "metrics", 2) // next_offset 4 → 6, lag 1 → 3
	expect2 := `
# HELP colca_cursor_lag_records Records the cursor has not read yet: next_offset - position, floored at 0.
# TYPE colca_cursor_lag_records gauge
colca_cursor_lag_records{cursor="hub",stream="metrics"} 3
# HELP colca_stream_next_offset Next offset the stream will assign (derived from the store at scrape time).
# TYPE colca_stream_next_offset gauge
colca_stream_next_offset{stream="commands"} 1
colca_stream_next_offset{stream="entities"} 3
colca_stream_next_offset{stream="metrics"} 6
`
	if err := testutil.GatherAndCompare(m.reg, strings.NewReader(expect2),
		"colca_cursor_lag_records", "colca_stream_next_offset"); err != nil {
		t.Fatal(err)
	}
}

// A cursor acked beyond the stream's next offset must floor lag at 0, never
// underflow into a huge uint.
func TestCursorLagFloorsAtZero(t *testing.T) {
	s := mustStore(t)
	seedRecords(t, s, "metrics", 1) // next_offset = 2
	if !s.CursorAck("eager", "metrics", 9) {
		t.Fatal("seed cursor")
	}
	m := New(s)
	expect := `
# HELP colca_cursor_lag_records Records the cursor has not read yet: next_offset - position, floored at 0.
# TYPE colca_cursor_lag_records gauge
colca_cursor_lag_records{cursor="eager",stream="metrics"} 0
`
	if err := testutil.GatherAndCompare(m.reg, strings.NewReader(expect),
		"colca_cursor_lag_records"); err != nil {
		t.Fatal(err)
	}
}

// Every counter/gauge family is present and zero-valued from the first scrape
// (pre-created children) — dashboards and Plan C queries never see a missing
// family on an idle node.
func TestAllFamiliesPresentZeroValuedBeforeAnyEvent(t *testing.T) {
	m := New(mustStore(t))
	got, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	families := map[string]int{} // family → number of children
	for _, mf := range got {
		families[mf.GetName()] = len(mf.GetMetric())
	}
	want := map[string]int{
		"colca_stream_next_offset":                      3, // one per stream
		"colca_ingest_records_total":                    3,
		"colca_rejected_publishes_total":                6, // one per reason
		"colca_uplink_last_success_timestamp_seconds":   3,
		"colca_uplink_push_failures_total":              3,
		"colca_downlink_last_success_timestamp_seconds": 1,
		"colca_downlink_fetch_failures_total":           1,
		"colca_retained_reseed_records":                 1,
	}
	for fam, children := range want {
		if families[fam] != children {
			t.Errorf("family %s: want %d zero-valued children on a fresh registry, got %d (families: %v)",
				fam, children, families[fam], families)
		}
	}
}

// The increment surface lands on the right child with the right value.
func TestIncrementSurface(t *testing.T) {
	m := New(mustStore(t))

	m.IngestRecord("metrics")
	m.IngestRecord("metrics")
	m.IngestRecord("entities")
	if got := testutil.ToFloat64(m.ingestBy["metrics"]); got != 2 {
		t.Errorf("ingest metrics = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.ingestBy["entities"]); got != 1 {
		t.Errorf("ingest entities = %v, want 1", got)
	}

	for i, reason := range []string{ReasonIdentity, ReasonGrammar, ReasonValidation, ReasonNoMount, ReasonNotCommand, ReasonAuth} {
		for j := 0; j <= i; j++ {
			m.RejectPublish(reason)
		}
		if got := testutil.ToFloat64(m.rejectedBy[reason]); got != float64(i+1) {
			t.Errorf("rejected %s = %v, want %d", reason, got, i+1)
		}
	}

	at := time.Unix(1_700_000_000, 500_000_000)
	m.UplinkPushed("metrics", at)
	if got := testutil.ToFloat64(m.uplinkOKBy["metrics"]); got != 1_700_000_000.5 {
		t.Errorf("uplink last success = %v, want 1700000000.5", got)
	}
	m.UplinkPushFailed("metrics")
	if got := testutil.ToFloat64(m.uplinkFailBy["metrics"]); got != 1 {
		t.Errorf("uplink failures = %v, want 1", got)
	}

	m.DownlinkFetched(at.Add(5 * time.Second))
	if got := testutil.ToFloat64(m.downlinkOK); got != 1_700_000_005.5 {
		t.Errorf("downlink last success = %v, want 1700000005.5", got)
	}
	m.DownlinkFetchFailed()
	if got := testutil.ToFloat64(m.downlinkFail); got != 1 {
		t.Errorf("downlink failures = %v, want 1", got)
	}

	m.SetReseedCount(42)
	if got := testutil.ToFloat64(m.reseed); got != 42 {
		t.Errorf("reseed = %v, want 42", got)
	}
}

// Every method on a nil *Metrics is a no-op — later tasks wire the handle
// through engine/broker/repl without nil conditionals.
func TestNilReceiverIsNoOp(t *testing.T) {
	var m *Metrics
	m.IngestRecord("metrics")
	m.RejectPublish(ReasonAuth)
	m.UplinkPushed("metrics", time.Now())
	m.UplinkPushFailed("metrics")
	m.DownlinkFetched(time.Now())
	m.DownlinkFetchFailed()
	m.SetReseedCount(3)
}
