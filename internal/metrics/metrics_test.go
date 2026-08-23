package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/alpamayo-solutions/colca/internal/config"
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

// seedRecordsAt is seedRecords with explicit timestamps base, base+step,
// base+2*step, … — for the retention gauges, which read record age off real
// wall-clock-relative timestamps.
func seedRecordsAt(t *testing.T, s *store.Store, stream string, n int, base, step int64) {
	t.Helper()
	recs := make([]store.Record, n)
	for i := range recs {
		recs[i] = store.Record{Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1}`), TS: base + int64(i)*step}
	}
	if _, _, err := s.Append(stream, recs); err != nil {
		t.Fatalf("seed %s: %v", stream, err)
	}
}

// gaugeValue scans a Gather() result for one family+label-set combination.
// The retention gauges are Desc-based ConstMetrics emitted by storeCollector
// (design: derived at scrape time, never a standalone Gauge object), so
// testutil.ToFloat64 does not apply — that helper requires a Collector that
// exposes exactly one metric, and storeCollector exposes many per Collect.
func gaugeValue(t *testing.T, m *Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, mm := range mf.GetMetric() {
			if labelsMatch(mm, labels) {
				return mm.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("family %s with labels %v not found in scrape", family, labels)
	return 0
}

// counterValue is gaugeValue's Counter-backed equivalent, for families whose
// children are plain prometheus.Counter (not derived by storeCollector).
//
// internal/metrics/metricstest.Value does the same job for every OTHER
// package (repl, engine, httpapi, mqttsrv, retention) and should stay the
// first choice there. It cannot be used HERE: this file is `package metrics`
// (package-internal, so it can reach m.reg directly), and metricstest imports
// metrics — importing metricstest from this file would be metrics →
// metricstest → metrics, an import cycle. Do not "fix" this by adding that
// import.
func counterValue(t *testing.T, m *Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, mm := range mf.GetMetric() {
			if labelsMatch(mm, labels) {
				return mm.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("family %s with labels %v not found in scrape", family, labels)
	return 0
}

func labelsMatch(mm *dto.Metric, want map[string]string) bool {
	if len(mm.GetLabel()) != len(want) {
		return false
	}
	for _, lp := range mm.GetLabel() {
		if want[lp.GetName()] != lp.GetValue() {
			return false
		}
	}
	return true
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

	m := New(s, config.Retention{}, nil)
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
colca_stream_next_offset{stream="alarms"} 1
colca_stream_next_offset{stream="audit"} 1
colca_stream_next_offset{stream="commands"} 1
colca_stream_next_offset{stream="definitions"} 1
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
colca_stream_next_offset{stream="alarms"} 1
colca_stream_next_offset{stream="audit"} 1
colca_stream_next_offset{stream="commands"} 1
colca_stream_next_offset{stream="definitions"} 1
colca_stream_next_offset{stream="entities"} 3
colca_stream_next_offset{stream="metrics"} 6
`
	if err := testutil.GatherAndCompare(m.reg, strings.NewReader(expect2),
		"colca_cursor_lag_records", "colca_stream_next_offset"); err != nil {
		t.Fatal(err)
	}
}

// The retention gauges (design §8) are derived from the store AND the node's
// retention policy at scrape time, never cached — values, not just presence.
// Scenario: 5 records spaced 10 minutes apart, oldest 2h old, against a 1h
// max_age. Nothing has been pruned (LWM stays 1), so every record is still
// "live" and the oldest one is what colca_retention_pressure measures. A
// cursor sitting at offset 3 (below the offset the unclamped age policy would
// reach) is exactly what colca_retention_blocked_by_cursor counts.
func TestRetentionGaugesDerivedFromStoreAndPolicy(t *testing.T) {
	s := mustStore(t)
	base := time.Now().Add(-2 * time.Hour).UnixMilli()
	seedRecordsAt(t, s, "metrics", 5, base, 10*60*1000) // offsets 1..5, 2h..80min old

	if !s.CursorAck("slow", "metrics", 3) { // consumed 1..2, unread from 3
		t.Fatal("seed cursor")
	}

	cfg := config.Retention{Streams: map[string]config.StreamRetention{
		"metrics": {MaxAge: config.Duration(time.Hour)},
	}}
	m := New(s, cfg, nil)

	if got, want := gaugeValue(t, m, "colca_stream_low_water_mark", map[string]string{"stream": "metrics"}), 1.0; got != want {
		t.Fatalf("colca_stream_low_water_mark = %v, want %v (nothing pruned yet)", got, want)
	}
	if got, want := gaugeValue(t, m, "colca_stream_live_bytes", map[string]string{"stream": "metrics"}), float64(s.StreamBytes("metrics")); got != want {
		t.Fatalf("colca_stream_live_bytes = %v, want %v (store.StreamBytes)", got, want)
	}

	// Oldest retained record (offset 1) is ~2h old against a 1h max_age:
	// pressure = age_used/max_age ≈ 2. Tolerance covers test wall-clock drift.
	if pressure := gaugeValue(t, m, "colca_retention_pressure", map[string]string{"stream": "metrics"}); pressure < 1.9 || pressure > 2.2 {
		t.Fatalf("colca_retention_pressure = %v, want ~2 (oldest record ~2h old, max_age 1h)", pressure)
	}

	// All 5 records are older than max_age, so the unclamped policy wants the
	// whole stream gone — the "slow" cursor at 3 is the one thing in the way.
	if blocked := gaugeValue(t, m, "colca_retention_blocked_by_cursor", map[string]string{"stream": "metrics"}); blocked != 1 {
		t.Fatalf("colca_retention_blocked_by_cursor = %v, want 1", blocked)
	}

	// Scrape-time derivation, not a snapshot at New: advancing the cursor past
	// the policy target must drop the block to 0 on the NEXT scrape with no
	// metrics call in between (mutation guard for blockedByCursor's position
	// comparison).
	if !s.CursorAck("slow", "metrics", 6) {
		t.Fatal("cursor advance must move")
	}
	if got := gaugeValue(t, m, "colca_retention_blocked_by_cursor", map[string]string{"stream": "metrics"}); got != 0 {
		t.Fatalf("colca_retention_blocked_by_cursor after the cursor catches up = %v, want 0", got)
	}
}

// colca_retention_blocked_by_cursor counts EVERY protecting cursor below the
// policy target, not just whether any exist — two independently blocking
// cursors must read 2, not be capped at 1 (mutation guard against an
// accidental early-return/boolean-collapse in blockedByCursor's loop).
func TestBlockedByCursorCountsEveryProtectingCursorBelowTarget(t *testing.T) {
	s := mustStore(t)
	base := time.Now().Add(-2 * time.Hour).UnixMilli()
	seedRecordsAt(t, s, "metrics", 5, base, 10*60*1000) // offsets 1..5, all older than max_age below

	// Two independent cursors, both below where the unclamped policy wants
	// to go (offset 6): "slower" at 2, "slow" at 3.
	if !s.CursorAck("slower", "metrics", 2) {
		t.Fatal("seed cursor")
	}
	if !s.CursorAck("slow", "metrics", 3) {
		t.Fatal("seed cursor")
	}

	cfg := config.Retention{Streams: map[string]config.StreamRetention{
		"metrics": {MaxAge: config.Duration(time.Hour)},
	}}
	m := New(s, cfg, nil)

	if blocked := gaugeValue(t, m, "colca_retention_blocked_by_cursor", map[string]string{"stream": "metrics"}); blocked != 2 {
		t.Fatalf("colca_retention_blocked_by_cursor = %v, want 2 (both cursors independently block)", blocked)
	}

	// Advancing only the FURTHER-BEHIND cursor past the target must drop the
	// count by exactly one, not to zero — pins that the count is a true sum,
	// not a boolean collapsed to 0/1.
	if !s.CursorAck("slower", "metrics", 6) {
		t.Fatal("cursor advance must move")
	}
	if blocked := gaugeValue(t, m, "colca_retention_blocked_by_cursor", map[string]string{"stream": "metrics"}); blocked != 1 {
		t.Fatalf("colca_retention_blocked_by_cursor after one cursor catches up = %v, want 1 (the other still blocks)", blocked)
	}
}

// A cursor sitting below LWM's records is NOT "blocked" when the policy does
// not want to prune that far in the first place (max_age far larger than any
// record's age) — blockedByCursor must compare against what the policy
// actually wants, not just "any cursor below next_offset".
func TestBlockedByCursorZeroWhenPolicyWantsNothingPruned(t *testing.T) {
	s := mustStore(t)
	seedRecordsAt(t, s, "commands", 3, time.Now().Add(-100*24*time.Hour).UnixMilli(), 1000)
	if !s.CursorAck("slow", "commands", 2) { // consumed offset 1, unread from offset 2
		t.Fatal("seed cursor")
	}
	cfg := config.Retention{Streams: map[string]config.StreamRetention{
		"commands": {MaxAge: config.Duration(1000 * 24 * time.Hour)}, // far older than any record
	}}
	m := New(s, cfg, nil)
	if got := gaugeValue(t, m, "colca_retention_blocked_by_cursor", map[string]string{"stream": "commands"}); got != 0 {
		t.Fatalf("colca_retention_blocked_by_cursor = %v, want 0 (policy wants nothing pruned)", got)
	}
}

// colca_retention_blocked_by_cursor's scrape-time scan must never walk the
// full backlog: in the state the gauge exists to alert on (a dead consumer,
// backlog growing without bound — spec §14 follow-up) an uncapped scan
// would JSON-decode the entire clamped backlog on every single scrape.
// Seed a backlog far larger than a small test cap, all old enough that the
// age policy wants every one of them pruned (so nothing early-exits the
// scan before the cap is reached), and prove:
//  1. the underlying PolicyPruneTarget scan stops at EXACTLY the cap — the
//     returned target is a FLOOR (lwm + cap), not the full backlog's end;
//  2. the gauge reports floor semantics — a cursor sitting between the
//     floor and the true (unscanned) backlog end is NOT counted as
//     blocking, only the cursor below the floor is.
//
// The uncapped comparison at the end is the mutation guard: it proves the
// same backlog, scanned without a cap (scanCap=0 — the pre-fix behavior),
// finds BOTH cursors blocking. If the cap is ever removed or bypassed, the
// capped assertion above (blocked==1) fails and reads 2 instead — this is
// the "assertion goes red" the cap's mutation check is built on.
func TestBlockedByCursorScanIsBoundedByCap(t *testing.T) {
	s := mustStore(t)
	const backlog = 1000
	const testCap = 50
	base := time.Now().Add(-2 * time.Hour).UnixMilli()
	seedRecordsAt(t, s, "metrics", backlog, base, 1000) // all well older than the 1h cutoff below

	// One cursor below the capped floor (must count), one well above it but
	// still below the true uncapped target (must NOT count while capped).
	if !s.CursorAck("below-floor", "metrics", 30) {
		t.Fatal("seed cursor")
	}
	if !s.CursorAck("above-floor", "metrics", 200) {
		t.Fatal("seed cursor")
	}

	cfg := config.Retention{Streams: map[string]config.StreamRetention{
		"metrics": {MaxAge: config.Duration(time.Hour)},
	}}

	// Direct PolicyPruneTarget check: with the cap, the scan examines
	// exactly `testCap` records (lwm starts at 1, every one of the first
	// testCap records is old enough to be shed) and reports hitScanCap.
	lwm := s.LWM("metrics")
	next := s.NextOffset("metrics")
	liveBytes := s.StreamBytes("metrics")
	target, clampedAtCap, hitScanCap, err := s.PolicyPruneTarget("metrics", lwm, next, time.Now(), time.Hour, 0, liveBytes, next, testCap)
	if err != nil {
		t.Fatal(err)
	}
	if clampedAtCap {
		t.Fatal("no cursor clamp in play here (blockedByCursor always calls with clamp=next); clampedAtCap must stay false")
	}
	if !hitScanCap {
		t.Fatal("scan must report hitting the record cap — backlog (1000) far exceeds the cap (50)")
	}
	if want := lwm + testCap; target != want {
		t.Fatalf("target = %d, want %d (lwm + cap: the scan must examine EXACTLY the cap's worth of records, not the full backlog)", target, want)
	}

	// Collector-level check: capped, only the below-floor cursor blocks.
	c := newStoreCollector(s, cfg, testCap)
	if blocked := c.blockedByCursor("metrics", time.Now()); blocked != 1 {
		t.Fatalf("colca_retention_blocked_by_cursor (capped) = %d, want 1 (only the below-floor cursor; the above-floor cursor sits beyond the scan cap and must not be reported as blocking)", blocked)
	}

	// Mutation guard: an uncapped scan (scanCap=0, the pre-fix behavior)
	// walks the entire 1000-record backlog and finds BOTH cursors blocking.
	uncapped := newStoreCollector(s, cfg, 0)
	if blocked := uncapped.blockedByCursor("metrics", time.Now()); blocked != 2 {
		t.Fatalf("colca_retention_blocked_by_cursor (uncapped, scanCap=0) = %d, want 2 (sanity check that the capped case above is actually exercising the cap, not returning 1 for some unrelated reason)", blocked)
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
	m := New(s, config.Retention{}, nil)
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
	m := New(mustStore(t), config.Retention{}, nil)
	got, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	families := map[string]int{} // family → number of children
	for _, mf := range got {
		families[mf.GetName()] = len(mf.GetMetric())
	}
	// Stream-labelled families are sized from the lists that BUILD them, not
	// from a literal. A hand-written count here cannot tell "a stream was
	// added and its child is correctly present" from "a stream was added and
	// this test is now wrong", so it would fail every time the set grows and
	// teach the next reader to bump the number rather than check the claim.
	// What is being pinned is which LIST each family follows — that is the
	// real claim, and it still fails if a family follows the wrong one.
	want := map[string]int{
		"colca_stream_next_offset":       len(streams), // one per stream
		"colca_ingest_records_total":     len(streams),
		"colca_rejected_publishes_total": 10, // one per reason
		"colca_auth_rejections_total":    24, // door × reason
		"colca_acl_denials_total":        2,  // one per action
		"colca_session_kicks_total":      1,
		// The uplink families cover only the streams that RISE: definitions
		// descend, so a gauge for them would sit at zero forever and read like
		// a broken uplink (definition-stream design §4).
		"colca_uplink_last_success_timestamp_seconds":   len(uplinkStreams),
		"colca_uplink_push_failures_total":              len(uplinkStreams),
		"colca_downlink_last_success_timestamp_seconds": 1,
		"colca_downlink_fetch_failures_total":           1,
		"colca_downlink_cursor_beyond_head_total":       1,
		"colca_downlink_head_absent_total":              1,
		"colca_retained_reseed_records":                 1,
		// Retention (design §8): collector-derived gauges, always one child per
		// known stream regardless of activity.
		"colca_stream_low_water_mark": len(streams),
		"colca_stream_live_bytes":     len(streams),
		// The retention families cover only the streams the POLICY prunes.
		// Definitions are compacted instead, so a pressure gauge for them would
		// report progress toward a policy that does not exist (design §6).
		"colca_retention_pressure":                     len(retentionStreams),
		"colca_retention_blocked_by_cursor":            len(retentionStreams),
		"colca_retention_pruned_records_total":         len(retentionStreams),
		"colca_retention_pruned_bytes_total":           len(retentionStreams),
		"colca_retention_prune_runs_total":             len(retentionStreams),
		"colca_retention_gap_records_total":            len(retentionStreams),
		"colca_retention_state_refresh_records_total":  1, // unlabeled
		"colca_retention_state_refresh_skipped_total":  1,
		"colca_retention_state_refresh_failures_total": 1,
		"colca_gap_served_total":                       len(streams) * len(gapSurfaces),
		"colca_gap_received_total":                     len(streams),
		// The definition channel (design §5). Applied is the happy path;
		// rejected is worth alerting on, because a refused definition parks the
		// node's cursor and nothing behind it arrives either.
		"colca_definitions_applied_total":  1,
		"colca_definitions_rejected_total": 1,
		// Move-drain (design §3.2/§3.4): colca_drains_active is unlabeled
		// (always one child, like the retention state-refresh counters) and
		// colca_drains_completed_total pre-creates all four outcomes
		// (delivered, expired, forced, gapped [delta]).
		"colca_drains_active":          1,
		"colca_drains_completed_total": 4,
		// Blob transfer and ingress-rejection counters (resources design
		// §5/§7): pre-created so every label combination scrapes at zero
		// before it first fires.
		"colca_blob_transfers_total": len(blobDirections) * len(blobResults),
		"colca_blob_rejects_total":   len(blobRejectReasons),
		"colca_record_rejects_total": len(recordRejectReasons),
		// colca_cursor_position/lag/last_advance_age, colca_child_hwm,
		// colca_repl_gap_applied_total and colca_drain_pending_commands are
		// dynamic (no series until a cursor, child or draining child exists)
		// and deliberately excluded here, same precedent as the pre-existing
		// cursor/child families.
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
	m := New(mustStore(t), config.Retention{}, nil)

	m.IngestRecord("metrics")
	m.IngestRecord("metrics")
	m.IngestRecord("entities")
	if got := testutil.ToFloat64(m.ingestBy["metrics"]); got != 2 {
		t.Errorf("ingest metrics = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.ingestBy["entities"]); got != 1 {
		t.Errorf("ingest entities = %v, want 1", got)
	}

	for i, reason := range []string{ReasonNodeID, ReasonGrammar, ReasonValidation, ReasonWriteDenied, ReasonCmdDenied, ReasonRegistryContract, ReasonHumanWrite} {
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

	m.RetentionPruneRun("metrics")
	m.RetentionPruneRun("metrics")
	if got := testutil.ToFloat64(m.pruneRunsBy["metrics"]); got != 2 {
		t.Errorf("prune runs metrics = %v, want 2", got)
	}

	m.RetentionPruned("metrics", 7, 350)
	if got := testutil.ToFloat64(m.prunedRecordsBy["metrics"]); got != 7 {
		t.Errorf("pruned records metrics = %v, want 7", got)
	}
	if got := testutil.ToFloat64(m.prunedBytesBy["metrics"]); got != 350 {
		t.Errorf("pruned bytes metrics = %v, want 350", got)
	}

	m.RetentionGapRecorded("entities")
	if got := testutil.ToFloat64(m.gapRecordsBy["entities"]); got != 1 {
		t.Errorf("gap records entities = %v, want 1", got)
	}

	m.StateRefreshApplied()
	m.StateRefreshApplied()
	m.StateRefreshSkipped()
	m.StateRefreshFailed()
	if got := testutil.ToFloat64(m.refreshRecords); got != 2 {
		t.Errorf("state refresh records = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.refreshSkipped); got != 1 {
		t.Errorf("state refresh skipped = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.refreshFailures); got != 1 {
		t.Errorf("state refresh failures = %v, want 1", got)
	}

	m.GapServed("commands", "downlink")
	m.GapServed("commands", "downlink")
	if got := testutil.ToFloat64(m.gapServedBy["commands"]["downlink"]); got != 2 {
		t.Errorf("gap served commands/downlink = %v, want 2", got)
	}

	m.GapReceived("metrics")
	if got := testutil.ToFloat64(m.gapReceivedBy["metrics"]); got != 1 {
		t.Errorf("gap received metrics = %v, want 1", got)
	}

	m.GapApplied("n-child", "entities")
	m.GapApplied("n-child", "entities")
	if got := testutil.ToFloat64(m.replGapApplied.WithLabelValues("n-child", "entities")); got != 2 {
		t.Errorf("repl gap applied n-child/entities = %v, want 2", got)
	}
}

// Every method on a nil *Metrics is a no-op — later tasks wire the handle
// through engine/broker/repl without nil conditionals.
func TestNilReceiverIsNoOp(t *testing.T) {
	var m *Metrics
	m.IngestRecord("metrics")
	m.RejectPublish(ReasonGrammar)
	m.UplinkPushed("metrics", time.Now())
	m.UplinkPushFailed("metrics")
	m.DownlinkFetched(time.Now())
	m.DownlinkFetchFailed()
	m.SetReseedCount(3)
	m.AuthReject(DoorMQTT, AuthUnknownKey)
	m.ACLDeny(ACLSub)
	m.SessionKick()
	m.RetentionPruneRun("metrics")
	m.RetentionPruned("metrics", 1, 1)
	m.RetentionGapRecorded("metrics")
	m.StateRefreshApplied()
	m.StateRefreshSkipped()
	m.StateRefreshFailed()
	m.GapServed("metrics", "fetch")
	m.GapReceived("metrics")
	m.GapApplied("n-child", "metrics")
	m.BlobTransfer("push", "ok")
	m.BlobRejected("too_large")
	m.RecordRejected("too_large")
}

// Every blob-transfer and ingress-rejection label combination must scrape
// before it first fires — the same "pre-created children" guarantee
// TestAllFamiliesPresentZeroValuedBeforeAnyEvent pins by cardinality, checked
// here by explicit label value instead.
func TestBlobMetricFamiliesScrapeAtZero(t *testing.T) {
	m := New(mustStore(t), config.Retention{}, nil)
	for _, tc := range []struct {
		family string
		labels map[string]string
	}{
		{"colca_blob_transfers_total", map[string]string{"direction": "push", "result": "ok"}},
		{"colca_blob_transfers_total", map[string]string{"direction": "pull", "result": "error"}},
		{"colca_blob_rejects_total", map[string]string{"reason": "too_large"}},
		{"colca_record_rejects_total", map[string]string{"reason": "too_large"}},
	} {
		if got := counterValue(t, m, tc.family, tc.labels); got != 0 {
			t.Fatalf("%s%v = %v, want 0 — every label combination must scrape before it first fires", tc.family, tc.labels, got)
		}
	}
}

func TestBlobTransferCounts(t *testing.T) {
	m := New(mustStore(t), config.Retention{}, nil)
	m.BlobTransfer("push", "ok")
	m.BlobTransfer("push", "ok")
	if got := testutil.ToFloat64(m.blobTransfersBy["push|ok"]); got != 2 {
		t.Fatalf("blob transfers push/ok = %v, want 2", got)
	}
	// Denominator: an untouched label combination stays at 0, so the count
	// above is not just every child rising together.
	if got := testutil.ToFloat64(m.blobTransfersBy["pull|error"]); got != 0 {
		t.Fatalf("blob transfers pull/error = %v, want 0", got)
	}
}

func TestBlobAndRecordRejectCounts(t *testing.T) {
	m := New(mustStore(t), config.Retention{}, nil)
	m.BlobRejected("too_large")
	m.BlobRejected("too_large")
	m.BlobRejected("digest_mismatch")
	if got := testutil.ToFloat64(m.blobRejectsBy["too_large"]); got != 2 {
		t.Fatalf("blob rejects too_large = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.blobRejectsBy["digest_mismatch"]); got != 1 {
		t.Fatalf("blob rejects digest_mismatch = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.blobRejectsBy["bad_digest"]); got != 0 {
		t.Fatalf("blob rejects bad_digest = %v, want 0", got)
	}

	m.RecordRejected("too_large")
	if got := testutil.ToFloat64(m.recordRejectsBy["too_large"]); got != 1 {
		t.Fatalf("record rejects too_large = %v, want 1", got)
	}
}
