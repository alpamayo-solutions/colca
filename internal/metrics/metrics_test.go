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

// seedRecordsAt is seedRecords with timestamps base, base+step and so on, for
// the retention gauges that read record age.
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

func TestCursorNextRecordAgeTracksBacklogNotCursorInactivity(t *testing.T) {
	s := mustStore(t)
	old := time.Now().Add(-30 * 24 * time.Hour).UnixMilli()
	seedRecordsAt(t, s, "metrics", 2, old, int64(time.Hour/time.Millisecond))
	if _, err := s.CursorSetIfAbsent("up:parent", "metrics", 1); err != nil {
		t.Fatal(err)
	}
	m := New(s, config.Retention{}, nil)
	labels := map[string]string{"cursor": "up:parent", "stream": "metrics"}
	for pos := uint64(1); pos <= 3; pos++ {
		if pos > 1 {
			s.CursorAck("up:parent", "metrics", pos)
		}
		got := gaugeValue(t, m, "colca_cursor_next_record_age_seconds", labels)
		if pos == 3 {
			if got != 0 {
				t.Fatalf("caught-up cursor reports queued age: %v", got)
			}
		} else {
			want := (30*24*time.Hour - time.Duration(pos-1)*time.Hour).Seconds()
			if got < want || got > want+10 {
				t.Fatalf("offset %d: backlog age=%v, want about %v", pos, got, want)
			}
		}
	}
	seedRecordsAt(t, s, "metrics", 1, time.Now().Add(time.Hour).UnixMilli(), 0)
	if got := gaugeValue(t, m, "colca_cursor_next_record_age_seconds", labels); got != 0 {
		t.Fatalf("future sample timestamp produced invalid age: %v", got)
	}
}

// gaugeValue finds one family and label set in a scrape. The collector emits
// many metrics per Collect, so testutil.ToFloat64 does not apply.
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

// counterValue is gaugeValue for plain counters. Other packages use
// metricstest.Value; this one cannot, because metricstest imports metrics.
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

// The collector families read the store at scrape time: a second scrape after a
// store change shows the new state with no metrics call in between.
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
colca_stream_next_offset{stream="annotations"} 1
colca_stream_next_offset{stream="audit"} 1
colca_stream_next_offset{stream="commands"} 1
colca_stream_next_offset{stream="definitions"} 1
colca_stream_next_offset{stream="entities"} 3
colca_stream_next_offset{stream="logs"} 1
colca_stream_next_offset{stream="metrics"} 4
`
	if err := testutil.GatherAndCompare(m.reg, strings.NewReader(expect),
		"colca_child_hwm", "colca_cursor_position", "colca_cursor_lag_records",
		"colca_stream_next_offset"); err != nil {
		t.Fatal(err)
	}

	// Change only the store; the next scrape must see it.
	seedRecords(t, s, "metrics", 2) // next_offset 4 → 6, lag 1 → 3
	expect2 := `
# HELP colca_cursor_lag_records Records the cursor has not read yet: next_offset - position, floored at 0.
# TYPE colca_cursor_lag_records gauge
colca_cursor_lag_records{cursor="hub",stream="metrics"} 3
# HELP colca_stream_next_offset Next offset the stream will assign (derived from the store at scrape time).
# TYPE colca_stream_next_offset gauge
colca_stream_next_offset{stream="alarms"} 1
colca_stream_next_offset{stream="annotations"} 1
colca_stream_next_offset{stream="audit"} 1
colca_stream_next_offset{stream="commands"} 1
colca_stream_next_offset{stream="definitions"} 1
colca_stream_next_offset{stream="entities"} 3
colca_stream_next_offset{stream="logs"} 1
colca_stream_next_offset{stream="metrics"} 6
`
	if err := testutil.GatherAndCompare(m.reg, strings.NewReader(expect2),
		"colca_cursor_lag_records", "colca_stream_next_offset"); err != nil {
		t.Fatal(err)
	}
}

// The retention gauges read the store and the retention policy at scrape time.
// Five records ten minutes apart, the oldest two hours old, against a one-hour
// max_age: nothing is pruned yet, and a cursor at offset 3 blocks the policy.
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

	// colca_retention_blocked_by_cursor is the pruner's: a scrape reads it,
	// it scans nothing. No pass ran, so it reads 0 although "slow" blocks.
	if blocked := gaugeValue(t, m, "colca_retention_blocked_by_cursor", map[string]string{"stream": "metrics"}); blocked != 0 {
		t.Fatalf("colca_retention_blocked_by_cursor = %v before any pruner pass, want 0", blocked)
	}
	m.RetentionBlockedByCursor("metrics", 1)
	if blocked := gaugeValue(t, m, "colca_retention_blocked_by_cursor", map[string]string{"stream": "metrics"}); blocked != 1 {
		t.Fatalf("colca_retention_blocked_by_cursor = %v after the pruner set 1", blocked)
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

// Every family is present and zero-valued from the first scrape, so dashboards
// never see a missing family on an idle node.
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
	// Stream-labelled families are sized from the lists that build them, so the
	// test checks which list each family follows rather than a count that changes
	// whenever a stream is added.
	want := map[string]int{
		"colca_stream_next_offset":       len(streams), // one per stream
		"colca_ingest_records_total":     len(streams),
		"colca_rejected_publishes_total": 10, // one per reason
		"colca_auth_rejections_total":    32, // door × reason
		"colca_acl_denials_total":        2,  // one per action
		"colca_session_kicks_total":      1,
		"colca_security_changes_total":   len(securityChangeKinds),
		// Uplink families only cover the streams that rise.
		"colca_uplink_last_success_timestamp_seconds":   len(uplinkStreams),
		"colca_uplink_push_failures_total":              len(uplinkStreams),
		"colca_downlink_last_success_timestamp_seconds": 1,
		"colca_downlink_fetch_failures_total":           1,
		"colca_downlink_cursor_beyond_head_total":       1,
		"colca_downlink_head_absent_total":              1,
		"colca_retained_reseed_records":                 1,
		// The store-incarnation check.
		"colca_store_incarnation_changes_total":          len(incarnationReasons),
		"colca_repl_child_ahead_of_store_total":          1,
		"colca_uplink_resyncs_total":                     1,
		"colca_uplink_resync_active":                     1,
		"colca_uplink_resync_unreplayable_records_total": len(uplinkStreams),
		"colca_downlink_commands_already_held_total":     1,
		"colca_clock_parent_samples_discarded_total":     1,
		// Collector gauges: one child per stream regardless of activity.
		"colca_stream_low_water_mark": len(streams),
		"colca_stream_live_bytes":     len(streams),
		// Retention families only cover the streams the policy prunes.
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
		// Definitions from the parent. A rejected one parks the node's cursor.
		"colca_definitions_applied_total":            1,
		"colca_definitions_rejected_total":           1,
		"colca_replication_integrity_failures_total": 1,
		// colca_drains_active is unlabeled; all four drain outcomes are pre-created.
		"colca_drains_active":          1,
		"colca_drains_completed_total": 4,
		// Blob transfer and ingress rejection counters, pre-created at zero.
		"colca_blob_transfers_total": len(blobDirections) * len(blobResults),
		"colca_blob_rejects_total":   len(blobRejectReasons),
		"colca_record_rejects_total": len(recordRejectReasons),
		// Cursor, child HWM, repl gap and drain-pending families only have series once
		// a cursor, child or draining child exists.
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
	if got := testutil.ToFloat64(m.replIntegrityFailures); got != 2 {
		t.Errorf("replication integrity failures = %v, want 2", got)
	}
}

func TestSecurityChangeSummaryCountsOnlySuccessfulRelevantCommands(t *testing.T) {
	m := New(mustStore(t), config.Retention{}, nil)

	m.NodeCmd("_CmdAdmin", "enroll", "ok")
	m.NodeCmd("_CmdAdmin", "revoke", "ok")
	m.NodeCmd("_CmdAdmin", "enroll", "conflict")
	m.NodeCmd("_CmdConfigure", "entity/upsert", "ok")
	m.NodeCmd("_CmdConfigure", "definition/upsert", "invalid")
	m.NodeCmd("_CmdEdit", "alarm/upsert", "ok")

	for kind, want := range map[string]float64{
		SecurityChangeEnroll:    1,
		SecurityChangeRevoke:    1,
		SecurityChangeConfigure: 1,
	} {
		if got := testutil.ToFloat64(m.securityChangeBy[kind]); got != want {
			t.Errorf("security change %s = %v, want %v", kind, got, want)
		}
	}
}

// Every method on a nil *Metrics is a no-op.
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

// Every blob transfer and ingress rejection label combination scrapes at zero
// before it first fires.
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
