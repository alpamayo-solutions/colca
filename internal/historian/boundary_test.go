//go:build boundary

package historian

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// The claim only a real database can prove: applying the same page twice leaves
// one row per (signal_id, timestamp). The marker is the first net; this unique
// index is the second, and the second is what still holds when the first is
// somehow lost — a restored backup, a truncated marker table, a bug.
//
// Runs against the world in colca/tests/docker-compose.historian.yaml
// (`go test -tags boundary ./internal/historian/`). Skipped without a DSN so a
// plain `go test ./...` stays hermetic.

// boundaryRunID makes every signal_id this suite writes unique to THIS process
// invocation. `scripts/dev.py test core --boundary` never wipes the Timescale
// volume between an up and a down (it tears the whole world down on the way
// out, but two `up`/`down`-free invocations against a container left running
// share it) — a rerun that reused the same literal "sig-boundary-1" would find
// last run's row still there and its own count(*)/xmin/column assertions
// would be answering last run's row, not this one. Distinct per process (not
// per test) so a query WHERE signal_id = 'sig-boundary-1-<runID>' inside one
// run still only ever matches the row that same run wrote.
var boundaryRunID = fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())

func sigID(base string) string {
	return fmt.Sprintf("%s-%s", base, boundaryRunID)
}

func testPool(t *testing.T) *Sink {
	t.Helper()
	dsn := os.Getenv("HISTORIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("HISTORIAN_TEST_DSN not set — boundary world is not up")
	}
	ctx := context.Background()
	pool, err := Open(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	t.Cleanup(pool.Close)

	sink := &Sink{Pool: pool}
	if err := sink.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensuring the offset table: %v", err)
	}
	// The metric table belongs to the api's migrations; the boundary world
	// creates it from the same DDL so this suite writes what production writes.
	if _, err := pool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS historian_metric (
            id            bigserial,
            timestamp     timestamptz NOT NULL,
            value_json    jsonb,
            value_number  double precision,
            value_text    text,
            value_bool    boolean,
            colca_node_id text,
            signal_id     text NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS historian_metric_signal_id_timestamp_uniq
            ON historian_metric (signal_id, timestamp);`); err != nil {
		t.Fatalf("creating the metric table: %v", err)
	}
	return sink
}

func TestApplyingTheSamePageTwiceLeavesOneRow(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	signalID := sigID("sig-boundary-1")
	at := time.Now().UTC().Truncate(time.Millisecond)
	value := 21.5
	rows := []Row{{Timestamp: at, SignalID: signalID, NodeID: "n1", Number: &value}}

	if err := sink.Apply(ctx, rows, "test:metrics", 1); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := sink.Apply(ctx, rows, "test:metrics", 1); err != nil {
		t.Fatalf("replay: %v", err)
	}

	var count int
	if err := sink.Pool.QueryRow(ctx,
		`SELECT count(*) FROM historian_metric WHERE signal_id = $1`, signalID).
		Scan(&count); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if count != 1 {
		t.Fatalf("%d rows after a replay, want 1 — ON CONFLICT DO NOTHING is not holding", count)
	}
}

func TestTheMarkerAndTheRowsCommitTogether(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	value := 1.0
	rows := []Row{{
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
		SignalID:  sigID("sig-boundary-2"), NodeID: "n1", Number: &value,
	}}
	if err := sink.Apply(ctx, rows, "test:together", 42); err != nil {
		t.Fatalf("apply: %v", err)
	}

	applied, err := sink.Applied(ctx, "test:together")
	if err != nil {
		t.Fatalf("Applied: %v", err)
	}
	if applied != 42 {
		t.Fatalf("marker = %d, want 42", applied)
	}
}

func TestAnEmptyBatchStillMovesTheMarker(t *testing.T) {
	// A page of nothing but tombstones would otherwise be re-fetched forever.
	ctx := context.Background()
	sink := testPool(t)

	if err := sink.Apply(ctx, nil, "test:empty", 7); err != nil {
		t.Fatalf("apply: %v", err)
	}
	applied, err := sink.Applied(ctx, "test:empty")
	if err != nil {
		t.Fatalf("Applied: %v", err)
	}
	if applied != 7 {
		t.Fatalf("marker = %d, want 7", applied)
	}
}

// The write-side half of the evaluator's idempotency contract (design §6): a
// metric is identified by (signal_id, timestamp), not by revision, so
// recomputing a window and re-publishing the same points must land the NEW
// value — not be dropped by the first-write-wins net the Kafka pipeline used.
func TestExactMatchOverwritesTheRow(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	signalID := sigID("sig-overwrite")
	at := time.Now().UTC().Truncate(time.Millisecond)
	first, second := 1.0, 2.0
	rows := []Row{{Timestamp: at, SignalID: signalID, NodeID: "n1", Number: &first}}

	if err := sink.Apply(ctx, rows, "test:overwrite", 1); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	rows[0].Number = &second
	if err := sink.Apply(ctx, rows, "test:overwrite", 2); err != nil {
		t.Fatalf("second apply: %v", err)
	}

	var count int
	var gotNumber float64
	if err := sink.Pool.QueryRow(ctx,
		`SELECT count(*), max(value_number) FROM historian_metric WHERE signal_id = $1`,
		signalID).Scan(&count, &gotNumber); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if count != 1 {
		t.Fatalf("%d rows after an overwrite, want 1 — the exact-match overwrite must not append", count)
	}
	if gotNumber != second {
		t.Fatalf("value_number = %v, want %v — the second apply's value did not win", gotNumber, second)
	}
}

// The other half of the same contract: an identical redelivery (the case the
// unique index alone already made safe against duplication) must still leave
// the row's storage untouched — no new tuple version, no replication churn.
// xmin is the honest way to see this: Postgres bumps it on any UPDATE that
// actually executes, including one whose new values equal the old ones, but
// the WHERE ... IS DISTINCT FROM guard is what suppresses the UPDATE itself
// on an identical redelivery, so xmin only stays put if that guard is
// working. A row count assertion alone cannot tell a suppressed UPDATE apart
// from one that ran and happened to write the same bytes back.
func TestIdenticalReapplyDoesNotChurnTheRow(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	signalID := sigID("sig-no-churn")
	at := time.Now().UTC().Truncate(time.Millisecond)
	value := 42.0
	rows := []Row{{Timestamp: at, SignalID: signalID, NodeID: "n1", Number: &value}}

	if err := sink.Apply(ctx, rows, "test:no-churn", 1); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	var before string
	if err := sink.Pool.QueryRow(ctx,
		`SELECT xmin::text FROM historian_metric WHERE signal_id = $1`, signalID).
		Scan(&before); err != nil {
		t.Fatalf("reading xmin before replay: %v", err)
	}

	// Same signal, same timestamp, same value: a pure redelivery.
	if err := sink.Apply(ctx, rows, "test:no-churn", 2); err != nil {
		t.Fatalf("replay: %v", err)
	}

	var after string
	if err := sink.Pool.QueryRow(ctx,
		`SELECT xmin::text FROM historian_metric WHERE signal_id = $1`, signalID).
		Scan(&after); err != nil {
		t.Fatalf("reading xmin after replay: %v", err)
	}
	if before != after {
		t.Fatalf("xmin moved %s -> %s on an identical redelivery — the row was rewritten with no value change",
			before, after)
	}
}

// The subtle failure mode: the value columns are mutually exclusive (a row
// sets at most one of value_json/value_number/value_text/value_bool), so a
// signal that changes kind at the SAME (signal_id, timestamp) — e.g. a
// recomputation that used to publish a number and now publishes text — must
// clear the stale column, not leave both populated. Setting only the
// incoming row's own column in the UPDATE would leave value_number sitting
// next to the new value_text, and the API reads value_json/value_number/
// value_text/value_bool in that fixed order — a leftover value_number would
// silently win over the real value_text forever.
func TestAValueTypeChangeClearsTheStaleColumn(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	signalID := sigID("sig-type-change")
	at := time.Now().UTC().Truncate(time.Millisecond)
	number := 3.0
	numeric := []Row{{Timestamp: at, SignalID: signalID, NodeID: "n1", Number: &number}}
	if err := sink.Apply(ctx, numeric, "test:type-change", 1); err != nil {
		t.Fatalf("apply numeric: %v", err)
	}

	text := "now-textual"
	textual := []Row{{Timestamp: at, SignalID: signalID, NodeID: "n1", Text: &text}}
	if err := sink.Apply(ctx, textual, "test:type-change", 2); err != nil {
		t.Fatalf("apply textual: %v", err)
	}

	var gotNumber *float64
	var gotText *string
	if err := sink.Pool.QueryRow(ctx,
		`SELECT value_number, value_text FROM historian_metric WHERE signal_id = $1`,
		signalID).Scan(&gotNumber, &gotText); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if gotNumber != nil {
		t.Fatalf("value_number = %v, want NULL — the stale numeric value was not cleared", *gotNumber)
	}
	if gotText == nil || *gotText != text {
		t.Fatalf("value_text = %v, want %q", gotText, text)
	}
}

// The marker's own guarantee — untouched by this change — is pinned
// elsewhere (TestTheMarkerAndTheRowsCommitTogether,
// TestAnEmptyBatchStillMovesTheMarker); this checks it holds specifically
// across an overwrite, not just a first write.
func TestTheMarkerStillMovesOnAnOverwritingApply(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	at := time.Now().UTC().Truncate(time.Millisecond)
	first, second := 1.0, 2.0
	rows := []Row{{Timestamp: at, SignalID: sigID("sig-marker-overwrite"), NodeID: "n1", Number: &first}}
	if err := sink.Apply(ctx, rows, "test:marker-overwrite", 5); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	rows[0].Number = &second
	if err := sink.Apply(ctx, rows, "test:marker-overwrite", 6); err != nil {
		t.Fatalf("overwriting apply: %v", err)
	}

	applied, err := sink.Applied(ctx, "test:marker-overwrite")
	if err != nil {
		t.Fatalf("Applied: %v", err)
	}
	if applied != 6 {
		t.Fatalf("marker = %d, want 6 — the marker must still advance on an overwriting batch", applied)
	}
}

func TestEachValueKindSurvivesTheRoundTrip(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	textSignalID, boolSignalID, jsonSignalID := sigID("sig-text"), sigID("sig-bool"), sigID("sig-json")
	at := time.Now().UTC().Truncate(time.Millisecond)
	text := "warm"
	yes := true
	rows := []Row{
		{Timestamp: at, SignalID: textSignalID, Text: &text},
		{Timestamp: at, SignalID: boolSignalID, Bool: &yes},
		{Timestamp: at, SignalID: jsonSignalID, JSON: []byte(`{"x":[1,2]}`)},
	}
	if err := sink.Apply(ctx, rows, "test:kinds", 3); err != nil {
		t.Fatalf("apply: %v", err)
	}

	var gotText string
	if err := sink.Pool.QueryRow(ctx,
		`SELECT value_text FROM historian_metric WHERE signal_id = $1`, textSignalID).Scan(&gotText); err != nil {
		t.Fatalf("reading text: %v", err)
	}
	if gotText != "warm" {
		t.Fatalf("value_text = %q", gotText)
	}

	var gotJSON string
	if err := sink.Pool.QueryRow(ctx,
		`SELECT value_json::text FROM historian_metric WHERE signal_id = $1`, jsonSignalID).Scan(&gotJSON); err != nil {
		t.Fatalf("reading json: %v", err)
	}
	if gotJSON == "" {
		t.Fatal("value_json came back empty")
	}
}
