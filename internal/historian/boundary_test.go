//go:build boundary

package historian

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// Only a real database can show that applying the same page twice leaves one row
// per (signal_id, timestamp): the unique index still holds if the marker is
// lost.
//
// Runs against tests/docker-compose.historian.yaml with
// `go test -tags boundary ./internal/historian/`, and is skipped without a DSN.

// boundaryRunID makes this run's signal IDs unique, so a rerun against a
// database left running does not assert on the previous run's rows.
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
	sink := &Sink{Pool: pool}
	if err := sink.EnsureSchema(ctx, 0); err != nil {
		t.Fatalf("ensuring the offset table and unlimited retention: %v", err)
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

	if _, err := sink.Apply(ctx, rows, "test:metrics", 1); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if _, err := sink.Apply(ctx, rows, "test:metrics", 1); err != nil {
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
	if _, err := sink.Apply(ctx, rows, "test:together", 42); err != nil {
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

	if _, err := sink.Apply(ctx, nil, "test:empty", 7); err != nil {
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

// A metric is identified by (signal_id, timestamp), so re-publishing a
// recomputed point must store the new value.
func TestExactMatchOverwritesTheRow(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	signalID := sigID("sig-overwrite")
	at := time.Now().UTC().Truncate(time.Millisecond)
	first, second := 1.0, 2.0
	rows := []Row{{Timestamp: at, SignalID: signalID, NodeID: "n1", Number: &first}}

	if _, err := sink.Apply(ctx, rows, "test:overwrite", 1); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	rows[0].Number = &second
	if _, err := sink.Apply(ctx, rows, "test:overwrite", 2); err != nil {
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

// An identical redelivery must not write a new row version. xmin changes on any
// executed UPDATE, even one writing the same values, so an unchanged xmin shows
// the IS DISTINCT FROM guard suppressed it.
func TestIdenticalReapplyDoesNotChurnTheRow(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	signalID := sigID("sig-no-churn")
	at := time.Now().UTC().Truncate(time.Millisecond)
	value := 42.0
	rows := []Row{{Timestamp: at, SignalID: signalID, NodeID: "n1", Number: &value}}

	if _, err := sink.Apply(ctx, rows, "test:no-churn", 1); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	var before string
	if err := sink.Pool.QueryRow(ctx,
		`SELECT xmin::text FROM historian_metric WHERE signal_id = $1`, signalID).
		Scan(&before); err != nil {
		t.Fatalf("reading xmin before replay: %v", err)
	}

	// Same signal, same timestamp, same value: a pure redelivery.
	if _, err := sink.Apply(ctx, rows, "test:no-churn", 2); err != nil {
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

// A row sets at most one value column. When a signal changes kind at the same
// (signal_id, timestamp), the old column must be cleared, or the API, which
// reads the columns in a fixed order, would keep returning the stale value.
func TestAValueTypeChangeClearsTheStaleColumn(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	signalID := sigID("sig-type-change")
	at := time.Now().UTC().Truncate(time.Millisecond)
	number := 3.0
	numeric := []Row{{Timestamp: at, SignalID: signalID, NodeID: "n1", Number: &number}}
	if _, err := sink.Apply(ctx, numeric, "test:type-change", 1); err != nil {
		t.Fatalf("apply numeric: %v", err)
	}

	text := "now-textual"
	textual := []Row{{Timestamp: at, SignalID: signalID, NodeID: "n1", Text: &text}}
	if _, err := sink.Apply(ctx, textual, "test:type-change", 2); err != nil {
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

// The marker still moves when an apply overwrites rows.
func TestTheMarkerStillMovesOnAnOverwritingApply(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	at := time.Now().UTC().Truncate(time.Millisecond)
	first, second := 1.0, 2.0
	rows := []Row{{Timestamp: at, SignalID: sigID("sig-marker-overwrite"), NodeID: "n1", Number: &first}}
	if _, err := sink.Apply(ctx, rows, "test:marker-overwrite", 5); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	rows[0].Number = &second
	if _, err := sink.Apply(ctx, rows, "test:marker-overwrite", 6); err != nil {
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
	if _, err := sink.Apply(ctx, rows, "test:kinds", 3); err != nil {
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
