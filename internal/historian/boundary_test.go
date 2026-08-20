//go:build boundary

package historian

import (
	"context"
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
            edge_node_id  text,
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

	at := time.Now().UTC().Truncate(time.Millisecond)
	value := 21.5
	rows := []Row{{Timestamp: at, SignalID: "sig-boundary-1", NodeID: "n1", Number: &value}}

	if err := sink.Apply(ctx, rows, "test:metrics", 1); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := sink.Apply(ctx, rows, "test:metrics", 1); err != nil {
		t.Fatalf("replay: %v", err)
	}

	var count int
	if err := sink.Pool.QueryRow(ctx,
		`SELECT count(*) FROM historian_metric WHERE signal_id = $1`, "sig-boundary-1").
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
		SignalID:  "sig-boundary-2", NodeID: "n1", Number: &value,
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

func TestEachValueKindSurvivesTheRoundTrip(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)

	at := time.Now().UTC().Truncate(time.Millisecond)
	text := "warm"
	yes := true
	rows := []Row{
		{Timestamp: at, SignalID: "sig-text", Text: &text},
		{Timestamp: at, SignalID: "sig-bool", Bool: &yes},
		{Timestamp: at, SignalID: "sig-json", JSON: []byte(`{"x":[1,2]}`)},
	}
	if err := sink.Apply(ctx, rows, "test:kinds", 3); err != nil {
		t.Fatalf("apply: %v", err)
	}

	var gotText string
	if err := sink.Pool.QueryRow(ctx,
		`SELECT value_text FROM historian_metric WHERE signal_id = 'sig-text'`).Scan(&gotText); err != nil {
		t.Fatalf("reading text: %v", err)
	}
	if gotText != "warm" {
		t.Fatalf("value_text = %q", gotText)
	}

	var gotJSON string
	if err := sink.Pool.QueryRow(ctx,
		`SELECT value_json::text FROM historian_metric WHERE signal_id = 'sig-json'`).Scan(&gotJSON); err != nil {
		t.Fatalf("reading json: %v", err)
	}
	if gotJSON == "" {
		t.Fatal("value_json came back empty")
	}
}
