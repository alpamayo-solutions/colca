//go:build boundary

package historian

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The marks against Postgres: what the statement calls late, that the marks
// commit with the rows, that they are bounded, and that the reader is woken.
// The relations are created as their owner (PREKIT's migrations) creates them;
// wanted lists the signals this test asks marks for.
func markingSink(t *testing.T, wanted ...string) *Sink {
	t.Helper()
	sink := testPool(t)
	ctx := context.Background()
	if _, err := sink.Pool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS historian_late_write (
            id        bigserial PRIMARY KEY,
            signal_id text NOT NULL,
            hour      timestamptz NOT NULL,
            UNIQUE (signal_id, hour)
        );
        CREATE TABLE IF NOT EXISTS historian_late_write_signal (signal_id text PRIMARY KEY)`); err != nil {
		t.Fatalf("creating the late write relations: %v", err)
	}
	for _, signalID := range wanted {
		if _, err := sink.Pool.Exec(ctx,
			`INSERT INTO historian_late_write_signal VALUES ($1) ON CONFLICT DO NOTHING`, signalID); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.WatchLateWrites(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if !sink.MarksLateWrites() {
		t.Fatal("the sink does not mark although the relations exist")
	}
	return sink
}

func marksOf(t *testing.T, sink *Sink, signalID string) []time.Time {
	t.Helper()
	rows, err := sink.Pool.(interface {
		Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	}).Query(context.Background(),
		`SELECT hour FROM historian_late_write WHERE signal_id = $1 ORDER BY id`, signalID)
	if err != nil {
		t.Fatal(err)
	}
	hours, err := pgx.CollectRows(rows, pgx.RowTo[time.Time])
	if err != nil {
		t.Fatal(err)
	}
	return hours
}

func number(v float64) *float64 { return &v }

func TestALatePageMarksEachHourOfEachSignalOnce(t *testing.T) {
	ctx := context.Background()
	late, current, unlisted := sigID("late"), sigID("current"), sigID("unlisted")
	sink := markingSink(t, late, current)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-5 * time.Hour)
	rows := []Row{
		{SignalID: late, NodeID: "n1", Timestamp: hour.Add(time.Minute), Number: number(1)},
		{SignalID: late, NodeID: "n1", Timestamp: hour.Add(2 * time.Minute), Number: number(2)},
		{SignalID: late, NodeID: "n1", Timestamp: hour.Add(61 * time.Minute), Number: number(3)},
		{SignalID: late, NodeID: "n1", Timestamp: hour.Add(62 * time.Minute)}, // a retraction is a write too
		{SignalID: current, NodeID: "n1", Timestamp: time.Now().UTC(), Number: number(4)},
		// Late, but nobody asked for marks of this signal.
		{SignalID: unlisted, NodeID: "n1", Timestamp: hour.Add(time.Minute), Number: number(5)},
	}
	if _, err := sink.Apply(ctx, rows, "test:late", 1, ""); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got := marksOf(t, sink, late)
	if len(got) != 2 || !got[0].Equal(hour) && !got[1].Equal(hour) || !got[0].Equal(hour.Add(time.Hour)) && !got[1].Equal(hour.Add(time.Hour)) {
		t.Fatalf("marks of the late signal = %v, want %v and %v once each", got, hour, hour.Add(time.Hour))
	}
	if got := marksOf(t, sink, current); len(got) != 0 {
		t.Fatalf("a current sample was marked: %v", got)
	}

	if got := marksOf(t, sink, unlisted); len(got) != 0 {
		t.Fatalf("a signal that is not listed was marked: %v", got)
	}

	// However many pages write into an hour, it has one mark.
	for offset := int64(2); offset < 5; offset++ {
		more := []Row{{SignalID: late, NodeID: "n1", Timestamp: hour.Add(time.Duration(offset) * time.Minute), Number: number(5)}}
		if _, err := sink.Apply(ctx, more, "test:late", offset, ""); err != nil {
			t.Fatalf("apply %d: %v", offset, err)
		}
	}
	if got := marksOf(t, sink, late); len(got) != 2 {
		t.Fatalf("marks after more pages into the same hour = %v, want still 2", got)
	}
}

// The reader deletes a mark, then reads the hour, in one transaction. A page
// that is still writing into that hour holds the mark, so the delete waits for
// it and the read sees its sample; without that the sample would be stored,
// its mark gone and the hour read without it.
func TestAReaderDeletingAMarkWaitsForThePageThatHoldsIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	signalID := sigID("held")
	sink := markingSink(t, signalID)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-8 * time.Hour)
	first := []Row{{SignalID: signalID, NodeID: "n1", Timestamp: hour.Add(time.Minute), Number: number(1)}}
	if _, err := sink.Apply(ctx, first, "test:late", 1, ""); err != nil {
		t.Fatal(err)
	}

	// A page in flight: its sample and its (already present) mark, not committed.
	page, err := sink.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = page.Rollback(context.Background()) }()
	if _, err := page.Exec(ctx, insertMetric, hour.Add(2*time.Minute), nil, number(2), nil, nil, "n1", signalID); err != nil {
		t.Fatal(err)
	}
	if _, err := page.Exec(ctx, markLateWrites, []string{signalID}, []time.Time{hour}); err != nil {
		t.Fatal(err)
	}

	seen := make(chan int, 1)
	failed := make(chan error, 1)
	go func() {
		reader, err := sink.Pool.Begin(ctx)
		if err != nil {
			failed <- err
			return
		}
		defer func() { _ = reader.Rollback(context.Background()) }()
		if _, err := reader.Exec(ctx, `DELETE FROM historian_late_write WHERE signal_id = $1 AND hour = $2`, signalID, hour); err != nil {
			failed <- err
			return
		}
		var n int
		if err := reader.QueryRow(ctx, `SELECT count(*) FROM historian_metric WHERE signal_id = $1`, signalID).Scan(&n); err != nil {
			failed <- err
			return
		}
		seen <- n
	}()

	select {
	case n := <-seen:
		t.Fatalf("the reader read %d samples while the page was still open", n)
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := page.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-seen:
		if n != 2 {
			t.Fatalf("the reader saw %d samples, want both", n)
		}
	case err := <-failed:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("the reader never finished")
	}
}

// The statement decides by the database's clock: an hour is late from 65
// minutes after its start.
func TestAnHourIsLateFromSixtyFiveMinutesAfterItsStart(t *testing.T) {
	ctx := context.Background()
	signalID := sigID("edge")
	sink := markingSink(t, signalID)
	var now time.Time
	if err := sink.Pool.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	now = now.UTC()
	hour := now.Truncate(time.Hour)
	previous := hour.Add(-time.Hour)
	rows := []Row{
		{SignalID: signalID, NodeID: "n1", Timestamp: hour, Number: number(1)},
		{SignalID: signalID, NodeID: "n1", Timestamp: previous.Add(59 * time.Minute), Number: number(2)},
		{SignalID: signalID, NodeID: "n1", Timestamp: previous.Add(-time.Minute), Number: number(3)},
	}
	if _, err := sink.Apply(ctx, rows, "test:late", 1, ""); err != nil {
		t.Fatalf("apply: %v", err)
	}
	want := map[time.Time]bool{previous.Add(-time.Hour): true}
	if now.Sub(previous) >= lateAfter {
		want[previous] = true
	}
	got := marksOf(t, sink, signalID)
	if len(got) != len(want) {
		t.Fatalf("marks = %v, want %v (now %v)", got, want, now)
	}
	for _, h := range got {
		if !want[h.UTC()] {
			t.Fatalf("marks = %v, want %v (now %v)", got, want, now)
		}
	}
}

func TestMarksReachTheListenerWhenThePageCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	signalID := sigID("notify")
	sink := markingSink(t, signalID)
	listener, err := pgx.Connect(ctx, os.Getenv("HISTORIAN_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close(context.Background())
	if _, err := listener.Exec(ctx, `LISTEN historian_late_write`); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-4 * time.Hour)
	if _, err := sink.Apply(ctx, []Row{{SignalID: signalID, NodeID: "n1", Timestamp: at, Number: number(1)}},
		"test:late", 1, ""); err != nil {
		t.Fatalf("apply: %v", err)
	}
	note, err := listener.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("no notification after a late page: %v", err)
	}
	if note.Channel != "historian_late_write" {
		t.Fatalf("channel = %q", note.Channel)
	}
	// The mark is there when the listener hears of it.
	var n int
	if err := listener.QueryRow(ctx, `SELECT count(*) FROM historian_late_write WHERE signal_id = $1`, signalID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("marks visible to the listener = %d, %v", n, err)
	}
}

// A page written over several connections marks in each share's transaction.
func TestAPartitionedPageMarksItsLateWrites(t *testing.T) {
	ctx := context.Background()
	prefix := sigID("part")
	var listed []string
	for i := range 20 {
		listed = append(listed, prefix+"-"+string(rune('a'+i)))
	}
	sink := markingSink(t, listed...)
	sink.Writers = 4
	base := time.Now().UTC().Truncate(time.Hour).Add(-6 * time.Hour)
	var rows []Row
	for i := range 40 {
		rows = append(rows, Row{SignalID: prefix + "-" + string(rune('a'+i%20)), NodeID: "n1",
			Timestamp: base.Add(time.Duration(i) * time.Second), Number: number(float64(i))})
	}
	if _, err := sink.Apply(ctx, rows, "test:late-part", 1, ""); err != nil {
		t.Fatalf("apply: %v", err)
	}
	var n int
	if err := sink.Pool.QueryRow(ctx,
		`SELECT count(*) FROM historian_late_write WHERE signal_id LIKE $1`, prefix+"-%").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 20 {
		t.Fatalf("marks = %d, want one per signal (20)", n)
	}
}
