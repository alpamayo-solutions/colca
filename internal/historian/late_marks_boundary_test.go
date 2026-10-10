//go:build boundary

package historian

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The marks against Postgres: what the statement calls late, that the marks
// commit with the rows and never hold a page up, and that the reader is woken.
// testPool creates the relations as their owner (PREKIT's migrations) does;
// wanted lists the signals this test asks marks for.
func markingSink(t *testing.T, wanted ...string) *Sink {
	t.Helper()
	sink := testPool(t)
	for _, signalID := range wanted {
		if _, err := sink.Pool.Exec(context.Background(),
			`INSERT INTO historian_late_write_signal VALUES ($1) ON CONFLICT DO NOTHING`, signalID); err != nil {
			t.Fatal(err)
		}
	}
	return sink
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// marksOf returns the hour of every mark row that names signalID, in the order
// they were written: an hour appears once per page that marked it.
func marksOf(t *testing.T, q querier, signalID string) []time.Time {
	t.Helper()
	rows, err := q.Query(context.Background(),
		`SELECT hour FROM historian_late_write WHERE signal_ids ? $1 ORDER BY id`, signalID)
	if err != nil {
		t.Fatal(err)
	}
	hours, err := pgx.CollectRows(rows, pgx.RowTo[time.Time])
	if err != nil {
		t.Fatal(err)
	}
	for i := range hours {
		hours[i] = hours[i].UTC()
	}
	return hours
}

func number(v float64) *float64 { return &v }

func TestALatePageMarksEachLateHourOnceWithItsSignals(t *testing.T) {
	ctx := context.Background()
	late, other, current, unlisted := sigID("late"), sigID("other"), sigID("current"), sigID("unlisted")
	sink := markingSink(t, late, other, current)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-5 * time.Hour)
	rows := []Row{
		{SignalID: late, NodeID: "n1", Timestamp: hour.Add(time.Minute), Number: number(1)},
		{SignalID: late, NodeID: "n1", Timestamp: hour.Add(2 * time.Minute), Number: number(2)},
		{SignalID: other, NodeID: "n1", Timestamp: hour.Add(3 * time.Minute), Number: number(2)},
		{SignalID: late, NodeID: "n1", Timestamp: hour.Add(61 * time.Minute), Number: number(3)},
		{SignalID: late, NodeID: "n1", Timestamp: hour.Add(62 * time.Minute)}, // a retraction is a write too
		{SignalID: current, NodeID: "n1", Timestamp: time.Now().UTC(), Number: number(4)},
		// Late, but nobody asked for marks of this signal.
		{SignalID: unlisted, NodeID: "n1", Timestamp: hour.Add(time.Minute), Number: number(5)},
	}
	if _, err := sink.Apply(ctx, rows, "test:late", 1, ""); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := marksOf(t, sink.Pool.(querier), late); !slices.Equal(got, []time.Time{hour, hour.Add(time.Hour)}) &&
		!slices.Equal(got, []time.Time{hour.Add(time.Hour), hour}) {
		t.Fatalf("marks of the late signal = %v, want %v and %v once each", got, hour, hour.Add(time.Hour))
	}
	var signals []string
	if err := sink.Pool.QueryRow(ctx, `SELECT signal_ids FROM historian_late_write WHERE hour = $1 AND signal_ids ? $2`,
		hour, late).Scan(&signals); err != nil {
		t.Fatal(err)
	}
	if want := []string{late, other}; !slices.Equal(signals, want) && !slices.Equal(signals, []string{other, late}) {
		t.Fatalf("the hour's mark names %v, want %v", signals, want)
	}
	if got := marksOf(t, sink.Pool.(querier), current); len(got) != 0 {
		t.Fatalf("a current sample was marked: %v", got)
	}
	if got := marksOf(t, sink.Pool.(querier), unlisted); len(got) != 0 {
		t.Fatalf("a signal that is not listed was marked: %v", got)
	}

	// Every page that writes into the hour late adds its own mark: nothing is
	// looked up or locked, the reader merges them.
	more := []Row{{SignalID: late, NodeID: "n1", Timestamp: hour.Add(5 * time.Minute), Number: number(5)}}
	if _, err := sink.Apply(ctx, more, "test:late", 2, ""); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := marksOf(t, sink.Pool.(querier), late); len(got) != 3 || !got[2].Equal(hour) {
		t.Fatalf("marks after another page into the hour = %v, want a third for %v", got, hour)
	}
}

// A reader that is handling marks (it has deleted them and not committed yet)
// does not hold a page up: the page appends its own mark and commits. And a
// page's mark is not visible before the page commits, so a reader never sees
// a mark whose samples it cannot read.
func TestAPageNeitherWaitsForTheReaderNorShowsItsMarkEarly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	signalID := sigID("free")
	sink := markingSink(t, signalID)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-8 * time.Hour)
	first := []Row{{SignalID: signalID, NodeID: "n1", Timestamp: hour.Add(time.Minute), Number: number(1)}}
	if _, err := sink.Apply(ctx, first, "test:late", 1, ""); err != nil {
		t.Fatal(err)
	}

	reader, err := sink.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Rollback(context.Background()) }()
	if _, err := reader.Exec(ctx, `DELETE FROM historian_late_write WHERE signal_ids ? $1`, signalID); err != nil {
		t.Fatal(err)
	}

	page, err := sink.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = page.Rollback(context.Background()) }()
	if _, err := page.Exec(ctx, insertMetric, hour.Add(2*time.Minute), nil, number(2), nil, nil, "n1", signalID); err != nil {
		t.Fatal(err)
	}
	quick, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	if _, err := page.Exec(quick, markLateWrites, []string{signalID}, []time.Time{hour}); err != nil {
		t.Fatalf("the page waited for the reader: %v", err)
	}
	if got := marksOf(t, reader, signalID); len(got) != 0 {
		t.Fatalf("the reader sees marks it deleted or a page has not committed: %v", got)
	}
	if err := page.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reader.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := marksOf(t, sink.Pool.(querier), signalID); len(got) != 1 || !got[0].Equal(hour) {
		t.Fatalf("marks after both committed = %v, want the page's mark for %v", got, hour)
	}
}

// The reader closes an hour once it is due and every transaction that was in
// flight then has ended (docs/operations.md). That holds a page that decided
// "not late" because the page has its transaction id from its first row,
// before the mark statement decides: it is older than any id the reader takes
// after the decision, and stays in flight for the reader until it commits.
func TestAPageThatHasWrittenIsInFlightForTheReaderUntilItCommits(t *testing.T) {
	ctx := context.Background()
	signalID := sigID("inflight")
	sink := markingSink(t, signalID)
	page, err := sink.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = page.Rollback(context.Background()) }()
	now := time.Now().UTC()
	if _, err := page.Exec(ctx, insertMetric, now, nil, number(1), nil, nil, "n1", signalID); err != nil {
		t.Fatal(err)
	}
	// Current: the statement decides it is not late and marks nothing.
	if _, err := page.Exec(ctx, markLateWrites, []string{signalID}, []time.Time{now.Truncate(time.Hour)}); err != nil {
		t.Fatal(err)
	}

	// The reader takes a transaction id once the hour is due; every page
	// that decided before then has a smaller one. It waits until the oldest
	// transaction still running is its own or younger.
	reader, err := sink.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Rollback(context.Background()) }()
	var due string
	if err := reader.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&due); err != nil {
		t.Fatal(err)
	}
	ended := func() bool {
		t.Helper()
		var done bool
		if err := reader.QueryRow(ctx,
			`SELECT pg_snapshot_xmin(pg_current_snapshot()) >= $1::xid8`, due).Scan(&done); err != nil {
			t.Fatal(err)
		}
		return done
	}
	if ended() {
		t.Fatal("the reader sees no page in flight although one has written and not committed")
	}
	if err := page.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if !ended() {
		t.Fatal("the page committed and the reader still waits for it")
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
	got := marksOf(t, sink.Pool.(querier), signalID)
	if len(got) != len(want) {
		t.Fatalf("marks = %v, want %v (now %v)", got, want, now)
	}
	for _, h := range got {
		if !want[h] {
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
	if got := marksOf(t, listener, signalID); len(got) != 1 {
		t.Fatalf("marks visible to the listener = %v", got)
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
	var n, marks int
	if err := sink.Pool.QueryRow(ctx, `
        SELECT count(DISTINCT s), count(DISTINCT w.id)
        FROM historian_late_write w, jsonb_array_elements_text(w.signal_ids) AS s
        WHERE s LIKE $1`, prefix+"-%").Scan(&n, &marks); err != nil {
		t.Fatal(err)
	}
	if n != 20 || marks < 1 || marks > sink.Writers {
		t.Fatalf("signals marked = %d in %d marks, want all 20 in at most one mark per share", n, marks)
	}
}
