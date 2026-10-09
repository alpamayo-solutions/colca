package historian

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func at(base time.Time, d time.Duration) time.Time { return base.Add(d) }

// What is offered to the statement: each (signal, hour) of the page once, and
// only hours that can be late. The statement decides exactly, so the cut here
// is early by lateSlack, never late.
func TestLateWritesOffersEachLateHourOfASignalOnce(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 30, 0, 0, time.UTC)
	hour := func(h int) time.Time { return time.Date(2026, 10, 9, h, 0, 0, 0, time.UTC) }
	rows := []Row{
		{SignalID: "a", Timestamp: at(hour(9), 5*time.Minute)},
		{SignalID: "a", Timestamp: at(hour(9), 50*time.Minute)}, // same hour: one mark
		{SignalID: "a", Timestamp: at(hour(8), time.Second)},
		{SignalID: "b", Timestamp: at(hour(9), time.Minute)},
		// The current hour cannot be late.
		{SignalID: "b", Timestamp: at(hour(12), 29*time.Minute)},
	}
	signals, hours := lateWrites(rows, now)
	type mark struct {
		signal string
		hour   time.Time
	}
	got := map[mark]int{}
	for i := range signals {
		got[mark{signals[i], hours[i]}]++
	}
	want := map[mark]int{{"a", hour(9)}: 1, {"a", hour(8)}: 1, {"b", hour(9)}: 1}
	if len(got) != len(want) {
		t.Fatalf("marks = %v, want %v", got, want)
	}
	for m, n := range want {
		if got[m] != n {
			t.Fatalf("mark %v offered %d times, want %d (all: %v)", m, got[m], n, got)
		}
	}
}

// An hour is offered from lateSlack before it is late, so a clock running that
// much behind the database's cannot keep a late sample from the statement.
func TestLateWritesOffersAnHourBeforeItIsLateByTheSlack(t *testing.T) {
	hour := time.Date(2026, 10, 9, 9, 0, 0, 0, time.UTC)
	rows := []Row{{SignalID: "a", Timestamp: hour.Add(time.Minute)}}
	if signals, _ := lateWrites(rows, hour.Add(lateAfter-lateSlack-time.Second)); len(signals) != 0 {
		t.Fatalf("offered before the slack: %v", signals)
	}
	if signals, _ := lateWrites(rows, hour.Add(lateAfter-lateSlack)); len(signals) != 1 {
		t.Fatalf("not offered within the slack: %v", signals)
	}
}

// Hours are UTC hours whatever zone the sample's time carries.
func TestLateWritesUsesUTCHours(t *testing.T) {
	zone := time.FixedZone("half", 5*3600+1800)
	sample := time.Date(2026, 10, 9, 10, 15, 0, 0, zone) // 04:45 UTC
	_, hours := lateWrites([]Row{{SignalID: "a", Timestamp: sample}}, sample.Add(3*time.Hour))
	if want := time.Date(2026, 10, 9, 4, 0, 0, 0, time.UTC); len(hours) != 1 || !hours[0].Equal(want) {
		t.Fatalf("hours = %v, want %v", hours, want)
	}
}

// marksPool answers the table question and records the batches it is sent.
type marksPool struct {
	exists  bool
	looks   int
	batches [][]string
	// failMarks, when set, is the error the mark statement returns once.
	failMarks error
}

type existsRow struct{ exists bool }

func (r existsRow) Scan(dest ...any) error {
	*(dest[0].(*bool)) = r.exists
	return nil
}

func (p *marksPool) Begin(context.Context) (pgx.Tx, error) {
	return &fakeTx{sendBatch: func(_ context.Context, b *pgx.Batch) pgx.BatchResults {
		var queued []string
		results := &fakeBatchResults{}
		for _, q := range b.QueuedQueries {
			queued = append(queued, q.SQL)
			fail := error(nil)
			if q.SQL == markLateWrites && p.failMarks != nil {
				fail, p.failMarks = p.failMarks, nil
			}
			results.execs = append(results.execs, func() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, fail })
		}
		p.batches = append(p.batches, queued)
		return results
	}}, nil
}

func (p *marksPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (p *marksPool) QueryRow(context.Context, string, ...any) pgx.Row {
	p.looks++
	return existsRow{p.exists}
}

func lateRow(signal string) Row {
	v := 1.0
	return Row{SignalID: signal, Timestamp: time.Now().Add(-3 * time.Hour), Number: &v}
}

func marked(batch []string) bool {
	for _, sql := range batch {
		if sql == markLateWrites {
			return true
		}
	}
	return false
}

// A sink that was never told to look writes no marks: a database without the
// table must see exactly the statements it saw before.
func TestASinkThatDoesNotWatchWritesNoMarks(t *testing.T) {
	pool := &marksPool{exists: true}
	sink := &Sink{Pool: pool}
	if _, err := sink.Apply(context.Background(), []Row{lateRow("a")}, Consumer, 1, ""); err != nil {
		t.Fatal(err)
	}
	if pool.looks != 0 || marked(pool.batches[0]) {
		t.Fatalf("looks=%d batch=%v", pool.looks, pool.batches[0])
	}
}

// With the table, a late page marks in the transaction that writes its rows,
// before the marker moves; a page of current samples queues no mark statement.
func TestAWatchingSinkMarksLatePagesInTheirTransaction(t *testing.T) {
	pool := &marksPool{exists: true}
	sink := &Sink{Pool: pool}
	var heard []bool
	if err := sink.WatchLateWrites(context.Background(), func(on bool) { heard = append(heard, on) }); err != nil {
		t.Fatal(err)
	}
	if !sink.MarksLateWrites() || len(heard) != 1 || !heard[0] {
		t.Fatalf("marks=%v heard=%v", sink.MarksLateWrites(), heard)
	}
	if _, err := sink.Apply(context.Background(), []Row{lateRow("a")}, Consumer, 1, ""); err != nil {
		t.Fatal(err)
	}
	batch := pool.batches[0]
	if len(batch) != 3 || batch[0] != insertMetrics || batch[1] != markLateWrites || batch[2] != upsertOffset {
		t.Fatalf("batch = %v", batch)
	}

	v := 2.0
	current := []Row{{SignalID: "a", Timestamp: time.Now(), Number: &v}}
	if _, err := sink.Apply(context.Background(), current, Consumer, 2, ""); err != nil {
		t.Fatal(err)
	}
	if marked(pool.batches[1]) {
		t.Fatalf("a current page queued a mark statement: %v", pool.batches[1])
	}
}

// Without the table the sink looks again when it writes, at most once per
// lateMarkRecheck, and starts marking when the table has appeared.
func TestASinkWithoutTheTableLooksAgainWhenItWrites(t *testing.T) {
	pool := &marksPool{}
	sink := &Sink{Pool: pool}
	var heard []bool
	if err := sink.WatchLateWrites(context.Background(), func(on bool) { heard = append(heard, on) }); err != nil {
		t.Fatal(err)
	}
	apply := func(offset int64) {
		t.Helper()
		if _, err := sink.Apply(context.Background(), []Row{lateRow("a")}, Consumer, offset, ""); err != nil {
			t.Fatal(err)
		}
	}
	apply(1)
	if pool.looks != 1 || marked(pool.batches[0]) {
		t.Fatalf("looked again within the interval: looks=%d batch=%v", pool.looks, pool.batches[0])
	}

	pool.exists = true
	sink.marks.mu.Lock()
	sink.marks.checked = time.Now().Add(-lateMarkRecheck)
	sink.marks.mu.Unlock()
	apply(2)
	if pool.looks != 2 || !marked(pool.batches[1]) {
		t.Fatalf("looks=%d batch=%v", pool.looks, pool.batches[1])
	}
	if len(heard) != 1 || !heard[0] {
		t.Fatalf("heard = %v, want one change to on", heard)
	}
}

// A table that is taken away must not stop the history: the page is written
// again without marks, and the change is announced.
func TestADroppedTableStopsTheMarksNotThePage(t *testing.T) {
	pool := &marksPool{exists: true, failMarks: &pgconn.PgError{Code: undefinedTable}}
	sink := &Sink{Pool: pool}
	var heard []bool
	if err := sink.WatchLateWrites(context.Background(), func(on bool) { heard = append(heard, on) }); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Apply(context.Background(), []Row{lateRow("a")}, Consumer, 1, ""); err != nil {
		t.Fatalf("the page failed: %v", err)
	}
	if len(pool.batches) != 2 || !marked(pool.batches[0]) || marked(pool.batches[1]) {
		t.Fatalf("batches = %v", pool.batches)
	}
	if sink.MarksLateWrites() || len(heard) != 2 || heard[1] {
		t.Fatalf("marks=%v heard=%v", sink.MarksLateWrites(), heard)
	}
}

// Any other failure of the mark statement fails the page, which is retried:
// rows must not commit without their marks.
func TestAFailedMarkFailsThePage(t *testing.T) {
	boom := errors.New("connection reset")
	pool := &marksPool{exists: true, failMarks: boom}
	sink := &Sink{Pool: pool}
	if err := sink.WatchLateWrites(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Apply(context.Background(), []Row{lateRow("a")}, Consumer, 1, ""); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the mark statement's", err)
	}
	if !sink.MarksLateWrites() {
		t.Fatal("a transient failure turned the marks off")
	}
}

// rowsPool refuses one signal's row as poison, so the page is applied row by
// row, and records what the mark statement was given.
type rowsPool struct {
	refuse string
	marked [][]string
	begun  int
}

type rowsTx struct {
	pgx.Tx
	pool  *rowsPool
	batch bool
}

func (p *rowsPool) Begin(context.Context) (pgx.Tx, error) {
	p.begun++
	return &rowsTx{pool: p, batch: p.begun == 1}, nil
}
func (p *rowsPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (p *rowsPool) QueryRow(context.Context, string, ...any) pgx.Row { return existsRow{true} }

func (tx *rowsTx) SendBatch(_ context.Context, b *pgx.Batch) pgx.BatchResults {
	results := &fakeBatchResults{}
	for range b.QueuedQueries {
		results.execs = append(results.execs, func() (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, &pgconn.PgError{Code: "22001"}
		})
	}
	return results
}

func (tx *rowsTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	switch sql {
	case insertMetric:
		if args[len(args)-1] == tx.pool.refuse {
			return pgconn.CommandTag{}, &pgconn.PgError{Code: "22001"}
		}
	case markLateWrites:
		tx.pool.marked = append(tx.pool.marked, args[0].([]string))
	}
	return pgconn.CommandTag{}, nil
}
func (tx *rowsTx) Commit(context.Context) error   { return nil }
func (tx *rowsTx) Rollback(context.Context) error { return nil }

// A page with a refused row is applied row by row. The rows that landed are
// marked in that transaction; the refused row, which wrote nothing, is not.
func TestARowByRowPageMarksTheRowsThatLanded(t *testing.T) {
	pool := &rowsPool{refuse: "refused"}
	sink := &Sink{Pool: pool}
	if err := sink.WatchLateWrites(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	rejections, err := sink.Apply(context.Background(), []Row{lateRow("landed"), lateRow("refused")}, Consumer, 1, "")
	if err != nil || len(rejections) != 1 {
		t.Fatalf("rejections=%d err=%v", len(rejections), err)
	}
	if len(pool.marked) != 1 || len(pool.marked[0]) != 1 || pool.marked[0][0] != "landed" {
		t.Fatalf("marked = %v, want only the row that landed", pool.marked)
	}
}
