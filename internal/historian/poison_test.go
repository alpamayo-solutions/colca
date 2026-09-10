package historian

// These tests cover a row the schema will never accept, such as a signal_id
// longer than historian_metric's varchar(26). Applied as one batch, such a row
// would fail every pass and stop the cursor, so nothing would be historised
// again. The fake transaction runs the retry logic without a database;
// boundary_test.go covers the same against Postgres.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ---- fakes ----------------------------------------------------------------

// fakeBatchResults plays back one closure per queued statement, in order.
type fakeBatchResults struct {
	execs []func() (pgconn.CommandTag, error)
	i     int
}

func (r *fakeBatchResults) Exec() (pgconn.CommandTag, error) {
	if r.i >= len(r.execs) {
		return pgconn.CommandTag{}, fmt.Errorf("fakeBatchResults: no more queued statements")
	}
	fn := r.execs[r.i]
	r.i++
	return fn()
}
func (r *fakeBatchResults) Query() (pgx.Rows, error) {
	panic("fakeBatchResults.Query: not used by Sink.Apply")
}
func (r *fakeBatchResults) QueryRow() pgx.Row {
	panic("fakeBatchResults.QueryRow: not used by Sink.Apply")
}
func (r *fakeBatchResults) Close() error { return nil }

// fakeTx fakes the pgx.Tx methods Sink.Apply uses. Any other method panics
// through the embedded nil interface, which means the fake needs to grow.
type fakeTx struct {
	pgx.Tx

	sendBatch func(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	// execRow decides one insertMetric during the row-by-row retry from its
	// arguments; the last one is the signal ID. nil means every row succeeds.
	execRow func(args []any) error

	committed  bool
	rolledBack bool
}

func (f *fakeTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return f.sendBatch(ctx, b)
}

func (f *fakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if sql != insertMetric {
		// Savepoints and the marker upsert always succeed here.
		return pgconn.CommandTag{}, nil
	}
	if f.execRow != nil {
		if err := f.execRow(args); err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	return pgconn.CommandTag{}, nil
}

func (f *fakeTx) Commit(ctx context.Context) error   { f.committed = true; return nil }
func (f *fakeTx) Rollback(ctx context.Context) error { f.rolledBack = true; return nil }

// fakePool fakes dbPool. The first Begin is applyBatch's transaction, the second
// applyRowByRow's.
type fakePool struct {
	begin func(attempt int) (pgx.Tx, error)
	calls int
}

func (p *fakePool) Begin(ctx context.Context) (pgx.Tx, error) {
	p.calls++
	return p.begin(p.calls)
}
func (p *fakePool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	panic("fakePool.Exec: not used by Sink.Apply")
}

// QueryRow backs Sink.Applied and always reports that there is no marker yet.
func (p *fakePool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return fakeNoRowsRow{}
}

type fakeNoRowsRow struct{}

func (fakeNoRowsRow) Scan(dest ...any) error { return pgx.ErrNoRows }

// batchTxFailingAt builds the batch transaction: every statement succeeds except
// the one at failAt (negative means none). Position len(rows) is the marker
// upsert.
func batchTxFailingAt(failAt int, failErr error) *fakeTx {
	return &fakeTx{
		sendBatch: func(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
			n := b.Len()
			execs := make([]func() (pgconn.CommandTag, error), n)
			for i := range execs {

				execs[i] = func() (pgconn.CommandTag, error) {
					if i == failAt {
						return pgconn.CommandTag{}, failErr
					}
					return pgconn.CommandTag{}, nil
				}
			}
			return &fakeBatchResults{execs: execs}
		},
	}
}

func poisonRow(signalID string) Row {
	return Row{SignalID: signalID, Offset: 1, Topic: "colca/v1/_Metric/m1/press3/temp"}
}

// ---- the pins ---------------------------------------------------------

// A page with one over-long signal_id: the valid rows land and the rejection is
// counted as value_too_long. Without the row-by-row fallback nothing would be
// written.
func TestAPoisonedRowLandsTheRestAndIsCounted(t *testing.T) {
	tooLong := poisonRow("this-signal-id-is-nowhere-near-a-valid-ulid-length")
	rows := []Row{
		{SignalID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Offset: 1},
		tooLong,
		{SignalID: "01ARZ3NDEKTSV4RRFFQ69G5FAW", Offset: 3},
	}
	pgErr := &pgconn.PgError{Code: "22001", Message: "value too long for type character varying(26)"}

	pool := &fakePool{}
	pool.begin = func(attempt int) (pgx.Tx, error) {
		switch attempt {
		case 1:
			return batchTxFailingAt(1, pgErr), nil // rows[1] is the poison row
		case 2:
			return &fakeTx{execRow: func(args []any) error {
				if signalID, _ := args[len(args)-1].(string); signalID == tooLong.SignalID {
					return pgErr
				}
				return nil
			}}, nil
		default:
			t.Fatalf("unexpected Begin call #%d — the algorithm should need exactly 2", attempt)
			return nil, nil
		}
	}

	sink := &Sink{Pool: pool}
	rejections, err := sink.Apply(context.Background(), rows, "test:poison", 99)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(rejections) != 1 {
		t.Fatalf("got %d rejections, want 1: %+v", len(rejections), rejections)
	}
	if rejections[0].Reason != "value_too_long" || rejections[0].SQLState != "22001" {
		t.Fatalf("rejection = %+v, want reason=value_too_long sqlstate=22001", rejections[0])
	}
	if rejections[0].Row.SignalID != tooLong.SignalID {
		t.Fatalf("rejected row = %q, want the poisoned one %q", rejections[0].Row.SignalID, tooLong.SignalID)
	}
	if pool.calls != 2 {
		t.Fatalf("Begin called %d times, want 2 (batch, then row-by-row)", pool.calls)
	}
}

// A transient batch failure is returned as an error and does not trigger the
// row-by-row fallback: it is retried, never skipped.
func TestATransientFailureIsRetriedNotIsolated(t *testing.T) {
	rows := []Row{{SignalID: "01ARZ3NDEKTSV4RRFFQ69G5FAV"}, {SignalID: "01ARZ3NDEKTSV4RRFFQ69G5FAW"}}
	transient := errors.New("timescale is down")

	pool := &fakePool{}
	pool.begin = func(attempt int) (pgx.Tx, error) {
		if attempt != 1 {
			t.Fatalf("unexpected Begin call #%d — a transient failure must not retry row by row", attempt)
		}
		return batchTxFailingAt(0, transient), nil
	}

	sink := &Sink{Pool: pool}
	rejections, err := sink.Apply(context.Background(), rows, "test:transient", 5)
	if err == nil {
		t.Fatal("a transient batch failure reported success")
	}
	if rejections != nil {
		t.Fatalf("rejections = %v on a transient failure, want nil — nothing was isolated", rejections)
	}
	if pool.calls != 1 {
		t.Fatalf("Begin called %d times, want 1 (no row-by-row fallback for a transient error)", pool.calls)
	}
}

// A Postgres error outside the poison list is transient too: classification is
// by code.
func TestAPgErrorOutsideThePoisonListIsStillTransient(t *testing.T) {
	rows := []Row{{SignalID: "01ARZ3NDEKTSV4RRFFQ69G5FAV"}}
	// 40001: serialization_failure — a concurrency conflict, retryable.
	serializationFailure := &pgconn.PgError{Code: "40001", Message: "could not serialize access"}

	pool := &fakePool{}
	pool.begin = func(attempt int) (pgx.Tx, error) {
		if attempt != 1 {
			t.Fatalf("unexpected Begin call #%d", attempt)
		}
		return batchTxFailingAt(0, serializationFailure), nil
	}

	sink := &Sink{Pool: pool}
	if _, err := sink.Apply(context.Background(), rows, "test:serialization", 1); err == nil {
		t.Fatal("a serialization failure reported success")
	}
	if pool.calls != 1 {
		t.Fatalf("Begin called %d times, want 1", pool.calls)
	}
}

// With every row valid, the plain batch path writes everything with no
// rejections and no fallback. Without this, the test above would also pass
// against a sink that always falls back.
func TestAllValidRowsLandWithNoRejectionsAndNoFallback(t *testing.T) {
	rows := []Row{
		{SignalID: "01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{SignalID: "01ARZ3NDEKTSV4RRFFQ69G5FAW"},
	}
	pool := &fakePool{}
	pool.begin = func(attempt int) (pgx.Tx, error) {
		if attempt != 1 {
			t.Fatalf("unexpected Begin call #%d — nothing failed, so no fallback should run", attempt)
		}
		return batchTxFailingAt(-1, nil), nil
	}

	sink := &Sink{Pool: pool}
	rejections, err := sink.Apply(context.Background(), rows, "test:clean", 2)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(rejections) != 0 {
		t.Fatalf("rejections = %v, want none", rejections)
	}
	if pool.calls != 1 {
		t.Fatalf("Begin called %d times, want 1 (the plain batch path)", pool.calls)
	}
}

// ---- Bridge-level: the cursor actually advances -----------------------

// A page containing a rejected row still acks the door's cursor past it, so
// the page is not fetched again.
func TestAPoisonedRowStillAdvancesTheDoorCursor(t *testing.T) {
	tooLong := "this-signal-id-is-nowhere-near-a-valid-ulid-length"
	pgErr := &pgconn.PgError{Code: "22001", Message: "value too long for type character varying(26)"}

	pool := &fakePool{}
	pool.begin = func(attempt int) (pgx.Tx, error) {
		switch attempt {
		case 1:
			return batchTxFailingAt(1, pgErr), nil // the 2nd of 2 rows is poisoned
		case 2:
			return &fakeTx{execRow: func(args []any) error {
				if signalID, _ := args[len(args)-1].(string); signalID == tooLong {
					return pgErr
				}
				return nil
			}}, nil
		default:
			t.Fatalf("unexpected Begin call #%d", attempt)
			return nil, nil
		}
	}
	sink := &Sink{Pool: pool}

	d := &fakeDoor{pages: []door.Page{page(3,
		door.Record{Offset: 1, Topic: "colca/v1/_Metric/n1/press3/temp",
			Payload: json.RawMessage(`{"signal_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","value":1}`), TS: 1},
		door.Record{Offset: 2, Topic: "colca/v1/_Metric/n1/press3/temp",
			Payload: json.RawMessage(fmt.Sprintf(`{"signal_id":%q,"value":2}`, tooLong)), TS: 1},
	)}}
	bridge := &Bridge{Door: d, Store: sink}

	written, err := bridge.Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v — the poisoned row must not wedge the page", err)
	}
	if written != 1 {
		t.Fatalf("wrote %d rows, want 1 (only the valid one)", written)
	}
	if len(d.acked) != 1 || d.acked[0] != 2 {
		t.Fatalf("acked %v, want [2] — the cursor must advance past the whole page, "+
			"poisoned row included, or the same page is re-fetched forever", d.acked)
	}
	if got := bridge.Rejected("value_too_long"); got != 1 {
		t.Fatalf("Rejected(value_too_long) = %d, want 1", got)
	}
}
