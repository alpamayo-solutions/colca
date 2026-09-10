package historian

// The defect this file pins: colca-historian's own log, from the level-4
// historian world (PR #544), repeating every ~5s with a GROWING offset —
//
//	historian pass failed, retrying  err="historian: applying a batch of 36 at
//	offset 36: ERROR: value too long for type character varying(26) (SQLSTATE 22001)"
//	... 46, 56, 66 ...
//
// `historian_metric.signal_id` is varchar(26) (a ULID, correct for
// production); one _Metric on the `metrics` stream carried a longer,
// non-ULID signal_id (a test/demo publisher's mistake). Sink.Apply applied a
// whole page as ONE batch, so that single row failed every pass, the cursor
// (colca_applied_offset AND the door's own fetch cursor — see
// TestAPoisonedRowStillAdvancesTheDoorCursor below) never moved, and NOTHING
// from ANY signal was ever historised again.
//
// pgx.Tx and pgx.BatchResults are interfaces "to allow tests to mock
// transactions" (pgx's own doc comment on Tx) and dbPool (sink.go) narrows
// *pgxpool.Pool to exactly the methods Sink.Apply calls — so the poison/retry
// algorithm that fixes this is provable here, at level 1, against a fake
// transaction with no real database. See boundary_test.go (build tag
// "boundary") for the same claims proven against real Postgres.

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

// fakeBatchResults plays back one closure per queued statement, in order —
// the same sequential-Exec() shape applyBatch drives.
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

// fakeTx fakes exactly the pgx.Tx methods Sink.Apply's two paths use
// (SendBatch for applyBatch; Exec/Commit/Rollback for applyRowByRow). Every
// other pgx.Tx method is promoted from the embedded nil interface, so calling
// one panics loudly instead of silently returning a zero value — a sign this
// fake needs to grow, not a gap to paper over.
type fakeTx struct {
	pgx.Tx

	sendBatch func(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	// execRow classifies one insertMetric attempt during the row-by-row
	// retry, by its bound arguments (the last argument is always
	// row.SignalID — see the parameter order shared by applyBatch and
	// applyRowByRow in sink.go). nil means every row succeeds.
	execRow func(args []any) error

	committed  bool
	rolledBack bool
}

func (f *fakeTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return f.sendBatch(ctx, b)
}

func (f *fakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if sql != insertMetric {
		// SAVEPOINT / RELEASE SAVEPOINT / ROLLBACK TO SAVEPOINT / upsertOffset:
		// none of these tests need any of these to fail.
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

// fakePool fakes dbPool (sink.go). begin is scripted per call: attempt 1 is
// always applyBatch's transaction; attempt 2, reached only after a
// poison-classified batch failure, is applyRowByRow's.
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

// QueryRow backs Sink.Applied, which Bridge.Once calls before Apply. Always
// reporting pgx.ErrNoRows is exactly Applied's "never run yet" case (offset
// 0) — the shape every test in this file needs, since none of them exercise
// the marker-skew handling bridge_test.go already pins.
func (p *fakePool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return fakeNoRowsRow{}
}

type fakeNoRowsRow struct{}

func (fakeNoRowsRow) Scan(dest ...any) error { return pgx.ErrNoRows }

// batchTxFailingAt builds the batch-attempt transaction: every queued
// statement succeeds except the one at position failAt (failAt < 0 means
// nothing fails). Position len(rows) is the marker upsert, exactly like
// applyBatch's own queueing order.
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

// (a) A page with one over-long signal_id among otherwise-valid rows: the
// valid rows land, the marker (and so the door's fetch cursor — see the
// Bridge-level test below) advances PAST the whole page, and the rejection is
// counted with reason value_too_long. This is the exact incident from the
// log line quoted at the top of this file: a batch of otherwise-good rows,
// one of them a non-ULID signal_id too long for varchar(26).
//
// Removing applyRowByRow (making Apply just return applyBatch's error) turns
// this red: written would be 0, err would be non-nil, and rejections nil —
// mutation-checked by hand against sink.go.
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

// (b) A TRANSIENT batch failure (connection lost — no PgError, or a PgError
// code that is not one of sink.go's poison codes) must be returned as an
// error and must NOT trigger the row-by-row fallback: a row rejected for a
// transient reason is a retry, exactly as before this fix, never a skip.
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

// A PgError whose code IS one of Postgres's, but not one sink.go treats as
// poison, must be treated the same as a non-PgError transient failure — the
// classification is by code, not merely "was this a PgError".
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

// (c) The denominator: the same shape with every row valid must still land
// every row via the plain batch path, with zero rejections and no row-by-row
// fallback attempted. Without this, (a) above would pass just as happily
// against a Sink that always falls back to row-by-row, whether or not
// anything was actually poisoned.
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

// The observable that proves the wedge is gone: the DOOR's own fetch cursor
// (Cursor = "c/historian/metrics", acked via Bridge.Once -> Door.Ack) moves
// past a page that contained a rejected row. Before this fix, Store.Apply
// returned an error for such a page, Once returned before ever calling
// Door.Ack, and the SAME page was re-fetched forever — which is exactly the
// growing "batch of 36, then 46, then 56..." in the incident log: offset 0
// was never acked, so every pass re-read the stream from the start and
// failed on the same poisoned record.
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
