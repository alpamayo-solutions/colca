package historian

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type closeLockPool struct {
	marksPool
	tx *closeLockTx
}

func (p *closeLockPool) Begin(context.Context) (pgx.Tx, error) { return p.tx, nil }

type closeLockTx struct {
	fakeTx
	locked bool
	wrote  bool
	fail   error
}

func (tx *closeLockTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if sql == "LOCK TABLE historian_late_write IN ROW EXCLUSIVE MODE" {
		if tx.fail != nil {
			return pgconn.CommandTag{}, tx.fail
		}
		tx.locked = true
	}
	if sql == insertMetric || sql == insertRetraction {
		tx.wrote = true
		if !tx.locked {
			return pgconn.CommandTag{}, errors.New("samples written before the closing lock")
		}
	}
	return tx.fakeTx.Exec(ctx, sql, args...)
}

func (tx *closeLockTx) SendBatch(_ context.Context, batch *pgx.Batch) pgx.BatchResults {
	results := &fakeBatchResults{}
	for range batch.QueuedQueries {
		results.execs = append(results.execs, func() (pgconn.CommandTag, error) {
			tx.wrote = true
			if !tx.locked {
				return pgconn.CommandTag{}, errors.New("samples written before the closing lock")
			}
			return pgconn.CommandTag{}, nil
		})
	}
	return results
}

// Even a current page, with no late mark statement, must participate in
// closing: its transaction could remain open until its hour becomes due.
func TestCurrentPagesLockBeforeWritingInBothPaths(t *testing.T) {
	for _, rowByRow := range []bool{false, true} {
		t.Run(map[bool]string{false: "batch", true: "row-by-row"}[rowByRow], func(t *testing.T) {
			tx := &closeLockTx{}
			sink := &Sink{Pool: &closeLockPool{tx: tx}}
			value := 1.0
			rows := []Row{{SignalID: "current", Timestamp: time.Now(), Number: &value}}
			var err error
			if rowByRow {
				_, err = sink.applyRowByRow(context.Background(), rows, Consumer, 1, "", true)
			} else {
				err = sink.applyBatch(context.Background(), rows, Consumer, 1, "", true)
			}
			if err != nil || !tx.locked || !tx.committed {
				t.Fatalf("err=%v locked=%v committed=%v", err, tx.locked, tx.committed)
			}
		})
	}
}

func TestFailedClosingLockWritesNeitherSamplesNorOffset(t *testing.T) {
	boom := errors.New("closing lock failed")
	tx := &closeLockTx{fail: boom}
	sink := &Sink{Pool: &closeLockPool{tx: tx}}
	if err := sink.applyBatch(context.Background(), []Row{lateRow("a")}, Consumer, 1, "", true); !errors.Is(err, boom) {
		t.Fatalf("err=%v, want the failed lock", err)
	}
	if tx.wrote || tx.committed || !tx.rolledBack {
		t.Fatalf("wrote=%v committed=%v rolledBack=%v", tx.wrote, tx.committed, tx.rolledBack)
	}
}
