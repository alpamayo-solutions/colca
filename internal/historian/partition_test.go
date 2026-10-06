package historian

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// markerPool counts the marker writes the partitioned path makes outside the
// shares' transactions; shares come from begin.
type markerPool struct {
	fakePool
	mu      sync.Mutex
	markers int
}

func (p *markerPool) Begin(ctx context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return p.begin(p.calls)
}

func (p *markerPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.markers++
	return pgconn.CommandTag{}, nil
}

func manySignals(n int) []Row {
	v := 1.0
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{SignalID: fmt.Sprintf("sig-%03d", i), Number: &v, Offset: int64(i + 1)}
	}
	return rows
}

// A share that fails transiently fails the page: the marker does not move, so
// the whole page is retried, and the shares that did commit replay harmlessly.
func TestAFailedShareFailsThePageAndLeavesTheMarker(t *testing.T) {
	var failures atomic.Int64
	pool := &markerPool{fakePool: fakePool{begin: func(attempt int) (pgx.Tx, error) {
		if attempt == 2 {
			failures.Add(1)
			return batchTxFailingAt(0, errors.New("connection reset")), nil
		}
		return batchTxFailingAt(-1, nil), nil
	}}}
	sink := &Sink{Pool: pool, Writers: 4}
	if _, err := sink.Apply(context.Background(), manySignals(40), Consumer, 40); err == nil {
		t.Fatal("a page with a failed share reported success")
	}
	if failures.Load() != 1 || pool.markers != 0 {
		t.Fatalf("failures %d markers %d: the marker must not move past a failed share", failures.Load(), pool.markers)
	}

	pool.calls = 0
	pool.begin = func(int) (pgx.Tx, error) { return batchTxFailingAt(-1, nil), nil }
	if _, err := sink.Apply(context.Background(), manySignals(40), Consumer, 40); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if pool.calls != 4 || pool.markers != 1 {
		t.Fatalf("retry used %d transactions and %d marker writes, want 4 and 1", pool.calls, pool.markers)
	}
}

func TestASignalAlwaysLandsInTheSameShare(t *testing.T) {
	for _, sig := range []string{"01M46JYMZM2TEFPT0VVAZRRQW2", "a", ""} {
		first := partitionOf(sig, 4)
		for range 3 {
			if partitionOf(sig, 4) != first || first < 0 || first >= 4 {
				t.Fatalf("%q moved between shares", sig)
			}
		}
	}
}

// A poisoned row in one share while another share fails transiently: the
// failed page reports no rejection, the retried page reports it once, so it
// is logged and counted once.
func TestAPoisonedRowIsReportedOnceAcrossARetriedPartitionedPage(t *testing.T) {
	var flaky atomic.Int64
	flaky.Store(1)
	tx := func() pgx.Tx {
		return &fakeTx{
			sendBatch: func(_ context.Context, b *pgx.Batch) pgx.BatchResults {
				var err error
				for _, q := range b.QueuedQueries {
					if len(q.Arguments) == 7 {
						for _, sig := range q.Arguments[6].([]string) {
							if sig == "poison" {
								err = &pgconn.PgError{Code: "22001"}
							}
							if sig == "flaky" && flaky.Add(-1) >= 0 {
								err = errors.New("connection reset")
							}
						}
					}
				}
				execs := make([]func() (pgconn.CommandTag, error), b.Len())
				for i := range execs {
					execs[i] = func() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, err }
				}
				return &fakeBatchResults{execs: execs}
			},
			execRow: func(args []any) error {
				if args[len(args)-1] == "poison" {
					return &pgconn.PgError{Code: "22001"}
				}
				return nil
			},
		}
	}
	pool := &markerPool{fakePool: fakePool{begin: func(int) (pgx.Tx, error) { return tx(), nil }}}
	// Four writers and eight rows: the page is split (a page needs two rows
	// per writer), and poison and flaky land in different shares.
	sink := &Sink{Pool: pool, Writers: 4}
	v := 1.0
	var rows []Row
	for _, sig := range []string{"poison", "flaky", "a", "b", "c", "d", "e", "f"} {
		rows = append(rows, Row{SignalID: sig, Number: &v})
	}
	if partitionOf("poison", 4) == partitionOf("flaky", 4) {
		t.Skip("poison and flaky share a partition; the test needs them apart")
	}
	rej, err := sink.Apply(context.Background(), rows, Consumer, 8)
	if err == nil || len(rej) != 0 {
		t.Fatalf("first attempt: %d rejections, err %v; want none and the transient error", len(rej), err)
	}
	rej, err = sink.Apply(context.Background(), rows, Consumer, 8)
	if err != nil || len(rej) != 1 || rej[0].Row.SignalID != "poison" {
		t.Fatalf("retry: %v, err %v; want the poisoned row once", rej, err)
	}
}
