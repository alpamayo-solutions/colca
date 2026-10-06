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
