//go:build boundary

package historian

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alpamayo-solutions/colca/door"
)

// Against a real Timescale: a pipelined drain over four connections, crashed
// part-way and restarted, leaves historian_metric exactly as one page at a
// time would, values, retractions, rewritten keys and late data included.
func TestAPipelinedDrainCrashedAndRestartedLandsAsOnePageAtATime(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)
	sink.Writers = 4
	if _, err := sink.Pool.Exec(ctx, `DELETE FROM colca_applied_offset WHERE consumer LIKE 'historian:metrics%'`); err != nil {
		t.Fatal(err)
	}
	prefix := sigID("pipe") + "-"
	s := newStreamNamed(3000, 17, prefix)
	var signal1, signal2 door.Signal

	b := &Bridge{Door: s, Store: sink, Max: 100, Pipeline: 4, Changes: signal1.Changes}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- b.Run(runCtx) }()
	time.Sleep(40 * time.Millisecond) // crash part-way through the drain
	cancel()
	<-done
	// A commit the crashed run abandoned may still finish on the server; a
	// restart reads the markers after it does (a real restart takes longer
	// than a commit). Wait until no other backend is busy.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var busy int
		if err := sink.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid() AND state <> 'idle'`).Scan(&busy); err != nil {
			t.Fatal(err)
		}
		if busy == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d backends still busy after the crash", busy)
		}
		time.Sleep(10 * time.Millisecond)
	}
	marker, err := sink.Applied(ctx, Consumer)
	if err != nil {
		t.Fatal(err)
	}
	if marker >= s.head() {
		t.Fatalf("the drain finished before the crash (marker %d); the test proves nothing", marker)
	}
	if acked := s.lastAck(); acked > marker {
		t.Fatalf("acked %d past the marker %d", acked, marker)
	}

	b = &Bridge{Door: s, Store: sink, Max: 100, Pipeline: 4, Changes: signal2.Changes}
	runToHead(t, b, s)

	rows, err := sink.Pool.(interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	}).Query(ctx, `SELECT signal_id, timestamp, value_number FROM historian_metric WHERE signal_id LIKE $1`, prefix+"%")
	if err != nil {
		t.Fatal(err)
	}
	got := table{}
	for rows.Next() {
		var sig string
		var ts time.Time
		var v *float64
		if err := rows.Scan(&sig, &ts, &v); err != nil {
			t.Fatal(err)
		}
		got[key{sig, ts.UnixMilli()}] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := sequential(s).String(); got.String() != want {
		t.Fatalf("historian_metric differs from a one-page-at-a-time run\n got %s\nwant %s",
			strings.TrimSpace(got.String()), strings.TrimSpace(want))
	}
	if marker, err := sink.Applied(ctx, Consumer); err != nil || marker != s.head() {
		t.Fatalf("marker %d, %v; want %d", marker, err, s.head())
	}
}

// Against a real Timescale: colcad's volume is recreated while the historian
// runs. The partition markers are read and zeroed with the sink's own SQL,
// and both streams end up in historian_metric as one page at a time leaves
// them.
func TestANewStreamWhileRunningAgainstTimescale(t *testing.T) {
	ctx := context.Background()
	sink := testPool(t)
	sink.Writers = 4
	if _, err := sink.Pool.Exec(ctx, `DELETE FROM colca_applied_offset WHERE consumer LIKE 'historian:metrics%'`); err != nil {
		t.Fatal(err)
	}
	prefixA, prefixB := sigID("reset-a")+"-", sigID("reset-b")+"-"
	s := newStreamNamed(800, 9, prefixA)
	oldRecs := s.recs
	var signal door.Signal
	b := &Bridge{Door: s, Store: sink, Max: 100, Pipeline: 4, Changes: signal.Changes}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- b.Run(runCtx) }()
	waitAcked := func(what string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for s.lastAck() != s.head() {
			if time.Now().After(deadline) {
				cancel()
				t.Fatalf("%s: acked %d of %d", what, s.lastAck(), s.head())
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitAcked("old stream")
	markers, err := sink.PartitionMarkers(ctx, Consumer)
	if err != nil || len(markers) != 4 {
		t.Fatalf("partition markers %v, %v; want four", markers, err)
	}

	fresh := newStreamNamed(450, 7, prefixB)
	s.replace(fresh.recs)
	signal.Notify()
	waitAcked("new stream")
	cancel()
	<-done

	markers, err = sink.PartitionMarkers(ctx, Consumer)
	if err != nil {
		t.Fatal(err)
	}
	for name, m := range markers {
		if m > 450 {
			t.Fatalf("%s at %d, beyond the new stream", name, m)
		}
	}
	got := table{}
	rows, err := sink.Pool.(interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	}).Query(ctx, `SELECT signal_id, timestamp, value_number FROM historian_metric WHERE signal_id LIKE $1 OR signal_id LIKE $2`,
		prefixA+"%", prefixB+"%")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var sig string
		var ts time.Time
		var v *float64
		if err := rows.Scan(&sig, &ts, &v); err != nil {
			t.Fatal(err)
		}
		got[key{sig, ts.UnixMilli()}] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if want := oracle(oldRecs, fresh.recs).String(); got.String() != want {
		t.Fatalf("historian_metric differs from one page at a time\n got %s\nwant %s",
			strings.TrimSpace(got.String()), strings.TrimSpace(want))
	}
	if marker, err := sink.Applied(ctx, Consumer); err != nil || marker != 450 {
		t.Fatalf("marker %d, %v; want 450", marker, err)
	}
}
