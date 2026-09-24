package historian

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpamayo-solutions/colca/door"
)

// Consumer is this service's name in the marker table and its cursor, so
// another follower of the same stream cannot move its progress.
const Consumer = "historian:metrics"

// Cursor is namespaced by service, which is how the door keeps two local
// services from colliding on /ack.
const Cursor = "c/historian/metrics"

const gapContract = "/_StreamGap/"

// Store is what the bridge writes through, an interface so the loop can be
// tested without a database. Apply returns permanently refused rows with a nil
// error: the rest of the page applied and the caller must ack past it. An
// error means nothing applied and the page is retried.
type Store interface {
	Applied(ctx context.Context, consumer string) (int64, error)
	Apply(ctx context.Context, rows []Row, consumer string, offset int64) ([]Rejection, error)
}

// Fetcher is the half of the door the bridge uses.
type Fetcher interface {
	Fetch(ctx context.Context, stream, cursor string, limit int) (door.Page, error)
	Ack(ctx context.Context, stream, cursor string, offset int64) (bool, error)
}

// Bridge follows `metrics` into the hypertables.
type Bridge struct {
	Door  Fetcher
	Store Store
	Log   *slog.Logger

	Max       int
	IdleSleep time.Duration

	// gaps counts pruned ranges. The bridge keeps going: the records are gone and
	// stopping would only add a blackout. Atomic because /metrics reads it from
	// another goroutine.
	gaps atomic.Int64

	// rejected counts permanently refused rows by reason. A sync.Map of counters
	// keeps the zero Bridge usable without a constructor.
	rejected sync.Map // reason string -> *atomic.Int64
}

// Gaps returns how many pruned ranges could not be historised. Safe for
// concurrent use.
func (b *Bridge) Gaps() int64 { return b.gaps.Load() }

// Rejected returns how many rows the schema refused for reason. Safe for
// concurrent use.
func (b *Bridge) Rejected(reason string) int64 {
	v, ok := b.rejected.Load(reason)
	if !ok {
		return 0
	}
	return v.(*atomic.Int64).Load()
}

func (b *Bridge) countRejection(reason string) {
	counter, _ := b.rejected.LoadOrStore(reason, new(atomic.Int64))
	counter.(*atomic.Int64).Add(1)
}

func (b *Bridge) logger() *slog.Logger {
	if b.Log != nil {
		return b.Log
	}
	return slog.Default()
}

func (b *Bridge) max() int {
	if b.Max > 0 {
		return b.Max
	}
	return 500
}

// Once applies at most one page and reports how many rows were written. Rows and
// marker commit together and the ack follows; a crash in between replays the
// page, which the marker makes a no-op.
func (b *Bridge) Once(ctx context.Context) (int, error) {
	_, written, err := b.pass(ctx)
	return written, err
}

// pass is Once that also reports how many records the page held, which is what
// decides whether the stream has more waiting.
func (b *Bridge) pass(ctx context.Context) (fetched, written int, err error) {
	page, err := b.Door.Fetch(ctx, "metrics", Cursor, b.max())
	if err != nil {
		return 0, 0, err
	}
	fetched = len(page.Records)
	if fetched == 0 {
		return 0, 0, nil
	}

	applied, err := b.Store.Applied(ctx, Consumer)
	if err != nil {
		return fetched, 0, err
	}
	first, last := page.Records[0].Offset, page.Records[len(page.Records)-1].Offset

	// Ignore the marker when it cannot be a position in this stream: the page starts
	// at offset 1 while the marker is positive, or the marker is past the page's
	// end. That happens when colcad's data volume is recreated but Timescale keeps
	// the marker, and trusting it would silently drop a page. At worst this
	// re-applies the very first page, which the upsert absorbs.
	if applied > 0 && (first == 1 || applied > last) {
		b.logger().Warn("the applied-offset marker is not a position in this stream — ignoring it",
			"marker", applied, "page_first", first, "page_last", last,
			"detail", "colca's data volume was recreated while Timescale kept the marker "+
				"(or the marker outran the stream). Applying this page in full rather than "+
				"reading it as already durable; the marker is rewritten to this stream's position.")
		applied = 0
	}

	rows := make([]Row, 0, len(page.Records))
	for _, record := range page.Records {
		if record.Offset <= applied {
			continue // already durable: a replay after a crash between commit and ack
		}
		if strings.Contains(record.Topic, gapContract) {
			b.gaps.Add(1)
			b.logger().Error("metrics were pruned before this bridge read them",
				"offset", record.Offset,
				"detail", "history has a hole that cannot be filled: the records are gone from "+
					"the stream. Continuing, because stopping would add a blackout to a hole.")
			continue
		}
		row, err := RowFrom(record.Topic, record.Payload, record.TS)
		if err != nil {
			if errors.Is(err, ErrNotAMeasurement) {
				continue // a tombstone: nothing to historise
			}
			// One bad record must not wedge the stream forever, but it must not
			// vanish either.
			b.logger().Warn("skipping an unhistorisable record",
				"offset", record.Offset, "topic", record.Topic, "err", err)
			continue
		}
		row.Offset = record.Offset
		row.Topic = record.Topic
		rows = append(rows, row)
	}

	rejections, err := b.Store.Apply(ctx, rows, Consumer, last)
	if err != nil {
		return fetched, 0, err
	}
	for _, rej := range rejections {
		b.countRejection(rej.Reason)
		b.logger().Warn("a record was refused by the schema and set aside — the rest of the page still historised",
			"offset", rej.Row.Offset, "topic", rej.Row.Topic,
			"signal_id", truncateForLog(rej.Row.SignalID, 40),
			"sqlstate", rej.SQLState, "reason", rej.Reason, "err", rej.Err)
	}
	// The marker moved past any rejected rows, so ack too, or the page would be
	// fetched forever.
	if _, err := b.Door.Ack(ctx, "metrics", Cursor, last); err != nil {
		// The rows are durable; the next pass re-reads and the marker skips them.
		b.logger().Warn("applied but could not ack", "offset", last, "err", err)
	}
	return fetched, len(rows) - len(rejections), nil
}

// truncateForLog shortens an untrusted value before it goes into a log line.
func truncateForLog(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}

// DefaultIdleSleep is the pause after a page that was not full.
const DefaultIdleSleep = 500 * time.Millisecond

// Run follows until ctx ends. Only a full page means more is waiting, so only a
// full page is followed at once; anything shorter waits IdleSleep. Fetching
// again after every non-empty page would poll the node as fast as records
// arrive and run into its per-caller fetch limit.
func (b *Bridge) Run(ctx context.Context) error {
	idle := b.IdleSleep
	if idle <= 0 {
		idle = DefaultIdleSleep
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetched, _, err := b.pass(ctx)
		pause := idle
		switch {
		case err != nil:
			b.logger().Error("historian pass failed, retrying", "err", err)
			pause = 5 * time.Second
		case fetched >= b.max():
			continue
		}
		if !sleep(ctx, pause) {
			return ctx.Err()
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
