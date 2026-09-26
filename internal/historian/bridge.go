package historian

import (
	"context"
	"errors"
	"fmt"
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

	Max int
	// Wake receives whenever the metrics stream may have grown (a /watch hint).
	// After a page that was not full the bridge waits for it, and reads nothing
	// on a timer: the first hint of every watch connection names the stream, so
	// a reconnect drains too.
	Wake       <-chan struct{}
	Strict     bool
	Drained    bool
	NowMS      int64
	FetchedAt  time.Time
	Coordinate func(context.Context) (bool, error)

	// Health, when set, hears after every pass whether it succeeded and, when
	// not, why. It is called on every pass; the receiver decides what changed.
	Health func(ok bool, detail string)

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
	b.Drained = false
	page, err := b.Door.Fetch(ctx, "metrics", Cursor, b.max())
	if err != nil {
		return 0, 0, err
	}
	fetched = len(page.Records)
	b.NowMS, b.FetchedAt = page.NowMS, time.Now()
	if b.Strict && page.Gap != nil {
		return fetched, 0, fmt.Errorf("coordinated history has a stream gap")
	}
	if fetched == 0 {
		b.Drained = true
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
			if b.Strict {
				return fetched, 0, fmt.Errorf("coordinated history has a stream gap")
			}
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
			if b.Strict {
				return fetched, 0, err
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
	if b.Strict && len(rejections) > 0 {
		return fetched, 0, fmt.Errorf("coordinated history refused %d rows", len(rejections))
	}
	// The marker moved past any rejected rows, so ack too, or the page would be
	// fetched forever.
	if _, err := b.Door.Ack(ctx, "metrics", Cursor, last); err != nil {
		if b.Strict {
			return fetched, 0, err
		}
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

// CoordinateEvery is how often a coordinated bridge (factory clock steps)
// checks the clock gate while no records arrive. It is the gate's cadence, not
// a read of the stream: the stream is read on a wake.
const CoordinateEvery = 500 * time.Millisecond

// errorPause is the wait after a failed pass before the page is read again.
const errorPause = 5 * time.Second

// Run follows until ctx ends. A full page means more is waiting and is followed
// at once; after anything shorter the bridge waits for the next wake. How often
// wakes come (the watch interval) is what batches a steady trickle into pages.
func (b *Bridge) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetched, _, err := b.pass(ctx)
		if err == nil && b.Coordinate != nil {
			_, err = b.Coordinate(ctx)
		}
		if b.Health != nil {
			if err != nil {
				b.Health(false, err.Error())
			} else {
				b.Health(true, "")
			}
		}
		switch {
		case err != nil:
			b.logger().Error("historian pass failed, retrying", "err", err)
			if !sleep(ctx, errorPause) {
				return ctx.Err()
			}
			continue
		case fetched >= b.max():
			continue
		}
		if !b.wait(ctx) {
			return ctx.Err()
		}
	}
}

// wait blocks until the next wake, or the next gate check when coordinated.
func (b *Bridge) wait(ctx context.Context) bool {
	var gate <-chan time.Time
	if b.Coordinate != nil {
		timer := time.NewTimer(CoordinateEvery)
		defer timer.Stop()
		gate = timer.C
	}
	select {
	case <-ctx.Done():
		return false
	case <-b.Wake:
	case <-gate:
	}
	return true
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
