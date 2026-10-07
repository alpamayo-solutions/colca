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

// SignalFilter decides, per sample, whether its signal is historised.
type SignalFilter interface {
	Wait(ctx context.Context) error
	Logged(signalID string) bool
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

	Max            int
	Strict         bool
	Drained        bool
	Acknowledged   int64
	NowMS          int64
	FetchedAt      time.Time
	Coordinate     func(context.Context) (bool, error)
	WaitCoordinate func(context.Context)
	Changes        func() <-chan struct{}
	BatchInterval  time.Duration

	// Health, when set, hears after every pass whether it succeeded and, when
	// not, why. It is called on every pass; the receiver decides what changed.
	Health func(ok bool, detail string)

	// Head, when set, reports the newest metrics head the node announced (the
	// watch hints' next offset), 0 when none is known yet. A drain that reached
	// the head captured with its wakeup is complete without one more, empty,
	// fetch.
	Head func() int64

	// Pipeline, above 1, lets Run hold that many pages between fetch and
	// marker: writers start on the next page before every partition of the
	// previous one is written (runPipelined). 0 or 1 applies one page at a time.
	Pipeline int

	// ReadAhead fetches the next page while the current one is written, when
	// the page was full and the door can read ahead of its cursor. Run sets it.
	ReadAhead bool

	// Signals, when set, says which signals are historised. A pass waits until
	// it has loaded, and samples of a signal whose definition says
	// "is_logged": false are consumed without a row.
	Signals SignalFilter

	// notLogged counts samples left out because their signal is not
	// historised. Atomic because /metrics reads it from another goroutine.
	notLogged atomic.Int64

	// gaps counts pruned ranges. The bridge keeps going: the records are gone and
	// stopping would only add a blackout. Atomic because /metrics reads it from
	// another goroutine.
	gaps atomic.Int64

	// marker is this consumer's applied offset as last committed, valid while
	// markerKnown: this bridge is the marker's only writer, so it is read from
	// the database once and after a failed write, not on every page.
	marker      int64
	markerKnown bool
	// skip is what this bridge skips at or below: the marker, or further when
	// a pipelined run's partition markers say so.
	skip int64
	// next is the page end of the last pass.
	next int64
	// ahead delivers the page read ahead of the cursor, nil when none is in flight.
	ahead chan aheadPage

	// rejected counts permanently refused rows by reason. A sync.Map of counters
	// keeps the zero Bridge usable without a constructor.
	rejected sync.Map // reason string -> *atomic.Int64
}

type aheadPage struct {
	from int64
	page door.Page
	err  error
}

// aheadFetcher is a door that can read ahead of its cursor (door.Client).
type aheadFetcher interface {
	FetchWithOptions(ctx context.Context, options door.FetchOptions) (door.Page, error)
}

// takeAhead returns the page read ahead from offset from, if one is in flight
// and arrived without an error.
func (b *Bridge) takeAhead(from int64) (door.Page, bool) {
	if b.ahead == nil {
		return door.Page{}, false
	}
	r := <-b.ahead
	b.ahead = nil
	// The page must start where this pass reads: requested there, and served
	// from there (a node reports where a page started; an older one does not,
	// and its page is taken as requested).
	if r.err != nil || r.from != from || (r.page.From != 0 && r.page.From != from) {
		return door.Page{}, false
	}
	return r.page, true
}

// dropAhead waits for a read-ahead in flight and discards it.
func (b *Bridge) dropAhead() {
	if b.ahead != nil {
		<-b.ahead
		b.ahead = nil
	}
}

// Gaps returns how many pruned ranges could not be historised. Safe for
// concurrent use.
func (b *Bridge) Gaps() int64 { return b.gaps.Load() }

// NotLogged returns how many samples were consumed without a row because their
// signal says is_logged false. Safe for concurrent use.
func (b *Bridge) NotLogged() int64 { return b.notLogged.Load() }

// Rejected returns how many rows the schema refused for reason. Safe for
// concurrent use.
func (b *Bridge) Rejected(reason string) int64 {
	v, ok := b.rejected.Load(reason)
	if !ok {
		return 0
	}
	return v.(*atomic.Int64).Load()
}

// reject counts and logs a row the schema refused.
func (b *Bridge) reject(rej Rejection) {
	b.countRejection(rej.Reason)
	b.logger().Warn("a record was refused by the schema and set aside — the rest of the page still historised",
		"offset", rej.Row.Offset, "topic", rej.Row.Topic,
		"signal_id", truncateForLog(rej.Row.SignalID, 40),
		"sqlstate", rej.SQLState, "reason", rej.Reason, "err", rej.Err)
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
	return 5000
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
	if b.Signals != nil {
		if err := b.Signals.Wait(ctx); err != nil {
			return 0, 0, err
		}
	}
	page, ok := b.takeAhead(b.next)
	if !ok {
		page, err = b.Door.Fetch(ctx, "metrics", Cursor, b.max())
		if err != nil {
			return 0, 0, err
		}
	}
	fetched = len(page.Records)
	b.next = page.Next
	b.NowMS, b.FetchedAt = page.NowMS, time.Now()
	if b.Strict && page.Gap != nil {
		return fetched, 0, fmt.Errorf("coordinated history has a stream gap")
	}
	if fetched == 0 {
		b.Drained = true
		b.Acknowledged = page.Next - 1
		return 0, 0, nil
	}

	ps, pipelineStore := b.Store.(PipelineStore)
	if !b.markerKnown {
		if b.marker, err = b.Store.Applied(ctx, Consumer); err != nil {
			return fetched, 0, err
		}
		b.skip = b.marker
		// A pipelined run before this one may have written past the page
		// marker; a complete set of its partition markers says how far.
		if pipelineStore {
			markers, err := ps.PartitionMarkers(ctx, Consumer)
			if err != nil {
				return fetched, 0, err
			}
			b.skip, _ = startMarkers(b.marker, markers, 0)
		}
		b.markerKnown = true
	}
	last := page.Records[len(page.Records)-1].Offset
	if b.newStream(page, b.marker) {
		// A later pipelined run must not trust the old stream's partition
		// markers (see runPipelined).
		if pipelineStore {
			if err := ps.ResetPartitionMarkers(ctx, Consumer); err != nil {
				return fetched, 0, err
			}
		}
		b.marker, b.skip = 0, 0
	}
	rows, err := b.rowsOf(page, b.skip)
	if err != nil {
		return fetched, 0, err
	}

	// A full page means more is waiting: read it while this one is written.
	if ahead, ok := b.Door.(aheadFetcher); ok && b.ReadAhead && fetched >= b.max() && page.Gap == nil {
		from := last + 1
		ch := make(chan aheadPage, 1)
		b.ahead = ch
		go func() {
			p, err := ahead.FetchWithOptions(ctx, door.FetchOptions{Stream: "metrics", Cursor: Cursor, Max: b.max(), From: uint64(from)}) //nolint:gosec // offsets are positive
			ch <- aheadPage{from: from, page: p, err: err}
		}()
	}

	rejections, err := b.Store.Apply(ctx, rows, Consumer, last)
	if err != nil {
		b.markerKnown = false
		b.dropAhead()
		return fetched, 0, err
	}
	b.marker, b.skip = last, last
	for _, rej := range rejections {
		b.reject(rej)
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
	} else {
		b.Acknowledged = last
	}
	return fetched, len(rows) - len(rejections), nil
}

// rowsOf turns a non-empty page into the rows to write: records at or below the
// applied marker (a replay), gaps, tombstones, unhistorisable records and
// samples of signals that are not logged are consumed without a row.
func (b *Bridge) rowsOf(page door.Page, applied int64) ([]Row, error) {
	rows := make([]Row, 0, len(page.Records))
	for _, record := range page.Records {
		if record.Offset <= applied {
			continue // already durable: a replay after a crash between commit and ack
		}
		if strings.Contains(record.Topic, gapContract) {
			if b.Strict {
				return nil, fmt.Errorf("coordinated history has a stream gap")
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
				return nil, err
			}
			// One bad record must not wedge the stream forever, but it must not
			// vanish either.
			b.logger().Warn("skipping an unhistorisable record",
				"offset", record.Offset, "topic", record.Topic, "err", err)
			continue
		}
		// The flag in force when the sample is ingested decides. The record is
		// still consumed: the marker and the ack move past it with the page.
		if b.Signals != nil && !b.Signals.Logged(row.SignalID) {
			b.notLogged.Add(1)
			continue
		}
		row.Offset = record.Offset
		row.Topic = record.Topic
		rows = append(rows, row)
	}
	return rows, nil
}

// newStream reports whether the page marker cannot be a position in the
// stream a non-empty page comes from: the page starts at offset 1 while the
// marker is positive, or the marker is past the page's end. That happens when
// colcad's data volume is recreated but Timescale keeps the marker, and
// trusting it would silently drop a page. The caller then applies from the
// stream's start; at worst that re-applies the very first page.
func (b *Bridge) newStream(page door.Page, marker int64) bool {
	first, last := page.Records[0].Offset, page.Records[len(page.Records)-1].Offset
	if marker <= 0 || (first != 1 && marker <= last) {
		return false
	}
	b.logger().Warn("the applied-offset marker is not a position in this stream — ignoring it",
		"marker", marker, "page_first", first, "page_last", last,
		"detail", "colca's data volume was recreated while Timescale kept the marker "+
			"(or the marker outran the stream). Applying this page in full rather than "+
			"reading it as already durable; the markers are rewritten to this stream's positions.")
	return true
}

// truncateForLog shortens an untrusted value before it goes into a log line.
func truncateForLog(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}

// Run drains all available pages, then waits for a stream hint. Backpressure
// batches drain starts; a short page never delays the rest of the queue.
func (b *Bridge) Run(ctx context.Context) error {
	if b.Coordinate != nil {
		return b.runCoordinated(ctx)
	}
	if b.Changes == nil {
		return fmt.Errorf("historian requires a stream subscription")
	}
	if p, ok := b.pipelined(); ok {
		return p.run(ctx)
	}
	started := time.Now()
	var changed <-chan struct{}
	var head int64
	retry := time.Second
	b.ReadAhead = true
	defer b.dropAhead()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Keep the version captured before the FIRST page through the final
		// read. Updates arriving during a drain remain owed. The head announced
		// by then is the boundary this drain must reach.
		if changed == nil && b.Changes != nil {
			changed = b.Changes()
			head = 0
			if b.Head != nil {
				head = b.Head()
			}
		}
		fetched, _, err := b.pass(ctx)
		if b.Health != nil {
			if err != nil {
				b.Health(false, err.Error())
			} else {
				b.Health(true, "")
			}
		}
		if err != nil {
			b.logger().Error("historian pass failed, retrying", "err", err)
			if !sleep(ctx, door.RetryDelay(err, retry)) {
				return ctx.Err()
			}
			retry = min(30*time.Second, retry*2)
			continue
		}
		retry = time.Second
		// More is waiting unless the page reached the head this drain started
		// with; without a known head only an empty page proves it.
		if fetched > 0 && (head == 0 || b.next < head || b.ahead != nil) {
			continue
		}
		if err == nil && b.Changes != nil {
			if !door.WaitChange(ctx, changed, -1, started.Add(b.BatchInterval)) {
				return ctx.Err()
			}
			started, changed = time.Now(), nil
			continue
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

// Coordinated acquisition has a known upstream completion marker. Fetch only
// after that marker, instead of polling empty metrics between windows. This
// leaves the fetch budget for durable drains (including their final empty read).
func (b *Bridge) runCoordinated(ctx context.Context) error {
	if b.WaitCoordinate == nil {
		return fmt.Errorf("coordinated historian requires commit wakeups")
	}
	retry := time.Second
	for ctx.Err() == nil {
		_, err := b.Coordinate(ctx)
		if b.Health != nil {
			if err != nil {
				b.Health(false, err.Error())
			} else {
				b.Health(true, "")
			}
		}
		if err != nil {
			b.logger().Error("coordinated historian failed, retrying", "err", err)
			if !sleep(ctx, door.RetryDelay(err, retry)) {
				return ctx.Err()
			}
			retry = min(30*time.Second, retry*2)
			continue
		}
		retry = time.Second
		b.WaitCoordinate(ctx)
	}
	return ctx.Err()
}
