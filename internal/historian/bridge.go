package historian

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/internal/door"
)

// Consumer is this service's name in the marker table and in its cursor. Both
// carry it so a second follower of the same stream — the audit projector, a
// future one — cannot move this one's progress.
const Consumer = "historian:metrics"

// Cursor is namespaced by service, which is how the door keeps two local
// services from colliding on /ack.
const Cursor = "c/historian/metrics"

const gapContract = "/_StreamGap/"

// Store is what the bridge writes through. An interface because the loop's
// guarantees are worth testing without a database.
type Store interface {
	Applied(ctx context.Context, consumer string) (int64, error)
	Apply(ctx context.Context, rows []Row, consumer string, offset int64) error
}

// Fetcher is the half of the door the bridge uses.
type Fetcher interface {
	Fetch(ctx context.Context, stream, cursor string, max int) (door.Page, error)
	Ack(ctx context.Context, stream, cursor string, offset int64) (bool, error)
}

// Bridge follows `metrics` into the hypertables.
type Bridge struct {
	Door  Fetcher
	Store Store
	Log   *slog.Logger

	Max       int
	IdleSleep time.Duration

	// Gaps counts pruned-record incidents. Unlike the cache projector, the
	// bridge does NOT stop on one: `metrics` retention is short, the records
	// are gone, and there is nothing to rebuild from — halting historisation
	// over data that no longer exists anywhere would trade a hole for a
	// blackout. It is counted and logged so it is visible as the incident it is.
	Gaps int64
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

// Once applies at most one page and reports how many rows were written.
//
// The order is the contract (projector design §4): rows and marker commit
// together, and the ack happens only after that commit. A crash in between
// replays the page, which the marker makes a no-op; the other order loses every
// record in flight.
func (b *Bridge) Once(ctx context.Context) (int, error) {
	page, err := b.Door.Fetch(ctx, "metrics", Cursor, b.max())
	if err != nil {
		return 0, err
	}
	if len(page.Records) == 0 {
		return 0, nil
	}

	applied, err := b.Store.Applied(ctx, Consumer)
	if err != nil {
		return 0, err
	}

	rows := make([]Row, 0, len(page.Records))
	for _, record := range page.Records {
		if record.Offset <= applied {
			continue // already durable: a replay after a crash between commit and ack
		}
		if strings.Contains(record.Topic, gapContract) {
			b.Gaps++
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
		rows = append(rows, row)
	}

	last := page.Records[len(page.Records)-1].Offset
	if err := b.Store.Apply(ctx, rows, Consumer, last); err != nil {
		return 0, err
	}
	if _, err := b.Door.Ack(ctx, "metrics", Cursor, last); err != nil {
		// The rows ARE durable; only the cursor is behind. The next pass
		// re-reads and the marker makes it a no-op, so this is reported and
		// retried rather than treated as a write failure.
		b.logger().Warn("applied but could not ack", "offset", last, "err", err)
	}
	return len(rows), nil
}

// Run follows until ctx ends.
func (b *Bridge) Run(ctx context.Context) error {
	idle := b.IdleSleep
	if idle <= 0 {
		idle = 500 * time.Millisecond
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		written, err := b.Once(ctx)
		switch {
		case err != nil:
			b.logger().Error("historian pass failed, retrying", "err", err)
			if !sleep(ctx, 5*time.Second) {
				return ctx.Err()
			}
		case written == 0:
			if !sleep(ctx, idle) {
				return ctx.Err()
			}
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
