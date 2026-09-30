package historian

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpamayo-solutions/colca/door"
)

// SignalCursor follows the _Signal definitions on the entities stream. It is
// acknowledged as the follower drains, so it never holds back retention.
const SignalCursor = "c/historian/signals"

// signalStream is where the node keeps _Signal records (uns.ClassEntity).
const signalStream = "entities"

const signalContract = "_Signal"

// SignalDoor is the part of the local door the signal follower uses.
type SignalDoor interface {
	KV(ctx context.Context, prefix string, contracts ...string) ([]door.KVEntry, error)
	FetchWithOptions(ctx context.Context, options door.FetchOptions) (door.Page, error)
	Ack(ctx context.Context, stream, cursor string, offset int64) (bool, error)
}

// errResync means the follower cannot continue from its cursor and has to
// reload the definitions.
var errResync = errors.New("historian: the signal definitions have a stream gap")

// signalEntry is what one _Signal topic currently holds.
type signalEntry struct {
	offset  int64
	id      string
	logged  bool
	deleted bool
}

// signalFlag is what the follower knows about one signal id: the topic that
// defines it and whether it is historised.
type signalFlag struct {
	topic  string
	offset int64
	logged bool
}

// Signals keeps the is_logged flag of every _Signal the node holds, so the
// bridge can leave out samples of signals that are not historised.
//
// It loads the definitions once from /kv and then follows the entities stream
// with its own cursor, woken by the node's stream hints; it never reads on a
// timer and never looks up a definition per sample. The stream position is
// captured before the snapshot, and each topic remembers the offset it was
// last written at, so a record the snapshot already reflects cannot roll a
// flag back.
//
// A signal is historised unless its current definition says
// "is_logged": false. A missing flag, a missing definition and an undecodable
// one all historise: leaving history out needs an explicit decision.
type Signals struct {
	Door SignalDoor
	Log  *slog.Logger
	// Changes returns the next wakeup for the entities stream. Required by Run.
	Changes func() <-chan struct{}
	// Max is the page size for the stream drain.
	Max int

	mu      sync.RWMutex
	byTopic map[string]signalEntry
	byID    map[string]signalFlag
	loaded  bool

	readyOnce sync.Once
	ready     chan struct{}

	notLogged atomic.Int64
	problem   atomic.Value // string: the last failure, "" once a drain succeeded
}

func (s *Signals) logger() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

func (s *Signals) max() int {
	if s.Max > 0 {
		return s.Max
	}
	return 500
}

func (s *Signals) readyChan() chan struct{} {
	s.readyOnce.Do(func() { s.ready = make(chan struct{}) })
	return s.ready
}

// Wait blocks until the definitions are loaded, or ctx ends.
func (s *Signals) Wait(ctx context.Context) error {
	select {
	case <-s.readyChan():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Logged reports whether samples of signalID are historised. Safe for
// concurrent use.
func (s *Signals) Logged(signalID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	flag, ok := s.byID[signalID]
	return !ok || flag.logged
}

// NotLogged returns how many signals currently say is_logged false. Safe for
// concurrent use.
func (s *Signals) NotLogged() int64 { return s.notLogged.Load() }

// Problem is why the last attempt to load or follow the definitions failed, ""
// while they are current. Safe for concurrent use.
func (s *Signals) Problem() string {
	v, _ := s.problem.Load().(string)
	return v
}

// Run loads the definitions and follows them until ctx ends.
func (s *Signals) Run(ctx context.Context) error {
	if s.Changes == nil {
		return fmt.Errorf("historian: the signal follower requires a stream subscription")
	}
	retry := time.Second
	for {
		// Captured before the drain, so a change during it wakes the next one.
		changed := s.Changes()
		err := s.Sync(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			s.problem.Store(err.Error())
			s.logger().Error("following the signal definitions failed, retrying", "err", err)
			if !sleep(ctx, door.RetryDelay(err, retry)) {
				return ctx.Err()
			}
			retry = min(30*time.Second, retry*2)
			continue
		}
		s.problem.Store("")
		retry = time.Second
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Sync loads the definitions if they are not loaded yet, then applies every
// _Signal record up to the stream's end.
func (s *Signals) Sync(ctx context.Context) error {
	s.mu.RLock()
	loaded := s.loaded
	s.mu.RUnlock()
	if !loaded {
		if err := s.load(ctx); err != nil {
			return err
		}
	}
	err := s.drain(ctx)
	if errors.Is(err, errResync) {
		s.logger().Warn("the entities stream was pruned past the signal cursor — reloading the definitions")
		if err := s.load(ctx); err != nil {
			return err
		}
		return s.drain(ctx)
	}
	return err
}

// load replaces the index with a snapshot of the node's _Signal records. The
// stream head is captured first and the cursor moved to it: every record before
// the head is in the snapshot or superseded by something that is.
func (s *Signals) load(ctx context.Context) error {
	tail, err := s.Door.FetchWithOptions(ctx, door.FetchOptions{
		Stream: signalStream, Cursor: SignalCursor, Max: 1, Tail: true,
	})
	if err != nil {
		return fmt.Errorf("historian: capturing the entities position: %w", err)
	}
	entries, err := s.Door.KV(ctx, "", signalContract)
	if err != nil {
		return fmt.Errorf("historian: loading the signal definitions: %w", err)
	}
	byTopic := make(map[string]signalEntry, len(entries))
	byID := make(map[string]signalFlag, len(entries))
	for _, entry := range entries {
		apply(byTopic, byID, entry.Topic, entry.Offset, entry.Payload, s.logger())
	}
	if head := tail.Next - 1; head > 0 {
		if _, err := s.Door.Ack(ctx, signalStream, SignalCursor, head); err != nil {
			return fmt.Errorf("historian: moving the signal cursor to the snapshot: %w", err)
		}
	}
	s.mu.Lock()
	s.byTopic, s.byID, s.loaded = byTopic, byID, true
	s.notLogged.Store(countNotLogged(byID))
	s.mu.Unlock()
	return nil
}

// drain applies _Signal records from the cursor until a page makes no progress,
// acknowledging each page after it is applied. The index lives in memory, so
// applying is the commit; a crash reloads the snapshot.
func (s *Signals) drain(ctx context.Context) error {
	for {
		page, err := s.Door.FetchWithOptions(ctx, door.FetchOptions{
			Stream: signalStream, Cursor: SignalCursor, Max: s.max(), Contracts: []string{signalContract},
		})
		if err != nil {
			return fmt.Errorf("historian: reading the signal definitions: %w", err)
		}
		if page.Gap != nil {
			return errResync
		}
		if len(page.Records) > 0 {
			s.mu.Lock()
			for _, record := range page.Records {
				apply(s.byTopic, s.byID, record.Topic, record.Offset, record.Payload, s.logger())
			}
			s.notLogged.Store(countNotLogged(s.byID))
			s.mu.Unlock()
		}
		// A filtered page can be empty and still move past other contracts, so
		// progress is judged by next, not by the record count.
		if len(page.Records) == 0 && page.Next <= page.From {
			s.readyOnceClose()
			return nil
		}
		if _, err := s.Door.Ack(ctx, signalStream, SignalCursor, page.Next-1); err != nil {
			return fmt.Errorf("historian: acknowledging the signal definitions: %w", err)
		}
		if len(page.Records) == 0 && page.From == 0 {
			// A node that does not report where the page started cannot show
			// progress; an empty page is its end.
			s.readyOnceClose()
			return nil
		}
	}
}

func (s *Signals) readyOnceClose() {
	ch := s.readyChan()
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// apply writes one _Signal record into the index unless the topic already holds
// a newer one. An empty payload retires the topic.
func apply(byTopic map[string]signalEntry, byID map[string]signalFlag, topic string, offset int64,
	payload json.RawMessage, log *slog.Logger) {
	current, known := byTopic[topic]
	if known && current.offset >= offset {
		return
	}
	if known && !current.deleted {
		if flag, ok := byID[current.id]; ok && flag.topic == topic {
			delete(byID, current.id)
		}
	}
	entry := signalEntry{offset: offset, deleted: true}
	if len(payload) > 0 && string(payload) != "null" {
		var body struct {
			ID       string `json:"id"`
			IsLogged *bool  `json:"is_logged"`
		}
		if err := json.Unmarshal(payload, &body); err != nil || body.ID == "" {
			log.Warn("an undecodable _Signal definition — its samples are historised",
				"topic", topic, "offset", offset, "err", err)
		} else {
			entry = signalEntry{offset: offset, id: body.ID, logged: body.IsLogged == nil || *body.IsLogged}
		}
	}
	byTopic[topic] = entry
	if entry.deleted {
		return
	}
	// A moved signal is written at its new topic and retired at the old one; the
	// newer write decides, whichever order they arrive in.
	if flag, ok := byID[entry.id]; ok && flag.topic != topic && flag.offset > offset {
		return
	}
	byID[entry.id] = signalFlag{topic: topic, offset: offset, logged: entry.logged}
}

func countNotLogged(byID map[string]signalFlag) int64 {
	var n int64
	for _, flag := range byID {
		if !flag.logged {
			n++
		}
	}
	return n
}
