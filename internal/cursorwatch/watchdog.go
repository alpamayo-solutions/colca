package cursorwatch

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Author is the WrittenBy of every finding the watchdog writes, which is how it
// finds its own findings again after a restart.
const Author = "colca-cursorwatch"

// Reason is the _Finding.reason of a cursor that fell behind.
const Reason = "cursor_lag"

// scanCap bounds how many records one check reads past a cursor looking for
// the first one its consumer reads. A cursor far behind is scanned a slice per
// check, from where the previous check stopped.
const scanCap = 50_000

// Owners finds the identity a cursor belongs to (the registry).
type Owners interface {
	Get(ulid string) (*uns.Entry, bool)
	ByName(name string) (*uns.Entry, bool)
}

// Elements resolves an identity's bound element to its path.
type Elements interface {
	PathOf(id string) (string, bool)
}

// Gauges receives the unread age of every cursor.
type Gauges interface {
	CursorUnreadAge(cursor, stream string, seconds float64)
	ForgetCursorUnreadAge(cursor, stream string)
}

// Publish writes a record as the node; an empty payload retires the path.
type Publish func(topic string, payload []byte) error

// Watchdog checks every cursor on a fixed cadence; see the package comment.
type Watchdog struct {
	Store    *store.Store
	Filters  *Filters
	Owners   Owners
	Elements Elements
	Gauges   Gauges
	Publish  Publish
	NodeID   string
	// After is how old a cursor's oldest unread record may get before the
	// finding stands. 0 keeps the gauge but writes no finding.
	After time.Duration
	Log   *slog.Logger

	cursors   map[key]*cursorState
	published map[string]string // finding topic -> which cursors it named
	adopted   bool
}

type cursorState struct {
	position  uint64
	gen       uint64
	scannedTo uint64
	found     bool
	offset    uint64
	ts        int64
}

type lagging struct {
	Cursor   string  `json:"cursor"`
	Stream   string  `json:"stream"`
	WaitingS float64 `json:"waiting_s"`
	Offset   uint64  `json:"from_offset"`
}

// Every is the cadence for a threshold: a quarter of it, at most 5 s, at
// least 1 s.
func Every(after time.Duration) time.Duration {
	every := min(after/4, 5*time.Second)
	return max(every, time.Second)
}

// Run checks until stop closes.
func (w *Watchdog) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(Every(w.After))
	defer ticker.Stop()
	for {
		w.Check(time.Now())
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
	}
}

// Check takes every cursor's unread age once and updates the findings.
func (w *Watchdog) Check(now time.Time) {
	if w.cursors == nil {
		w.cursors = map[key]*cursorState{}
		w.published = map[string]string{}
	}
	if !w.adopted && w.After > 0 {
		w.adopted = w.adopt()
	}
	standing := map[string][]lagging{}
	owners := map[string]string{}
	seen := map[key]bool{}
	for _, cur := range w.Store.Cursors() {
		k := key{cur.Name, cur.Stream}
		seen[k] = true
		age, offset := w.unreadAge(k, cur.Position, now)
		if w.Gauges != nil {
			w.Gauges.CursorUnreadAge(cur.Name, cur.Stream, age)
		}
		if w.After <= 0 || age < w.After.Seconds() {
			continue
		}
		topic, owner, ok := w.findingTopic(cur.Name)
		if !ok {
			continue
		}
		owners[topic] = owner
		standing[topic] = append(standing[topic], lagging{
			Cursor: cur.Name, Stream: cur.Stream, WaitingS: float64(int64(age*10)) / 10, Offset: offset,
		})
	}
	for k := range w.cursors {
		if !seen[k] {
			delete(w.cursors, k)
			if w.Gauges != nil {
				w.Gauges.ForgetCursorUnreadAge(k.cursor, k.stream)
			}
		}
	}
	if w.After <= 0 {
		return
	}
	for topic, cursors := range standing {
		slices.SortFunc(cursors, func(a, b lagging) int { return strings.Compare(a.Cursor+a.Stream, b.Cursor+b.Stream) })
		names := make([]string, len(cursors))
		for i, c := range cursors {
			names[i] = c.Stream + ":" + c.Cursor
		}
		signature := strings.Join(names, ",")
		if held, ok := w.published[topic]; ok && held == signature {
			continue
		}
		payload, err := json.Marshal(w.finding(owners[topic], cursors, now))
		if err != nil {
			continue
		}
		if err := w.Publish(topic, payload); err != nil {
			w.logger().Warn("cursor lag finding not written", "topic", topic, "err", err)
			continue
		}
		w.logger().Warn("a consumer stopped reading", "service", owners[topic], "cursors", signature)
		w.published[topic] = signature
	}
	for topic := range w.published {
		if _, ok := standing[topic]; ok {
			continue
		}
		if err := w.Publish(topic, nil); err != nil {
			w.logger().Warn("cursor lag finding not retired", "topic", topic, "err", err)
			continue
		}
		w.logger().Info("a consumer caught up again", "topic", topic)
		delete(w.published, topic)
	}
}

// unreadAge returns the age in seconds of the oldest record past the cursor
// that its consumer reads, and that record's offset; 0 when there is none.
func (w *Watchdog) unreadAge(k key, position uint64, now time.Time) (float64, uint64) {
	pos := max(position, w.Store.LWM(k.stream))
	head := w.Store.NextOffset(k.stream)
	if pos >= head {
		delete(w.cursors, k)
		return 0, 0
	}
	filter, gen := w.Filters.get(k.cursor, k.stream)
	state := w.cursors[k]
	if state == nil || state.position != pos || state.gen != gen {
		state = &cursorState{position: pos, gen: gen, scannedTo: pos}
		w.cursors[k] = state
	}
	if state.found {
		// The record may have left the consumer's view since (a command the node
		// retired), and then the one after it is the oldest.
		if _, still, err := w.Store.FirstMatch(k.stream, state.offset, state.offset+1, filter); err == nil && !still {
			state.found, state.scannedTo = false, state.offset+1
		}
	}
	if !state.found && state.scannedTo < head {
		upTo := min(head, state.scannedTo+scanCap)
		rec, found, err := w.Store.FirstMatch(k.stream, state.scannedTo, upTo, filter)
		if err != nil {
			w.logger().Warn("cursor check could not read the stream", "cursor", k.cursor, "stream", k.stream, "err", err)
			return 0, 0
		}
		if found {
			state.found, state.offset, state.ts = true, rec.Offset, rec.TS
		} else {
			state.scannedTo = upTo
		}
	}
	if !state.found {
		return 0, 0
	}
	return max(0, now.Sub(time.UnixMilli(state.ts)).Seconds()), state.offset
}

// findingTopic is where the finding about the cursor's owner sits: next to the
// service's own record, {mount}/{service}/cursor_lag. A cursor that belongs to
// no enrolled identity (a replication lane, a removed service) has none.
func (w *Watchdog) findingTopic(cursor string) (topic, owner string, ok bool) {
	var entry *uns.Entry
	if rest, local := strings.CutPrefix(cursor, uns.LocalCursorPrefix); local {
		name, _, _ := strings.Cut(rest, "/")
		entry, ok = w.Owners.ByName(name)
	} else {
		ulid, _, _ := strings.Cut(cursor, "/")
		entry, ok = w.Owners.Get(ulid)
	}
	if !ok || entry == nil {
		return "", "", false
	}
	mount := ""
	if entry.Element != "" {
		if mount, ok = w.Elements.PathOf(entry.Element); !ok {
			return "", "", false
		}
	}
	name := entry.CatalogueName()
	return FindingTopic(w.NodeID, uns.ServiceContext(mount, name)), name, true
}

// FindingTopic is where the cursor_lag finding about a service sits, the
// service's context being uns.ServiceContext(mount, name): the same levels as
// its _ServiceDetails record, with cursor_lag in place of the _service leaf. A
// service subscribes to it to fail its own health check while it stands.
func FindingTopic(node string, context []string) string {
	return uns.Prefix() + "_Finding/" + node + "/" + strings.Join(context, "/") + "/" + Reason
}

func (w *Watchdog) finding(owner string, cursors []lagging, now time.Time) map[string]any {
	oldest := cursors[0]
	for _, c := range cursors[1:] {
		if c.WaitingS > oldest.WaitingS {
			oldest = c
		}
	}
	return map[string]any{
		"reason": Reason,
		"summary": fmt.Sprintf("%s has records on %s waiting %.0f s that it has not read",
			owner, oldest.Stream, oldest.WaitingS),
		"observed_at":        float64(now.UnixMilli()) / 1000,
		"suggested_severity": "warning",
		"detail":             map[string]any{"cursors": cursors, "after_s": w.After.Seconds()},
		"remedy": "The service stopped reading its stream: a lost wake-up or a stuck loop. " +
			"Check its log; restarting it drains the stream from its cursor.",
	}
}

// adopt takes over the findings a previous run left standing, so they are
// retired when their cursor has caught up.
func (w *Watchdog) adopt() bool {
	after := ""
	for {
		entries, next, err := w.Store.KVScanPage("", after, 1000, []string{"_Finding"})
		if err != nil {
			w.logger().Warn("cursor lag findings of the previous run not read", "err", err)
			return false
		}
		for _, entry := range entries {
			if entry.WrittenBy == Author && strings.HasSuffix(entry.Topic, "/"+Reason) {
				w.published[entry.Topic] = ""
			}
		}
		if next == "" {
			return true
		}
		after = next
	}
}

func (w *Watchdog) logger() *slog.Logger {
	if w.Log != nil {
		return w.Log
	}
	return slog.Default()
}
