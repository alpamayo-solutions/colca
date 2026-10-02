package cursorwatch

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Author is the WrittenBy of every finding the watchdog writes, which is how it
// finds its own findings again after a restart.
const Author = "colca-cursorwatch"

// Reason is the _Finding.reason of a cursor that fell behind.
const Reason = "cursor_lag"

// StaleReason is the _Finding.reason of the node's finding about cursors that
// stopped moving while their stream grew (see Stale).
const StaleReason = "stale_cursors"

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

// Watchdog checks on changes and schedules unread-record age deadlines.
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
	// StaleAfter is how long a cursor may stand still while records wait past
	// it before the node's stale_cursors finding names it. 0 writes none.
	StaleAfter time.Duration
	Log        *slog.Logger

	cursors   map[key]*cursorState
	published map[string]string // finding topic -> which cursors it named
	adopted   bool
	retry     bool
	// staleDue is when the next cursor turns stale, -1 when none will.
	staleDue time.Duration
}

// writesFindings reports whether the watchdog writes any finding at all.
func (w *Watchdog) writesFindings() bool { return w.After > 0 || w.StaleAfter > 0 }

// Stale reports whether a cursor stood still for after while its stream grew
// past it: records wait beyond its position and its last advance is at least
// after old. Such a cursor holds back retention whether or not anyone still
// reads it. A cursor without a recorded advance is not stale (the pruner stamps
// it on sight), and after <= 0 makes nothing stale.
func Stale(c store.CursorInfo, head uint64, now time.Time, after time.Duration) bool {
	return after > 0 && c.Position < head && c.LastAdvanceMS > 0 && now.Sub(time.UnixMilli(c.LastAdvanceMS)) >= after
}

// StaleFindingTopic is where the node's stale_cursors finding sits: on the node
// itself, as the cursors it names may belong to nobody any more.
func StaleFindingTopic(node string) string {
	return uns.Prefix() + "_Finding/" + node + "/" + StaleReason
}

type staleCursor struct {
	Cursor         string  `json:"cursor"`
	Stream         string  `json:"stream"`
	Position       uint64  `json:"position"`
	LagRecords     uint64  `json:"lag_records"`
	IdleS          float64 `json:"idle_s"`
	ReadSinceStart bool    `json:"read_since_start"`
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

// Run subscribes before each check. Batch starts are bounded to one second;
// an incomplete scan continues immediately, and an idle node does no reads.
func (w *Watchdog) Run(stop <-chan struct{}) {
	var notBefore time.Time
	retryDelay := time.Second
	for {
		select {
		case <-stop:
			return
		default:
		}
		changed := w.Store.BacklogChanges()
		filters := w.Filters.changed.Changes()
		w.Check(time.Now())
		if w.retry || (w.writesFindings() && !w.adopted) {
			timer := time.NewTimer(door.RetryDelay(nil, retryDelay))
			select {
			case <-stop:
				timer.Stop()
				return
			case <-timer.C:
			}
			retryDelay = min(30*time.Second, retryDelay*2)
			continue
		}
		retryDelay = time.Second
		delay := w.nextDelay(time.Now())
		if delay == 0 {
			continue
		} // bounded scan has more records
		notBefore = time.Now().Add(time.Second)
		var timer *time.Timer
		var due <-chan time.Time
		if delay >= 0 {
			timer = time.NewTimer(delay)
			due = timer.C
		}
		select {
		case <-stop:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-changed:
		case <-filters:
		case <-due:
		}
		if timer != nil {
			timer.Stop()
		}
		// New hints cannot postpone the batch indefinitely.
		if wait := time.Until(notBefore); wait > 0 {
			timer = time.NewTimer(wait)
			select {
			case <-stop:
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func (w *Watchdog) nextDelay(now time.Time) time.Duration {
	delay := w.staleDue
	if w.retry || (w.writesFindings() && !w.adopted) {
		delay = time.Second
	}
	for k, state := range w.cursors {
		if !w.retry && !state.found && state.scannedTo < w.Store.NextOffset(k.stream) {
			return 0
		}
		if state.found && w.After > 0 {
			due := time.UnixMilli(state.ts).Add(w.After).Sub(now)
			if due > 0 && (delay < 0 || due < delay) {
				delay = due
			}
		}
	}
	return delay
}

// Check takes every cursor's unread age once and updates the findings.
func (w *Watchdog) Check(now time.Time) {
	w.retry = false
	if w.cursors == nil {
		w.cursors = map[key]*cursorState{}
		w.published = map[string]string{}
	}
	if !w.adopted && w.writesFindings() {
		w.adopted = w.adopt()
	}
	standing := map[string][]lagging{}
	owners := map[string]string{}
	seen := map[key]bool{}
	var stale []staleCursor
	w.staleDue = -1
	for _, cur := range w.Store.Cursors() {
		k := key{cur.Name, cur.Stream}
		seen[k] = true
		w.checkStale(cur, now, &stale)
		age, offset := w.unreadAge(k, cur.Position, now)
		if w.Gauges != nil {
			w.Gauges.CursorUnreadAge(cur.Name, cur.Stream, age)
		}
		if w.After <= 0 || age < w.After.Seconds() {
			continue
		}
		if _, gen := w.Filters.get(cur.Name, cur.Stream); gen == 0 {
			// Nobody fetched it since the node started: an abandoned cursor (a
			// previous buffer generation, a renamed consumer). Every live
			// consumer drains on reconnect, which fetches. The gauge and
			// colca_retention_blocked_by_cursor show it; a finding would fail
			// the health of a service that is reading fine on its current cursor.
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
	if !w.writesFindings() {
		return
	}
	type wanted struct {
		signature string
		finding   func() map[string]any
	}
	findings := map[string]wanted{}
	for topic, cursors := range standing {
		slices.SortFunc(cursors, func(a, b lagging) int { return strings.Compare(a.Cursor+a.Stream, b.Cursor+b.Stream) })
		names := make([]string, len(cursors))
		for i, c := range cursors {
			names[i] = c.Stream + ":" + c.Cursor
		}
		owner := owners[topic]
		findings[topic] = wanted{strings.Join(names, ","), func() map[string]any { return w.finding(owner, cursors, now) }}
	}
	if len(stale) > 0 {
		slices.SortFunc(stale, func(a, b staleCursor) int { return strings.Compare(a.Cursor+a.Stream, b.Cursor+b.Stream) })
		names := make([]string, len(stale))
		for i, c := range stale {
			names[i] = c.Stream + ":" + c.Cursor
		}
		findings[StaleFindingTopic(w.NodeID)] = wanted{strings.Join(names, ","), func() map[string]any { return w.staleFinding(stale, now) }}
	}
	for topic, want := range findings {
		if held, ok := w.published[topic]; ok && held == want.signature {
			continue
		}
		payload, err := json.Marshal(want.finding())
		if err != nil {
			continue
		}
		if err := w.Publish(topic, payload); err != nil {
			w.retry = true
			w.logger().Warn("cursor finding not written", "topic", topic, "err", err)
			continue
		}
		if topic == StaleFindingTopic(w.NodeID) {
			w.logger().Warn("cursors stopped moving while their stream grew; they hold back retention", "cursors", want.signature)
		} else {
			w.logger().Warn("a consumer stopped reading", "service", owners[topic], "cursors", want.signature)
		}
		w.published[topic] = want.signature
	}
	for topic := range w.published {
		if _, ok := findings[topic]; ok {
			continue
		}
		if err := w.Publish(topic, nil); err != nil {
			w.retry = true
			w.logger().Warn("cursor finding not retired", "topic", topic, "err", err)
			continue
		}
		w.logger().Info("a cursor finding cleared", "topic", topic)
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
			w.retry = true
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

// checkStale adds cur to stale when it is stale now, else moves staleDue to
// when it would turn stale if it keeps standing still.
func (w *Watchdog) checkStale(cur store.CursorInfo, now time.Time, stale *[]staleCursor) {
	if w.StaleAfter <= 0 || cur.LastAdvanceMS <= 0 {
		return
	}
	head := w.Store.NextOffset(cur.Stream)
	if cur.Position >= head {
		return
	}
	idle := now.Sub(time.UnixMilli(cur.LastAdvanceMS))
	if !Stale(cur, head, now, w.StaleAfter) {
		if due := w.StaleAfter - idle; w.staleDue < 0 || due < w.staleDue {
			w.staleDue = due
		}
		return
	}
	_, gen := w.Filters.get(cur.Name, cur.Stream)
	*stale = append(*stale, staleCursor{
		Cursor: cur.Name, Stream: cur.Stream, Position: cur.Position, LagRecords: head - cur.Position,
		IdleS: float64(int64(idle.Seconds()*10)) / 10, ReadSinceStart: gen != 0,
	})
}

func (w *Watchdog) staleFinding(cursors []staleCursor, now time.Time) map[string]any {
	return map[string]any{
		"reason": StaleReason,
		"summary": fmt.Sprintf("%d cursor(s) stood still for %s or longer while their stream grew; they hold back retention",
			len(cursors), w.StaleAfter),
		"observed_at":        float64(now.UnixMilli()) / 1000,
		"suggested_severity": "warning",
		"detail":             map[string]any{"cursors": cursors, "after_s": w.StaleAfter.Seconds()},
		"remedy": "GET /backlog lists the cursors with their last ack. A cursor of a removed consumer is retired with " +
			"POST /ack {\"cursor\":…,\"stream\":…,\"delete\":true}, by its owner or the admin. A live consumer that " +
			"filters its fetch must ack the page's ack_offset also when nothing in it was its own. " +
			"retention.streams.<stream>.ignore_cursors_after lets the pruner pass a stale cursor with a _StreamGap.",
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
			if entry.WrittenBy == Author && (strings.HasSuffix(entry.Topic, "/"+Reason) || entry.Topic == StaleFindingTopic(w.NodeID)) {
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
