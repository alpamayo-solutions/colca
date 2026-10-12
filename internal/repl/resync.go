package repl

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The child side of the store-incarnation check (incarnation.go has the
// parent's).
//
// A child records the incarnation of its parent's store and re-syncs when the
// parent answers with another one, or says so:
//
//   - its commands and definitions positions move to where the parent's store
//     stands for it. Commands it already holds are recognised by content and
//     not executed again (commandMemory), so the move can go backwards;
//   - each uplink cursor moves back to just past what the parent holds of that
//     stream, as far as local retention still has it, so the child sends again
//     what the parent lacks. The parent's high-water mark drops what it has.
//
// All of it is one batch (store.ResyncParent). The downlink loop is the only
// one that re-syncs; the uplink loop holds its lanes meanwhile and discards a
// push that was in flight across it (Client.epoch).

// parentVerdict is what a child does with its parent's answer.
type parentVerdict int

const (
	// parentPredatesIncarnations: the parent sent no incarnation. Nothing is
	// checked and nothing is recorded.
	parentPredatesIncarnations parentVerdict = iota
	// parentFirstSeen: the first incarnation this node learns of the parent.
	parentFirstSeen
	// parentUnchanged: the incarnation this node recorded.
	parentUnchanged
	// parentChanged: another incarnation, or the parent asked for a re-sync.
	parentChanged
)

// judgeParent decides from the incarnation this node recorded ("" for none),
// the one the parent answered with ("" from a parent that predates them) and
// whether the parent asked for a re-sync.
func judgeParent(recorded, answered string, asked bool) parentVerdict {
	switch {
	case answered == "":
		return parentPredatesIncarnations
	case asked:
		return parentChanged
	case recorded == "":
		return parentFirstSeen
	case recorded == answered:
		return parentUnchanged
	default:
		return parentChanged
	}
}

// parentState is the document a node keeps about one parent
// (store.Store.ParentState).
type parentState struct {
	// Store is the incarnation of the parent's store this node's positions
	// against it belong to.
	Store string `json:"store,omitempty"`
	// Resync is the last re-sync with this parent, running or complete.
	Resync *Resync `json:"resync,omitempty"`
}

// Resync describes one re-sync with a parent whose store changed incarnation.
type Resync struct {
	// State is "replaying" while records the parent lacks are still being sent
	// again, then "complete".
	State string `json:"state"`
	// Reason says what made this node re-sync.
	Reason string `json:"reason"`
	// ParentStore is the incarnation re-synced with, PreviousParentStore the
	// one this node knew before ("" when it had recorded none).
	ParentStore         string     `json:"parent_store"`
	PreviousParentStore string     `json:"previous_parent_store,omitempty"`
	Started             time.Time  `json:"started"`
	Completed           *time.Time `json:"completed,omitempty"`
	// Streams are the ranges sent again, per stream. Position is filled in
	// when the status is read.
	Streams map[string]ResyncRange `json:"streams,omitempty"`
	// Gaps are the ranges the parent lacks that this node no longer holds.
	// They stay listed after the re-sync completed: they were not replayed.
	Gaps []ResyncGap `json:"gaps,omitempty"`
	// CommandsRehandedBelow is the parent's commands head at the handshake.
	// What its stream holds below was there before this node re-synced, so a
	// command from there that this node already holds is handed a second time.
	CommandsRehandedBelow uint64 `json:"commands_rehanded_below,omitempty"`
}

const (
	resyncReplaying = "replaying"
	resyncComplete  = "complete"
)

// ResyncRange is the part of one stream sent again: offsets From up to, not
// including, To.
type ResyncRange struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
	// Position is the next offset to send, From..To.
	Position uint64 `json:"position,omitempty"`
}

// ResyncGap is a range the parent lacks and local retention already pruned.
type ResyncGap struct {
	Stream     string `json:"stream"`
	FromOffset uint64 `json:"from_offset"`
	ToOffset   uint64 `json:"to_offset"`
}

// uplinkReplay is what a re-sync does to one uplink stream.
type uplinkReplay struct {
	// rewindTo is where the cursor is placed; 0 leaves it alone.
	rewindTo uint64
	// until is where the cursor stood: the replay is done once it is back.
	until uint64
	// gap is the range that cannot be replayed, nil when there is none.
	gap *ResyncGap
}

// planReplay decides one stream's replay from where its cursor stands, the
// highest offset the parent holds of it and the lowest offset still retained
// here. Only a cursor ahead of the parent moves, and only back.
func planReplay(stream string, cursor, parentHWM, lwm uint64) uplinkReplay {
	want := parentHWM + 1
	if want >= cursor {
		return uplinkReplay{} // the parent holds everything this node sent
	}
	plan := uplinkReplay{until: cursor}
	from := max(want, lwm)
	if from > want {
		plan.gap = &ResyncGap{Stream: stream, FromOffset: want, ToOffset: min(from, cursor) - 1}
	}
	if from < cursor {
		plan.rewindTo = from
	}
	return plan
}

// loadState reads the document kept for this parent, once.
func (c *Client) loadState(st *store.Store) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	if c.stateLoaded {
		return nil
	}
	raw, err := st.ParentState(c.parentPub)
	if err != nil {
		return err
	}
	if raw != nil {
		if err := json.Unmarshal(raw, &c.state); err != nil {
			return fmt.Errorf("state kept for parent %s: %w", short(c.parentPub), err)
		}
	}
	c.stateLoaded = true
	c.replaying.Store(c.state.Resync != nil && c.state.Resync.State == resyncReplaying)
	return nil
}

// parentStore is the parent incarnation this node recorded, "" for none.
func (c *Client) parentStore() string {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	return c.state.Store
}

// uplinkEpoch names the current set of uplink positions, with the parent
// incarnation they belong to. A re-sync starts a new one; a page read under an
// older one must not move a cursor.
func (c *Client) uplinkEpoch() (epoch uint64, parentStore string) {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	return c.epoch, c.state.Store
}

// ackUplink moves an uplink cursor past a page, with the parent's mark when
// the page was pushed, unless a re-sync placed the cursors since the page was
// read: then the page is void and is read again from the new position.
func (c *Client) ackUplink(st *store.Store, epoch uint64, stream string, next uint64, pushed bool, hwm uint64) bool {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	if c.epoch != epoch {
		return false
	}
	up := uns.UplinkCursor(c.parentPub)
	if !pushed {
		st.CursorAck(up, stream, next)
		return true
	}
	if _, err := st.CursorAckMarked(up, stream, next, c.parentPub, hwm); err != nil {
		c.log.Error("uplink cursor not persisted — the page is sent again and the parent drops what it has",
			"stream", stream, "err", err)
	}
	return true
}

// requestResync is how the uplink loop hands a refusal to the downlink loop:
// it flags the re-sync and ends the poll in flight, which would otherwise sit
// out its wait at a position of the old incarnation.
func (c *Client) requestResync() {
	c.resyncWanted.Store(true)
	c.syncMu.Lock()
	cancel := c.pollCancel
	c.syncMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// commandsRehandedBelow is the parent offset below which a command this node
// already holds is one the parent hands a second time (see Resync), 0 when
// this node never re-synced with the parent's current store.
func (c *Client) commandsRehandedBelow() uint64 {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	if c.state.Resync == nil || c.state.Resync.ParentStore != c.state.Store {
		return 0
	}
	return c.state.Resync.CommandsRehandedBelow
}

// recordParent stores the first incarnation this node learns of its parent.
func (c *Client) recordParent(st *store.Store, id string) {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	next := c.state
	next.Store = id
	raw, err := json.Marshal(next)
	if err == nil {
		err = st.SetParentState(c.parentPub, raw)
	}
	if err != nil {
		c.log.Warn("the parent's store incarnation was not recorded — it is recorded on the next contact",
			"parent_store", id, "err", err)
		return
	}
	c.state = next
}

// resync places this node against the parent's store as the handshake
// describes it. It reports whether the positions were persisted; on false
// nothing changed and the caller repeats the handshake.
//
// replay is false when the parent's marks say nothing about this node's
// streams (see checkOwnStore): the uplink cursors then stay where they are.
func (c *Client) resync(eng *engine.Engine, m *metrics.Metrics, res downResult, replay bool) bool {
	st := eng.Store()
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	previous := c.state.Store
	reason := "the parent's store has another incarnation than the one this node knew"
	switch previous {
	case "":
		reason = "the parent asked for a re-sync: this node's positions are ahead of its store"
	case res.Store:
		reason = "the parent asked for a re-sync"
	}
	r := &Resync{
		State: resyncReplaying, Reason: reason, ParentStore: res.Store, PreviousParentStore: previous,
		Started: time.Now().UTC(), Streams: map[string]ResyncRange{}, CommandsRehandedBelow: res.Head,
	}
	cursors := []store.CursorPlacement{
		{Name: uns.DownlinkCursor(c.parentPub), Stream: downlinkStream, To: max(res.Resume, 1)},
		{Name: uns.DownlinkDefCursor(c.parentPub), Stream: downlinkDefStream, To: max(res.DefResume, 1)},
	}
	up := uns.UplinkCursor(c.parentPub)
	marks := map[string]uint64{}
	for _, stream := range uplinkStreams() {
		if !replay {
			break
		}
		hwm := res.HWMs[stream]
		marks[stream] = hwm
		cursor := st.CursorGet(up, stream)
		plan := planReplay(stream, cursor, hwm, st.LWM(stream))
		if plan.gap != nil {
			r.Gaps = append(r.Gaps, *plan.gap)
		}
		span := ResyncRange{From: cursor, To: cursor}
		if plan.rewindTo != 0 {
			cursors = append(cursors, store.CursorPlacement{Name: up, Stream: stream, To: plan.rewindTo})
			span.From = plan.rewindTo
		}
		// A re-sync that interrupts one still replaying takes over what that
		// one had left to send.
		if was := c.state.Resync; was != nil && was.State == resyncReplaying {
			span.To = max(span.To, was.Streams[stream].To)
		}
		if span.From < span.To {
			r.Streams[stream] = span
		}
	}
	if len(r.Streams) == 0 {
		now := r.Started
		r.State, r.Completed = resyncComplete, &now
	}
	next := parentState{Store: res.Store, Resync: r}
	raw, err := json.Marshal(next)
	if err == nil {
		err = st.ResyncParent(c.parentPub, raw, cursors, marks)
	}
	if err != nil {
		c.log.Error("re-sync with the parent not persisted — nothing was moved, the handshake is repeated", "err", err)
		return false
	}
	c.state = next
	c.epoch++
	c.replaying.Store(r.State == resyncReplaying)
	m.UplinkResynced()
	m.SetUplinkResyncActive(r.State == resyncReplaying)
	// Loud when the parent lacks something; a re-sync that finds nothing to
	// send again, as after an operator's change, is a warning.
	level := slog.LevelError
	if r.State == resyncComplete && len(r.Gaps) == 0 {
		level = slog.LevelWarn
	}
	c.log.Log(context.Background(), level, "re-syncing with the parent: its store was restored from a backup, "+
		"replaced, or given a new incarnation. Commands and definitions are taken from where it stands; what it "+
		"lacks of this node's records is sent again",
		"parent", c.base, "reason", reason, "parent_store", res.Store, "previous_parent_store", previous,
		"commands_from", cursors[0].To, "definitions_from", cursors[1].To, "replayed", replaySummary(r.Streams))
	for _, gap := range r.Gaps {
		m.UplinkResyncGap(gap.Stream, gap.ToOffset-gap.FromOffset+1)
		c.log.Error("re-sync gap: the parent lacks records this node no longer holds — local retention pruned "+
			"them, they are not sent again",
			"stream", gap.Stream, "from_offset", gap.FromOffset, "to_offset", gap.ToOffset)
	}
	if r.State == resyncComplete {
		c.log.Info("re-sync with the parent complete: it held everything this node had sent", "parent", c.base)
	}
	return true
}

func replaySummary(streams map[string]ResyncRange) string {
	names := make([]string, 0, len(streams))
	for stream := range streams {
		names = append(names, stream)
	}
	sort.Strings(names)
	out := ""
	for _, stream := range names {
		if out != "" {
			out += " "
		}
		out += fmt.Sprintf("%s:%d-%d", stream, streams[stream].From, streams[stream].To-1)
	}
	return out
}

// noteReplayProgress completes a running re-sync once every replayed stream's
// cursor is back where it stood.
func (c *Client) noteReplayProgress(st *store.Store, m *metrics.Metrics) {
	if !c.replaying.Load() {
		return
	}
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	r := c.state.Resync
	if r == nil || r.State != resyncReplaying {
		c.replaying.Store(false)
		return
	}
	up := uns.UplinkCursor(c.parentPub)
	for stream, span := range r.Streams {
		if st.CursorGet(up, stream) < span.To {
			return
		}
	}
	done := *r
	now := time.Now().UTC()
	done.State, done.Completed = resyncComplete, &now
	next := parentState{Store: c.state.Store, Resync: &done}
	raw, err := json.Marshal(next)
	if err == nil {
		err = st.SetParentState(c.parentPub, raw)
	}
	if err != nil {
		c.log.Warn("re-sync completion not recorded — it is recorded after the next push", "err", err)
		return
	}
	c.state = next
	c.replaying.Store(false)
	m.SetUplinkResyncActive(false)
	c.log.Info("re-sync with the parent complete: everything it lacked and this node still held was sent again",
		"parent", c.base, "took", now.Sub(r.Started).Round(time.Second), "gaps", len(r.Gaps))
}

// ResyncStatus is the last re-sync with the parent, nil when there never was
// one. While it is replaying, each stream's Position says how far it got.
func (c *Client) ResyncStatus(st *store.Store) *Resync {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	if c.state.Resync == nil {
		return nil
	}
	out := *c.state.Resync
	out.Streams = map[string]ResyncRange{}
	up := uns.UplinkCursor(c.parentPub)
	for stream, span := range c.state.Resync.Streams {
		span.Position = min(max(st.CursorGet(up, stream), span.From), span.To)
		out.Streams[stream] = span
	}
	return &out
}

// commandMemory recognises a command this node already holds, whatever offset
// the parent hands it at. The downlink cursor says which offsets were read; it
// says nothing once the parent's store regressed, where an old offset names a
// new command and a new offset can name an old one that came down again.
//
// A command is its topic, payload and timestamp: the parent hands a record down
// with the timestamp it was written with, so the same bytes are the same
// command. It is built from the local commands stream, which holds every
// command this node took, and forgets what retention pruned there.
type commandMemory struct {
	// seen maps a command to the local offset it was stored at or before.
	seen map[[16]byte]uint64
	// pruneAt is the size at which entries below the stream's LWM are dropped.
	pruneAt int
}

func commandKey(topic string, payload []byte, ts int64) [16]byte {
	h := sha256.New()
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(topic)))
	h.Write(n[:])
	h.Write([]byte(topic))
	binary.BigEndian.PutUint64(n[:], uint64(ts)) //nolint:gosec // hashed bit for bit
	h.Write(n[:])
	h.Write(payload)
	var key [16]byte
	copy(key[:], h.Sum(nil))
	return key
}

// commandMemoryPage is how many records one read of the local stream takes.
const commandMemoryPage = 2000

// loadCommandMemory reads every command the local commands stream retains.
func loadCommandMemory(eng *engine.Engine) (*commandMemory, error) {
	st := eng.Store()
	mem := &commandMemory{seen: map[[16]byte]uint64{}}
	isCommand := func(topic string) bool {
		p, err := uns.Parse(topic)
		return err == nil && uns.IsCommand(eng.ClassOf(p.Contract))
	}
	from := st.LWM("commands")
	for {
		recs, next, err := st.Read("commands", from, commandMemoryPage, isCommand)
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			mem.seen[commandKey(r.Topic, r.Payload, r.TS)] = r.Offset
		}
		if next == from {
			break
		}
		from = next
	}
	mem.pruneAt = 2*len(mem.seen) + 4096
	return mem, nil
}

func (m *commandMemory) holds(topic string, payload []byte, ts int64) bool {
	_, ok := m.seen[commandKey(topic, payload, ts)]
	return ok
}

// add remembers a command stored at or before local offset at, and drops what
// retention pruned once the memory doubled.
func (m *commandMemory) add(topic string, payload []byte, ts int64, at, lwm uint64) {
	m.seen[commandKey(topic, payload, ts)] = at
	if len(m.seen) < m.pruneAt {
		return
	}
	for key, offset := range m.seen {
		if offset < lwm {
			delete(m.seen, key)
		}
	}
	m.pruneAt = 2*len(m.seen) + 4096
}
