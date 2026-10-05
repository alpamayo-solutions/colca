// Package retention is the background pruner: per-stream age and size policy,
// clamped at the slowest protected cursor, with a staleness override that
// leaves a durable _StreamGap marker, and a state refresh that restores the
// parent's view of entities after an overridden prune. The pruner only picks a
// target; store.Prune commits it in one atomic batch.
package retention

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// streams mirrors the store's stream set. definitions is left out on purpose: a
// pruned definition cannot be fetched again, so that stream is compacted
// instead.
var streams = []string{"metrics", "entities", "commands", "audit", "alarms", "annotations", "logs"}

// definitionsStream is reclaimed by compaction instead (compactDefinitions).
const definitionsStream = "definitions"

// Pruner is the per-node background pruner. One instance per node; Run is its
// only goroutine entry point.
type Pruner struct {
	st  *store.Store
	eng *engine.Engine
	cfg config.Retention
	m   *metrics.Metrics // nil-safe; prune, gap and refresh counters
	// ulid is this node's ULID; level 4 of a _StreamGap topic names the pruning
	// node.
	ulid string
	log  *slog.Logger
	now  func() time.Time // injectable clock for tests; defaults to time.Now
	// publish appends a state refresh, engine.IngestRefresh by default. It is
	// guarded on the snapshot's Offset, so a tombstone or newer write racing the
	// refresh wins; applied=false with a nil error is that skip. Tests replace it.
	publish func(topic string, payload []byte, ifKVOffset uint64) (applied bool, err error)
	// beforePrune (tests only) runs between the policy evaluation and Prune, the
	// window the store's in-batch cursor recheck closes.
	beforePrune func(stream string)
	// evictFrom is where the foreign node-private sweep resumes per stream. It lives
	// in memory only; after a restart the sweep starts over at the LWM, which is
	// harmless.
	evictFrom map[string]uint64
	// signalFrom is the signal the per-signal pass resumes at per stream, "" for
	// the start of the index. In memory only, like evictFrom.
	signalFrom map[string]string
	// passBudget bounds how long one stream may keep pruning within a cycle when
	// a pass stopped at its scan cap. Real time, not the injected clock.
	passBudget time.Duration
	// scanCap bounds one policy scan, store.DefaultPolicyScanCap outside tests.
	scanCap uint64
}

// defaultPassBudget is how long one stream may keep pruning in one cycle. A
// large hub appends more records per interval than one capped scan covers, so
// the pruner repeats the pass instead of falling behind by the difference.
const defaultPassBudget = 30 * time.Second

// signalVisitCap bounds the index entries one PruneSignals call visits; the
// pass repeats within passBudget and resumes next cycle where it stopped.
const signalVisitCap = 200_000

// NewPruner builds a pruner. ulid is this node's ULID, level 4 of every
// _StreamGap topic it emits. m may be nil.
func NewPruner(st *store.Store, eng *engine.Engine, cfg config.Retention, m *metrics.Metrics, ulid string) *Pruner {
	return &Pruner{
		st: st, eng: eng, cfg: cfg, m: m, ulid: ulid,
		log:        slog.Default().With("node", ulid, "comp", "retention"),
		now:        time.Now,
		evictFrom:  map[string]uint64{},
		signalFrom: map[string]string{},
		passBudget: defaultPassBudget,
		scanCap:    store.DefaultPolicyScanCap,
		publish: func(topic string, payload []byte, ifKVOffset uint64) (bool, error) {
			_, applied, err := eng.IngestRefresh(topic, payload, ifKVOffset)
			return applied, err
		},
	}
}

// Run runs one prune cycle every EffectiveInterval until stop is closed; an
// interval of 0 disables the pruner and Run returns at once. A running cycle
// always completes before Run returns. Before the first cycle, Run finishes any
// state refresh a previous process left pending.
func (p *Pruner) Run(stop <-chan struct{}) {
	interval := p.cfg.EffectiveInterval()
	if interval <= 0 {
		p.log.Info("retention pruner disabled (interval 0)")
		return
	}
	p.completePendingRefresh()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			p.runOnce()
		}
	}
}

// runOnce is one cycle: pending refreshes first, then at most one Prune per
// stream, then definitions compaction and the sweep of node-private state
// authored elsewhere. A failing stream is logged and does not stop the others.
func (p *Pruner) runOnce() {
	p.completePendingRefresh()
	for _, stream := range streams {
		// Repeat while a pass stopped at its scan cap and still removed records, so a
		// stream that grows faster than one scan per interval cannot fall behind.
		for start := time.Now(); p.pruneStream(stream); {
			if time.Since(start) >= p.passBudget {
				break
			}
		}
		p.pruneSignals(stream)
	}
	p.compactDefinitions()
	p.evictForeignPrivateState()
}

// evictForeignPrivateState removes node-private state (uns.IsNodePrivate) that
// another node authored, such as the Edit replay receipts ancestors collected.
// Only the author reads it, and nothing will ever retire it here. The records
// are scattered rather than a prefix, so each cycle walks at most
// DefaultPolicyScanCap records and the next resumes where it stopped.
func (p *Pruner) evictForeignPrivateState() {
	removedKV, err := p.st.EvictKV(p.strandedHere)
	if err != nil {
		p.log.Error("evicting foreign node-private KV entries failed — they stay in every /kv listing until this clears", "err", err)
	}
	var removedRecords, bytes uint64
	for _, stream := range uns.NodePrivateStreams() {
		st, err := p.st.EvictRecords(stream, p.evictFrom[stream], store.DefaultPolicyScanCap, p.strandedHere)
		if err != nil {
			p.log.Error("evicting foreign node-private records failed — the sweep restarts next cycle",
				"stream", stream, "err", err)
			p.evictFrom[stream] = 0
			continue
		}
		if st.Done {
			p.evictFrom[stream] = 0
		} else {
			p.evictFrom[stream] = st.Resume
		}
		removedRecords += st.Removed
		bytes += st.Bytes
	}
	if removedKV == 0 && removedRecords == 0 {
		return
	}
	p.log.Info("evicted node-private state authored by other nodes",
		"kv_entries", removedKV, "records", removedRecords, "bytes", bytes)
}

// strandedHere reports whether topic is node-private state authored by another
// node: the uplink's leavesTheNode check, turned around.
func (p *Pruner) strandedHere(topic string) bool {
	parsed, err := uns.Parse(topic)
	return err == nil && uns.IsNodePrivate(parsed.Contract) && parsed.NodeID != p.ulid
}

// compactDefinitions drops definitions a later record superseded, wherever they
// sit, which a prefix prune cannot express. There is nothing to tune: a
// superseded definition is dead once its successor lands.
func (p *Pruner) compactDefinitions() {
	st, err := p.st.Compact(definitionsStream)
	if err != nil {
		p.log.Error("compacting the definitions stream failed — it keeps growing until this clears",
			"stream", definitionsStream, "err", err)
		return
	}
	if st.Superseded == 0 && st.Tombstones == 0 {
		return
	}
	p.log.Info("definitions compacted", "stream", definitionsStream,
		"superseded", st.Superseded, "tombstones", st.Tombstones, "bytes", st.Bytes)
}

// overriddenCursor is one cursor the staleness policy stopped protecting and
// the current run is about to prune past.
type overriddenCursor struct {
	name     string
	pos      uint64
	staleFor time.Duration
}

// gapPayload is the _StreamGap payload, with offsets in the pruning node's local
// coordinates. The json tags are the wire format uns.Validate checks.
type gapPayload struct {
	Stream            string   `json:"stream"`
	FromOffset        uint64   `json:"from_offset"`
	ToOffset          uint64   `json:"to_offset"`
	FirstTS           int64    `json:"first_ts"`
	LastTS            int64    `json:"last_ts"`
	OverriddenCursors []string `json:"overridden_cursors"`
}

// pruneStream evaluates one stream's retention policy and commits at most one
// Prune. again reports that the policy scan stopped at its cap after removing
// records, so another pass would prune further.
func (p *Pruner) pruneStream(stream string) (again bool) {
	pol := p.cfg.EffectiveStream(stream)
	maxAge := time.Duration(pol.MaxAge)
	maxBytes := uint64(pol.MaxBytes)
	window := time.Duration(pol.IgnoreCursorsAfter)
	if maxAge <= 0 && maxBytes == 0 {
		return false // no policy configured for this stream — nothing can ever prune
	}
	now := p.now()
	nowMS := now.UnixMilli()

	// Read state without the store mutex.
	lwm := p.st.LWM(stream)
	next := p.st.NextOffset(stream)
	liveBytes := p.st.StreamBytes(stream)

	clamp, blocking, stale := p.cursorFloor(stream, next, now, window)

	// The policy scan stops at the protected floor and after scanCap records, so it
	// cannot prune past a live cursor. If the scan cap stops it, newLWM is still a
	// safe floor and runOnce repeats the pass while its time budget lasts.
	newLWM, clamped, capped, scanErr := p.st.PolicyPruneTarget(stream, lwm, next, now, maxAge, maxBytes, liveBytes, clamp, p.scanCap)
	if scanErr != nil {
		p.log.Error("retention policy scan failed", "stream", stream, "err", scanErr)
		return false
	}
	if clamped {
		p.log.Warn("retention policy cursor-clamped: policy wants to prune further but a live cursor forbids it",
			"stream", stream, "cursor", blocking, "clamp", clamp, "lwm", lwm)
	}
	if capped {
		p.log.Debug("retention policy scan hit its record cap: pruning to its floor now, then scanning again",
			"stream", stream, "scan_cap", p.scanCap, "lwm", lwm, "target", newLWM)
	}
	if newLWM <= lwm {
		return false // nothing to prune this cycle
	}

	// One durable _StreamGap marker per run that prunes past a stale cursor,
	// describing this run's span. The store may shrink the range if a cursor acked
	// meanwhile, so the marker and the refresh range are built in the plan callback
	// from the effective span; cursors outside it are no longer overridden.
	var candidates []overriddenCursor
	for _, c := range stale {
		if c.pos < newLWM {
			candidates = append(candidates, c)
		}
	}
	names := make([]string, len(candidates))
	for i, c := range candidates {
		names[i] = c.name
	}
	// Commands still queued for a delivery cursor that is overridden will never be
	// handed on: each gets a 410 _Ack in the prune batch, so its sender learns it
	// was dropped. Collected before Prune; plan keeps those in the effective span.
	dropped := p.droppedCommands(stream, candidates, newLWM)
	var answeredDrops []queuedDrop // set by plan; valid only when Prune committed
	var applied []overriddenCursor // set by plan; valid only when Prune committed
	var effectiveShed uint64       // set by plan from the EFFECTIVE (post-recheck) span; valid only when Prune committed
	plan := func(span store.PruneSpan) store.PruneOutcome {
		effectiveShed = span.Shed
		var eff []overriddenCursor
		minPos := uint64(0)
		for _, c := range candidates {
			if c.pos <= span.To {
				eff = append(eff, c)
				if minPos == 0 || c.pos < minPos {
					minPos = c.pos
				}
			}
		}
		applied = eff
		if len(eff) == 0 {
			return store.PruneOutcome{}
		}
		effNames := make([]string, len(eff))
		for i, c := range eff {
			effNames[i] = c.name
		}
		payload, err := json.Marshal(gapPayload{
			Stream:            stream,
			FromOffset:        span.From,
			ToOffset:          span.To,
			FirstTS:           span.FirstTS,
			LastTS:            span.LastTS,
			OverriddenCursors: effNames,
		})
		if err != nil {
			// Unreachable for this struct; if it ever fires, prune without a
			// marker rather than deadlock the policy, and say so loudly.
			p.log.Error("retention gap marker encode failed — pruning WITHOUT a marker", "stream", stream, "err", err)
			applied = nil
			return store.PruneOutcome{}
		}
		// A gap marker is an event with no KV projection. It is appended in the prune
		// batch after the new LWM, so it survives its own prune.
		out := store.PruneOutcome{GapRecords: []store.Record{{
			Topic:   uns.Prefix() + "_StreamGap/" + p.ulid + "/" + stream,
			Payload: payload,
			TS:      nowMS,
		}}}
		answeredDrops = nil
		for _, d := range dropped {
			if d.offset <= span.To && d.cursorPos <= span.To {
				out.GapRecords = append(out.GapRecords, d.answer)
				answeredDrops = append(answeredDrops, d)
			}
		}
		if stream == "entities" {
			// The refresh obligation goes into the prune batch as rp/{stream}, so a crash
			// before the refresh leaves it owed.
			out.Refresh = &store.RefreshRange{From: minPos, To: span.To + 1}
		}
		return out
	}

	if p.beforePrune != nil {
		p.beforePrune(stream)
	}
	// One atomic, synced batch; the store rechecks cursors against names.
	removed, err := p.st.Prune(stream, newLWM, names, plan)
	if err != nil {
		p.log.Error("prune failed", "stream", stream, "up_to", newLWM, "err", err)
		return false
	}
	if removed == 0 {
		return false // shrunk to a no-op by the in-batch recheck: nothing was deleted
	}
	// A run is a cycle that removed something. The bytes come from the store's count
	// after its recheck (PruneSpan.Shed), not from shed above, which overstates them
	// when the range shrank.
	p.m.RetentionPruneRun(stream)
	p.m.RetentionPruned(stream, removed, effectiveShed)
	if len(applied) > 0 {
		p.m.RetentionGapRecorded(stream) // exactly one _StreamGap marker per overriding run
	}
	for _, d := range answeredDrops {
		p.m.CommandDropped(metrics.CommandDropPruned)
		p.log.Error("retention pruned a command before it was delivered; its sender got a 410 _Ack",
			"stream", stream, "offset", d.offset, "for", d.who)
	}
	for _, c := range applied {
		p.log.Error("retention staleness override: pruned past a stale cursor; the consumer will see a gap",
			"stream", stream, "cursor", c.name, "position", c.pos,
			"stale_for", c.staleFor, "window", window)
	}
	p.log.Info("pruned", "stream", stream, "records", removed,
		"lwm", lwm+removed, "overridden_cursors", len(applied))

	// Refresh entities once the prune batch, and with it the marker, has committed.
	// Only entities need this: metrics refresh themselves and commands have no
	// current state.
	if stream == "entities" && len(applied) > 0 {
		p.completePendingRefresh()
	}
	return capped
}

// cursorFloor is the protected-cursor floor: every cursor on this stream
// protects it until the staleness window has passed since its last advance.
// Cursors on other streams, such as commands-parent, never count. It returns
// the floor (next when no cursor protects), the cursor that sets it, and the
// stale cursors the caller may pass with a _StreamGap marker.
func (p *Pruner) cursorFloor(stream string, next uint64, now time.Time, window time.Duration) (clamp uint64, blocking string, stale []overriddenCursor) {
	protecting, staleCursors := p.st.ProtectedCursors(stream, now, window)
	clamp = next
	for _, c := range protecting {
		if c.LastAdvanceMS == 0 {
			// A cursor from before ct/ timestamps counts as advancing now. Only the pruner
			// persists that stamp, since it is about to prune.
			p.st.CursorMarkSeen(c.Name, stream, now.UnixMilli())
		}
		if c.Position < clamp {
			clamp = c.Position
			blocking = c.Name
		}
	}
	for _, c := range staleCursors {
		// staleCursors never holds a cursor without a timestamp, so this recomputes the
		// value ProtectedCursors used.
		staleFor := now.Sub(time.UnixMilli(c.LastAdvanceMS))
		stale = append(stale, overriddenCursor{name: c.Name, pos: c.Position, staleFor: staleFor})
	}
	return clamp, blocking, stale
}

// pruneSignals applies the stream's per-signal rules (retention.streams.<s>.
// signals): it removes each matching signal's records older than the rule's
// window, keeping the newest of them as the value in force when the window
// opens, below the same cursor floor as the stream prune. A stale cursor it
// passes gets a _StreamGap marker naming it, as with the stream prune; the
// marker's range is sparse, since other signals' records in it stay.
func (p *Pruner) pruneSignals(stream string) {
	pol := p.cfg.EffectiveStream(stream)
	if len(pol.Signals) == 0 {
		return
	}
	now := p.now()
	next := p.st.NextOffset(stream)
	clamp, _, stale := p.cursorFloor(stream, next, now, time.Duration(pol.IgnoreCursorsAfter))
	names := make([]string, len(stale))
	for i, c := range stale {
		names[i] = c.name
	}
	markers := 0
	marker := func(span store.PruneSpan, passed []string) []store.Record {
		payload, err := json.Marshal(gapPayload{
			Stream: stream, FromOffset: span.From, ToOffset: span.To,
			FirstTS: span.FirstTS, LastTS: span.LastTS, OverriddenCursors: passed,
		})
		if err != nil {
			p.log.Error("retention gap marker encode failed — pruning WITHOUT a marker", "stream", stream, "err", err)
			return nil
		}
		markers++
		return []store.Record{{
			Topic:   uns.Prefix() + "_StreamGap/" + p.ulid + "/" + stream,
			Payload: payload,
			TS:      now.UnixMilli(),
		}}
	}
	start := time.Now()
	var removed, bytes uint64
	var overridden []string
	for {
		res, err := p.st.PruneSignals(stream, p.signalFrom[stream], now, pol.SignalMaxAge, clamp, names, signalVisitCap, marker)
		removed += res.Removed
		bytes += res.Bytes
		overridden = append(overridden, res.Overridden...)
		if err != nil {
			p.log.Error("per-signal prune failed — the pass restarts next cycle", "stream", stream, "err", err)
			p.signalFrom[stream] = ""
			break
		}
		p.signalFrom[stream] = res.Resume
		if res.Resume == "" || time.Since(start) >= p.passBudget {
			break
		}
	}
	if removed == 0 {
		return
	}
	p.m.RetentionPruneRun(stream)
	p.m.RetentionPruned(stream, removed, bytes)
	for range markers {
		p.m.RetentionGapRecorded(stream)
	}
	for _, name := range overridden {
		p.log.Error("retention staleness override: per-signal retention pruned past a stale cursor; the consumer will see a gap",
			"stream", stream, "cursor", name, "window", time.Duration(pol.IgnoreCursorsAfter))
	}
	p.log.Info("pruned per signal", "stream", stream, "records", removed, "bytes", bytes,
		"overridden_cursors", len(overridden))
}

// queuedDrop is a command a prune removes before the delivery cursor that held
// it passed it, with the 410 _Ack that answers it.
type queuedDrop struct {
	offset    uint64
	cursorPos uint64
	who       string
	answer    store.Record
}

// droppedScanBatch bounds one read of droppedCommands; the loop continues past it.
const droppedScanBatch = 4096

// droppedCommands lists the live commands in [cursor, upTo) of each overridden
// delivery cursor (a child node's downlink cursor, a machine's delivery cursor)
// on the commands stream, each with its answer. A command two cursors held is
// answered once.
func (p *Pruner) droppedCommands(stream string, overridden []overriddenCursor, upTo uint64) []queuedDrop {
	if stream != "commands" || p.eng == nil {
		return nil
	}
	var out []queuedDrop
	seen := map[uint64]bool{}
	for _, c := range overridden {
		queued, who, ok := p.eng.QueuedFor(c.name)
		if !ok {
			continue
		}
		message := "dropped: retention pruned it before " + who + " received it"
		for from := c.pos; from < upTo; {
			recs, next, err := p.st.ReadRecordsBounded(context.Background(), stream, from, 500, int(min(upTo-from, uint64(droppedScanBatch))), queued)
			if err != nil {
				p.log.Error("retention: queued commands about to be pruned could not be read; their senders will not be answered",
					"cursor", c.name, "err", err)
				break
			}
			for _, r := range recs {
				if r.Offset >= upTo || seen[r.Offset] {
					continue
				}
				if answer, ok := p.eng.QueuedAnswer(r, 410, message); ok {
					seen[r.Offset] = true
					out = append(out, queuedDrop{offset: r.Offset, cursorPos: c.pos, who: who, answer: answer})
				}
			}
			if next <= from {
				break
			}
			from = next
		}
	}
	return out
}

// completePendingRefresh runs the persisted entities refresh, if any: at
// startup, at each cycle start and right after an overriding entities prune. The
// rp/ key is cleared only once every affected append succeeded; paths already
// refreshed drop out on retry because their KV Offset has moved past the range.
func (p *Pruner) completePendingRefresh() {
	r, ok := p.st.RefreshPending("entities")
	if !ok {
		return
	}
	if !p.refreshEntities(r.From, r.To) {
		return // failures logged per path; the obligation stays pending
	}
	if err := p.st.ClearRefreshPending("entities"); err != nil {
		p.log.Error("clearing completed refresh obligation failed (will re-run, refresh is idempotent)", "err", err)
	}
}

// refreshNamesLogged bounds how many refreshed topics one log line lists.
const refreshNamesLogged = 32

// refreshEntities appends the current KV entry of every entity path whose
// Offset is in [from, to) again: state an overridden consumer never read and
// that is now pruned. Each append goes through publish with the refresh time as
// timestamp, so it is not pruned again at once, and is guarded on the
// snapshot's Offset, so a tombstone or newer write wins and the skip counts as
// done. It reports whether every append succeeded; a payload that fails
// validation keeps the range pending.
func (p *Pruner) refreshEntities(from, to uint64) bool {
	refreshed, skipped, failed := 0, 0, 0
	// The refreshed topics for the log line, capped so a large refresh cannot write
	// an unbounded line.
	var names []string
	entries, err := p.st.KVScan("")
	if err != nil {
		// Like a failed append: the range stays pending and is retried next cycle.
		p.log.Error("entities state refresh KV scan failed (range stays pending, retried next cycle)", "err", err)
		return false
	}
	for _, e := range entries {
		if e.Offset < from || e.Offset >= to {
			continue
		}
		parsed, err := uns.Parse(e.Topic)
		if err != nil || !uns.NeedsStateRefresh(p.eng.ClassOf(parsed.Contract)) {
			continue // only entity-class entries are refreshed
		}
		applied, err := p.publish(e.Topic, e.Payload, e.Offset)
		if err != nil {
			p.log.Error("entities state refresh append failed (range stays pending, retried next cycle)", "topic", e.Topic, "err", err)
			failed++
			p.m.StateRefreshFailed()
			continue
		}
		if !applied {
			p.log.Warn("entities state refresh skipped: path retired or superseded since the snapshot",
				"topic", e.Topic, "snapshot_offset", e.Offset)
			skipped++
			p.m.StateRefreshSkipped()
			continue
		}
		refreshed++
		if len(names) < refreshNamesLogged {
			names = append(names, e.Topic)
		}
		p.m.StateRefreshApplied()
	}
	if refreshed > 0 || skipped > 0 || failed > 0 {
		p.log.Info("entities state refresh", "paths", refreshed, "skipped", skipped, "failed", failed,
			"offset_range_from", from, "offset_range_to", to-1,
			"topics", strings.Join(names, " "),
			"topics_truncated", refreshed > len(names))
	}
	return failed == 0
}
