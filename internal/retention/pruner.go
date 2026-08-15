// Package retention implements the background pruner (the retention design §4–§6): per-stream age/size
// policy evaluation, the cursor clamp (§5 — never prune past the slowest
// protected cursor), the staleness override with its durable _StreamGap
// marker (§6.4), and the entities state refresh that heals a parent's
// current-state view after an overridden prune (§6.5).
//
// The pruner never deletes anything itself: it decides a target LWM and hands
// it to store.Prune, which commits the range tombstone, LWM, byte counter,
// journal entry and the gap marker in ONE atomic synced batch.
package retention

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// streams mirrors the store's fixed stream set (same precedent as the metrics
// package).
var streams = []string{"metrics", "entities", "commands"}

// Pruner is the per-node background pruner. One instance per node; Run is its
// only goroutine entry point.
type Pruner struct {
	st  *store.Store
	eng *engine.Engine
	cfg config.Retention
	m   *metrics.Metrics // nil-safe surface; a later change adds retention families
	// ulid is the node's own ULID: level 4 of the _StreamGap topic is the
	// PRUNING node (design §6.4), so the marker needs the node identity.
	ulid string
	log  *slog.Logger
	now  func() time.Time // injectable clock for tests; defaults to time.Now
}

// NewPruner builds a pruner. ulid is the node's own ULID — it becomes level 4
// of every _StreamGap topic this pruner emits (design §6.4: "level 4 is the
// pruning node"). m may be nil (every Metrics method is nil-safe).
func NewPruner(st *store.Store, eng *engine.Engine, cfg config.Retention, m *metrics.Metrics, ulid string) *Pruner {
	return &Pruner{
		st: st, eng: eng, cfg: cfg, m: m, ulid: ulid,
		log: slog.Default().With("node", ulid, "comp", "retention"),
		now: time.Now,
	}
}

// Run executes one prune cycle every EffectiveInterval until stop is closed.
// An EffectiveInterval of 0 is the operator's explicit "pruner disabled"
// (config contract: the absent-vs-0 translation already happened in
// EffectiveInterval) — Run returns immediately and never touches the store.
// A cycle in progress always completes before Run returns; the caller's
// WaitGroup discipline (node.Stop waits before closing the store) is what
// makes that sufficient.
func (p *Pruner) Run(stop <-chan struct{}) {
	interval := p.cfg.EffectiveInterval()
	if interval <= 0 {
		p.log.Info("retention pruner disabled (interval 0)")
		return
	}
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

// runOnce is one full cycle: one policy evaluation and at most one Prune per
// stream. Per-stream failures are logged and never abort the other streams.
func (p *Pruner) runOnce() {
	for _, stream := range streams {
		p.pruneStream(stream)
	}
}

// overriddenCursor is one cursor the staleness policy stopped protecting and
// the current run is about to prune past.
type overriddenCursor struct {
	name     string
	pos      uint64
	staleFor time.Duration
}

// gapPayload is the _StreamGap contract payload (design §6.4), offsets in the
// pruning node's local coordinates. The json tags are the wire contract
// uns.Validate enforces — do not rename them.
type gapPayload struct {
	Stream            string   `json:"stream"`
	FromOffset        uint64   `json:"from_offset"`
	ToOffset          uint64   `json:"to_offset"`
	FirstTS           int64    `json:"first_ts"`
	LastTS            int64    `json:"last_ts"`
	OverriddenCursors []string `json:"overridden_cursors"`
}

// pruneStream evaluates the retention policy of one stream and commits at
// most one Prune (design §4.1 steps 1–3, §5, §6.4, §6.5).
func (p *Pruner) pruneStream(stream string) {
	pol := p.cfg.EffectiveStream(stream)
	maxAge := time.Duration(pol.MaxAge)
	maxBytes := uint64(pol.MaxBytes)
	window := time.Duration(pol.IgnoreCursorsAfter)
	if maxAge <= 0 && maxBytes == 0 {
		return // no policy configured for this stream — nothing can ever prune
	}
	now := p.now()
	nowMS := now.UnixMilli()

	// §4.1 step 1 — read state, no store mutex.
	lwm := p.st.LWM(stream)
	next := p.st.NextOffset(stream)
	liveBytes := p.st.StreamBytes(stream)

	// §5.1/§5.2 — the protected-cursor floor. Every named cursor on this
	// stream protects it unless the staleness window has passed since its
	// last advance. Cursors on other streams — including the child-side
	// "commands-parent" pseudo-stream, which tracks PARENT offsets — never
	// protect this stream (the existing "never mix the two" rule holds by
	// the Stream field match).
	clamp := next
	blocking := ""
	var stale []overriddenCursor
	for _, c := range p.st.Cursors() {
		if c.Stream != stream {
			continue
		}
		last := c.LastAdvanceMS
		if last == 0 {
			// §5.2 upgrade case: a cursor predating the ct/ timestamps is
			// treated as advancing NOW at first sighting (persisted, so the
			// staleness clock does not restart on every process restart).
			p.st.CursorMarkSeen(c.Name, c.Stream, nowMS)
			last = nowMS
		}
		staleFor := now.Sub(time.UnixMilli(last))
		if window > 0 && staleFor > window {
			stale = append(stale, overriddenCursor{name: c.Name, pos: c.Position, staleFor: staleFor})
			continue // past the window: this cursor stops protecting the stream
		}
		if c.Position < clamp {
			clamp = c.Position
			blocking = c.Name
		}
	}

	// §4.1 step 2 — policy scan from the LWM, capped at the protected floor.
	// The cap makes over-pruning structurally impossible, not a checked
	// condition. The scan runs one record PAST the cap only to learn whether
	// the policy is cursor-clamped (the §5.2 WARN + pressure signal); it
	// never advances newLWM past it.
	cutoff := nowMS - maxAge.Milliseconds()
	newLWM := lwm
	var shed uint64
	var firstTS, lastTS int64
	var pruned uint64
	clamped := false
	scanErr := p.st.ScanRecords(stream, lwm, next, func(off uint64, ts int64, size uint64) bool {
		ageWants := maxAge > 0 && ts < cutoff
		sizeWants := maxBytes > 0 && liveBytes > shed+maxBytes // liveBytes−shed > maxBytes, underflow-safe
		if !ageWants && !sizeWants {
			return false // first record the policy keeps — early exit (§4.1)
		}
		if off >= clamp {
			clamped = true // policy wants more, a live cursor forbids it
			return false
		}
		if pruned == 0 || ts < firstTS {
			firstTS = ts
		}
		if pruned == 0 || ts > lastTS {
			lastTS = ts
		}
		shed += size
		pruned++
		newLWM = off + 1
		return true
	})
	if scanErr != nil {
		p.log.Error("retention policy scan failed", "stream", stream, "err", scanErr)
		return
	}
	if clamped {
		p.log.Warn("retention policy cursor-clamped: policy wants to prune further but a live cursor forbids it (spec §5.2)",
			"stream", stream, "cursor", blocking, "clamp", clamp, "lwm", lwm)
	}
	if newLWM <= lwm {
		return // nothing to prune this cycle
	}

	// §6.4 — one durable _StreamGap marker per overridden prune run.
	// "Overridden" = a stale cursor the range [lwm, newLWM) is being pruned
	// past: its position is below the new LWM, so records it had not read
	// are now gone. The marker describes THIS run's pruned span [lwm,
	// newLWM-1] and its time span — the same numbers as the run's journal
	// entry; spans destroyed by earlier runs were covered by earlier markers.
	var overridden []overriddenCursor
	for _, c := range stale {
		if c.pos < newLWM {
			overridden = append(overridden, c)
		}
	}
	var gapRecords []store.Record
	if len(overridden) > 0 {
		names := make([]string, len(overridden))
		for i, c := range overridden {
			names[i] = c.name
			p.log.Error("retention staleness override: pruning past a stale cursor (spec §5.2) — the consumer will see a gap",
				"stream", stream, "cursor", c.name, "position", c.pos,
				"stale_for", c.staleFor, "window", window, "new_lwm", newLWM)
		}
		payload, err := json.Marshal(gapPayload{
			Stream:            stream,
			FromOffset:        lwm,
			ToOffset:          newLWM - 1,
			FirstTS:           firstTS,
			LastTS:            lastTS,
			OverriddenCursors: names,
		})
		if err != nil {
			p.log.Error("retention gap marker encode failed — skipping prune to stay honest", "stream", stream, "err", err)
			return
		}
		// ClassGap: an event, no KV projection (KVPath empty), not retained.
		// Appended in the prune batch itself, post-LWM, so it survives its
		// own prune run (§6.4).
		gapRecords = append(gapRecords, store.Record{
			Topic:   "colca/v1/_StreamGap/" + p.ulid + "/" + stream,
			Payload: payload,
			TS:      nowMS,
		})
	}

	// §4.1 step 3 — one atomic synced batch inside the store.
	removed, err := p.st.Prune(stream, newLWM, gapRecords)
	if err != nil {
		p.log.Error("prune failed", "stream", stream, "up_to", newLWM, "err", err)
		return
	}
	p.log.Info("pruned", "stream", stream, "records", removed, "bytes", shed,
		"lwm", newLWM, "overridden_cursors", len(overridden))

	// §6.5 — entities state refresh, AFTER the prune batch (and therefore
	// after the marker) committed: gap-then-state, in offsets. Only for
	// entities — metrics self-heal at their own cadence (re-presenting a
	// dead signal's pre-outage reading as fresh state is the wrong move) and
	// commands have no current state at all.
	if stream == "entities" && len(overridden) > 0 {
		minPos := overridden[0].pos
		for _, c := range overridden[1:] {
			if c.pos < minPos {
				minPos = c.pos
			}
		}
		p.refreshEntities(minPos, newLWM)
	}
}

// refreshEntities re-appends the current KV entry of every affected entity
// path to the entities stream (design §6.5). Affected = entity-class KV
// entries whose Offset ∈ [minPos, newLWM): produced by a record some
// overridden consumer had not yet read and that is now gone. Entries below
// minPos were already consumed; entries at or above newLWM still exist in the
// stream and flow normally.
//
// Each refresh goes through engine.IngestAdmin — the on-node service path
// into the single delivery point (persistTS): original topic and payload in
// local coordinates (no rewrite), contract validation, atomic append with a
// new offset and new KV Offset, local bus mirror and retained republish for
// free, and the uplink picks it up like any other entities record. The
// record's timestamp is the refresh time, which is what keeps a refresh from
// being age-pruned again immediately (a preserved original timestamp would
// recreate the hole on the next cycle).
func (p *Pruner) refreshEntities(minPos, newLWM uint64) {
	refreshed := 0
	for _, e := range p.st.KVScan("") {
		if e.Offset < minPos || e.Offset >= newLWM {
			continue
		}
		parsed, err := uns.Parse(e.Topic)
		if err != nil || uns.ClassOf(parsed.Contract) != uns.ClassEntity {
			continue // metrics-class KV entries share the projection; only entities refresh (§6.5)
		}
		if _, err := p.eng.IngestAdmin(e.Topic, e.Payload); err != nil {
			p.log.Error("entities state refresh append failed", "topic", e.Topic, "err", err)
			continue
		}
		refreshed++
	}
	p.log.Info("entities state refresh", "paths", refreshed, "offset_range_from", minPos, "offset_range_to", newLWM-1)
}
