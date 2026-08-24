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
//
// `definitions` is deliberately absent: it is never pruned by age or size
// (definition-stream design §6). A definition has no read side to fall back on,
// so a pruned one is a node that no longer knows what a type or a group is,
// with nowhere to ask. It is compacted instead — latest per path — which is a
// different algorithm and lives outside this policy loop.
var streams = []string{"metrics", "entities", "commands", "audit", "alarms"}

// definitionsStream is reclaimed by compaction instead (compactDefinitions).
const definitionsStream = "definitions"

// Pruner is the per-node background pruner. One instance per node; Run is its
// only goroutine entry point.
type Pruner struct {
	st  *store.Store
	eng *engine.Engine
	cfg config.Retention
	m   *metrics.Metrics // nil-safe surface: prune/gap/refresh counters (design §8)
	// ulid is the node's own ULID: level 4 of the _StreamGap topic is the
	// PRUNING node (design §6.4), so the marker needs the node identity.
	ulid string
	log  *slog.Logger
	now  func() time.Time // injectable clock for tests; defaults to time.Now
	// publish is the §6.5 refresh append path, defaulting to
	// engine.IngestRefresh: guarded on the KVScan snapshot's Offset so a
	// tombstone or newer write racing the refresh is skipped, never
	// resurrected (spec §6.5 [delta]/§7.1). applied=false with nil error is
	// the guard skip. A seam so tests can fail or interleave individual
	// appends deterministically.
	publish func(topic string, payload []byte, ifKVOffset uint64) (applied bool, err error)
	// beforePrune, when set (tests only), runs between the policy evaluation
	// and the Prune call — the exact window the store's in-batch cursor
	// recheck (spec §5.2 [delta]) exists to close.
	beforePrune func(stream string)
}

// NewPruner builds a pruner. ulid is the node's own ULID — it becomes level 4
// of every _StreamGap topic this pruner emits (design §6.4: "level 4 is the
// pruning node"). m may be nil (every Metrics method is nil-safe).
func NewPruner(st *store.Store, eng *engine.Engine, cfg config.Retention, m *metrics.Metrics, ulid string) *Pruner {
	return &Pruner{
		st: st, eng: eng, cfg: cfg, m: m, ulid: ulid,
		log: slog.Default().With("node", ulid, "comp", "retention"),
		now: time.Now,
		publish: func(topic string, payload []byte, ifKVOffset uint64) (bool, error) {
			_, applied, err := eng.IngestRefresh(topic, payload, ifKVOffset)
			return applied, err
		},
	}
}

// Run executes one prune cycle every EffectiveInterval until stop is closed.
// An EffectiveInterval of 0 is the operator's explicit "pruner disabled"
// (config contract: the absent-vs-0 translation already happened in
// EffectiveInterval) — Run returns immediately and never touches the store.
// A cycle in progress always completes before Run returns; the caller's
// WaitGroup discipline (node.Stop waits before closing the store) is what
// makes that sufficient.
//
// Before the first cycle, Run completes any state-refresh obligation a
// previous process persisted but did not finish (spec §6.5 [delta] — the
// rp/ key survives a crash between the prune batch and the refresh).
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

// runOnce is one full cycle: any pending refresh first (crash recovery and
// retry of previously failed appends), then one policy evaluation and at
// most one Prune per stream. Per-stream failures are logged and never abort
// the other streams.
func (p *Pruner) runOnce() {
	p.completePendingRefresh()
	for _, stream := range streams {
		p.pruneStream(stream)
	}
	p.compactDefinitions()
}

// compactDefinitions runs the definitions stream's own reclamation
// (definition-stream design §6). It is not in the loop above because it is not
// the same operation: the policy prunes a contiguous prefix by age and size,
// and this drops whatever a later record superseded, wherever it sits.
//
// No policy knobs, deliberately. There is nothing to tune — a superseded
// definition is dead the moment its successor lands, and a retraction lives
// exactly until every consumer has read it.
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
	// store.ProtectedCursors' own Stream match). Shared with the metrics
	// collector (design §8) so both always classify identically — one scan,
	// not two hand-kept copies.
	protecting, staleCursors := p.st.ProtectedCursors(stream, now, window)
	clamp := next
	blocking := ""
	var stale []overriddenCursor
	for _, c := range protecting {
		if c.LastAdvanceMS == 0 {
			// §5.2 upgrade case: a cursor predating the ct/ timestamps is
			// treated as advancing NOW at first sighting (persisted, so the
			// staleness clock does not restart on every process restart).
			// ProtectedCursors is read-only and never does this itself —
			// only the pruner, which is actually about to prune this cycle,
			// persists the stamp.
			p.st.CursorMarkSeen(c.Name, stream, nowMS)
		}
		if c.Position < clamp {
			clamp = c.Position
			blocking = c.Name
		}
	}
	for _, c := range staleCursors {
		// staleCursors never contains a first-sighting (LastAdvanceMS==0)
		// cursor — ProtectedCursors' own staleFor-vs-window classification
		// always resolves that case into protecting — so recomputing
		// staleFor from the raw timestamp here reproduces the exact value
		// ProtectedCursors used internally.
		staleFor := now.Sub(time.UnixMilli(c.LastAdvanceMS))
		stale = append(stale, overriddenCursor{name: c.Name, pos: c.Position, staleFor: staleFor})
	}

	// §4.1 step 2 — policy scan from the LWM, capped at the protected floor
	// AND at store.DefaultPolicyScanCap records examined. The cursor cap
	// makes over-pruning structurally impossible, not a checked condition.
	// The scan runs one record PAST the cursor cap only to learn whether the
	// policy is cursor-clamped (the §5.2 WARN + pressure signal); it never
	// advances newLWM past it. Shared with the metrics collector (design §8,
	// called there with an uncapped clamp).
	//
	// The scan cap matters here too: when the staleness override is active
	// and a cursor has just crossed into "stale" (spec §5.2), it drops out of
	// the clamp entirely — clamp reverts to next, uncapped — so a long-dead
	// consumer plus a large backlog can make this a very long walk. Hitting
	// the scan cap in that state is not an error: newLWM is still a safe
	// floor (never past what the policy actually examined and accepted), so
	// the pruner simply advances as far as it scanned and picks up the rest
	// on the next cycle(s) — chunked pruning across cycles, not a single
	// unbounded pass.
	newLWM, clamped, capped, scanErr := p.st.PolicyPruneTarget(stream, lwm, next, now, maxAge, maxBytes, liveBytes, clamp, store.DefaultPolicyScanCap)
	if scanErr != nil {
		p.log.Error("retention policy scan failed", "stream", stream, "err", scanErr)
		return
	}
	if clamped {
		p.log.Warn("retention policy cursor-clamped: policy wants to prune further but a live cursor forbids it (spec §5.2)",
			"stream", stream, "cursor", blocking, "clamp", clamp, "lwm", lwm)
	}
	if capped {
		p.log.Info("retention policy scan hit the per-cycle record cap: pruning this cycle's floor now, continuing next cycle",
			"stream", stream, "scan_cap", store.DefaultPolicyScanCap, "lwm", lwm, "target", newLWM)
	}
	if newLWM <= lwm {
		return // nothing to prune this cycle
	}

	// §6.4 — one durable _StreamGap marker per overridden prune run.
	// "Overridden" = a stale cursor the pruned range is being pruned past:
	// its position is below the new LWM, so records it had not read are now
	// gone. The marker describes THIS run's pruned span and its time span —
	// the same numbers as the run's journal entry; spans destroyed by
	// earlier runs were covered by earlier markers.
	//
	// The store's in-batch cursor recheck (spec §5.2 [delta]) may SHRINK the
	// range below newLWM if a live cursor was acked concurrently, so the
	// marker and the refresh range are built inside the plan callback from
	// the EFFECTIVE span the store hands it — one source of truth for the
	// span, shared with the journal entry. Candidates whose position falls
	// outside the effective span are no longer overridden and drop out.
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
		// ClassGap: an event, no KV projection (KVPath empty), not retained.
		// Appended in the prune batch itself, post-LWM, so it survives its
		// own prune run (§6.4).
		out := store.PruneOutcome{GapRecords: []store.Record{{
			Topic:   "colca/v1/_StreamGap/" + p.ulid + "/" + stream,
			Payload: payload,
			TS:      nowMS,
		}}}
		if stream == "entities" {
			// §6.5 [delta]: the refresh obligation rides the prune batch as
			// rp/{stream} — a crash between batch and refresh leaves it owed,
			// not lost.
			out.Refresh = &store.RefreshRange{From: minPos, To: span.To + 1}
		}
		return out
	}

	if p.beforePrune != nil {
		p.beforePrune(stream)
	}
	// §4.1 step 3 — one atomic synced batch inside the store (which also
	// runs the §5.2 in-batch cursor recheck against `names`).
	removed, err := p.st.Prune(stream, newLWM, names, plan)
	if err != nil {
		p.log.Error("prune failed", "stream", stream, "up_to", newLWM, "err", err)
		return
	}
	if removed == 0 {
		return // shrunk to a no-op by the in-batch recheck: nothing was deleted
	}
	// design §8: a "run" is one cycle that actually removed something —
	// distinct from pruned records/bytes, which measure the removed span
	// itself. effectiveShed comes from the store's own post-in-batch-recheck
	// accounting (store.PruneSpan.Shed, set inside plan above), not the
	// pre-commit scan's `shed` local — that value describes the INTENDED
	// span and can overstate the true figure whenever the in-batch recheck
	// shrinks upTo (spec §5.2 [delta]).
	p.m.RetentionPruneRun(stream)
	p.m.RetentionPruned(stream, removed, effectiveShed)
	if len(applied) > 0 {
		p.m.RetentionGapRecorded(stream) // exactly one _StreamGap marker per overriding run
	}
	for _, c := range applied {
		p.log.Error("retention staleness override: pruned past a stale cursor (spec §5.2) — the consumer will see a gap",
			"stream", stream, "cursor", c.name, "position", c.pos,
			"stale_for", c.staleFor, "window", window)
	}
	p.log.Info("pruned", "stream", stream, "records", removed,
		"lwm", lwm+removed, "overridden_cursors", len(applied))

	// §6.5 — entities state refresh, AFTER the prune batch (and therefore
	// after the marker) committed: gap-then-state, in offsets. Executed via
	// the persisted obligation so the crash window between batch and refresh
	// is closed. Only entities carry one — metrics self-heal at their own
	// cadence (re-presenting a dead signal's pre-outage reading as fresh
	// state is the wrong move) and commands have no current state at all.
	if stream == "entities" && len(applied) > 0 {
		p.completePendingRefresh()
	}
}

// completePendingRefresh executes the persisted entities refresh obligation,
// if any (spec §6.5 [delta]). Runs at pruner startup (crash recovery), at
// every cycle start (retry of previously failed appends) and immediately
// after an overriding entities prune (the normal same-run path). The rp/ key
// is cleared — its own synced write — only once EVERY affected append
// succeeded; partial failure keeps the range pending and is retried next
// cycle, with already-refreshed paths naturally dropping out because their KV
// Offset now points past the range.
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

// refreshEntities re-appends the current KV entry of every affected entity
// path to the entities stream (design §6.5). Affected = entity-class KV
// entries whose Offset ∈ [from, to): produced by a record some overridden
// consumer had not yet read and that is now gone. Entries below the range
// were already consumed; entries at or above it still exist in the stream and
// flow normally. Returns whether every affected append succeeded.
//
// Each refresh goes through the publish seam — engine.IngestRefresh, the
// guarded variant of the on-node admin path: original topic and payload in
// local coordinates (no rewrite), contract validation, atomic append with a
// new offset and new KV Offset, local bus mirror and retained republish for
// free, and the uplink picks it up like any other entities record. The
// record's timestamp is the refresh time, which is what keeps a refresh from
// being age-pruned again immediately (a preserved original timestamp would
// recreate the hole on the next cycle).
//
// The append is a CAS on the snapshot's KV Offset (spec §6.5 [delta]): a
// tombstone (§7) or newer write landing between this function's KVScan and an
// entry's publish makes the guard fail and the entry is SKIPPED — re-stating
// it would resurrect a retired path or clobber the newer value, and in either
// case the obligation for that path is void (nothing current to refresh, or
// the newer record flows through the stream normally). Skips therefore count
// toward completion. A payload that fails re-validation (contract drift) logs
// ERROR each cycle and keeps the range pending: bounded noise, honest, never
// silent.
func (p *Pruner) refreshEntities(from, to uint64) bool {
	refreshed, skipped, failed := 0, 0, 0
	entries, err := p.st.KVScan("")
	if err != nil {
		// Same treatment as an individual append failure below: the
		// obligation stays pending and is retried next cycle rather than
		// silently completing on a scan the store could not actually finish.
		p.log.Error("entities state refresh KV scan failed (range stays pending, retried next cycle)", "err", err)
		return false
	}
	for _, e := range entries {
		if e.Offset < from || e.Offset >= to {
			continue
		}
		parsed, err := uns.Parse(e.Topic)
		if err != nil || !uns.NeedsStateRefresh(p.eng.ClassOf(parsed.Contract)) {
			continue // metrics-class KV entries share the projection; only entities refresh (§6.5)
		}
		applied, err := p.publish(e.Topic, e.Payload, e.Offset)
		if err != nil {
			p.log.Error("entities state refresh append failed (range stays pending, retried next cycle)", "topic", e.Topic, "err", err)
			failed++
			p.m.StateRefreshFailed()
			continue
		}
		if !applied {
			p.log.Warn("entities state refresh skipped: path retired or superseded since the snapshot (guard, spec §6.5/§7.1)",
				"topic", e.Topic, "snapshot_offset", e.Offset)
			skipped++
			p.m.StateRefreshSkipped()
			continue
		}
		refreshed++
		p.m.StateRefreshApplied()
	}
	if refreshed > 0 || skipped > 0 || failed > 0 {
		p.log.Info("entities state refresh", "paths", refreshed, "skipped", skipped, "failed", failed,
			"offset_range_from", from, "offset_range_to", to-1)
	}
	return failed == 0
}
