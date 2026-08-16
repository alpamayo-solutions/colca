// Package engine is the single place every write converges: the MQTT hook, the
// HTTP publish endpoint, replication apply and downlink apply all go through
// one of the Ingest* methods. Grammar, identity rule, mount rewrite, payload
// validation and the atomic persist live here and nowhere else.
package engine

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// LocalDeliver hands a record to the local MQTT broker (nil when the node has
// no broker, and in unit tests). retain is true for the state contracts, so the
// broker's retained set and the store's KV projection are the same thing seen
// from two sides.
type LocalDeliver func(topic string, payload []byte, retain bool)

// Result describes what an ingest did. Topic is the post-rewrite topic, i.e.
// exactly what was persisted.
type Result struct {
	Persisted bool
	Stream    string
	Offset    uint64
	Topic     string // post-rewrite
}

// Mounts resolves identities to their registry entries — implemented by
// *registry.Manager. The engine consults it for every identity question and
// holds no identity state of its own (auth design §8).
type Mounts interface {
	MountOf(ulid string) (string, bool)
	Get(ulid string) (*uns.Entry, bool)
	// DrainingMount reports whether path falls under a currently draining
	// kind=node child's mount (move-drain design §3.2 item 2) — consulted
	// for every ClassCmd admission so a draining mount stops accepting new
	// commands instead of chasing a moving tail.
	DrainingMount(path string) bool
}

type Engine struct {
	store   *store.Store
	cfg     *config.Config
	deliver LocalDeliver
	ids     Mounts
	log     *slog.Logger
	metrics *metrics.Metrics // nil-safe: every method on a nil receiver is a no-op
	clk     *clock.Clock
}

// New builds an engine. ids is the identity registry (a client without a
// mount is a read-only observer: MountOf misses, so IngestClient rejects its
// publishes with "no mount registered").
//
// m may be nil (unit tests and any caller that does not care about metrics) —
// every Metrics method is nil-safe.
//
// clk may be nil, in which case New builds a default one from cfg (root =
// cfg.Parent == nil, time-sync design §2.1) using the real wall clock. A
// caller that also wires *metrics.Metrics to read this node's clock state
// (node.Start does; most unit tests do not need to) MUST build the *clock.Clock
// itself and pass the SAME instance to both constructors — metrics.New's
// gauges and this engine's offset state must be the same object, or the
// gauges report state nobody ever updates.
func New(s *store.Store, cfg *config.Config, ids Mounts, deliver LocalDeliver, m *metrics.Metrics, clk *clock.Clock) *Engine {
	if clk == nil {
		clk = clock.New(cfg.Parent == nil, time.Now)
	}
	return &Engine{store: s, cfg: cfg, deliver: deliver, ids: ids, log: slog.Default().With("node", cfg.ULID), metrics: m, clk: clk}
}

func (e *Engine) Store() *store.Store { return e.store }

// AuthoritativeNow returns this node's current best estimate of the time
// authority's clock (time-sync design §2.1): wall_now + the offset learned
// from the most recent /downlink or /replicate response, or raw wall time
// on the root and on a node that has never synced.
func (e *Engine) AuthoritativeNow() time.Time {
	return e.clk.AuthoritativeNow()
}

// ApplyClockSample records one offset sample learned from a parent's
// now_ms (time-sync design §2.1/§2.3 rule 4) and warns when the resulting
// drift exceeds time_sync.drift_warn_ms (design §2.4). Callers (RunUplink,
// RunDownlink) apply this BEFORE ingesting the records that arrived in the
// same response, so the first poll after reconnect refreshes time before any
// expiry decision downstream of it. A no-op on the root (clock.ApplySample).
func (e *Engine) ApplyClockSample(nowMS int64) (offsetMS int64) {
	offsetMS = e.clk.ApplySample(nowMS)
	if warn := e.cfg.TimeSync.EffectiveDriftWarnMS(); abs64(offsetMS) > warn {
		e.log.Warn("clock drift exceeds warn threshold (time-sync design §2.4)",
			"offset_ms", offsetMS, "drift_warn_ms", warn)
	}
	return offsetMS
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// MountOf resolves the mount a child or client is attached under.
func (e *Engine) MountOf(ulid string) (string, bool) {
	return e.ids.MountOf(ulid)
}

// IngestClient: a directly attached MQTT client (machine/service) publishes.
// Rules: uns grammar, class must be data/entity/ack, level-4 == identity, mount
// rewrite, validate, persist. A non-UNS topic is not an error — it is normal
// broker traffic that simply is not persisted.
func (e *Engine) IngestClient(identity, topic string, payload []byte) (Result, error) {
	if !uns.IsUns(topic) {
		return Result{Persisted: false}, nil // normal broker behavior outside colca/#
	}
	p, err := uns.Parse(topic)
	if err != nil {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, err
	}
	// Registry entries enter through the enrollment door ONLY (auth §3): no
	// client may author an _EdgeNode, not even its own.
	if p.Contract == "_EdgeNode" {
		e.metrics.RejectPublish(metrics.ReasonRegistryContract)
		return Result{}, fmt.Errorf("client %s may not publish _EdgeNode — registry entries are enrollment-door only", identity)
	}
	class := uns.ClassOf(p.Contract)
	if class == uns.ClassTimeSync {
		// Time-sync design §2.2/§4: _TimeSync is node-local-publish-only —
		// only this node's own beacon loop may ever produce it, straight to
		// the local bus. A client attempting it (even the exact canonical
		// shape) is rejected here with its own reason, never with the
		// generic "grammar" reason Parse's 4-segment relaxation would
		// otherwise fall through to.
		e.metrics.RejectPublish(metrics.ReasonTimeSync)
		return Result{}, fmt.Errorf("client %s may not publish _TimeSync: ephemeral, node-local-publish-only (time-sync design §2.2)", identity)
	}
	if class == uns.ClassCmd {
		// Move-drain design §3.2 item 2: a mount under an active drain stops
		// accepting new commands at admission, independent of the caller's
		// own grants — checked first so a draining destination is rejected
		// for the reason that actually explains it, not folded into
		// cmd_denied.
		if e.ids.DrainingMount(p.Path) {
			e.metrics.RejectPublish(metrics.ReasonDraining)
			return Result{}, fmt.Errorf("client %s: %s is draining — no new commands admitted (move-drain design §3.2)", identity, p.Path)
		}
		// A command needs a covering cmd grant (auth §5.3 ActCmd). Commands
		// target ABSOLUTE node-local paths: no mount rewrite, no level-4
		// identity requirement — the author is not the target's owner.
		entry, ok := e.ids.Get(identity)
		if !ok || !uns.Authorize(entry, uns.ActCmd, topic) {
			e.metrics.RejectPublish(metrics.ReasonCmdDenied)
			return Result{}, fmt.Errorf("client %s: no cmd grant covers %s", identity, topic)
		}
		if err := uns.Validate(p.Contract, payload); err != nil {
			e.metrics.RejectPublish(metrics.ReasonValidation)
			return Result{}, err
		}
		return e.persist(class, p, topic, payload)
	}
	if class == uns.ClassNone {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, fmt.Errorf("client %s may not publish %s", identity, p.Contract)
	}
	if p.NodeID != identity {
		e.metrics.RejectPublish(metrics.ReasonIdentity)
		return Result{}, fmt.Errorf("identity rule: level-4 %q != authenticated identity %q", p.NodeID, identity)
	}
	if err := uns.Validate(p.Contract, payload); err != nil {
		e.metrics.RejectPublish(metrics.ReasonValidation)
		return Result{}, err
	}
	mount, ok := e.ids.MountOf(identity)
	if !ok {
		e.metrics.RejectPublish(metrics.ReasonNoMount)
		return Result{}, fmt.Errorf("no mount registered for %s", identity)
	}
	rewritten := uns.MountInsert(topic, mount)
	rp, err := uns.Parse(rewritten)
	if err != nil {
		// Defensive: MountInsert only adds a path segment to an already-parsed
		// topic, so this can't fail in practice — but a rejection is a
		// rejection, and it is still a grammar failure if it ever does.
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, err
	}
	return e.persist(class, rp, rewritten, payload)
}

// IngestAdmin: local HTTP API with admin token — publishes in node-local
// coordinates, no rewrite, commands allowed, still validated.
func (e *Engine) IngestAdmin(topic string, payload []byte) (Result, error) {
	if !uns.IsUns(topic) {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, fmt.Errorf("admin publish must be colca/#")
	}
	p, err := uns.Parse(topic)
	if err != nil {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, err
	}
	// Even the admin token may not author registry entries through /publish —
	// enrollment has its own door with its own validation (auth §3, §4).
	if p.Contract == "_EdgeNode" {
		e.metrics.RejectPublish(metrics.ReasonRegistryContract)
		return Result{}, fmt.Errorf("_EdgeNode is enrollment-door only — use POST /enroll")
	}
	class := uns.ClassOf(p.Contract)
	if class == uns.ClassTimeSync {
		// Same rule as IngestClient: _TimeSync is node-local-publish-only,
		// not even the admin token may author it through /publish.
		e.metrics.RejectPublish(metrics.ReasonTimeSync)
		return Result{}, fmt.Errorf("admin may not publish _TimeSync: ephemeral, node-local-publish-only (time-sync design §2.2)")
	}
	if class == uns.ClassCmd && e.ids.DrainingMount(p.Path) {
		// Same admission rule as IngestClient (move-drain design §3.2 item
		// 2): not even the admin token may address a draining mount with a
		// new command.
		e.metrics.RejectPublish(metrics.ReasonDraining)
		return Result{}, fmt.Errorf("admin: %s is draining — no new commands admitted (move-drain design §3.2)", p.Path)
	}
	if class == uns.ClassNone {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, fmt.Errorf("unknown contract %s", p.Contract)
	}
	if err := uns.Validate(p.Contract, payload); err != nil {
		e.metrics.RejectPublish(metrics.ReasonValidation)
		return Result{}, err
	}
	return e.persist(class, p, topic, payload)
}

// IngestRefresh is the retention pruner's §6.5 state-refresh entry (spec §6.5
// [delta]) — an admin-grade publish that applies ONLY IF the KV entry for the
// topic's (path, node) still sits at ifKVOffset, evaluated as a true CAS under
// the store mutex (AppendIfKVUnchanged). A tombstone (§7) or newer write
// landing between the pruner's KVScan snapshot and this call makes the
// re-statement stale: applying it would resurrect a retired path or clobber
// the newer value, so the WHOLE record is skipped — no stream append, no
// metrics, and crucially no bus delivery (applied=false, nil error).
//
// Records that DO apply keep the full IngestAdmin semantics: uns grammar,
// contract validation, atomic append with KV projection, bus mirror with the
// retained flag. Only KV-projecting classes are accepted (the guard is
// meaningless for anything else) and an empty payload is rejected — a refresh
// re-states current state and must never smuggle in a tombstone.
//
// Failures here are NOT counted against colca_rejected_publishes_total: that
// family describes rejected client/admin publishes, and a refresh is the
// pruner's own internal repair traffic, never a client's. The caller (the
// pruner's refreshEntities) is what turns a returned error into
// colca_retention_state_refresh_failures_total — this method just reports
// success/failure honestly.
func (e *Engine) IngestRefresh(topic string, payload []byte, ifKVOffset uint64) (Result, bool, error) {
	if !uns.IsUns(topic) {
		return Result{}, false, fmt.Errorf("refresh publish must be colca/#")
	}
	p, err := uns.Parse(topic)
	if err != nil {
		return Result{}, false, err
	}
	class := uns.ClassOf(p.Contract)
	if class != uns.ClassData && class != uns.ClassEntity {
		return Result{}, false, fmt.Errorf("refresh publish requires a KV-projecting contract, got %s", p.Contract)
	}
	if len(payload) == 0 {
		return Result{}, false, fmt.Errorf("refresh publish must not be empty (a refresh cannot tombstone)")
	}
	if err := uns.Validate(p.Contract, payload); err != nil {
		return Result{}, false, err
	}
	streamName := uns.StreamFor(class)
	rec := store.Record{Topic: topic, Payload: payload, TS: time.Now().UnixMilli(), KVPath: p.Path, KVNode: p.NodeID}
	off, applied, err := e.store.AppendIfKVUnchanged(streamName, rec, ifKVOffset)
	if err != nil {
		return Result{}, false, err
	}
	if !applied {
		return Result{}, false, nil // superseded snapshot: skipped, nothing delivered
	}
	e.metrics.IngestRecord(streamName)
	e.log.Debug("refresh ingest", "stream", streamName, "offset", off, "topic", topic)
	if e.deliver != nil {
		e.deliver(topic, payload, retainFor(class))
	}
	return Result{Persisted: true, Stream: streamName, Offset: off, Topic: topic}, true, nil
}

// IngestDownlink: a command received from the parent (already mount-stripped to
// local coords). Trusted (parent authenticated) and persisted to the own
// commands stream with the ORIGINAL parent timestamp — expiry must not be
// refreshed by a hop. Local MQTT delivery is not done here: persistTS mirrors
// every appended record onto the bus, so doing it again would publish twice.
func (e *Engine) IngestDownlink(topic string, payload []byte, ts int64) (Result, error) {
	p, err := uns.Parse(topic)
	if err != nil {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, err
	}
	class := uns.ClassOf(p.Contract)
	if class == uns.ClassCmd && e.ids.DrainingMount(p.Path) {
		// Move-drain design §3.2 item 2: the SAME admission rule as
		// IngestClient/IngestAdmin, extended to this relay door. Without this
		// check, a command authored ABOVE this node's own parent — where THIS
		// node's own draining child is invisible — is admitted upstream,
		// relays down through the downlink-poll loop, and lands straight in a
		// mount this node is actively draining: the "chasing a moving tail"
		// failure the admission gate exists to prevent, reachable in a
		// multi-hop tree even though the direct doors (client/admin) are
		// covered. The caller (repl.RunDownlink) already treats a per-record
		// ingest error as "log and drop, cursor still advances" — the same
		// handling every other IngestDownlink error gets today — so no retry
		// loop, no stuck cursor, no gap-jump side effect: this is a single
		// record rejected at persistence time, not a batch operation.
		e.metrics.RejectPublish(metrics.ReasonDraining)
		return Result{}, fmt.Errorf("downlink: %s is draining — no new commands admitted (move-drain design §3.2)", p.Path)
	}
	return e.persistTS(class, p, topic, payload, ts)
}

// IngestReplicated applies a batch pushed by a child: dedupe by high-water-mark,
// then mirror every NEWLY applied record onto the local MQTT bus. Replication is
// the fourth way a record enters a node's store and it must converge here like
// the other three — the replication server never talks to the store directly.
func (e *Engine) IngestReplicated(child, stream string, recs []store.ReplRecord) (applied int, hwm uint64, err error) {
	recs, droppedTimeSync := e.rejectTimeSync(child, recs)
	prev := e.store.HWMGet(child, stream)
	got, hwm, err := e.store.ApplyReplicated(child, stream, recs)
	if err != nil {
		return 0, hwm, err
	}
	e.logOffsetJumps(child, stream, prev, got, droppedTimeSync)
	// Replication is a fourth entry path into this node's store, so it counts
	// against colca_ingest_records_total exactly like the other three — the
	// family measures records entering the store, not records entering
	// through any one specific path.
	for range got {
		e.metrics.IngestRecord(stream)
	}
	if e.deliver == nil {
		return len(got), hwm, nil
	}
	for _, r := range got {
		p, perr := uns.Parse(r.Topic)
		if perr != nil {
			// Durable already — only the bus mirror is skipped, never the apply.
			e.log.Warn("replicated record not mirrored to the local bus: unparseable topic",
				"child", child, "stream", stream, "topic", r.Topic, "err", perr)
			continue
		}
		e.deliver(r.Topic, r.Payload, retainFor(uns.ClassOf(p.Contract)))
	}
	return len(got), hwm, nil
}

// rejectTimeSync drops any _TimeSync record from a replicated batch before it
// ever reaches the store (time-sync design §2.2/§4): _TimeSync is ephemeral
// and node-local-publish-only, so a well-behaved child's own store can never
// legitimately contain one (its own engine already rejects it at the client
// and admin doors, and the beacon loop never touches the store at all) — a
// record with this shape arriving over replication can only be a forged or
// buggy child offset. It is rejected exactly like a client/admin publish is,
// with the same reject reason, and the rest of the batch is still applied
// unchanged.
//
// It also returns the set of dropped ChildOffsets (nil if none were
// dropped), so logOffsetJumps can tell a gap this drop itself created apart
// from a genuine one: without that, a batch shaped
// [real @1, forged _TimeSync @2, real @3] applies {1,3}, and the resulting
// ChildOffset jump from 1 to 3 would otherwise be indistinguishable from a
// real child-side data loss — logged at Error and counted against
// colca_gap_applied, a metric whose whole point is "real, investigatable
// loss," for an event that lost nothing.
func (e *Engine) rejectTimeSync(child string, recs []store.ReplRecord) (filtered []store.ReplRecord, dropped map[uint64]bool) {
	filtered = recs[:0:0]
	for _, r := range recs {
		if p, err := uns.Parse(r.Topic); err == nil && uns.ClassOf(p.Contract) == uns.ClassTimeSync {
			e.metrics.RejectPublish(metrics.ReasonTimeSync)
			e.log.Warn("rejected _TimeSync record from replication: ephemeral, node-local-publish-only (time-sync design §2.2)",
				"child", child, "topic", r.Topic)
			if dropped == nil {
				dropped = make(map[uint64]bool)
			}
			dropped[r.ChildOffset] = true
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered, dropped
}

// logOffsetJumps is the second net of spec §6.4: a child's stream offsets are
// gapless and the uplink reads them contiguously, so an applied ChildOffset
// above HWM+1 means records the parent never received are gone at the child —
// a gap — and a child that failed to emit its _StreamGap marker would
// otherwise pass unnoticed. Detection only, from values IngestReplicated
// already has (prev HWM + the applied batch); the store is not involved.
//
// The commands stream is exempt: its uplink is filtered (only _Ack and
// _StreamGap travel up), so child-offset holes there are the filter working,
// not data loss — the premise "gapless offsets" does not hold on that wire.
//
// droppedTimeSync (rejectTimeSync's return) exempts a jump this SAME call's
// own _TimeSync filtering created: a jump is only
// logged/counted when at least one offset in the gap is NOT accounted for by
// a dropped _TimeSync record — a gap partially explained by a drop but also
// missing a genuinely unaccounted offset still logs, so this only removes
// the false positive, never masks a real one.
func (e *Engine) logOffsetJumps(child, stream string, prev uint64, applied []store.ReplRecord, droppedTimeSync map[uint64]bool) {
	if stream == "commands" {
		return
	}
	last := prev
	for _, r := range applied {
		if r.ChildOffset > last+1 && !jumpFullyExplainedByDroppedTimeSync(last, r.ChildOffset, droppedTimeSync) {
			e.log.Error("replication offset jump: this node never received the child offsets between have and got — likely pruned at the child before replication (spec §6.4 second net)",
				"child", child, "stream", stream, "have", last, "got", r.ChildOffset)
			e.metrics.GapApplied(child, stream)
		}
		last = r.ChildOffset
	}
}

// jumpFullyExplainedByDroppedTimeSync reports whether EVERY offset strictly
// between last and childOffset was dropped by this batch's own _TimeSync
// filtering — i.e. the jump has no unaccounted offset and is not a real gap.
func jumpFullyExplainedByDroppedTimeSync(last, childOffset uint64, dropped map[uint64]bool) bool {
	if len(dropped) == 0 {
		return false
	}
	for o := last + 1; o < childOffset; o++ {
		if !dropped[o] {
			return false
		}
	}
	return true
}

// retainFor decides how a record appears on the local MQTT bus. Data and
// entities are STATE: they are retained, which is exactly the set that also
// gets a KV projection. Commands and acks are EVENTS: retaining them would
// re-deliver stale commands to every new subscriber.
func retainFor(c uns.Class) bool { return c == uns.ClassData || c == uns.ClassEntity }

func (e *Engine) persist(class uns.Class, p uns.Parsed, topic string, payload []byte) (Result, error) {
	return e.persistTS(class, p, topic, payload, time.Now().UnixMilli())
}

// persistTS writes the record (plus, for data/entity, its KV projection) in one
// atomic batch and then mirrors it onto the local MQTT bus under the STORED
// topic. p must be the parse of topic, i.e. post-rewrite, so KVPath and KVNode
// carry the local coordinates and the originating node id.
//
// The order is load-bearing: the bus must never show something that is not
// durable, so delivery happens only after Append returned successfully. This is
// the single place that guarantees the rule "everything appended to a node's
// stream is also published on that node's bus" for client, admin and downlink
// ingest alike.
func (e *Engine) persistTS(class uns.Class, p uns.Parsed, topic string, payload []byte, ts int64) (Result, error) {
	streamName := uns.StreamFor(class)
	rec := store.Record{Topic: topic, Payload: payload, TS: ts}
	if class == uns.ClassData || class == uns.ClassEntity {
		rec.KVPath, rec.KVNode = p.Path, p.NodeID
		// Empty payload on a KV-projecting class is the tombstone (retention
		// design §7.1): the record is appended as history, the KV key is
		// DELETED in the same batch, and the delivery below — retain=true with
		// the empty payload — makes mochi clear the retained message per the
		// MQTT spec. Retained set ≡ KV stays one contract.
		rec.Delete = len(payload) == 0
	}
	first, _, err := e.store.Append(streamName, []store.Record{rec})
	if err != nil {
		return Result{}, err
	}
	e.metrics.IngestRecord(streamName)
	e.log.Debug("ingest", "stream", streamName, "offset", first, "topic", topic)
	if e.deliver != nil {
		e.deliver(topic, payload, retainFor(class))
	}
	return Result{Persisted: true, Stream: streamName, Offset: first, Topic: topic}, nil
}
