// Package engine is the single place every write converges: the MQTT hook, the
// HTTP publish endpoint, replication apply and downlink apply all go through
// one of the Ingest* methods. Grammar, identity rule, mount rewrite, payload
// validation and the atomic persist live here and nowhere else.
package engine

import (
	"fmt"
	"log/slog"
	"time"

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

type Engine struct {
	store   *store.Store
	cfg     *config.Config
	deliver LocalDeliver
	mounts  map[string]string // identity ulid → mount (clients + children)
	log     *slog.Logger
	metrics *metrics.Metrics // nil-safe: every method on a nil receiver is a no-op
}

// New builds an engine. The mount map covers both children and clients: they
// share one mount namespace (config.Validate guarantees no collisions).
//
// A client without a mount is a read-only observer and is deliberately left OUT
// of the map, so IngestClient rejects its publishes with "no mount registered"
// instead of rewriting them into a topic with an empty path segment.
//
// m may be nil (unit tests and any caller that does not care about metrics) —
// every Metrics method is nil-safe.
func New(s *store.Store, cfg *config.Config, deliver LocalDeliver, m *metrics.Metrics) *Engine {
	mounts := map[string]string{}
	for _, c := range cfg.Clients {
		if c.Mount != "" {
			mounts[c.ULID] = c.Mount
		}
	}
	for _, c := range cfg.Children {
		mounts[c.ULID] = c.Mount
	}
	return &Engine{store: s, cfg: cfg, deliver: deliver, mounts: mounts, log: slog.Default().With("node", cfg.ULID), metrics: m}
}

func (e *Engine) Store() *store.Store { return e.store }

// MountOf resolves the mount a child or client is attached under.
func (e *Engine) MountOf(ulid string) (string, bool) {
	m, ok := e.mounts[ulid]
	return m, ok
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
	class := uns.ClassOf(p.Contract)
	if class == uns.ClassCmd {
		e.metrics.RejectPublish(metrics.ReasonNotCommand)
		return Result{}, fmt.Errorf("client %s may not publish %s", identity, p.Contract)
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
	mount, ok := e.mounts[identity]
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
	class := uns.ClassOf(p.Contract)
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
	return e.persistTS(uns.ClassOf(p.Contract), p, topic, payload, ts)
}

// IngestReplicated applies a batch pushed by a child: dedupe by high-water-mark,
// then mirror every NEWLY applied record onto the local MQTT bus. Replication is
// the fourth way a record enters a node's store and it must converge here like
// the other three — the replication server never talks to the store directly.
func (e *Engine) IngestReplicated(child, stream string, recs []store.ReplRecord) (applied int, hwm uint64, err error) {
	prev := e.store.HWMGet(child, stream)
	got, hwm, err := e.store.ApplyReplicated(child, stream, recs)
	if err != nil {
		return 0, hwm, err
	}
	e.logOffsetJumps(child, stream, prev, got)
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
func (e *Engine) logOffsetJumps(child, stream string, prev uint64, applied []store.ReplRecord) {
	if stream == "commands" {
		return
	}
	last := prev
	for _, r := range applied {
		if r.ChildOffset > last+1 {
			e.log.Error("replication offset jump: this node never received the child offsets between have and got — likely pruned at the child before replication (spec §6.4 second net)",
				"child", child, "stream", stream, "have", last, "got", r.ChildOffset)
			e.metrics.GapApplied(child, stream)
		}
		last = r.ChildOffset
	}
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
