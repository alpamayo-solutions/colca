// Package engine is the single place every write converges: the MQTT hook, the
// HTTP publish endpoint, replication apply and downlink apply all go through
// one of the Ingest* methods. Grammar, the level-4-is-this-node rule, the
// write-scope authorization check, payload validation and the atomic persist
// live here and nowhere else.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// LocalDeliver hands a record to the local MQTT broker (nil when the node has
// no broker, and in unit tests). retain is true for the state contracts, so the
// broker's retained set and the store's KV projection are the same thing seen
// from two sides.
type LocalDeliver func(topic string, payload []byte, retain bool)

// HasSubscriberFor reports whether the identity `ulid` currently has a live
// subscription matching topic on this node's local MQTT bus (nil when there is
// no broker, and in unit tests that do not wire one; callers treat "unknown"
// as "make no claim" — see deliverCommand).
//
// The identity argument is not a refinement, it is the whole question. This
// answer is the command delivery floor: a true answer advances a machine's
// cursor and forfeits the replay. An identity-blind "does ANYONE subscribe to
// this topic" would let a passive reader — an observer holding read:# and
// subscribed to colca/v1/_CmdParam/# for diagnostics — mark another machine's
// commands delivered while that machine is offline, silently reopening the
// exact gap redelivery exists to close.
//
// Read the rest literally, not as "the bytes arrived": it answers subscription
// existence, not byte-level delivery confirmation. A per-client write mochi
// attempts and fails is invisible here (swallowed inside mochi at Debug), so
// the undelivered counter can undercount real failures and cannot overcount
// them.
type HasSubscriberFor func(topic, ulid string) bool

// Result describes what an ingest did. Topic is exactly what was persisted —
// a client's own publish stores its topic unchanged (local-service-trust
// design §2/§5: no client-path rewrite any more), but a replicated record
// still carries the child's mount inserted by IngestReplicated's parent-side
// hop, so Topic there is post-insertion.
type Result struct {
	Persisted bool
	Stream    string
	Offset    uint64
	Topic     string // as persisted — post mount-insertion for replicated records
	Command   *CommandOutcome
}

// Attribution is the immutable authorship envelope stored with a record.
// WrittenBy is the authenticated publishing identity. ActorID is the stable
// subject it acted as; ActorLabel is only a display snapshot.
type Attribution struct {
	WrittenBy  string
	ActorID    string
	ActorLabel string
	ActorKind  string
	// ActorGroups are the group ids a human actor's grants were resolved
	// from at the door that verified them (uns.Entry.Groups). Persisted with
	// the record and replicated with it, so the node that finally executes a
	// downlinked command can reconstitute the same person against its own
	// _Group definitions (node-side command authorization design §3B).
	ActorGroups []string
}

func attributionForEntry(entry *uns.Entry) Attribution {
	if entry == nil {
		return Attribution{}
	}
	label := entry.Name
	if label == "" {
		label = entry.ULID
	}
	return Attribution{
		WrittenBy: entry.ULID, ActorID: entry.ULID,
		ActorLabel: label, ActorKind: entry.ActorKind(),
		ActorGroups: append([]string(nil), entry.Groups...),
	}
}

// actorForAttested reconstitutes a human whose token this node never saw:
// the record's attribution carries the group ids a door verified — an
// ancestor's, for a command replicated down; a local service's stated
// fallback, for a job acting after the request ended — and this node
// resolves them against the _Group definitions it holds, the same
// resolution a token gets at its own human doors. A group this node does not hold contributes nothing (logged),
// so the person may arrive holding no grant at all — and is then refused by
// the plan authorization, never widened. A record with no human attribution
// or no attested groups yields no actor: the executor then refuses a
// _CmdEdit rather than guessing.
func (e *Engine) actorForAttested(attribution Attribution) *uns.Entry {
	if attribution.ActorKind != "human" || attribution.ActorID == "" || len(attribution.ActorGroups) == 0 {
		return nil
	}
	entry, problems, err := uns.TokenEntryWithGroups(attribution.ActorID, nil, attribution.ActorGroups, e.Groups())
	if err != nil {
		e.log.Warn("attested actor: cannot reconstitute the acting human", "actor", attribution.ActorID, "err", err)
		return nil
	}
	for _, problem := range problems {
		e.log.Warn("attested actor: group unresolved for the acting human", "actor", attribution.ActorID, "err", problem)
	}
	if attribution.ActorLabel != "" && attribution.ActorLabel != attribution.ActorID {
		entry.Username = attribution.ActorLabel // the verifying door's preferred_username, kept as the label
	}
	return entry
}

// Mounts resolves identities to their registry entries — implemented by
// *registry.Manager. The engine consults it for every identity question and
// holds no identity state of its own (auth design §8).
type Mounts interface {
	// Get resolves a ULID to its entry. An implementation must answer
	// (nil, false) for an unknown identity and never (nil, true): every
	// caller here reads as `entry, ok := ids.Get(id); if !ok || !entry.X()`,
	// so a nil paired with true would be dereferenced. *registry.Manager
	// cannot produce that pair; the requirement is stated because the
	// interface would otherwise permit a fake that does, and because it is
	// the contract new implementations are held to. uns.Entry's predicates
	// are nil-safe as the belt behind it — a nil identity holds no door and
	// publishes no audit — so a fake that breaks the rule gets a refusal
	// rather than a panic.
	Get(ulid string) (*uns.Entry, bool)
	// DrainingMount reports whether path falls under a currently draining
	// kind=node child's mount (move-drain design §3.2 item 2) — consulted
	// for every ClassCmd admission so a draining mount stops accepting new
	// commands instead of chasing a moving tail.
	DrainingMount(path string) bool
}

// routableMounts is the OPTIONAL half of Mounts: "could a command at this path
// reach any child at all?" (registry.Manager.RoutesUnder).
//
// Optional rather than part of Mounts on purpose. It feeds a counter and
// nothing else — no admission decision, no delivery decision — so an
// implementation that cannot answer it should lose the observability, not fail
// to build. Every unit fake in this package therefore stays valid unchanged,
// and the real registry supplies it.
type routableMounts interface {
	RoutesUnder(path string) bool
}

type Engine struct {
	store   *store.Store
	cfg     *config.Config
	deliver LocalDeliver
	ids     Mounts
	log     *slog.Logger
	metrics *metrics.Metrics // nil-safe: every method on a nil receiver is a no-op
	clk     *clock.Clock

	// hasSubscriber answers deliverCommand's and ReplayOwedCommands'
	// one question (nil until SetSubscriberCheck — a node with no broker, or
	// a unit test, never counts a false undelivered command, and never
	// replays, for want of an answer it cannot give). Wired late, like exec
	// and observer below, for the same reason: node assembly needs the broker
	// built before it can offer this.
	hasSubscriber HasSubscriberFor

	// replayLocks serializes ReplayOwedCommands per identity (redelivery.go
	// explains which overlaps are real). replayMu guards the map itself, not
	// the replays — a replay holds only its own identity's mutex, so two
	// machines reconnecting together never wait on each other.
	replayMu    sync.Mutex
	replayLocks map[string]*sync.Mutex

	// The node's position in the tree (id-grants design §4): the chain of
	// elements from the root down to the one this node binds to, taught by the
	// parent on the downlink, persisted, unknown until first taught. The root
	// sets the empty chain at startup (known-empty by construction — it has no
	// parent). The root-frame prefix is rendered from it, never stored.
	posMu         sync.RWMutex
	ancestry      uns.Ancestry
	ancestryKnown bool

	// exec executes commands addressed to this node (cmdadmin design §5). The
	// engine owns the mechanism; the executor owns what a verb means, which is
	// how domain knowledge stays out of the core. Nil until SetExecutor — the
	// node then executes nothing.
	exec CommandExecutor

	// observer is told about records persisted through this node's own doors,
	// so the domain plugin can react to state the core does not interpret.
	observer RecordObserver

	// onPosition is told when this node learns or changes where it sits. The
	// node describes itself from that moment (its `_Node` record), and
	// its position is the one fact about itself it cannot read from config.
	onPosition func(uns.Ancestry)

	// contracts is the loaded schema-bundle table (nil = builtin floor).
	// Static per process: set once at startup, before any door serves.
	contracts *contracts.Table

	// elements is this node's id → local path projection of the
	// `_SystemElement` records it holds (id-grants design §4). The engine owns
	// it because every door already reaches the engine, and because observe()
	// is where records land — so the index is current without anyone
	// remembering to refresh it.
	elements *uns.ElementIndex

	// auditID is injectable for deterministic event tests. Audit writes bypass
	// the public ingest doors; see audit.go.
	auditID func(time.Time) string

	// unboundLog rate-limits the "_Metric with no _Signal" log line per path
	// (SDK design §7 gap 6, unbound.go).
	unboundLog *unboundMetricLog
}

// New builds an engine. ids is the identity registry: IngestClient admits a
// publish iff the identity's write scope (auth §5, writeZones) covers the
// topic's path — a local service unplaced or otherwise, a machine with no
// covering write: grant.
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
	e := &Engine{store: s, cfg: cfg, deliver: deliver, ids: ids, log: slog.Default().With("node", cfg.ULID), metrics: m, clk: clk, auditID: newAuditID, unboundLog: newUnboundMetricLog()}
	e.elements = uns.NewElementIndex(e.EntityStore())
	if raw, ok := s.AncestryGet(); ok {
		var a uns.Ancestry
		if err := json.Unmarshal(raw, &a); err != nil {
			// Fail loud, not closed-and-quiet: a node that silently forgot where
			// it sits answers every scoped grant with "no", which looks like a
			// permissions problem and is really a corrupt store.
			e.log.Error("persisted ancestry is unreadable — this node does not know its position "+
				"until its parent teaches it again", "err", err)
		} else {
			e.ancestry, e.ancestryKnown = a, true
			m.SetNodePrefix(a.Prefix())
		}
	}
	return e
}

// Ancestry returns the node's position in the tree; ok=false until first
// taught.
func (e *Engine) Ancestry() (uns.Ancestry, bool) {
	e.posMu.RLock()
	defer e.posMu.RUnlock()
	return e.ancestry, e.ancestryKnown
}

// Prefix renders the node's root-frame path from its ancestry; ok=false until
// first taught. Derived on every call — the path is never the stored truth, so
// there is no second copy of it to go stale.
func (e *Engine) Prefix() (string, bool) {
	a, ok := e.Ancestry()
	if !ok {
		return "", false
	}
	return a.Prefix(), true
}

// SetAncestry stores a (re-)taught position. Idempotent: an unchanged chain
// writes nothing. A change is persisted synchronously, logged, and reflected in
// colca_node_prefix_info — new token verifications use it immediately; live
// human sessions keep their at-connect translation.
func (e *Engine) SetAncestry(a uns.Ancestry) {
	raw, err := json.Marshal(a)
	if err != nil {
		e.log.Error("ancestry encode failed — position not updated", "err", err)
		return
	}
	e.posMu.Lock()
	if e.ancestryKnown && slices.Equal(e.ancestry, a) {
		e.posMu.Unlock()
		return
	}
	old, hadOld := e.ancestry, e.ancestryKnown
	e.ancestry, e.ancestryKnown = a, true
	e.posMu.Unlock()
	if err := e.store.AncestryPut(raw); err != nil {
		e.log.Error("ancestry persistence failed — active in-memory only", "ancestry", a, "err", err)
	}
	e.metrics.SetNodePrefix(a.Prefix())
	if hadOld {
		e.log.Info("node position changed", "old", old.Prefix(), "new", a.Prefix())
	} else {
		e.log.Info("node position learned", "prefix", a.Prefix())
	}
	if fn := e.onPosition; fn != nil {
		fn(a)
	}
}

// SetOnPosition registers what to do when this node learns or changes its
// position. Called once at assembly, before the root's known-empty ancestry
// is set, so the very first position is reported too.
func (e *Engine) SetOnPosition(fn func(uns.Ancestry)) { e.onPosition = fn }

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

// Elements is this node's namespace: the element index every placement
// question resolves through.
func (e *Engine) Elements() *uns.ElementIndex { return e.elements }

// scope couples the two halves of "where is that element": the index answers
// for everything at or below this node, the ancestry for everything above it.
// A grant may name either, so the decision function gets both through here.
type scope struct{ e *Engine }

func (s scope) PathOf(elementID string) (string, bool) { return s.e.elements.PathOf(elementID) }

func (s scope) Reaches(elementID string) bool {
	a, ok := s.e.Ancestry()
	return ok && a.Covers(elementID)
}

// Scope is what Authorize resolves grants through at this node.
func (e *Engine) Scope() uns.Scope { return scope{e} }

// Groups resolves a token's group ids against the `_Group` definitions this
// node holds (definition-stream design §8).
func (e *Engine) Groups() *uns.GroupIndex { return uns.NewGroupIndex(e.EntityStore()) }

// NodeID is the identity this node publishes under.
func (e *Engine) NodeID() string { return e.cfg.ULID }

func stateIdentity(payload []byte) (id, colcaNodeID string, err error) {
	if len(payload) == 0 { // a tombstone carries its identity in the topic
		return "", "", nil
	}
	var value struct {
		ID           string `json:"id"`
		ColcaNodeID string `json:"colca_node_id"`
	}
	if err := json.Unmarshal(payload, &value); err != nil {
		return "", "", err
	}
	return value.ID, value.ColcaNodeID, nil
}

// validateClientStateAuthor adds payload-level authorship checks after the
// local-trust door has pinned level 4 to this node. The authenticated service
// identity remains evidence and authority; it never replaces the node in the
// topic.
func (e *Engine) validateClientStateAuthor(identity string, p uns.Parsed, payload []byte) error {
	if uns.IsDefinition(e.ClassOf(p.Contract)) {
		return fmt.Errorf("%s is node-authored definition state", p.Contract)
	}
	if p.Contract == "_Node" || p.Contract == "_ExternalReference" {
		return fmt.Errorf("%s is node-authored entity state", p.Contract)
	}
	if p.Contract != "_ServiceDetails" {
		return nil
	}
	id, colcaNodeID, err := stateIdentity(payload)
	if err != nil {
		return err
	}
	if id != "" && id != identity {
		return fmt.Errorf("_ServiceDetails id %q must equal authenticated identity %q", id, identity)
	}
	if colcaNodeID != "" && colcaNodeID != e.cfg.ULID {
		return fmt.Errorf("_ServiceDetails colca_node_id %q must equal local node %q", colcaNodeID, e.cfg.ULID)
	}
	if p.Path != "_service" && !strings.HasSuffix(p.Path, "/_service") {
		return fmt.Errorf("_ServiceDetails path %q must end in reserved _service leaf", p.Path)
	}
	return nil
}

// validateAdminStateAuthor pins node-authored records to this node. Replicated
// records do not pass through this door and keep their original node author.
func (e *Engine) validateAdminStateAuthor(p uns.Parsed, payload []byte) error {
	if p.Contract == "_ServiceDetails" {
		return fmt.Errorf("_ServiceDetails is observed state authored by the service identity")
	}
	class := e.ClassOf(p.Contract)
	if p.Contract != "_Node" && p.Contract != "_ExternalReference" && !uns.IsDefinition(class) {
		return nil
	}
	if p.NodeID != e.cfg.ULID {
		return fmt.Errorf("%s author %q must equal local node %q", p.Contract, p.NodeID, e.cfg.ULID)
	}
	id, _, err := stateIdentity(payload)
	if err != nil {
		return err
	}
	if p.Contract == "_Node" {
		wantPath := "_colca/nodes/" + e.cfg.ULID
		if p.Path != wantPath || (id != "" && id != e.cfg.ULID) {
			return fmt.Errorf("_Node must describe local node %q at %q", e.cfg.ULID, wantPath)
		}
		return nil
	}
	if p.Contract == "_ExternalReference" {
		if id != "" && p.Path != "_colca/external-references/"+id {
			return fmt.Errorf("_ExternalReference path %q does not name payload id %q", p.Path, id)
		}
		return nil
	}
	if id != "" && p.Path != id {
		return fmt.Errorf("%s path %q does not name definition id %q", p.Contract, p.Path, id)
	}
	return nil
}

// IngestClient: a directly attached MQTT client (machine/service) publishes.
// Rules: uns grammar, class must be data/entity/ack, level-4 == this node,
// write-scope authorization, validate, persist. A non-UNS topic is not an
// error — it is normal broker traffic that simply is not persisted.
func (e *Engine) IngestClient(identity, topic string, payload []byte) (Result, error) {
	return e.ingestClientAttributed(identity, topic, payload, nil)
}

// IngestLocalAttributed accepts a stable actor envelope only from a registered
// local service. The local HTTP door is the trust boundary; external machine
// and human doors derive actors from their verified credentials instead.
func (e *Engine) IngestLocalAttributed(identity, topic string, payload []byte, actor Attribution) (Result, error) {
	entry, ok := e.ids.Get(identity)
	if !ok || !entry.MayUseDoor(uns.DoorLocal) {
		return e.rejectDenied(metrics.ReasonWriteDenied, attributionForEntry(entry), "publish", nil,
			"identity %s may not supply local actor attribution", identity)
	}
	if actor.ActorID == "" || !uns.ValidActorKind(actor.ActorKind) {
		return e.reject(metrics.ReasonIdentity, "local actor attribution requires actor_id and a valid actor_kind")
	}
	actor.WrittenBy = entry.ULID
	if actor.ActorLabel == "" {
		actor.ActorLabel = actor.ActorID
	}
	return e.ingestClientAttributed(identity, topic, payload, &actor)
}

func (e *Engine) ingestClientAttributed(identity, topic string, payload []byte, supplied *Attribution) (Result, error) {
	actorFor := func(entry *uns.Entry) Attribution {
		if supplied != nil {
			return *supplied
		}
		return attributionForEntry(entry)
	}
	if !uns.IsUns(topic) {
		return Result{Persisted: false}, nil // normal broker behavior outside colca/#
	}
	p, err := uns.Parse(topic)
	if err != nil {
		return e.reject(metrics.ReasonGrammar, "%w", err)
	}
	// Registry entries enter through the enrollment door ONLY (auth §3): no
	// client may author an _EnrolledIdentity, not even its own.
	if p.Contract == "_EnrolledIdentity" {
		return e.reject(metrics.ReasonRegistryContract, "client %s may not publish _EnrolledIdentity — registry entries are enrollment-door only", identity)
	}
	class := e.ClassOf(p.Contract)
	if uns.IsNodeLocal(class) {
		// Time-sync design §2.2/§4: _TimeSync is node-local-publish-only —
		// only this node's own beacon loop may ever produce it, straight to
		// the local bus. A client attempting it (even the exact canonical
		// shape) is rejected here with its own reason, never with the
		// generic "grammar" reason Parse's 4-segment relaxation would
		// otherwise fall through to.
		return e.reject(metrics.ReasonTimeSync, "client %s may not publish _TimeSync: ephemeral, node-local-publish-only (time-sync design §2.2)", identity)
	}
	if uns.IsCommand(class) {
		// Move-drain design §3.2 item 2: a mount under an active drain stops
		// accepting new commands at admission, independent of the caller's
		// own grants — checked first so a draining destination is rejected
		// for the reason that actually explains it, not folded into
		// cmd_denied.
		if e.ids.DrainingMount(p.Path) {
			return e.reject(metrics.ReasonDraining, "client %s: %s is draining — no new commands admitted (move-drain design §3.2)", identity, p.Path)
		}
		// A command needs a covering cmd grant (auth §5.3 ActCmd). Commands
		// target ABSOLUTE node-local paths: no mount rewrite, no level-4
		// identity requirement — the author is not the target's owner.
		entry, ok := e.ids.Get(identity)
		if !ok {
			return e.rejectDenied(metrics.ReasonCmdDenied,
				Attribution{ActorID: identity, ActorLabel: identity, ActorKind: "service"},
				"execute", &p, "client %s: no cmd grant covers %s", identity, topic)
		}
		attribution := actorFor(entry)
		// The command is judged AS the person a local service attested for
		// (node-side command authorization design §3B fallback): the person
		// reconstituted from their group ids against this node's own _Group
		// definitions, holding exactly their grants — never the service's
		// implicit configure, never wider than the person. A service
		// publishing as itself is judged as itself.
		actor := entry
		if attested := e.actorForAttested(attribution); attested != nil {
			actor = attested
		}
		implicitLocalConfigure := actor == entry && p.NodeID == e.cfg.ULID && entry.MayImplicitlyConfigure(p.Contract)
		if !implicitLocalConfigure && !uns.Authorize(e.Scope(), actor, uns.ActCmd, topic) {
			return e.rejectDenied(metrics.ReasonCmdDenied, attribution, "execute", &p, "client %s: no cmd grant covers %s", identity, topic)
		}
		if err := e.validateContract(p.Contract, payload); err != nil {
			return e.reject(metrics.ReasonValidation, "%w", err)
		}
		res, err := e.persistAttributed(class, p, topic, payload, attribution)
		if err == nil {
			res.Command = e.maybeExec(p, payload, attribution, actor) // return the synchronous outcome to local API callers
		}
		return res, err
	}
	if uns.IsAudit(class) {
		entry, ok := e.ids.Get(identity)
		if !ok || !entry.MayPublishAudit() {
			actor := Attribution{ActorID: identity, ActorLabel: identity, ActorKind: "service"}
			if ok {
				actor = actorFor(entry)
			}
			return e.rejectDenied(metrics.ReasonWriteDenied, actor, "publish", &p, "client %s may not publish _AuditEvent — the audit door is local-only", identity)
		}
		if p.NodeID != e.cfg.ULID {
			return e.reject(metrics.ReasonNodeID, "level-4 %q is not this node (%q)", p.NodeID, e.cfg.ULID)
		}
		if err := e.validateContract(p.Contract, payload); err != nil {
			return e.reject(metrics.ReasonValidation, "%w", err)
		}
		if err := uns.ValidateAuditTopic(p, payload); err != nil {
			return e.reject(metrics.ReasonIdentity, "%w", err)
		}
		return e.persistAttributed(class, p, topic, payload, actorFor(entry))
	}
	if !uns.IsKnown(class) {
		return e.reject(metrics.ReasonGrammar, "client %s may not publish %s", identity, p.Contract)
	}
	// Ownership resolves to NODES, never to services: level 4 is this node's
	// ULID for every publisher here, so a service's identity decides whether a
	// write is allowed and never appears in the topic (local-service-trust
	// design §2, §5).
	if p.NodeID != e.cfg.ULID {
		return e.reject(metrics.ReasonNodeID, "level-4 %q is not this node (%q)", p.NodeID, e.cfg.ULID)
	}
	if err := e.validateContract(p.Contract, payload); err != nil {
		return e.reject(metrics.ReasonValidation, "%w", err)
	}
	if err := e.validateClientStateAuthor(identity, p, payload); err != nil {
		return e.reject(metrics.ReasonIdentity, "%w", err)
	}
	entry, ok := e.ids.Get(identity)
	if !ok || !uns.Authorize(e.Scope(), entry, uns.ActPub, topic) {
		actor := Attribution{ActorID: identity, ActorLabel: identity, ActorKind: "service"}
		if ok {
			actor = actorFor(entry)
		}
		return e.rejectDenied(metrics.ReasonWriteDenied, actor, "publish", &p, "client %s: no write scope covers %s", identity, topic)
	}
	// The client already publishes the canonical absolute node-local topic.
	// Preserve the newer immutable author attribution without reviving the
	// removed client-path mount rewrite.
	res, err := e.persistAttributed(class, p, topic, payload, actorFor(entry))
	if err == nil {
		// State a machine published here, offered to the domain plugin — the
		// core does not interpret it (data-model binding design §7).
		e.observe(p, topic, payload)
		// SDK design §7 gap 6: a _Metric with no _Signal is a silent,
		// invisible write otherwise — count and (rate-limited) log it.
		e.checkMetricBinding(p)
	}
	return res, err
}

// IngestHuman: a verified human (KindHuman entry from a token) publishes.
// Humans command and NOTHING else (World-2 rule, human-authz design §5.2):
// the only accepted class is ClassCmd, gated by the entry's cmd grants —
// data, entity and ack contracts are rejected with ReasonHumanWrite and no
// grant can override that. Commands target ABSOLUTE node-local paths, no
// rewrite, no level-4 identity rule — exactly like admin-issued commands.
func (e *Engine) IngestHuman(entry *uns.Entry, topic string, payload []byte) (Result, error) {
	return e.IngestHumanAttributed(entry, entry.ULID, topic, payload)
}

// IngestHumanAttributed is IngestHuman with the verified display label retained in
// the record envelope. Authorization still uses entry; the label is evidence,
// never an authority input.
func (e *Engine) IngestHumanAttributed(entry *uns.Entry, actorLabel, topic string, payload []byte) (Result, error) {
	if !uns.IsUns(topic) {
		return e.reject(metrics.ReasonGrammar, "human publish must be colca/#")
	}
	p, err := uns.Parse(topic)
	if err != nil {
		return e.reject(metrics.ReasonGrammar, "%w", err)
	}
	if p.Contract == "_EnrolledIdentity" {
		return e.reject(metrics.ReasonRegistryContract, "_EnrolledIdentity is enrollment-door only — use POST /enroll")
	}
	class := e.ClassOf(p.Contract)
	if !uns.IsKnown(class) {
		return e.reject(metrics.ReasonGrammar, "unknown contract %s", p.Contract)
	}
	if uns.IsNodeLocal(class) {
		// Same rule as IngestClient/IngestAdmin (time-sync design §2.2/§4):
		// _TimeSync is node-local-publish-only. Checked before the World-2
		// rule so the rejection carries the reason that actually explains it
		// (time_sync), not the generic human_write.
		e.metrics.RejectPublish(metrics.ReasonTimeSync)
		return Result{}, fmt.Errorf("human %s may not publish _TimeSync: ephemeral, node-local-publish-only (time-sync design §2.2)", entry.ULID)
	}
	if uns.IsAudit(class) {
		return e.rejectDenied(metrics.ReasonWriteDenied, attributionForEntry(entry), "publish", &p,
			"human %s may not publish _AuditEvent — the audit producer is local-only", entry.ULID)
	}
	if !uns.IsCommand(class) {
		return e.rejectDenied(metrics.ReasonHumanWrite, attributionForEntry(entry), "publish", &p,
			"human %s may not publish %s — humans command, machines write state", entry.ULID, p.Contract)
	}
	if !entry.MayPublishContract(p.Contract) {
		return e.rejectDenied(metrics.ReasonHumanWrite, attributionForEntry(entry), "publish", &p,
			"human %s may not publish %s — a person configures through _CmdEdit, where each write is authorized as them (node-side command authorization design §3F)", entry.ULID, p.Contract)
	}
	if e.ids.DrainingMount(p.Path) {
		// Move-drain design §3.2 item 2 — "at every door": a human's cmd
		// grant does not exempt them from the draining admission gate.
		// Checked first (like IngestClient) so a draining destination is
		// rejected for the reason that explains it, not folded into
		// cmd_denied.
		return e.reject(metrics.ReasonDraining, "human %s: %s is draining — no new commands admitted (move-drain design §3.2)", entry.ULID, p.Path)
	}
	if !uns.Authorize(e.Scope(), entry, uns.ActCmd, topic) {
		return e.rejectDenied(metrics.ReasonCmdDenied, attributionForEntry(entry), "execute", &p,
			"human %s: no cmd grant covers %s", entry.ULID, topic)
	}
	if err := e.validateContract(p.Contract, payload); err != nil {
		return e.reject(metrics.ReasonValidation, "%w", err)
	}
	attribution := Attribution{
		WrittenBy: entry.ULID, ActorID: entry.ULID,
		ActorLabel: actorLabel, ActorKind: "human",
		ActorGroups: append([]string(nil), entry.Groups...),
	}
	res, err := e.persistAttributed(class, p, topic, payload, attribution)
	if err == nil {
		res.Command = e.maybeExec(p, payload, attribution, entry) // commands addressed to this node execute here (cmdadmin design §5)
	}
	return res, err
}

// IngestAdmin: local HTTP API with admin token — publishes in node-local
// coordinates, no rewrite, commands allowed, still validated.
func (e *Engine) IngestAdmin(topic string, payload []byte) (Result, error) {
	return e.IngestAdminAttributed(topic, payload, Attribution{
		WrittenBy: "admin", ActorID: "admin", ActorLabel: "admin", ActorKind: "system",
	})
}

// IngestAdminAttributed is the attributed admin-service door. The static token
// is the authority; the envelope is evidence supplied by that trusted caller.
func (e *Engine) IngestAdminAttributed(topic string, payload []byte, attribution Attribution) (Result, error) {
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
	if p.Contract == "_EnrolledIdentity" {
		e.metrics.RejectPublish(metrics.ReasonRegistryContract)
		return Result{}, fmt.Errorf("_EnrolledIdentity is enrollment-door only — use POST /enroll")
	}
	class := e.ClassOf(p.Contract)
	if uns.IsNodeLocal(class) {
		// Same rule as IngestClient: _TimeSync is node-local-publish-only,
		// not even the admin token may author it through /publish.
		e.metrics.RejectPublish(metrics.ReasonTimeSync)
		return Result{}, fmt.Errorf("admin may not publish _TimeSync: ephemeral, node-local-publish-only (time-sync design §2.2)")
	}
	if uns.IsAudit(class) {
		return e.rejectDenied(metrics.ReasonWriteDenied, attribution, "publish", &p,
			"admin may not publish _AuditEvent — use the local audit producer")
	}
	if uns.IsCommand(class) && e.ids.DrainingMount(p.Path) {
		// Same admission rule as IngestClient (move-drain design §3.2 item
		// 2): not even the admin token may address a draining mount with a
		// new command.
		e.metrics.RejectPublish(metrics.ReasonDraining)
		return Result{}, fmt.Errorf("admin: %s is draining — no new commands admitted (move-drain design §3.2)", p.Path)
	}
	if !uns.IsKnown(class) {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, fmt.Errorf("unknown contract %s", p.Contract)
	}
	if err := e.validateContract(p.Contract, payload); err != nil {
		e.metrics.RejectPublish(metrics.ReasonValidation)
		return Result{}, err
	}
	if err := e.validateAdminStateAuthor(p, payload); err != nil {
		e.metrics.RejectPublish(metrics.ReasonIdentity)
		return Result{}, err
	}
	res, err := e.persistAttributed(class, p, topic, payload, attribution)
	if err == nil {
		res.Command = e.maybeExec(p, payload, attribution, nil) // the admin door presents a token, not an identity
	}
	return res, err
}

// ingestAdminStateBatch commits the complete state result of one domain
// command. Validation is intentionally front-loaded: every topic, schema,
// author and tombstone rule is checked before Store.Append sees a record, then
// one synced Pebble batch appends the stream history and updates every KV
// projection. A late invalid record therefore cannot leave an early record
// applied.
//
// One batch is one stream. Append writes a single stream's history and its
// offset meta key in that Pebble batch, so records for two streams could only
// be committed as two batches — which is exactly the half-applied outcome this
// path exists to prevent. No command mixes them (a verb edits the entity graph
// or files definitions, never both), so the rule costs nothing and the refusal
// below is what keeps it true.
func (e *Engine) ingestAdminStateBatch(records []uns.StateRecord, attribution Attribution) ([]Result, error) {
	if len(records) == 0 {
		// A command that decided on nothing — every path already present, every
		// entry already bound — is a successful no-op, not a failed write.
		return []Result{}, nil
	}

	type preparedRecord struct {
		parsed uns.Parsed
		class  uns.Class
		record store.Record
	}
	prepared := make([]preparedRecord, 0, len(records))
	ts := time.Now().UnixMilli()
	stream := ""
	for i, input := range records {
		if !uns.IsUns(input.Topic) {
			e.metrics.RejectPublish(metrics.ReasonGrammar)
			return nil, fmt.Errorf("admin state batch record %d must be colca/#", i)
		}
		parsed, err := uns.Parse(input.Topic)
		if err != nil {
			e.metrics.RejectPublish(metrics.ReasonGrammar)
			return nil, fmt.Errorf("admin state batch record %d: %w", i, err)
		}
		if parsed.Contract == "_EnrolledIdentity" {
			e.metrics.RejectPublish(metrics.ReasonRegistryContract)
			return nil, fmt.Errorf("admin state batch record %d (%s): _EnrolledIdentity is enrollment-door only — use POST /enroll", i, input.Topic)
		}
		class := e.ClassOf(parsed.Contract)
		if uns.IsNodeLocal(class) {
			e.metrics.RejectPublish(metrics.ReasonTimeSync)
			return nil, fmt.Errorf("admin state batch record %d may not publish _TimeSync: ephemeral, node-local-publish-only", i)
		}
		if !uns.IsKnown(class) {
			e.metrics.RejectPublish(metrics.ReasonGrammar)
			return nil, fmt.Errorf("admin state batch record %d: unknown contract %s", i, parsed.Contract)
		}
		if !uns.IsCommandAuthoredState(class) {
			e.metrics.RejectPublish(metrics.ReasonValidation)
			return nil, fmt.Errorf("admin state batch record %d (%s): %s is not state a command may author",
				i, input.Topic, parsed.Contract)
		}
		if recordStream := uns.StreamFor(class); stream == "" {
			stream = recordStream
		} else if recordStream != stream {
			e.metrics.RejectPublish(metrics.ReasonValidation)
			return nil, fmt.Errorf("admin state batch record %d (%s) belongs to stream %q, not %q — "+
				"one command's records commit as one batch on one stream", i, input.Topic, recordStream, stream)
		}
		if parsed.NodeID != e.cfg.ULID {
			e.metrics.RejectPublish(metrics.ReasonIdentity)
			return nil, fmt.Errorf("admin state batch record %d: author %q must equal local node %q", i, parsed.NodeID, e.cfg.ULID)
		}
		if err := e.validateContract(parsed.Contract, input.Payload); err != nil {
			e.metrics.RejectPublish(metrics.ReasonValidation)
			return nil, fmt.Errorf("admin state batch record %d (%s): %w", i, input.Topic, err)
		}
		if err := e.validateAdminStateAuthor(parsed, input.Payload); err != nil {
			e.metrics.RejectPublish(metrics.ReasonIdentity)
			return nil, fmt.Errorf("admin state batch record %d (%s): %w", i, input.Topic, err)
		}

		prepared = append(prepared, preparedRecord{
			parsed: parsed,
			class:  class,
			record: store.Record{
				Topic: input.Topic, Payload: input.Payload, TS: ts,
				WrittenBy: attribution.WrittenBy, ActorID: attribution.ActorID,
				ActorLabel: attribution.ActorLabel, ActorKind: attribution.ActorKind,
				ActorGroups: attribution.ActorGroups,
				KVPath:      parsed.Path, KVNode: parsed.NodeID, Delete: len(input.Payload) == 0,
			},
		})
	}

	storeRecords := make([]store.Record, len(prepared))
	for i := range prepared {
		storeRecords[i] = prepared[i].record
	}
	first, _, err := e.store.Append(stream, storeRecords)
	if err != nil {
		if errors.Is(err, store.ErrRecordTooLarge) {
			e.metrics.RecordRejected("too_large")
		}
		return nil, err
	}

	results := make([]Result, len(prepared))
	for i, item := range prepared {
		offset := first + uint64(i)
		e.metrics.IngestRecord(stream)
		e.log.Debug("atomic state ingest", "stream", stream, "offset", offset, "topic", item.record.Topic)
		e.elements.Observe(item.parsed.Contract, item.record.Topic, item.record.Payload)
		if e.deliver != nil {
			e.deliver(item.record.Topic, item.record.Payload, retainFor(item.class))
		}
		results[i] = Result{Persisted: true, Stream: stream, Offset: offset, Topic: item.record.Topic}
	}
	return results, nil
}

// ingestAdminEvent commits ONE append-only event a command executor authored
// directly (EditExec's annotation intent today, via PublishEvent) — the
// sibling of ingestAdminStateBatch above for uns.IsCommandAuthoredEvent
// classes rather than uns.IsCommandAuthoredState ones.
//
// It cannot reuse ingestAdminStateBatch: that path unconditionally treats
// every record as KV-projecting state (it always sets KVPath/KVNode), which
// is exactly right for the entity/definition classes it admits and exactly
// wrong for an event class — ClassAnnotation is deliberately excluded from
// IsState (dataops-evaluator design §8) precisely so a part-cycle producer's
// ~1M annotations/year/machine never grows a KV entry. So this path never
// sets KVPath at all, and it commits exactly one record: an event carries no
// current value for anything else to be atomic WITH, unlike a command's
// whole entity-graph transition.
func (e *Engine) ingestAdminEvent(record uns.StateRecord, attribution Attribution) (Result, error) {
	if !uns.IsUns(record.Topic) {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, fmt.Errorf("admin event record must be colca/#")
	}
	parsed, err := uns.Parse(record.Topic)
	if err != nil {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, fmt.Errorf("admin event record: %w", err)
	}
	class := e.ClassOf(parsed.Contract)
	if !uns.IsKnown(class) {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, fmt.Errorf("admin event record: unknown contract %s", parsed.Contract)
	}
	if !uns.IsCommandAuthoredEvent(class) {
		e.metrics.RejectPublish(metrics.ReasonValidation)
		return Result{}, fmt.Errorf("admin event record (%s): %s is not an event a command may author",
			record.Topic, parsed.Contract)
	}
	if parsed.NodeID != e.cfg.ULID {
		e.metrics.RejectPublish(metrics.ReasonIdentity)
		return Result{}, fmt.Errorf("admin event record: author %q must equal local node %q", parsed.NodeID, e.cfg.ULID)
	}
	if len(record.Payload) == 0 {
		e.metrics.RejectPublish(metrics.ReasonValidation)
		return Result{}, fmt.Errorf("admin event record (%s): an event cannot tombstone", record.Topic)
	}
	if err := e.validateContract(parsed.Contract, record.Payload); err != nil {
		e.metrics.RejectPublish(metrics.ReasonValidation)
		return Result{}, fmt.Errorf("admin event record (%s): %w", record.Topic, err)
	}

	stream := uns.StreamFor(class)
	ts := time.Now().UnixMilli()
	storeRecord := store.Record{
		Topic: record.Topic, Payload: record.Payload, TS: ts,
		WrittenBy: attribution.WrittenBy, ActorID: attribution.ActorID,
		ActorLabel: attribution.ActorLabel, ActorKind: attribution.ActorKind,
		ActorGroups: attribution.ActorGroups,
		// Deliberately no KVPath/KVNode: this class is never state (IsState is
		// false), so there is nothing to project and nothing to retract.
	}
	first, _, err := e.store.Append(stream, []store.Record{storeRecord})
	if err != nil {
		return Result{}, err
	}
	e.metrics.IngestRecord(stream)
	e.log.Debug("atomic event ingest", "stream", stream, "offset", first, "topic", record.Topic)
	e.elements.Observe(parsed.Contract, record.Topic, record.Payload)
	if e.deliver != nil {
		e.deliver(record.Topic, record.Payload, retainFor(class))
	}
	return Result{Persisted: true, Stream: stream, Offset: first, Topic: record.Topic}, nil
}

// IngestRefresh is the retention pruner's §6.5 state-refresh entry (spec §6.5
// [delta]) — an admin-grade publish that applies ONLY IF the KV entry for the
// topic's (contract, path, node) still sits at ifKVOffset, evaluated as a true
// CAS under the store mutex (AppendIfKVUnchanged). A tombstone (§7) or newer write
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
	class := e.ClassOf(p.Contract)
	if !uns.IsOwnedState(class) {
		return Result{}, false, fmt.Errorf("refresh publish requires a KV-projecting contract, got %s", p.Contract)
	}
	if len(payload) == 0 {
		return Result{}, false, fmt.Errorf("refresh publish must not be empty (a refresh cannot tombstone)")
	}
	if err := e.validateContract(p.Contract, payload); err != nil {
		return Result{}, false, err
	}
	streamName := uns.StreamFor(class)
	rec := store.Record{
		Topic: topic, Payload: payload, TS: time.Now().UnixMilli(),
		WrittenBy: "colca-retention", KVPath: p.Path, KVNode: p.NodeID,
	}
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
	return e.IngestDownlinkAttributed(topic, payload, ts, Attribution{})
}

// IngestDownlinkAttributed preserves the authorship stamped at the command's
// origin while keeping the original timestamp.
// Its errors are typed the way every door types them: a deliberate refusal
// (grammar, draining) is a *RejectError, anything else is the store failing.
// The downlink loop tells them apart to decide whether the record may be
// acked past — see repl.RunDownlink.
func (e *Engine) IngestDownlinkAttributed(topic string, payload []byte, ts int64, attribution Attribution) (Result, error) {
	p, err := uns.Parse(topic)
	if err != nil {
		return e.reject(metrics.ReasonGrammar, "%w", err)
	}
	class := e.ClassOf(p.Contract)
	if uns.IsCommand(class) && e.ids.DrainingMount(p.Path) {
		// Move-drain design §3.2 item 2: the SAME admission rule as
		// IngestClient/IngestAdmin, extended to this relay door. Without this
		// check, a command authored ABOVE this node's own parent — where THIS
		// node's own draining child is invisible — is admitted upstream,
		// relays down through the downlink-poll loop, and lands straight in a
		// mount this node is actively draining: the "chasing a moving tail"
		// failure the admission gate exists to prevent, reachable in a
		// multi-hop tree even though the direct doors (client/admin) are
		// covered. The caller (repl.RunDownlink) skips past a record refused
		// this way — a *RejectError, like every deliberate refusal at every
		// door — because offering it again can only produce the same refusal.
		// No retry loop, no stuck cursor, no gap-jump side effect: this is a
		// single record rejected at persistence time, not a batch operation.
		// Only a record the STORE could not take holds its cursor.
		return e.reject(metrics.ReasonDraining,
			"downlink: %s is draining — no new commands admitted (move-drain design §3.2)", p.Path)
	}
	res, err := e.persistTSAttributed(class, p, topic, payload, ts, attribution)
	if err == nil {
		e.maybeExec(p, payload, attribution, e.actorForAttested(attribution)) // the target executes downlinked commands (cmdadmin design §5)
	}
	return res, err
}

// IngestDownlinkDefinition applies a definition handed down by the parent
// (definition-stream design §5).
//
// The difference from IngestDownlink is the whole difference between the two
// downward flows: a command is EXECUTED at its target, a definition is APPLIED
// everywhere it lands. So this persists, KV-projects and mirrors retained onto
// the bus — and never calls maybeExec, because there is nothing to run.
//
// The topic is stored exactly as it arrived. A definition has no position, so
// there is no mount to strip and nothing to rewrite: what the parent holds and
// what this node holds are the same bytes.
func (e *Engine) IngestDownlinkDefinition(topic string, payload []byte, ts int64) (Result, error) {
	return e.IngestDownlinkDefinitionAttributed(topic, payload, ts, Attribution{})
}

// IngestDownlinkDefinitionAttributed preserves definition authorship through
// every descendant that stores the record.
func (e *Engine) IngestDownlinkDefinitionAttributed(topic string, payload []byte, ts int64, attribution Attribution) (Result, error) {
	p, err := uns.Parse(topic)
	if err != nil {
		return e.reject(metrics.ReasonGrammar, "%w", err)
	}
	class := e.ClassOf(p.Contract)
	if !uns.IsDefinition(class) {
		// The parent sent something that is not a definition on the definitions
		// channel. Refuse it rather than filing it: the channel's whole contract
		// is that what arrives on it is applied unconditionally.
		return e.reject(metrics.ReasonGrammar,
			"downlink definitions: %s is %v, not a definition", p.Contract, class)
	}
	if err := e.validateContract(p.Contract, payload); err != nil {
		return e.reject(metrics.ReasonValidation, "%w", err)
	}
	return e.persistTSAttributed(class, p, topic, payload, ts, attribution)
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
	for _, r := range got {
		p, perr := uns.Parse(r.Topic)
		if perr != nil {
			// Durable already — only the bus mirror is skipped, never the apply.
			e.log.Warn("replicated record not mirrored to the local bus: unparseable topic",
				"child", child, "stream", stream, "topic", r.Topic, "err", perr)
			continue
		}
		// An element a child published is a position in THIS node's namespace
		// too, at the mount-inserted path — that is how an ancestor can answer a
		// grant naming an element deep in its subtree (id-grants design §4).
		e.elements.Observe(p.Contract, r.Topic, r.Payload)
		if e.deliver != nil {
			e.deliver(r.Topic, r.Payload, retainFor(e.ClassOf(p.Contract)))
		}
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
		if p, err := uns.Parse(r.Topic); err == nil && uns.IsNodeLocal(uns.ClassOf(p.Contract)) {
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
// A stream whose uplink is a filtered subset is exempt — the domain says
// which (uns.UplinkCarriesEveryRecord): `commands`, where only _Ack and
// _StreamGap travel up, and `entities`, where the node-private Edit
// receipt stays at its author. Child-offset holes there are the filter
// working, not data loss — the premise "gapless offsets" does not hold on
// that wire, and the durable _StreamGap marker (the first net) is the only
// honesty mechanism left on it.
//
// droppedTimeSync (rejectTimeSync's return) exempts a jump this SAME call's
// own _TimeSync filtering created: a jump is only
// logged/counted when at least one offset in the gap is NOT accounted for by
// a dropped _TimeSync record — a gap partially explained by a drop but also
// missing a genuinely unaccounted offset still logs, so this only removes
// the false positive, never masks a real one.
func (e *Engine) logOffsetJumps(child, stream string, prev uint64, applied []store.ReplRecord, droppedTimeSync map[uint64]bool) {
	if !uns.UplinkCarriesEveryRecord(stream) {
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
func retainFor(c uns.Class) bool { return uns.IsState(c) }

func (e *Engine) persist(class uns.Class, p uns.Parsed, topic string, payload []byte) (Result, error) {
	return e.persistAttributed(class, p, topic, payload, Attribution{})
}

func (e *Engine) persistAttributed(class uns.Class, p uns.Parsed, topic string, payload []byte, attribution Attribution) (Result, error) {
	return e.persistTSAttributed(class, p, topic, payload, time.Now().UnixMilli(), attribution)
}

// persistTS writes the record (plus, for data/entity, its KV projection) in one
// atomic batch and then mirrors it onto the local MQTT bus under the STORED
// topic. p must be the parse of topic exactly as it will be persisted — for a
// client publish that is the topic unchanged, for a replicated record it is
// already mount-inserted — so KVPath and KVNode always carry this node's own
// local coordinates and the originating node id.
//
// The order is load-bearing: the bus must never show something that is not
// durable, so delivery happens only after Append returned successfully. This is
// the single place that guarantees the rule "everything appended to a node's
// stream is also published on that node's bus" for client, admin and downlink
// ingest alike.
func (e *Engine) persistTS(class uns.Class, p uns.Parsed, topic string, payload []byte, ts int64) (Result, error) {
	return e.persistTSAttributed(class, p, topic, payload, ts, Attribution{})
}

func (e *Engine) persistTSAttributed(class uns.Class, p uns.Parsed, topic string, payload []byte, ts int64, attribution Attribution) (Result, error) {
	streamName := uns.StreamFor(class)
	rec := store.Record{
		Topic: topic, Payload: payload, TS: ts,
		WrittenBy: attribution.WrittenBy, ActorID: attribution.ActorID,
		ActorLabel: attribution.ActorLabel, ActorKind: attribution.ActorKind,
		ActorGroups: attribution.ActorGroups,
	}
	if uns.IsState(class) {
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
		if errors.Is(err, store.ErrRecordTooLarge) {
			e.metrics.RecordRejected("too_large")
		}
		return Result{}, err
	}
	e.metrics.IngestRecord(streamName)
	e.log.Debug("ingest", "stream", streamName, "offset", first, "topic", topic)
	// The namespace index tracks every persisted record, not just the ones a
	// machine published: an element authored through the data-model door lands
	// via IngestAdmin, and a stale index would mount identities at positions
	// that moved. This sits below every Ingest* path for that reason — unlike
	// the plugin observer, which is deliberately only offered what a machine
	// published.
	e.elements.Observe(p.Contract, topic, payload)
	// Asked here rather than beside the delivery below, because routability is
	// a question about the TREE and has nothing to do with whether this node
	// has a local bus: a command that can reach nobody is just as invisible on
	// a node whose broker is not wired.
	if uns.IsCommand(class) {
		e.countIfUnroutable(p, topic, payload)
	}
	if e.deliver != nil {
		// A command addressed to a machine enrolled here is delivered through
		// its own path, because for that one case "publish it" and "record
		// that it was delivered" are a single decision that has to be made
		// together — see deliverCommand in redelivery.go. Everything else
		// (state, samples, and commands merely relaying through this node)
		// goes straight to the bus.
		if uns.IsCommand(class) {
			e.deliverCommand(class, p, topic, payload, first)
		} else {
			e.deliver(topic, payload, retainFor(class))
		}
	}
	return Result{Persisted: true, Stream: streamName, Offset: first, Topic: topic}, nil
}
