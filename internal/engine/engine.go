// Package engine is where every write converges: the MQTT hook, HTTP publish,
// replication and downlink all call an Ingest* method. Grammar, the level-4 rule,
// write authorization, validation and the atomic persist live here and nowhere
// else.
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

// LocalDeliver hands a record to the local MQTT broker (nil without a broker).
// retain is true for state contracts, so the retained set mirrors the KV
// projection.
type LocalDeliver func(topic string, payload []byte, retain bool)

// HasSubscriberFor reports whether identity ulid has a live subscription
// matching topic on the local bus. It is nil without a broker, and callers then
// make no claim.
//
// It must answer for that identity, not for anyone: a true answer advances the
// machine's delivery cursor, so an observer subscribed for diagnostics would
// otherwise mark an offline machine's commands delivered. It reports
// subscriptions, not bytes on the wire, so the undelivered counter can undercount
// failures but never overcount them.
type HasSubscriberFor func(topic, ulid string) bool

// Result describes what an ingest did. Topic is what was persisted: unchanged for
// a client's own publish, with the child's mount inserted for a replicated record.
type Result struct {
	Persisted bool
	// Duplicate is a command whose correlation id the node already accepted from
	// the same sender: nothing was stored or run, and its ack, if there is one yet,
	// went out again.
	Duplicate bool
	Stream    string
	Offset    uint64
	Topic     string // as persisted, with the mount inserted for replicated records
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
	// ActorGroups are the group ids a human's grants were resolved from at the
	// verifying door. They travel with the record, so the node that executes a
	// downlinked command can resolve the same person against its own _Group
	// definitions.
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

// actorForAttested reconstitutes a human from the group ids in a record's
// attribution, resolved against this node's _Group definitions. Unknown groups
// contribute nothing, so the person may end up with no grants and be refused,
// never widened. Without attested groups there is no actor, and the executor
// refuses a _CmdEdit.
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
		var unknown *uns.UnknownGroupError
		if errors.As(problem, &unknown) {
			if e.unknownGroups.First(unknown.ID) {
				e.log.Info("attested actor names a group this node does not define; it grants nothing here "+
					"(logged once per group)", "group", unknown.ID)
			}
			continue
		}
		e.log.Warn("attested actor: group unresolved for the acting human", "actor", attribution.ActorID, "err", problem)
	}
	if attribution.ActorLabel != "" && attribution.ActorLabel != attribution.ActorID {
		entry.Username = attribution.ActorLabel // the verifying door's preferred_username, kept as the label
	}
	return entry
}

// Mounts resolves identities to registry entries (*registry.Manager). The engine
// keeps no identity state of its own.
type Mounts interface {
	// Get resolves a ULID to its entry. It must return (nil, false) for an unknown
	// identity, never (nil, true). Entry's predicates are nil-safe as a backstop, so
	// a fake that breaks this gets a refusal rather than a panic.
	Get(ulid string) (*uns.Entry, bool)
	// DrainingMount reports whether path lies under a draining child node's mount;
	// new commands there are refused at admission.
	DrainingMount(path string) bool
}

// routableMounts is the optional half of Mounts: could a command at this path
// reach any child? It only feeds a counter, so an implementation without it loses
// that metric rather than failing to build.
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
	// unknownGroups keeps an attested group this node does not define to one log line.
	unknownGroups uns.GroupNotices
	clk           *clock.Clock

	// hasSubscriber is nil until SetSubscriberCheck; without it no command counts as
	// undelivered and nothing is replayed. It is wired late because the broker is
	// built after the engine.
	hasSubscriber HasSubscriberFor

	// replayLocks serializes ReplayOwedCommands per identity. replayMu guards only
	// the map, so replays for different machines never wait on each other.
	replayMu    sync.Mutex
	replayLocks map[string]*sync.Mutex

	// The node's position: the chain of elements from the root down to its own,
	// taught by the parent, persisted, unknown until first taught. The root sets the
	// empty chain at startup. The prefix is rendered from it, never stored.
	posMu         sync.RWMutex
	ancestry      uns.Ancestry
	ancestryKnown bool

	// ledger remembers each command's sender and ack by correlation id.
	ledger *commandLedger

	// exec executes commands addressed to this node. The engine owns the mechanism,
	// the executor what a verb means. Nil until SetExecutor, and then nothing
	// executes.
	exec CommandExecutor

	// observer is told about records persisted through this node's own doors,
	// so the domain plugin can react to state the core does not interpret.
	observer RecordObserver

	// onPosition is called when this node learns or changes its position, so the
	// node can describe itself in its _Node record.
	onPosition func(uns.Ancestry)

	// contracts is the loaded schema-bundle table (nil = builtin floor).
	// Static per process: set once at startup, before any door serves.
	contracts *contracts.Table

	// elements maps element ids to local paths for the _SystemElement records this
	// node holds. It lives in the engine because every record lands here, which keeps
	// the index current.
	elements *uns.ElementIndex

	// auditID is injectable for deterministic event tests. Audit writes bypass
	// the public ingest doors; see audit.go.
	auditID func(time.Time) string

	// unboundLog rate-limits the "_Metric with no _Signal" log line per path
	// (unbound.go).
	unboundLog *unboundMetricLog
}

// New builds an engine. ids is the identity registry: a publish is admitted when
// the identity's write scope covers the topic's path. m may be nil; every Metrics
// method is nil-safe.
//
// clk may be nil, and New then builds one from cfg. A caller that also passes a
// clock to metrics.New must build it itself and pass the same instance to both,
// or the clock metrics never change.
func New(s *store.Store, cfg *config.Config, ids Mounts, deliver LocalDeliver, m *metrics.Metrics, clk *clock.Clock) *Engine {
	if clk == nil {
		clk = clock.New(cfg.Parent == nil, time.Now)
	}
	e := &Engine{store: s, cfg: cfg, deliver: deliver, ids: ids, log: slog.Default().With("node", cfg.ULID), metrics: m, clk: clk, auditID: newAuditID, unboundLog: newUnboundMetricLog(),
		ledger: newCommandLedger()}
	e.elements = uns.NewElementIndex(e.EntityStore())
	if raw, ok := s.AncestryGet(); ok {
		var a uns.Ancestry
		if err := json.Unmarshal(raw, &a); err != nil {
			// Fail loudly: a node that forgot its position denies every scoped grant, which
			// looks like a permissions problem but is a corrupt store.
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

// Prefix renders the node's root-frame path from its ancestry; ok is false until
// first taught. The path is derived on each call, so it cannot go stale.
func (e *Engine) Prefix() (string, bool) {
	a, ok := e.Ancestry()
	if !ok {
		return "", false
	}
	return a.Prefix(), true
}

// SetAncestry stores a taught position. An unchanged chain writes nothing; a
// change is persisted, logged and reflected in colca_node_prefix_info. New token
// verifications use it at once; live human sessions keep what they had at
// connect.
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

// SetOnPosition registers the position callback. Call it before the root sets
// its empty ancestry, so the first position is reported too.
func (e *Engine) SetOnPosition(fn func(uns.Ancestry)) { e.onPosition = fn }

func (e *Engine) Store() *store.Store { return e.store }

// AuthoritativeNow is this node's estimate of the root's clock: wall time plus
// the offset from the latest parent response, or raw wall time on the root and
// before the first sync.
func (e *Engine) AuthoritativeNow() time.Time {
	return e.clk.AuthoritativeNow()
}

// ApplyClockSample records the offset from a parent's now_ms and warns when drift
// exceeds time_sync.drift_warn_ms. Callers apply it before ingesting the records
// from the same response, so expiry decisions use the corrected time. A no-op on
// the root.
func (e *Engine) ApplyClockSample(nowMS int64) (offsetMS int64) {
	offsetMS = e.clk.ApplySample(nowMS)
	if warn := e.cfg.TimeSync.EffectiveDriftWarnMS(); abs64(offsetMS) > warn {
		e.log.Warn("clock drift exceeds warn threshold",
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

// Groups resolves a token's group ids against the _Group definitions this node
// holds.
func (e *Engine) Groups() *uns.GroupIndex {
	idx := uns.NewGroupIndex(e.EntityStore())
	if e.cfg.Standalone {
		idx.WithAuthority(e.cfg.ULID)
	}
	return idx
}

// NodeID is the identity this node publishes under.
func (e *Engine) NodeID() string { return e.cfg.ULID }

func stateIdentity(payload []byte) (id, colcaNodeID string, err error) {
	if len(payload) == 0 { // a tombstone carries its identity in the topic
		return "", "", nil
	}
	var value struct {
		ID          string `json:"id"`
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

// IngestClient ingests a publish from a directly attached MQTT client: grammar,
// allowed class, level 4 is this node, write authorization, validation, persist.
// A non-UNS topic is ordinary broker traffic and is not persisted.
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
	// Registry entries enter only through the enrollment door; no client may publish
	// an _EnrolledIdentity, not even its own.
	if p.Contract == "_EnrolledIdentity" {
		return e.reject(metrics.ReasonRegistryContract, "client %s may not publish _EnrolledIdentity — registry entries are enrollment-door only", identity)
	}
	class := e.ClassOf(p.Contract)
	if uns.IsNodeLocal(class) {
		// _TimeSync is published only by this node's beacon. A client attempting it is
		// rejected with its own reason rather than the generic grammar reason.
		return e.reject(metrics.ReasonTimeSync, "client %s may not publish _TimeSync: it is node-local and never replicated", identity)
	}
	if uns.IsCommand(class) {
		// A mount under an active drain accepts no new commands, whatever the caller's
		// grants. Checked first so the rejection gives the real reason.
		if e.ids.DrainingMount(p.Path) {
			return e.reject(metrics.ReasonDraining, "client %s: %s is draining, no new commands admitted", identity, p.Path)
		}
		// A command needs a covering cmd grant. Commands target absolute node-local
		// paths: no mount rewrite and no level-4 rule, since the author does not own the
		// target.
		entry, ok := e.ids.Get(identity)
		if !ok {
			return e.rejectDenied(metrics.ReasonCmdDenied,
				Attribution{ActorID: identity, ActorLabel: identity, ActorKind: "service"},
				"execute", &p, "client %s: no cmd grant covers %s", identity, topic)
		}
		attribution := actorFor(entry)
		// A local service attesting a person is judged as that person, resolved against
		// this node's _Group definitions: never with the service's own configure grant,
		// never wider than the person. A service publishing as itself is judged as
		// itself.
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
		id, repeat, err := e.admitCommand(p, payload, attribution.ActorID)
		if err != nil {
			return Result{}, err
		}
		if repeat {
			return e.repeated(payload), nil
		}
		res, err := e.persistAttributed(class, p, topic, payload, attribution)
		if err != nil {
			e.ledger.forget(id)
			return res, err
		}
		res.Command = e.maybeExec(p, payload, attribution, actor) // return the synchronous outcome to local API callers
		return res, nil
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
	// Level 4 is this node's ULID for every publisher. A service's identity decides
	// whether a write is allowed but never appears in the topic.
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
	// The client already publishes the canonical node-local topic; only the
	// attribution is added.
	res, err := e.persistAttributed(class, p, topic, payload, actorFor(entry))
	if err == nil {
		// Offer state a machine published to the domain plugin; the core does not
		// interpret it.
		e.observe(p, topic, payload)
		// A _Metric with no _Signal would otherwise be an invisible write: count it and
		// log it, rate-limited.
		e.checkMetricBinding(p)
	}
	return res, err
}

// IngestHuman ingests a publish from a verified human. Humans only send commands,
// gated by their cmd grants; data, entity and ack contracts are rejected with
// ReasonHumanWrite whatever the grants. Commands target absolute node-local
// paths, as admin commands do.
func (e *Engine) IngestHuman(entry *uns.Entry, topic string, payload []byte) (Result, error) {
	return e.IngestHumanAttributed(entry, entry.ULID, topic, payload)
}

// IngestHumanAttributed is IngestHuman with the verified display label retained in
// the record envelope. Authorization still uses entry; the label is evidence,
// never an authority input.
func (e *Engine) IngestHumanAttributed(entry *uns.Entry, actorLabel, topic string, payload []byte) (Result, error) {
	if !uns.IsUns(topic) {
		return e.reject(metrics.ReasonGrammar, "human publish must be %s/#", uns.Root())
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
		// _TimeSync is node-local only, as at the other doors. Checked before the
		// human-write rule so the rejection gives the real reason.
		e.metrics.RejectPublish(metrics.ReasonTimeSync)
		return Result{}, fmt.Errorf("human %s may not publish _TimeSync: it is node-local and never replicated", entry.ULID)
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
			"human %s may not publish %s: people configure through _CmdEdit, where each write is authorized as them", entry.ULID, p.Contract)
	}
	if e.ids.DrainingMount(p.Path) {
		// A draining mount refuses commands at every door, a human's grants
		// notwithstanding. Checked first, as in IngestClient.
		return e.reject(metrics.ReasonDraining, "human %s: %s is draining, no new commands admitted", entry.ULID, p.Path)
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
	id, repeat, err := e.admitCommand(p, payload, attribution.ActorID)
	if err != nil {
		return Result{}, err
	}
	if repeat {
		return e.repeated(payload), nil
	}
	res, err := e.persistAttributed(class, p, topic, payload, attribution)
	if err != nil {
		e.ledger.forget(id)
		return res, err
	}
	res.Command = e.maybeExec(p, payload, attribution, entry) // commands addressed to this node execute here
	return res, nil
}

// IngestAdmin ingests a publish through the admin-token HTTP API: node-local
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
		return Result{}, fmt.Errorf("admin publish must be %s/#", uns.Root())
	}
	p, err := uns.Parse(topic)
	if err != nil {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, err
	}
	// Not even the admin token may publish registry entries; enrollment has its own
	// door.
	if p.Contract == "_EnrolledIdentity" {
		e.metrics.RejectPublish(metrics.ReasonRegistryContract)
		return Result{}, fmt.Errorf("_EnrolledIdentity is enrollment-door only — use POST /enroll")
	}
	class := e.ClassOf(p.Contract)
	if uns.IsNodeLocal(class) {
		// Same rule as IngestClient: _TimeSync is node-local-publish-only,
		// not even the admin token may author it through /publish.
		e.metrics.RejectPublish(metrics.ReasonTimeSync)
		return Result{}, fmt.Errorf("admin may not publish _TimeSync: it is node-local and never replicated")
	}
	if uns.IsAudit(class) {
		return e.rejectDenied(metrics.ReasonWriteDenied, attribution, "publish", &p,
			"admin may not publish _AuditEvent — use the local audit producer")
	}
	if uns.IsCommand(class) && e.ids.DrainingMount(p.Path) {
		// Same admission rule as IngestClient: not even the admin token may command a
		// draining mount.
		e.metrics.RejectPublish(metrics.ReasonDraining)
		return Result{}, fmt.Errorf("admin: %s is draining, no new commands admitted", p.Path)
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
	var id string
	if uns.IsCommand(class) {
		var repeat bool
		if id, repeat, err = e.admitCommand(p, payload, attribution.ActorID); err != nil {
			return Result{}, err
		}
		if repeat {
			return e.repeated(payload), nil
		}
	}
	res, err := e.persistAttributed(class, p, topic, payload, attribution)
	if err != nil {
		e.ledger.forget(id)
		return res, err
	}
	res.Command = e.maybeExec(p, payload, attribution, nil) // the admin door presents a token, not an identity
	return res, nil
}

// ingestAdminStateBatch commits the complete state result of one domain command.
// Every record is validated before Store.Append runs, and one synced Pebble batch
// appends the history and updates the KV projection, so a late invalid record
// cannot leave earlier ones applied.
//
// A batch covers one stream, because Append writes one stream per Pebble batch.
// No command mixes streams, and the check below keeps it that way.
func (e *Engine) ingestAdminStateBatch(records []uns.StateRecord, attribution Attribution) ([]Result, error) {
	if len(records) == 0 {
		// A command that decided on nothing, with every path present and every entry
		// bound, is a successful no-op.
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
			return nil, fmt.Errorf("admin state batch record %d must be %s/#", i, uns.Root())
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

// ingestAdminEvent commits one append-only event a command executor authored,
// such as an annotation. Unlike ingestAdminStateBatch it never projects into KV:
// annotations are events, and a producer may write about a million per machine a
// year.
func (e *Engine) ingestAdminEvent(record uns.StateRecord, attribution Attribution) (Result, error) {
	if !uns.IsUns(record.Topic) {
		e.metrics.RejectPublish(metrics.ReasonGrammar)
		return Result{}, fmt.Errorf("admin event record must be %s/#", uns.Root())
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

// IngestRefresh is the retention pruner's state refresh: an admin publish applied
// only if the topic's KV entry still sits at ifKVOffset, checked as a CAS under
// the store mutex. If a tombstone or newer write landed since the pruner's scan,
// the whole record is skipped: no append, no metrics, no bus delivery
// (applied=false, nil error).
//
// An applied record gets full IngestAdmin semantics. Only KV-projecting classes
// are accepted, and an empty payload is rejected, since a refresh must never
// carry a tombstone. Failures are not counted as rejected publishes; the pruner
// counts them.
func (e *Engine) IngestRefresh(topic string, payload []byte, ifKVOffset uint64) (Result, bool, error) {
	if !uns.IsUns(topic) {
		return Result{}, false, fmt.Errorf("refresh publish must be %s/#", uns.Root())
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

// IngestDownlink persists a command received from the parent, already in local
// coordinates, with the parent's original timestamp so a hop does not extend its
// expiry. persistTS mirrors it onto the bus.
func (e *Engine) IngestDownlink(topic string, payload []byte, ts int64) (Result, error) {
	return e.IngestDownlinkAttributed(topic, payload, ts, Attribution{})
}

// IngestDownlinkAttributed is IngestDownlink with the authorship stamped at the
// command's origin. A deliberate refusal is a *RejectError and anything else is a
// store failure; repl.RunDownlink uses that to decide whether it may ack past the
// record.
func (e *Engine) IngestDownlinkAttributed(topic string, payload []byte, ts int64, attribution Attribution) (Result, error) {
	p, err := uns.Parse(topic)
	if err != nil {
		return e.reject(metrics.ReasonGrammar, "%w", err)
	}
	class := e.ClassOf(p.Contract)
	if uns.IsCommand(class) && e.ids.DrainingMount(p.Path) {
		// A draining mount refuses new commands at this relay door too. Otherwise a
		// command authored above this node's parent, where the draining child is
		// invisible, would relay down into the mount being drained. The downlink loop
		// skips a record refused this way, since offering it again gives the same answer;
		// only a store failure holds its cursor.
		return e.reject(metrics.ReasonDraining,
			"downlink: %s is draining, no new commands admitted", p.Path)
	}
	res, err := e.persistTSAttributed(class, p, topic, payload, ts, attribution)
	if err == nil {
		e.maybeExec(p, payload, attribution, e.actorForAttested(attribution)) // the target executes downlinked commands
	}
	return res, err
}

// IngestDownlinkDefinition applies a definition handed down by the parent. A
// command is executed at its target; a definition is applied everywhere it
// lands, so this persists, projects into KV and mirrors retained, and never calls
// maybeExec. A definition has no position, so the topic is stored exactly as it
// arrived.
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
		// The parent sent something that is not a definition on the definitions channel.
		// Refuse it: what arrives on this channel is applied unconditionally.
		return e.reject(metrics.ReasonGrammar,
			"downlink definitions: %s is %v, not a definition", p.Contract, class)
	}
	if err := e.validateContract(p.Contract, payload); err != nil {
		return e.reject(metrics.ReasonValidation, "%w", err)
	}
	return e.persistTSAttributed(class, p, topic, payload, ts, attribution)
}

// IngestReplicated applies a batch pushed by a child: dedupe by high-water mark,
// then mirror every newly applied record onto the local bus. The replication
// server never writes to the store directly.
func (e *Engine) IngestReplicated(child, stream string, recs []store.ReplRecord) (applied int, hwm uint64, err error) {
	recs, droppedTimeSync := e.rejectTimeSync(child, recs)
	prev := e.store.HWMGet(child, stream)
	got, hwm, err := e.store.ApplyReplicated(child, stream, recs)
	if err != nil {
		return 0, hwm, err
	}
	e.logOffsetJumps(child, stream, prev, got, droppedTimeSync)
	// Replicated records count toward colca_ingest_records_total like the other entry
	// paths.
	for _, r := range got {
		if r.SkipFrom == 0 {
			e.metrics.IngestRecord(stream)
			applied++
		}
	}
	for _, r := range got {
		if r.SkipFrom != 0 {
			continue
		}
		p, perr := uns.Parse(r.Topic)
		if perr != nil {
			// The record is already durable; only the bus mirror is skipped.
			e.log.Warn("replicated record not mirrored to the local bus: unparseable topic",
				"child", child, "stream", stream, "topic", r.Topic, "err", perr)
			continue
		}
		// An element a child published is a position in this node's namespace too, at
		// the mount-inserted path, so an ancestor can resolve grants naming it.
		e.elements.Observe(p.Contract, r.Topic, r.Payload)
		if e.deliver != nil {
			e.deliver(r.Topic, r.Payload, retainFor(e.ClassOf(p.Contract)))
		}
	}
	return applied, hwm, nil
}

// rejectTimeSync drops _TimeSync records from a replicated batch before they
// reach the store. A well-behaved child never stores one, so such a record is
// forged or buggy; the rest of the batch still applies.
//
// It returns the dropped child offsets, so logOffsetJumps does not report the
// hole a drop leaves as data loss.
func (e *Engine) rejectTimeSync(child string, recs []store.ReplRecord) (filtered []store.ReplRecord, dropped map[uint64]bool) {
	filtered = recs[:0:0]
	for _, r := range recs {
		if p, err := uns.Parse(r.Topic); err == nil && uns.IsNodeLocal(uns.ClassOf(p.Contract)) {
			e.metrics.RejectPublish(metrics.ReasonTimeSync)
			e.log.Warn("rejected _TimeSync record from replication: it is node-local and never replicated",
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

// logOffsetJumps is the second check for lost records: a child's stream offsets
// are gapless and read contiguously, so an applied ChildOffset above HWM+1 means
// records are gone at the child, even if it never emitted a _StreamGap marker.
// Detection only.
//
// Streams whose uplink is filtered (uns.UplinkCarriesEveryRecord) are exempt, and
// so are holes fully explained by this batch's dropped _TimeSync records.
func (e *Engine) logOffsetJumps(child, stream string, prev uint64, applied []store.ReplRecord, droppedTimeSync map[uint64]bool) {
	if !uns.UplinkCarriesEveryRecord(stream) {
		return
	}
	last := prev
	for _, r := range applied {
		first := r.ChildOffset
		if r.SkipFrom != 0 {
			first = r.SkipFrom
		}
		if first > last+1 && !jumpFullyExplainedByDroppedTimeSync(last, first, droppedTimeSync) {
			e.log.Error("replication offset jump: this node never received the child offsets between have and got, most likely pruned at the child before replication",
				"child", child, "stream", stream, "have", last, "got", r.ChildOffset)
			e.metrics.GapApplied(child, stream)
		}
		last = r.ChildOffset
	}
}

// jumpFullyExplainedByDroppedTimeSync reports whether every offset strictly
// between last and childOffset was a dropped _TimeSync record.
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

// retainFor decides how a record appears on the local bus. State classes are
// retained, exactly the set with a KV projection. Commands and acks are events:
// retaining them would redeliver stale commands to every new subscriber.
func retainFor(c uns.Class) bool { return uns.IsState(c) }

func (e *Engine) persistAttributed(class uns.Class, p uns.Parsed, topic string, payload []byte, attribution Attribution) (Result, error) {
	return e.persistTSAttributed(class, p, topic, payload, time.Now().UnixMilli(), attribution)
}

// persistTSAttributed writes the record, and its KV projection for state, in one
// atomic batch, then mirrors it onto the local bus under the stored topic. p must
// parse the topic as persisted, so KVPath and KVNode carry local coordinates and
// the originating node.
//
// Delivery happens only after Append succeeded, so the bus never shows anything
// that is not durable. This is the one place that guarantees every appended
// record is also published on the node's bus.
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
		// An empty payload on a KV-projecting class is a tombstone: the record is
		// appended as history, the KV key is deleted in the same batch, and the retained
		// empty delivery below makes mochi clear the retained message.
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
	if uns.IsAck(class) {
		if id := correlationID(payload); id != "" {
			e.ledger.acked(id, topic, payload)
		}
	}
	// The element index tracks every persisted record, not just machine publishes:
	// an element authored through IngestAdmin must be visible too. The plugin
	// observer, by contrast, only sees what machines published.
	e.elements.Observe(p.Contract, topic, payload)
	// Routability is a question about the tree, not the local bus, so it is asked
	// even on a node without a broker.
	if uns.IsCommand(class) {
		e.countIfUnroutable(p, topic, payload)
	}
	if e.deliver != nil {
		// A command for a machine enrolled here goes through deliverCommand, where
		// publishing it and recording its delivery are one decision. Everything else goes
		// straight to the bus.
		if uns.IsCommand(class) {
			e.deliverCommand(class, p, topic, payload, first)
		} else {
			e.deliver(topic, payload, retainFor(class))
		}
	}
	return Result{Persisted: true, Stream: streamName, Offset: first, Topic: topic}, nil
}
