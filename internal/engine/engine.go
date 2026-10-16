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
	"sync/atomic"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/cursorwatch"
	"github.com/alpamayo-solutions/colca/internal/loggate"
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
	// Answered is a command the node answered itself instead of storing it: no
	// service announced it (see routes.go). Command holds the answer.
	Answered bool
	Stream   string
	Offset   uint64
	Topic    string // as persisted, with the mount inserted for replicated records
	Command  *CommandOutcome
	// Withheld is a _Log record the log gate accepted without storing it:
	// "collapsed" (counted in its window's summary) or "rate_limited" (counted
	// in its service's drop notice). Persisted is false; Stream and Topic are
	// set.
	Withheld string
}

// Attribution is the immutable authorship envelope stored with a record.
// WrittenBy is the authenticated publishing identity. ActorID is the stable
// subject it acted as. ActorLabel is optional: what that subject was called
// when it acted, for readers to show. Nothing is authorized or keyed by it.
type Attribution struct {
	WrittenBy  string
	ActorID    string
	ActorLabel string
	ActorKind  string
	// ActorClient is the client id of a Keycloak service account acting through
	// a token door, "" for a person. Like ActorGroups it travels with the
	// record, so the node that executes a downlinked command knows the account
	// from a person without reading its label.
	ActorClient string
	// ActorGroups are the group ids a human's grants were resolved from at the
	// verifying door. They travel with the record, so the node that executes a
	// downlinked command can resolve the same person against its own _Group
	// definitions.
	ActorGroups []string
	// Door is the door that admitted a command here and checked its sender's
	// grants: doorClient or doorHuman. It is stored with the command so the grants
	// can be checked again when the command is forwarded (ForwardRefusal).
	Door string
}

// Doors whose grant check ForwardRefusal repeats.
const (
	doorClient = "client"
	doorHuman  = "human"
)

func attributionForEntry(entry *uns.Entry) Attribution {
	if entry == nil {
		return Attribution{}
	}
	label := entry.ActorLabel()
	if label == "" && !entry.IsHuman() {
		label = entry.ULID
	}
	return Attribution{
		WrittenBy: entry.ULID, ActorID: entry.ULID,
		ActorLabel: label, ActorKind: entry.ActorKind(),
		ActorClient: entry.ServiceClient,
		ActorGroups: append([]string(nil), entry.Groups...),
	}
}

// actorForAttested reconstitutes a human from the group ids in a record's
// attribution, resolved against this node's _Group definitions. Unknown groups
// contribute nothing, so the person may end up with no grants and be refused,
// never widened. Without attested groups there is no actor, and the executor
// refuses a _CmdEdit.
//
// signedInHere says whether the person was admitted at this node (its human
// door, or a local service of this deployment attesting them) rather than on a
// command that came down from the parent. Only then do their node-relative
// ($node) grants resolve here; see uns.Entry.SignedInHere.
func (e *Engine) actorForAttested(attribution Attribution, signedInHere bool) *uns.Entry {
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
	entry.DisplayName, entry.ServiceClient = attribution.ActorLabel, attribution.ActorClient
	entry.SignedInHere = signedInHere
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
	replBus replBus
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

	// bindMu holds a one-holder check and the commit of the write it guards
	// together, so two writes taking the same thing at the same time cannot
	// both pass: a tag bound to one signal (uns.CheckTagBindings), a target
	// named by one setpoint (uns.CheckSignalRelationships), and an external
	// reference held by one record (uns.CheckExternalReferences).
	bindMu sync.Mutex

	// The node's position: the chain of elements from the root down to its own,
	// taught by the parent, persisted, unknown until first taught. The root sets the
	// empty chain at startup. The prefix is rendered from it, never stored.
	posMu         sync.RWMutex
	ancestry      uns.Ancestry
	ancestryKnown bool

	// ledger remembers each command's sender and ack by correlation id.
	ledger *commandLedger

	// answered remembers which queued commands this node answered itself
	// (queued.go), so a repeated refusal is not written twice.
	answered answeredHere

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
	// onGroups is called after a _Group definition was stored here, so the
	// grants of people already signed in follow it (SetOnGroupsChanged).
	onGroups atomic.Pointer[func()]

	// contracts is the loaded schema-bundle table (nil = builtin floor).
	// Static per process: set once at startup, before any door serves.
	contracts *contracts.Table

	// elements maps element ids to local paths for the _SystemElement records this
	// node holds. It lives in the engine because every record lands here, which keeps
	// the index current.
	elements *uns.ElementIndex

	// catalogues maps data tag ids to the connector catalogues on this node that
	// hold them, so the metric door can tell a bound signal's producer
	// (producer.go). Kept current the same way as elements.
	catalogues *uns.CatalogueIndex

	// auditID is injectable for deterministic event tests. Audit writes bypass
	// the public ingest doors; see audit.go.
	auditID func(time.Time) string
	// auditRepeats counts the repeats of a denial instead of writing each.
	auditRepeats auditRepeats

	// unboundLog rate-limits the "_Metric with no _Signal" log line per path
	// (unbound.go).
	unboundLog *unboundMetricLog

	// adminMetricLog rate-limits, per path, the line that says the admin token
	// wrote a bound signal's metric (producer.go).
	adminMetricLog *unboundMetricLog

	// cursorFilters remembers what each cursor's consumer reads, from its last
	// fetch, so the cursor watchdog counts only records it would be woken for.
	cursorFilters *cursorwatch.Filters

	// logs collapses repeated _Log records and caps each service's log rate
	// (logs.go). Static per process.
	logs *loggate.Gate[Attribution]
}

// New builds an engine. ids is the identity registry: a publish is admitted when
// the identity's write scope covers the topic's path. m may be nil; every Metrics
// method is nil-safe.
//
// clk is the node's wall clock and may be nil; New then builds one from cfg. A
// caller that also passes a clock to metrics.New must build it itself and pass
// the same instance to both, or the parent skew metrics never change.
func New(s *store.Store, cfg *config.Config, ids Mounts, deliver LocalDeliver, m *metrics.Metrics, clk *clock.Clock) *Engine {
	if clk == nil {
		clk = clock.New(cfg.Parent == nil, time.Now)
	}
	e := &Engine{store: s, cfg: cfg, deliver: deliver, ids: ids, log: slog.Default().With("node", cfg.ULID), metrics: m, clk: clk, auditID: newAuditID, unboundLog: newUnboundMetricLog(), adminMetricLog: newUnboundMetricLog(),
		ledger: newCommandLedger(), cursorFilters: cursorwatch.NewFilters(),
		logs: newLogGate(loggate.Config{
			Window: cfg.Logs.EffectiveWindow(), MaxPerService: cfg.Logs.EffectiveMaxPerService(),
			MaxTracked: cfg.Logs.EffectiveMaxTracked(),
		})}
	e.replBus.queue = make(chan []busMsg, replBusQueue)
	e.elements = uns.NewElementIndex(e.EntityStore())
	e.catalogues = uns.NewCatalogueIndex(e.EntityStore())
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

// SetOnGroupsChanged registers the callback for a stored _Group definition,
// whatever door it came through: written here, or handed down by the parent. It
// runs after the record is in KV, so a resolution it starts sees the new
// definition. The callback must be cheap and must not ingest.
func (e *Engine) SetOnGroupsChanged(fn func()) { e.onGroups.Store(&fn) }

func (e *Engine) Store() *store.Store { return e.store }

// CursorFilters is what each cursor's consumer reads, as its last fetch said.
func (e *Engine) CursorFilters() *cursorwatch.Filters { return e.cursorFilters }

// Now is this node's wall clock, the one time every expiry, drain and audit
// decision reads. Tests inject it through the engine's clock.Clock.
func (e *Engine) Now() time.Time { return e.clk.Now() }

// ClockStatus is the /health clock block: the wall clock, the kernel's verdict
// on it, and the skew to the parent. What is not known is null, never an
// invented value: the host fields when the kernel's verdict cannot be read, the
// host's estimate where its NTP service keeps none, the parent fields on the
// root and before the first response from the parent.
func (e *Engine) ClockStatus() map[string]any {
	now := e.clk.Now()
	host := map[string]any{"synchronized": nil, "estimated_error_ms": nil, "max_error_ms": nil, "stale_bound_ms": nil}
	if h := e.clk.Host(); h.Reported {
		ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
		host["synchronized"] = h.Synchronized
		// Null on a host whose NTP service keeps no estimate, see hostclock.Status.
		if h.EstimateReported {
			host["estimated_error_ms"] = ms(h.EstimatedError)
		}
		host["max_error_ms"] = ms(h.MaxError)
		host["stale_bound_ms"] = ms(h.StaleBound())
	}
	status := map[string]any{
		"wall_now_ms": now.UnixMilli(), "is_root": e.clk.IsRoot(), "host": host,
		"parent_skew_ms": nil, "parent_sample_age_s": nil,
	}
	if skew, ok := e.clk.ParentSkew(); ok {
		status["parent_skew_ms"] = skew.Milliseconds()
	}
	if age, ok := e.clk.ParentSampleAge(now); ok {
		status["parent_sample_age_s"] = age.Seconds()
	}
	return status
}

// ObserveParentClock records the skew between this node's clock and the
// parent's, from one replication response and the round trip that bounds it
// (clock.ParentSample). It is a measurement for the metrics and /health; no
// decision is corrected by it. A sample whose round trip is too long to say
// anything is dropped. It logs once when the skew passes clock.SkewAlarm and
// once when it is back within it.
func (e *Engine) ObserveParentClock(s clock.ParentSample) {
	skew, transition, used := e.clk.ObserveParent(s)
	if !used {
		if !e.clk.IsRoot() && s.NowMS > 0 {
			e.metrics.ClockParentSampleDiscarded()
			e.log.Debug("parent clock sample discarded: its round trip is too long to bound a skew",
				"round_trip_ms", (s.Received.Sub(s.Sent) - s.Held).Milliseconds(), "max_ms", clock.MaxRoundTrip.Milliseconds())
		}
		return
	}
	switch transition {
	case clock.Exceeded:
		e.log.Warn("clock skew to the parent exceeds the alarm threshold: this node and its parent judge command expiry differently; check NTP on both hosts",
			"parent_skew_ms", skew.Milliseconds(), "threshold_ms", clock.SkewAlarm.Milliseconds())
	case clock.Recovered:
		e.log.Info("clock skew to the parent is back within the alarm threshold",
			"parent_skew_ms", skew.Milliseconds(), "threshold_ms", clock.SkewAlarm.Milliseconds())
	case clock.Unchanged:
	}
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

func (s scope) Binds(elementID string) bool {
	a, ok := s.e.Ancestry()
	return ok && a.Binds(elementID)
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
	// A person's label is a name or nothing: their subject is never repeated
	// as one. Any other actor is called by its id when the service names none.
	if actor.ActorLabel == "" && actor.ActorKind != "human" {
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
		attribution.Door = doorClient
		// A local service attesting a person is judged as that person, resolved against
		// this node's _Group definitions: never with the service's own configure grant,
		// never wider than the person. A service publishing as itself is judged as
		// itself.
		actor := entry
		if attested := e.actorForAttested(attribution, true); attested != nil {
			actor = attested
		}
		implicitLocalConfigure := actor == entry && p.NodeID == e.cfg.ULID &&
			(entry.MayImplicitlyConfigure(p.Contract) || entry.MayImplicitlyDeliverAppNotification(p.Contract, p.Path))
		if !implicitLocalConfigure && !uns.Authorize(e.Scope(), actor, uns.ActCmd, topic) {
			e.refuseDeniedCommand(p, payload, attribution.ActorID)
			return e.rejectDenied(metrics.ReasonCmdDenied, attribution, "execute", &p, "client %s: no cmd grant covers %s", identity, topic)
		}
		if err := e.validateContract(p.Contract, payload); err != nil {
			e.refuseCommandPayload(p, payload, attribution.ActorID, err)
			return e.reject(metrics.ReasonValidation, "%w", err)
		}
		if message, ok := e.unannounced(p); ok {
			return e.answeredUnannounced(p, payload, attribution.ActorID, message), nil
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
		e.noteQueued(p, payload, attribution)
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
	attribution, err := e.admitClientData(identity, p, class, topic, payload, actorFor)
	if err != nil {
		return Result{}, err
	}
	if res, held := e.gateLog(class, p, topic, payload, attribution); held {
		return res, nil
	}
	// The client already publishes the canonical node-local topic; only the
	// attribution is added.
	res, err := e.persistAttributed(class, p, topic, payload, attribution)
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

// admitClientData checks a client's data, state or ack record (not a command,
// not audit): known contract, level 4 is this node, schema, author, write
// scope. It returns the attribution the record is stored with.
func (e *Engine) admitClientData(identity string, p uns.Parsed, class uns.Class, topic string, payload []byte,
	actorFor func(*uns.Entry) Attribution) (Attribution, error) {
	if !uns.IsKnown(class) {
		_, err := e.reject(metrics.ReasonGrammar, "client %s may not publish %s", identity, p.Contract)
		return Attribution{}, err
	}
	// Level 4 is this node's ULID for every publisher. A service's identity decides
	// whether a write is allowed but never appears in the topic.
	if p.NodeID != e.cfg.ULID {
		_, err := e.reject(metrics.ReasonNodeID, "level-4 %q is not this node (%q)", p.NodeID, e.cfg.ULID)
		return Attribution{}, err
	}
	if err := e.validateContract(p.Contract, payload); err != nil {
		_, err = e.reject(metrics.ReasonValidation, "%w", err)
		return Attribution{}, err
	}
	if err := e.validateClientStateAuthor(identity, p, payload); err != nil {
		_, err = e.reject(metrics.ReasonIdentity, "%w", err)
		return Attribution{}, err
	}
	entry, ok := e.ids.Get(identity)
	if !ok || !uns.Authorize(e.Scope(), entry, uns.ActPub, topic) {
		actor := Attribution{ActorID: identity, ActorLabel: identity, ActorKind: "service"}
		if ok {
			actor = actorFor(entry)
		}
		_, err := e.rejectDenied(metrics.ReasonWriteDenied, actor, "publish", &p, "client %s: no write scope covers %s", identity, topic)
		return Attribution{}, err
	}
	// A write zone says where a service may write, not whose values a signal
	// shows: a bound signal's metric comes from its producer only.
	if uns.IsMetric(p.Contract) {
		if err := e.checkMetricProducer(entry, p); err != nil {
			_, err = e.rejectDenied(metrics.ReasonNotProducer, actorFor(entry), "publish", &p, "client %s: %w", identity, err)
			return Attribution{}, err
		}
	}
	return actorFor(entry), nil
}

// IngestHuman ingests a publish from a verified human. Humans only send commands,
// gated by their cmd grants; data, entity and ack contracts are rejected with
// ReasonHumanWrite whatever the grants. Commands target absolute node-local
// paths, as admin commands do.
//
// The record's envelope names the person by their subject (actor_id) and keeps
// what their token called them (actor_label) as a hint; authorization uses
// entry's grants, never the name.
func (e *Engine) IngestHuman(entry *uns.Entry, topic string, payload []byte) (Result, error) {
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
		e.refuseDeniedCommand(p, payload, entry.ULID)
		return e.rejectDenied(metrics.ReasonCmdDenied, attributionForEntry(entry), "execute", &p,
			"human %s: no cmd grant covers %s", entry.ULID, topic)
	}
	if err := e.validateContract(p.Contract, payload); err != nil {
		e.refuseCommandPayload(p, payload, entry.ULID, err)
		return e.reject(metrics.ReasonValidation, "%w", err)
	}
	if message, ok := e.unannounced(p); ok {
		return e.answeredUnannounced(p, payload, entry.ULID, message), nil
	}
	attribution := attributionForEntry(entry)
	attribution.Door = doorHuman
	// Forwarding checks the person again from the groups alone, so the door is
	// recorded only when the groups alone authorize the command. Grants carried
	// in the token itself cannot change during its life and are not checked again.
	if attested := e.actorForAttested(attribution, true); attested == nil || !uns.Authorize(e.Scope(), attested, uns.ActCmd, topic) {
		attribution.Door = ""
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
	e.noteQueued(p, payload, attribution)
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
		if uns.IsCommand(class) {
			e.refuseCommandPayload(p, payload, attribution.ActorID, err)
		}
		return Result{}, err
	}
	if err := e.validateAdminStateAuthor(p, payload); err != nil {
		e.metrics.RejectPublish(metrics.ReasonIdentity)
		return Result{}, err
	}
	if uns.IsMetric(p.Contract) && p.NodeID == e.cfg.ULID {
		// The admin token may still write a bound signal's metric (a correction, a
		// tombstone); the record keeps the admin attribution, and the log says whose
		// value it replaced.
		if tag := e.boundTag(p); tag != "" && e.adminMetricLog.shouldLog(p.Path, e.clk.Now()) {
			e.log.Warn("admin published a _Metric for a signal bound to a producer",
				"path", p.Path, "data_tag", tag, "actor", attribution.ActorID)
		}
	}
	var id string
	if uns.IsCommand(class) {
		if message, ok := e.unannounced(p); ok {
			return e.answeredUnannounced(p, payload, attribution.ActorID, message), nil
		}
		var repeat bool
		if id, repeat, err = e.admitCommand(p, payload, attribution.ActorID); err != nil {
			return Result{}, err
		}
		if repeat {
			return e.repeated(payload), nil
		}
	} else if res, held := e.gateLog(class, p, topic, payload, attribution); held {
		return res, nil
	}
	res, err := e.persistAdminRecord(class, p, topic, payload, attribution)
	if err != nil {
		e.ledger.forget(id)
		return res, err
	}
	if uns.IsCommand(class) {
		e.noteQueued(p, payload, attribution)
	}
	res.Command = e.maybeExec(p, payload, attribution, nil) // the admin door presents a token, not an identity
	return res, nil
}

// persistAdminRecord persists one admin publish. An external reference this
// node authors is held to the rule a command's batch is held to: the record
// is the same whichever door wrote it.
func (e *Engine) persistAdminRecord(class uns.Class, p uns.Parsed, topic string, payload []byte, attribution Attribution) (Result, error) {
	if p.Contract == "_ExternalReference" && p.NodeID == e.cfg.ULID {
		e.bindMu.Lock()
		defer e.bindMu.Unlock()
		if err := e.checkExternalReferences([]uns.StateRecord{{Topic: topic, Payload: payload}}); err != nil {
			e.metrics.RejectPublish(metrics.ReasonValidation)
			return Result{}, err
		}
	}
	return e.persistAttributed(class, p, topic, payload, attribution)
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
				ActorGroups: attribution.ActorGroups, ActorClient: attribution.ActorClient,
				KVPath: parsed.Path, KVNode: parsed.NodeID, Delete: len(input.Payload) == 0,
			},
		})
	}

	storeRecords := make([]store.Record, len(prepared))
	writesSignals, writesReferences := false, false
	for i := range prepared {
		storeRecords[i] = prepared[i].record
		writesSignals = writesSignals || prepared[i].parsed.Contract == "_Signal"
		writesReferences = writesReferences || prepared[i].parsed.Contract == "_ExternalReference"
	}
	if writesSignals || writesReferences {
		e.bindMu.Lock()
		defer e.bindMu.Unlock()
	}
	if writesSignals {
		if err := e.checkSignals(records); err != nil {
			e.metrics.RejectPublish(metrics.ReasonValidation)
			return nil, err
		}
	}
	if writesReferences {
		if err := e.checkExternalReferences(records); err != nil {
			e.metrics.RejectPublish(metrics.ReasonValidation)
			return nil, err
		}
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
		e.observeIndexes(item.parsed.Contract, item.record.Topic, item.record.Payload)
		if e.deliver != nil {
			e.deliver(item.record.Topic, item.record.Payload, retainFor(item.class))
		}
		results[i] = Result{Persisted: true, Stream: stream, Offset: offset, Topic: item.record.Topic}
	}
	return results, nil
}

// checkSignals refuses a batch that would bind a tag another of this node's
// signals holds, or leave a signal with relationships that break a rule. The
// caller holds bindMu until the batch is appended.
func (e *Engine) checkSignals(records []uns.StateRecord) error {
	current, err := e.scanContract("_Signal", func(kv store.KVEntry) bool { return kv.NodeID == e.cfg.ULID })
	if err != nil {
		return fmt.Errorf("signal check: %w", err)
	}
	if err := uns.CheckTagBindings(current, records); err != nil {
		return err
	}
	return uns.CheckSignalRelationships(current, records)
}

// checkExternalReferences refuses a write that would give this node a second
// external reference with an identity one of its records already holds. The
// caller holds bindMu until the write is appended.
func (e *Engine) checkExternalReferences(records []uns.StateRecord) error {
	current, err := e.scanContract("_ExternalReference", func(kv store.KVEntry) bool { return kv.NodeID == e.cfg.ULID })
	if err != nil {
		return fmt.Errorf("external reference check: %w", err)
	}
	return uns.CheckExternalReferences(current, records)
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
		ActorGroups: attribution.ActorGroups, ActorClient: attribution.ActorClient,
		// Deliberately no KVPath/KVNode: this class is never state (IsState is
		// false), so there is nothing to project and nothing to retract.
	}
	first, _, err := e.store.Append(stream, []store.Record{storeRecord})
	if err != nil {
		return Result{}, err
	}
	e.metrics.IngestRecord(stream)
	e.log.Debug("atomic event ingest", "stream", stream, "offset", first, "topic", record.Topic)
	e.observeIndexes(parsed.Contract, record.Topic, record.Payload)
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
	return e.IngestDownlinkAttributed(topic, payload, ts, Attribution{}, nil)
}

// IngestDownlinkAttributed is IngestDownlink with the authorship stamped at the
// command's origin. A deliberate refusal is a *RejectError and anything else is a
// store failure; repl.RunDownlink uses that to decide whether it may ack past the
// record.
//
// at, when set, is the downlink cursor moved past this record in the same write
// as the record itself. A record the cursor already passed was stored before (the
// parent handed it again after a crash or a lost response): it is not stored,
// forwarded or executed a second time, and the Result says Duplicate.
func (e *Engine) IngestDownlinkAttributed(topic string, payload []byte, ts int64, attribution Attribution, at *store.CursorAdvance) (Result, error) {
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
	res, err := e.persistRecord(class, p, topic, payload, ts, attribution, at)
	if err == nil && res.Persisted {
		// The target executes downlinked commands. The person signed in at an
		// ancestor, so their $node grants name that node and resolve to nothing here.
		e.maybeExec(p, payload, attribution, e.actorForAttested(attribution, false))
	}
	return res, err
}

// DownlinkCommand is one command the parent handed down, in local coordinates,
// with the parent's timestamp and the authorship stamped at its origin.
type DownlinkCommand struct {
	Topic       string
	Payload     []byte
	TS          int64
	Attribution Attribution
}

// ErrNotOnePage says a downlink page cannot be stored in one write: it holds a
// record that is not a command. The caller stores it record by record.
var ErrNotOnePage = errors.New("downlink page holds a record that is not a command")

// IngestDownlinkPage stores the commands of one downlink page, and the downlink
// cursor past the page, in a single write. Nothing is delivered or executed
// before all of them are in the stream, so a consumer handed the first command
// of a page finds the rest of the page behind it. Delivery and execution then
// follow in the page's order.
//
// results has one entry per command: Persisted with its offset, or Err for a
// command this node refuses (a *RejectError), which is left out of the write
// like IngestDownlinkAttributed leaves it out. All are Duplicate when the
// cursor already stood at or past at.To: the page was stored before.
//
// err is for the page as a whole and nothing was written: ErrNotOnePage, or a
// store failure. A failure may be one record's (over max_record_bytes), so the
// caller then stores the page record by record to hold the cursor at that one.
func (e *Engine) IngestDownlinkPage(cmds []DownlinkCommand, at store.CursorAdvance) (results []PageResult, err error) {
	results = make([]PageResult, len(cmds))
	type accepted struct {
		i     int
		p     uns.Parsed
		class uns.Class
	}
	var keep []accepted
	var recs []store.Record
	stream := ""
	for i, c := range cmds {
		p, perr := uns.Parse(c.Topic)
		if perr != nil {
			_, results[i].Err = e.reject(metrics.ReasonGrammar, "%w", perr)
			continue
		}
		class := e.ClassOf(p.Contract)
		if !uns.IsCommand(class) || (stream != "" && uns.StreamFor(class) != stream) {
			return nil, ErrNotOnePage
		}
		stream = uns.StreamFor(class)
		if e.ids.DrainingMount(p.Path) {
			_, results[i].Err = e.reject(metrics.ReasonDraining,
				"downlink: %s is draining, no new commands admitted", p.Path)
			continue
		}
		keep = append(keep, accepted{i, p, class})
		a := c.Attribution
		recs = append(recs, store.Record{
			Topic: c.Topic, Payload: c.Payload, TS: c.TS,
			WrittenBy: a.WrittenBy, ActorID: a.ActorID, ActorLabel: a.ActorLabel, ActorKind: a.ActorKind,
			ActorGroups: a.ActorGroups, ActorClient: a.ActorClient, Door: a.Door,
		})
	}
	if len(recs) == 0 {
		return results, nil
	}
	first, _, applied, err := e.store.AppendAdvancing(stream, recs, at)
	if err != nil {
		if errors.Is(err, store.ErrRecordTooLarge) {
			return nil, err
		}
		return nil, &StorageError{Err: err}
	}
	if !applied {
		e.log.Info("downlink page already stored: not stored, forwarded or executed again",
			"cursor", at.Name, "position", at.To-1, "commands", len(recs))
		for _, k := range keep {
			results[k.i].Duplicate = true
		}
		return results, nil
	}
	for n, k := range keep {
		results[k.i].Persisted, results[k.i].Offset = true, first+uint64(n)
	}
	for _, k := range keep {
		c := cmds[k.i]
		e.afterPersist(k.class, k.p, c.Topic, c.Payload, results[k.i].Offset)
		// The target executes downlinked commands, as in IngestDownlinkAttributed.
		e.maybeExec(k.p, c.Payload, c.Attribution, e.actorForAttested(c.Attribution, false))
	}
	return results, nil
}

// PageResult is what IngestDownlinkPage did with one command.
type PageResult struct {
	Persisted bool
	Duplicate bool
	Offset    uint64
	Err       error
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
	recs = e.dropUnreplicable(child, stream, recs)
	prev := e.store.HWMGet(child, stream)
	got, hwm, err := e.store.ApplyReplicated(child, stream, recs)
	if err != nil {
		return 0, hwm, err
	}
	e.logOffsetJumps(child, stream, prev, got)
	// Replicated records count toward colca_ingest_records_total like the other entry
	// paths.
	for _, r := range got {
		if r.SkipFrom == 0 {
			e.metrics.IngestRecord(stream)
			applied++
		}
	}
	var bus []busMsg
	if e.deliver != nil {
		bus = make([]busMsg, 0, len(got))
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
		e.observeIndexes(p.Contract, r.Topic, r.Payload)
		if e.deliver != nil {
			bus = append(bus, busMsg{r.Topic, r.Payload, e.retainOnBus(p)})
		}
	}
	e.deliverReplicated(bus)
	return applied, hwm, nil
}

// dropUnreplicable keeps records a parent never stores out of a replicated
// batch before they reach the store; the rest of the batch still applies.
//
// A retired contract (uns.IsRetired) can still sit in an older child's metrics
// stream. Its record becomes a one-offset skip, so the HWM moves past it and a
// batch ending in one does not hold the lane. It is counted as a rejected
// publish.
func (e *Engine) dropUnreplicable(child, stream string, recs []store.ReplRecord) []store.ReplRecord {
	filtered := recs[:0:0]
	for _, r := range recs {
		p, err := uns.Parse(r.Topic)
		if err == nil && uns.IsRetired(p.Contract) && stream == "metrics" {
			e.metrics.RejectPublish(metrics.ReasonValidation)
			e.log.Debug("skipped a replicated record of a retired contract", "child", child, "topic", r.Topic)
			filtered = append(filtered, store.ReplRecord{ChildOffset: r.ChildOffset, SkipFrom: r.ChildOffset})
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered
}

// logOffsetJumps is the second check for lost records: a child's stream offsets
// are gapless and read contiguously, so an applied ChildOffset above HWM+1 means
// records are gone at the child, even if it never emitted a _StreamGap marker.
// Detection only.
//
// Streams whose uplink is filtered (uns.UplinkCarriesEveryRecord) are exempt.
func (e *Engine) logOffsetJumps(child, stream string, prev uint64, applied []store.ReplRecord) {
	if !uns.UplinkCarriesEveryRecord(stream) {
		return
	}
	last := prev
	for _, r := range applied {
		first := r.ChildOffset
		if r.SkipFrom != 0 {
			first = r.SkipFrom
		}
		if first > last+1 {
			e.log.Error("replication offset jump: this node never received the child offsets between have and got, most likely pruned at the child before replication",
				"child", child, "stream", stream, "have", last, "got", r.ChildOffset)
			e.metrics.GapApplied(child, stream)
		}
		last = r.ChildOffset
	}
}

// retainFor decides how a record appears on the local bus. State classes are
// retained, exactly the set with a KV projection. Commands and acks are events:
// retaining them would redeliver stale commands to every new subscriber.
func retainFor(c uns.Class) bool { return uns.IsState(c) }

// retainOnBus is retainFor for one record: a _Metric another node authored,
// which reached this node through replication, is not retained unless
// bus.retain_child_metrics says so (see config.Bus).
func (e *Engine) retainOnBus(p uns.Parsed) bool {
	if !retainFor(e.ClassOf(p.Contract)) {
		return false
	}
	return !e.childMetric(p)
}

// childMetric reports whether p is a _Metric another node authored and the
// bus does not retain such records.
func (e *Engine) childMetric(p uns.Parsed) bool {
	return p.Contract == "_Metric" && p.NodeID != e.cfg.ULID && !e.cfg.Bus.RetainChildMetrics
}

// RetainOnBus reports whether the record at topic is kept as a retained
// message on the local bus; the node's startup reseed asks it.
func (e *Engine) RetainOnBus(topic string) bool {
	p, err := uns.Parse(topic)
	if err != nil {
		return false
	}
	return e.retainOnBus(p)
}

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
	return e.persistRecord(class, p, topic, payload, ts, attribution, nil)
}

// persistRecord is persistTSAttributed with an optional cursor moved in the same
// batch (store.AppendAdvancing). When that cursor already passed the record,
// nothing is written or delivered and the Result says Duplicate.
func (e *Engine) persistRecord(class uns.Class, p uns.Parsed, topic string, payload []byte, ts int64,
	attribution Attribution, at *store.CursorAdvance) (Result, error) {
	streamName := uns.StreamFor(class)
	rec := store.Record{
		Topic: topic, Payload: payload, TS: ts,
		WrittenBy: attribution.WrittenBy, ActorID: attribution.ActorID,
		ActorLabel: attribution.ActorLabel, ActorKind: attribution.ActorKind,
		ActorGroups: attribution.ActorGroups, ActorClient: attribution.ActorClient, Door: attribution.Door,
	}
	if uns.IsState(class) {
		rec.KVPath, rec.KVNode = p.Path, p.NodeID
		// An empty payload on a KV-projecting class is a tombstone: the record is
		// appended as history, the KV key is deleted in the same batch, and the retained
		// empty delivery below makes mochi clear the retained message.
		rec.Delete = len(payload) == 0
	}
	var first uint64
	var err error
	if at != nil {
		var applied bool
		first, _, applied, err = e.store.AppendAdvancing(streamName, []store.Record{rec}, *at)
		if err == nil && !applied {
			e.log.Info("record already stored: not stored, forwarded or executed again",
				"topic", topic, "cursor", at.Name, "position", at.To-1)
			return Result{Duplicate: true}, nil
		}
	} else {
		first, _, err = e.store.Append(streamName, []store.Record{rec})
	}
	if err != nil {
		if errors.Is(err, store.ErrRecordTooLarge) {
			e.metrics.RecordRejected("too_large")
			return Result{}, err
		}
		return Result{}, &StorageError{Err: err}
	}
	e.afterPersist(class, p, topic, payload, first)
	return Result{Persisted: true, Stream: streamName, Offset: first, Topic: topic}, nil
}

// afterPersist is what follows a record's durable write at offset: the
// counters, the indexes and the local delivery.
func (e *Engine) afterPersist(class uns.Class, p uns.Parsed, topic string, payload []byte, offset uint64) {
	streamName := uns.StreamFor(class)
	e.metrics.IngestRecord(streamName)
	e.log.Debug("ingest", "stream", streamName, "offset", offset, "topic", topic)
	if uns.IsAck(class) {
		// A 202 progress ack is not the answer a repeated command gets.
		if id := correlationID(payload); id != "" && !isProgressAck(payload) {
			e.ledger.acked(id, topic, payload)
		}
	}
	// The element index tracks every persisted record, not just machine publishes:
	// an element authored through IngestAdmin must be visible too. The plugin
	// observer, by contrast, only sees what machines published.
	e.observeIndexes(p.Contract, topic, payload)
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
			e.deliverCommand(class, p, topic, payload, offset)
		} else {
			e.deliver(topic, payload, retainFor(class))
		}
	}
}
