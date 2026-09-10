// Infrastructure authorization: the registry entry shape, the grant grammar
// and the Authorize decision (the infra auth design
// §2.1, §5). This file answers every semantic auth question; the core owns
// doors, sessions and persistence and never re-implements grant logic (§8).

package uns

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Kind gates which door an identity may use: external services connect to the
// MQTT and HTTP doors, nodes to the replication door (§2.1, §6).
type Kind string

const (
	// KindExternal is any service outside the node's deployment — a machine,
	// a gateway, a customer's own integration — that publishes or subscribes
	// through the published doors with a registry-pinned key. Its placement
	// gives it a read zone only; every write it may perform is an explicit
	// grant (local-service-trust design §3.1).
	KindExternal Kind = "external"
	// KindNode is another node of the tree; nodes use the replication door.
	KindNode Kind = "node"
	// KindLocal is a service inside the node's own deployment. It is the one
	// kind with no pubkey: it presents itself at a door that is unreachable
	// from outside the deployment, and reaching that door is the proof
	// (local-service-trust design §3). It is also the one kind that may be
	// unplaced — see Entry.Element.
	KindLocal Kind = "local"
	// KindHuman is an EPHEMERAL kind: a verified OIDC token becomes a
	// KindHuman entry via TokenEntry (human-authz design §2.3). It is valid
	// for Authorize but rejected by enrollment validation — humans are
	// tokens, never registry entries.
	KindHuman Kind = "human"
)

// Entry is one registry entry — the identity triple plus its declared grants
// (§2.1). The JSON form is both the enrollment wire shape and the persisted
// r/{ulid} value.
type Entry struct {
	ULID   string `json:"ulid"`
	Pubkey string `json:"pubkey"` // hex ed25519 public key, pinned on connect. KindLocal holds none — the door is its proof.
	Kind   Kind   `json:"kind"`
	// Name identifies a KindLocal entry — it holds no pubkey, so this is how
	// the local door finds its entry (local-service-trust design §3).
	Name string `json:"name,omitempty"`
	// Element is the system element this identity binds to — its placement,
	// named by identity rather than by path (id-grants design §4). "" means
	// bound to THE NODE ITSELF: a complete position, not a missing value.
	// Only KindLocal may be unplaced — the local door already proved it
	// belongs to this deployment. An external service or a node must be placed
	// explicitly at enrollment: nothing proved that about an identity arriving
	// from outside the deployment. The path it mounts at is resolved through
	// the Namespace every time one is needed, so renaming or reparenting the
	// element moves the mount with no re-enrollment; a path stored here would
	// freeze the position as it was at enrollment.
	Element string   `json:"element,omitempty"`
	Grants  []string `json:"grants,omitempty"`
	// Groups are the group ids a KindHuman entry's grants were resolved from
	// (TokenEntryWithGroups). They travel with a command the human issues —
	// persisted as the record's attribution — so a node that executes it
	// after replication can reconstitute the same human against its own
	// _Group definitions (node-side command authorization design §3B). Never
	// set on a registry entry.
	Groups []string `json:"groups,omitempty"`
	// Username is a KindHuman entry's preferred_username, set by the door
	// that verified the token. It decides the KIND of identity behind the
	// sub — a Keycloak service account is named service-account-<client> —
	// which AnnotationSource needs. Never persisted, never on a registry
	// entry.
	Username string `json:"-"`
	// Status is the entry's lifecycle state (move-drain design §3.2):
	// StatusActive or "" (absent ⇒ active, so entries persisted before this
	// field existed need no migration) or StatusDraining. Only a kind=node
	// entry may be StatusDraining — external services are out of scope for
	// move-drain (design §3.2 [delta]: their delivery rides broker QoS-1 session
	// state, not a cursor, so there is nothing for a parent to drain
	// against).
	Status string `json:"status,omitempty"`
}

// Entry lifecycle states — the allowed values of Entry.Status (move-drain
// design §3.2).
const (
	StatusActive   = "active"
	StatusDraining = "draining"
)

// IsDraining reports whether this entry is mid-move: still enrolled and still
// serving reads, but admitting no new commands and being drained toward a new
// parent (move-drain design §3.2). The core asks this instead of comparing
// Status so the encoding of "draining" — including the absent-means-active
// rule — stays one fact in one place.
//
// Nil-safe, like MayUseDoor: "no identity" reads as "not draining" for the
// same reason "no identity" reads as "no door" — a caller that has already
// reduced a lookup to `entry, ok := m.byID[ulid]; if !ok { … }` and then
// still asks the entry a question should get the truthful null answer
// instead of a panic that only fires when the registry's own invariant
// (never pairing ok=true with a nil entry) is violated.
func (e *Entry) IsDraining() bool { return e != nil && e.Status == StatusDraining }

// MarkDraining moves the entry into the draining state. The transition belongs
// here rather than at the caller for the same reason IsDraining does: the
// registry owns WHEN an entry drains, this package owns what draining IS.
func (e *Entry) MarkDraining() { e.Status = StatusDraining }

// CanDrain reports whether this entry is eligible to drain at all. Only nodes
// are: a machine's delivery rides broker QoS-1 session state rather than a
// cursor, so there is nothing for a parent to drain against (move-drain design
// §3.2 [delta]).
//
// Nil-safe for the same reason IsDraining is: no identity is not a node, so
// it cannot drain.
func (e *Entry) CanDrain() bool { return e != nil && e.Kind == KindNode }

// LocalCursorPrefix namespaces a KindLocal entry's cursors by the NAME it
// presented rather than its minted ULID (local-service-trust design §4): a
// local caller never learns the ULID Register mints for it on first sight —
// only the name it presented — so cursors namespaced by ULID (every other
// kind's rule) would be permanently unreachable from the local door. The name
// is unique across the registry (enrollment's byName uniqueness check), so it
// still keeps two local services from colliding on /ack, which is the one
// thing this namespacing has to guarantee.
const LocalCursorPrefix = "c/"

// CursorPrefix names the prefix this entry's own cursors must carry — the
// identifier boundary /fetch and /ack use to refuse one identity moving
// another's cursor. Every kind but KindLocal owns cursors under its own ULID,
// which the caller already knows (a machine's pinned key, a human's token
// subject IS its ULID via TokenEntry). KindLocal is the one identity that
// does not: it knows only the name it presented, so it owns cursors under
// LocalCursorPrefix+name instead.
//
// Deliberately NOT nil-safe, unlike IsDraining/CanDrain/IsAdmin/MayUseDoor.
// Those are booleans, where "no identity" has a truthful null answer (not
// draining, cannot drain, not admin, no door). CursorPrefix has none: the
// only string a nil entry could return is "", and strings.HasPrefix(cursor,
// "") is true for every cursor — a caller that forwarded a nil entry's
// prefix straight into HasPrefix would treat a missing identity as owning
// every cursor in the store, the widest possible grant instead of the
// narrowest. Rather than push that fail-closed nil check out to every
// caller of THIS method, it lives once in httpapi's ownsCursor (the sole
// caller that can receive a nil entry), which returns "owns nothing" for a
// nil entry before ever reaching here. So CursorPrefix itself still panics
// on a nil receiver — Go gives that for free — but nothing production reaches
// it with one: every caller nil-checks first (`c.entry != nil && ...`), and
// ownsCursor's own nil guard means even a future caller that forgets the
// check gets the safe answer instead of a crash.
func (e *Entry) CursorPrefix() string {
	if e.Kind == KindLocal {
		return LocalCursorPrefix + e.Name + "/"
	}
	return e.ULID + "/"
}

// CatalogueName is the final segment of this entry's `_DataTags` catalogue
// topic — `colca/v1/_DataTags/{node}/{mount}/{CatalogueName}` — the one place
// a publisher's identity appears in a topic, and only as a human-readable
// tail on a record the publisher owns (local-service-trust design §2). A
// local entry presented a name and is known by it; every keyed entry is
// known by its ULID and has no name to give, so the ULID is the segment.
// Answered here rather than by reading Name at the call sites so that
// "what does an external service call its catalogue" is one rule, matched
// by the SDK's Service.external, and not a field an operator has to fill
// in at enrollment to make autobind find the record.
func (e *Entry) CatalogueName() string {
	if e.Name != "" {
		return e.Name
	}
	return e.ULID
}

// Door is one of the ways an identity can present itself to a node. Which kinds
// may use which door is a domain rule (auth §2.1, §6), so it is answered here
// rather than re-derived from Kind at each listener.
type Door int

// Doors an identity can arrive through.
const (
	DoorMQTT Door = iota // the machine-facing broker door
	DoorHTTP             // the machine-facing HTTP door
	DoorRepl             // the node-to-node replication door
	// DoorLocal is unpublished and plaintext, reachable only from inside the
	// node's own deployment network — reaching it is the credential
	// (local-service-trust design §3).
	DoorLocal
)

// MayUseDoor reports whether this identity is allowed to present itself at the
// given door. Machines connect to the MQTT and HTTP doors, nodes to the
// replication door, local services to the local door; humans arrive as tokens
// and are authorized per publish rather than per door, so they hold no door of
// their own.
//
// Nil-safe, like every other boolean predicate on *Entry (MayPublishAudit,
// ActorKind, MayImplicitlyConfigure, IsDraining, CanDrain, IsAdmin) — the one
// exception is CursorPrefix, which returns a string and cannot answer "no
// identity" truthfully (see its doc comment). Every caller reaches this
// through a registry lookup written as
// `entry, ok := ids.Get(id); if !ok || !entry.MayUseDoor(…)`, and Go evaluates
// the right half whenever ok is true. Production's Mounts (registry.Manager)
// never pairs a nil entry with ok — and the interface now says so — but a
// predicate that answers "no door" for "no identity" is the truthful answer
// anyway, and it is cheaper than repeating a nil check at five call sites.
func (e *Entry) MayUseDoor(d Door) bool {
	if e == nil {
		return false
	}
	switch d {
	case DoorMQTT, DoorHTTP:
		return e.Kind == KindExternal
	case DoorRepl:
		return e.Kind == KindNode
	case DoorLocal:
		return e.Kind == KindLocal
	}
	return false
}

// MayPublishContract is the door predicate beside MayUseDoor: may THIS kind of
// identity publish THIS contract at all, before any grant is consulted.
// Humans command through _CmdEdit only (node-side command authorization
// design §3F): a _CmdEdit is planned and every planned write authorized
// against the person, while _CmdConfigure executes what it is handed with no
// principal — its callers are the admin door and placed local services, whose
// authority is the token or their placement. A person holding a configure
// grant therefore still may not publish _CmdConfigure; the editor is the
// one door a person's configuration goes through. Every other kind keeps its
// contracts; what they may write is decided by their placement and grants.
func (e *Entry) MayPublishContract(contract string) bool {
	if e == nil {
		return false
	}
	if e.Kind == KindHuman {
		return contract != "_CmdConfigure"
	}
	return true
}

// Registry is the in-memory map the doors consult (ulid → entry). Plain data:
// lifecycle (loading, swapping, locking) is owned by the core's registry
// manager, never here.
type Registry map[string]*Entry

// Validate checks the entry's own shape (§4). Uniqueness across the registry
// is the registry manager's job — an entry cannot see its siblings.
func (e *Entry) Validate() error {
	if e.ULID == "" {
		return fmt.Errorf("entry: ulid is required")
	}
	switch e.Kind {
	case KindExternal, KindNode:
		if len(e.Pubkey) != 64 {
			return fmt.Errorf("entry %s: pubkey must be 64 hex chars (ed25519), got %d", e.ULID, len(e.Pubkey))
		}
		if _, err := hex.DecodeString(e.Pubkey); err != nil {
			return fmt.Errorf("entry %s: pubkey is not hex: %w", e.ULID, err)
		}
	case KindLocal:
		if e.Name == "" {
			return fmt.Errorf("entry %s: a local service needs a name — it is how the local door finds its entry", e.ULID)
		}
		if strings.ContainsAny(e.Name, "/+#") {
			return fmt.Errorf("entry %s: local service name %q must be one MQTT topic segment", e.ULID, e.Name)
		}
		if e.Pubkey != "" {
			return fmt.Errorf("entry %s: a local service holds no key; the door is its proof", e.ULID)
		}
	case KindHuman:
		return fmt.Errorf("entry %s: humans are tokens, not registry entries — KindHuman cannot be enrolled", e.ULID)
	default:
		return fmt.Errorf("entry %s: kind must be %q, %q or %q, got %q", e.ULID, KindExternal, KindNode, KindLocal, e.Kind)
	}
	// An element is optional, and absent means bound to the NODE — a complete
	// answer, not a missing value. Required for the kinds whose belonging to
	// this deployment nothing has proved: the local door already proved that
	// about a KindLocal entry, so it alone may go unplaced.
	if e.Element == "" && e.Kind != KindLocal {
		return fmt.Errorf("entry %s: a %s must be placed at a system element", e.ULID, e.Kind)
	}
	if e.Element != "" {
		if err := ValidElementID(e.Element); err != nil {
			return fmt.Errorf("entry %s: %w", e.ULID, err)
		}
	}
	switch e.Status {
	case "", StatusActive, StatusDraining:
	default:
		return fmt.Errorf("entry %s: status must be %q or %q, got %q", e.ULID, StatusActive, StatusDraining, e.Status)
	}
	if e.Status == StatusDraining && e.Kind != KindNode {
		return fmt.Errorf("entry %s: only kind=%q entries may drain — external services are out of scope (move-drain design §3.2)", e.ULID, KindNode)
	}
	for _, g := range e.Grants {
		pg, err := ParseGrant(g)
		if err != nil {
			return fmt.Errorf("entry %s: %w", e.ULID, err)
		}
		if pg.Verb == "admin" {
			// Registry identities may not hold admin: external provisioning is
			// the (deferred) _CmdAdmin flow, human admin rides in tokens.
			return fmt.Errorf("entry %s: %q — external services and nodes may not hold admin grants", e.ULID, g)
		}
	}
	return nil
}

// ValidElementID rejects anything that is not an identity. An element id names
// a thing, never a place: a value carrying "/" is somebody writing a path here,
// which is exactly the mistake this design removes, and wildcards would make an
// identity match more than one element.
//
// ":" is out too, because a grant is "verb:element:classes" — an id carrying a
// colon would split into a different grant than the one that was authored, and
// silently.
//
// Exported because the rule has to hold at every door that AUTHORS an element
// id, not only at the ones that read one back: an element written with id "#"
// is registered by grantsync as an authz resource named "#", and granting a
// group anything on it renders as "read:#" — the whole tree, not the element.
// Everything that mints or carries an element id checks here rather than
// restating the character rule.
func ValidElementID(id string) error {
	if strings.ContainsAny(id, "/+#:") {
		return fmt.Errorf("element %q: an element is named by identity, not by path", id)
	}
	return nil
}

// Namespace resolves an element id to this node's local path for it — the
// projection of the `_SystemElement` records the node holds (id-grants design
// §4). *ElementIndex implements it. Everything that needs a place asks here at
// the moment it needs one instead of remembering a path.
type Namespace interface {
	PathOf(elementID string) (string, bool)
}

// Scope is everything a node knows about where elements are: the ones it holds
// itself, plus whether an element is the node itself or one of its ancestors.
//
// A grant may name either end, so the decision function needs both halves. The
// node cannot answer the second one from its own records — an ancestor's
// `_SystemElement` records live at that ancestor — which is why its position is
// taught to it on the downlink.
type Scope interface {
	Namespace
	// Reaches reports whether the element is this node or above it. A grant on
	// such an element covers everything here.
	Reaches(elementID string) bool
}

// Grant is one parsed grant. Verb is "read", "write", "cmd" or "admin". A
// write grant names a zone and nothing else — hazard classes belong to cmd,
// which acts on equipment; writing state carries no such ladder (local-service-
// trust design §5). Element is the system element the grant names, without a
// trailing "/#" ("#" alone means everything); it covers that element and
// everything below it. Classes is non-empty exactly for cmd grants.
//
// A grant names an element, never a path (id-grants design §4): the same string
// then means the same subtree at every node, survives renames and reparents
// untouched, and needs no frame translation on the way down the tree.
type Grant struct {
	Verb    string
	Element string
	Classes []string
}

// Hazard classes. param/operate/maintain/admin form a ladder of how dangerous
// a command is to the equipment; "configure" is deliberately NOT on that
// ladder — it covers editing the node's data model, which touches nothing
// physical. Keeping it separate is what lets an editor bind signals without
// thereby being able to send maintenance commands to a PLC (data-model binding
// design §3.3).
var cmdClasses = map[string]bool{
	"param": true, "operate": true, "maintain": true, "configure": true, "admin": true,
}

// CmdClasses lists the hazard classes a cmd grant may name, sorted. Callers
// that need to enumerate them — a provisioning surface offering the choices, a
// service compiling grants — ask here rather than writing the list down again.
func CmdClasses() []string {
	out := make([]string, 0, len(cmdClasses))
	for class := range cmdClasses {
		out = append(out, class)
	}
	sort.Strings(out)
	return out
}

// FormatGrant renders a Grant back into the §5.1 grammar.
//
// It exists so that CONSTRUCTING a grant and PARSING one live in the same
// package: anything that builds grant strings elsewhere would be a second
// statement of the grammar, and the two would drift the first time it gains a
// verb. Round-trip: ParseGrant(FormatGrant(g)) == g for every valid g, which
// authz_test.go asserts.
//
// An empty Element means the whole namespace ("#"), the same convention
// ParseGrant reads.
func FormatGrant(g Grant) (string, error) {
	// ParseGrant yields "#" for the whole namespace; an empty Element means the
	// same thing, so both spellings are accepted and render identically.
	element := g.Element
	if element == "#" {
		element = ""
	}
	zone := "#"
	if element != "" {
		zone = element + "/#"
	}
	switch g.Verb {
	case "read":
		return "read:" + zone, nil
	case "write":
		return "write:" + zone, nil
	case "admin":
		if element != "" {
			return "", fmt.Errorf("grant: zone-scoped admin is reserved and not implemented")
		}
		return "admin:#", nil
	case "cmd":
		if len(g.Classes) == 0 {
			return "", fmt.Errorf("grant: a cmd grant needs at least one class")
		}
		classes := append([]string(nil), g.Classes...)
		sort.Strings(classes)
		for _, c := range classes {
			if !cmdClasses[c] {
				return "", fmt.Errorf("grant: unknown cmd class %q", c)
			}
		}
		return "cmd:" + zone + ":" + strings.Join(classes, ","), nil
	default:
		return "", fmt.Errorf("grant: verb must be read, write, cmd or admin, got %q", g.Verb)
	}
}

// ParseGrant parses the §5.1 grammar: "read:<element>/#" or
// "cmd:<element>/#:class,...". The zone is an element id, or "#" for
// everything.
func ParseGrant(s string) (Grant, error) {
	parts := strings.SplitN(s, ":", 3)
	switch parts[0] {
	case "read":
		if len(parts) != 2 {
			return Grant{}, fmt.Errorf("grant %q: read grant is read:<zone>", s)
		}
		z, err := parseZone(s, parts[1])
		if err != nil {
			return Grant{}, err
		}
		return Grant{Verb: "read", Element: z}, nil
	case "write":
		// A write grant names a zone and nothing else. Hazard classes belong to
		// commands, which act on equipment; writing state carries no such ladder
		// (design §5).
		if len(parts) != 2 {
			return Grant{}, fmt.Errorf("grant %q: write grant is write:<zone>", s)
		}
		z, err := parseZone(s, parts[1])
		if err != nil {
			return Grant{}, err
		}
		return Grant{Verb: "write", Element: z}, nil
	case "admin":
		if len(parts) != 2 {
			return Grant{}, fmt.Errorf("grant %q: admin grant is admin:#", s)
		}
		z, err := parseZone(s, parts[1])
		if err != nil {
			return Grant{}, err
		}
		if z != "#" {
			// Reserved grammar (human-authz design §3): the zone-scoped form
			// parses structurally but is not implemented — rejecting it here
			// keeps a later introduction additive instead of breaking.
			return Grant{}, fmt.Errorf("grant %q: zone-scoped admin is reserved and not implemented — use admin:#", s)
		}
		return Grant{Verb: "admin", Element: "#"}, nil
	case "cmd":
		if len(parts) != 3 {
			return Grant{}, fmt.Errorf("grant %q: cmd grant is cmd:<zone>:<class,...>", s)
		}
		z, err := parseZone(s, parts[1])
		if err != nil {
			return Grant{}, err
		}
		classes := strings.Split(parts[2], ",")
		if parts[2] == "" {
			return Grant{}, fmt.Errorf("grant %q: cmd grant needs at least one class", s)
		}
		for _, c := range classes {
			if !cmdClasses[c] {
				return Grant{}, fmt.Errorf("grant %q: unknown cmd class %q (param|operate|maintain|configure|admin)", s, c)
			}
		}
		return Grant{Verb: "cmd", Element: z, Classes: classes}, nil
	default:
		return Grant{}, fmt.Errorf("grant %q: verb must be read, write, cmd or admin", s)
	}
}

// TokenEntry builds the EPHEMERAL entry a verified OIDC token maps to
// (human-authz design §2.3): identity = the token's sub, no element (humans own
// no zone — the World-2 rule falls out structurally), grants = the colca_grants
// claim. Validated here, never by Entry.Validate (that is the enrollment gate
// and demands a pubkey), never persisted.
//
// Grants arrive exactly as authored and are stored exactly as authored. There
// is no frame to translate into any more: a grant names a system element, and
// an element id means the same thing at every node in the tree (id-grants
// design §4). What used to be prefix arithmetic at verification time is now a
// lookup at decision time — see zoneOf.
func TokenEntry(sub string, grants []string) (*Entry, error) {
	if sub == "" {
		return nil, fmt.Errorf("token entry: empty sub")
	}
	for _, g := range grants {
		if _, err := ParseGrant(g); err != nil {
			return nil, fmt.Errorf("token entry %s: %w", sub, err)
		}
	}
	return &Entry{ULID: sub, Kind: KindHuman, Grants: grants}, nil
}

// TokenEntryWithGroups is TokenEntry for a token that names GROUPS rather than
// carrying grant strings (definition-stream design §8): the entry's grants are
// the union of what those groups hold at this node, plus any grants the token
// carries directly.
//
// Membership therefore lives in the identity provider and grants live in the
// tree, which is what makes adding a person to a group change their token and
// nobody's node state.
//
// A group that does not resolve contributes nothing and comes back in problems
// for the caller to log. That is the fail-closed direction: the human keeps
// whatever else they hold and loses exactly the group that could not be found.
func TokenEntryWithGroups(sub string, grants, groupIDs []string, idx *GroupIndex) (*Entry, []error, error) {
	e, err := TokenEntry(sub, grants)
	if err != nil {
		return nil, nil, err
	}
	e.Groups = append([]string(nil), groupIDs...)
	if idx == nil || len(groupIDs) == 0 {
		return e, nil, nil
	}
	fromGroups, problems := idx.GrantsFor(groupIDs)
	for _, g := range fromGroups {
		if _, perr := ParseGrant(g); perr != nil {
			// A malformed grant inside a definition is an authoring error that
			// escaped validation upstream. Drop it, say so, keep the rest.
			problems = append(problems, fmt.Errorf("group grant %q: %w", g, perr))
			continue
		}
		e.Grants = append(e.Grants, g)
	}
	return e, problems, nil
}

// IsAdmin reports whether the entry carries the admin:# grant — it unlocks
// the ADMIN ROUTES only and never widens read or cmd (§3).
//
// Nil-safe, same rule as IsDraining and CanDrain: no identity holds no grant,
// so it is not admin.
func (e *Entry) IsAdmin() bool {
	if e == nil {
		return false
	}
	for _, g := range e.Grants {
		if pg, err := ParseGrant(g); err == nil && pg.Verb == "admin" {
			return true
		}
	}
	return false
}

// MayPublishAudit is the local-trust producer boundary for `_AuditEvent`.
// External machine, human and node identities never gain it through grants;
// the authenticated local registry kind is the authority.
func (e *Entry) MayPublishAudit() bool { return e != nil && e.Kind == KindLocal }

// ActorKind translates authenticated registry/token kinds into the stable
// audit-envelope vocabulary. Machine processes are service actors; the
// distinction between keyed and local services remains in WrittenBy and the
// registry rather than creating a second actor vocabulary.
func (e *Entry) ActorKind() string {
	if e == nil {
		return ""
	}
	switch e.Kind {
	case KindHuman:
		return "human"
	case KindNode:
		return "node"
	case KindExternal, KindLocal:
		return "service"
	default:
		return ""
	}
}

// MayImplicitlyConfigure reports the one command authority implied by local
// placement: an unplaced in-network service is node-scoped and may edit that
// node's model. A placed service is subtree-scoped and needs an explicit cmd
// grant. `_CmdAdmin` and every other command are never implicit.
func (e *Entry) MayImplicitlyConfigure(contract string) bool {
	// Only _CmdConfigure — not the whole configure class. A _CmdEdit
	// carries a PERSON's intent and is authorized against that person at the
	// executor (node-side command authorization design §3A); if an unplaced
	// local service such as the api could issue one under its own identity,
	// one forgotten header would launder any Edit write past every
	// grant. The api forwards the person's token instead (§3B).
	return e != nil && e.Kind == KindLocal && e.Element == "" && contract == "_CmdConfigure"
}

// AuthorizeCmdAt is the cmd decision for one POSITION: does a cmd grant of
// the class cover the path, resolved through the scope. The door applies it
// to a command's topic path; the Edit executor applies it to every
// position of the plan it composed (design §3C). One comparison, two
// callers, so the fine check cannot drift from the coarse one.
func AuthorizeCmdAt(sc Scope, e *Entry, class, path string) bool {
	if e == nil {
		return false
	}
	for _, g := range e.Grants {
		pg, err := ParseGrant(g)
		if err != nil || pg.Verb != "cmd" {
			continue
		}
		zone, ok := zoneOf(sc, pg.Element)
		if !ok || !coverPath(zone, path) {
			continue
		}
		for _, c := range pg.Classes {
			if c == class {
				return true
			}
		}
	}
	return false
}

// AuthorizedAtExecutor reports whether a command contract is authorized
// against the plan its executor composes rather than against its topic's
// path: true for _CmdEdit, whose path is the owning node's route, not a
// position. The door still requires the command class to be held.
func AuthorizedAtExecutor(contract string) bool { return contract == "_CmdEdit" }

// HoldsCmdClass reports whether any cmd grant the entry carries names the
// class, whatever element it names — the door's question for a command that
// is authorized at its executor.
func (e *Entry) HoldsCmdClass(class string) bool {
	if e == nil {
		return false
	}
	for _, g := range e.Grants {
		pg, err := ParseGrant(g)
		if err != nil || pg.Verb != "cmd" {
			continue
		}
		for _, c := range pg.Classes {
			if c == class {
				return true
			}
		}
	}
	return false
}

// IsHuman reports whether this entry is a verified person — a token at a
// human door, or a human reconstituted from attested groups on a replicated
// command. Nil-safe like the other predicates: no identity is not a person.
func (e *Entry) IsHuman() bool { return e != nil && e.Kind == KindHuman }

// parseZone accepts "#" (everything) or one element id, with or without the
// trailing "/#" that reads as "and below" — an element grant always covers the
// element's whole subtree, so both forms mean the same thing.
//
// A path-shaped zone is refused, and the message says why: a path means
// different things at different nodes and stops meaning anything at all when
// somebody renames a position, which is the entire reason grants moved to
// identities.
func parseZone(grant, z string) (string, error) {
	if z == "#" {
		return "#", nil
	}
	z = strings.TrimSuffix(z, "/#")
	if z == "" {
		return "", fmt.Errorf("grant %q: empty zone", grant)
	}
	if err := ValidElementID(z); err != nil {
		return "", fmt.Errorf("grant %q: a grant names one system element, not a path (%w)", grant, err)
	}
	return z, nil
}

// CmdClass maps a command contract to its hazard class (§5.1). Unknown _Cmd*
// contracts demand the highest class — conservative by construction.
func CmdClass(contract string) string {
	switch contract {
	case "_CmdParam":
		return "param"
	case "_CmdOperate":
		return "operate"
	case "_CmdMaintain":
		return "maintain"
	case "_CmdConfigure", "_CmdEdit":
		return "configure"
	default:
		return "admin"
	}
}

// Action selects which §5.3 rule Authorize applies.
type Action int

// Actions Authorize decides about.
const (
	ActSub        Action = iota // MQTT subscription filter (may contain wildcards)
	ActReadRecord               // one concrete stored record / KV entry
	ActCmd                      // publishing a _Cmd* contract
	// ActPub is publishing owned state. Who may write it follows from the
	// identity's write scope, not from the topic.
	ActPub
)

// coverPath reports whether zone covers path: exact zone or below it. "#"
// covers everything. "werk10" is NOT below "werk1" — the boundary is the
// path separator.
func coverPath(zone, path string) bool {
	if zone == "#" {
		return true
	}
	return path == zone || strings.HasPrefix(path, zone+"/")
}

// zoneOf resolves one element id into the local coverage it grants:
//
//   - "#" — everything, either because the grant says so or because the element
//     IS this node or an ancestor of it, in which case the whole node is inside
//     the granted subtree;
//   - a local path — the element sits here, and the grant reaches it and below;
//   - nothing at all — this node has never heard of the element, so the grant is
//     inert here. That is also the fail-closed answer for a node that has not
//     yet learned its position: it reaches nothing, holds nothing above itself,
//     and every scoped grant evaporates while "#" grants keep working.
//
// This is the whole of what changed when grants moved to identities. Everything
// after it — coverPath, the three actions — compares local paths exactly as
// before.
func zoneOf(sc Scope, elementID string) (string, bool) {
	if elementID == "#" {
		return "#", true
	}
	if sc == nil || elementID == "" {
		return "", false
	}
	if sc.Reaches(elementID) {
		return "#", true
	}
	return sc.PathOf(elementID)
}

// readZones is the entry's effective read scope: the default own zone (§5.2)
// plus every explicit read grant, each resolved through the node's scope at
// this moment — so a renamed element is read under its new path immediately
// and a reparented one moves with its subtree. An unplaced LOCAL service is
// bound to the node itself and therefore reads the whole node; an unplaced
// external identity still resolves no default zone.
func readZones(sc Scope, e *Entry) []string {
	var zones []string
	if e.Kind == KindLocal && e.Element == "" {
		zones = append(zones, "#")
	} else if zone, ok := zoneOf(sc, e.Element); ok {
		zones = append(zones, zone)
	}
	for _, g := range e.Grants {
		pg, err := ParseGrant(g)
		if err != nil || pg.Verb != "read" {
			continue
		}
		if zone, ok := zoneOf(sc, pg.Element); ok {
			zones = append(zones, zone)
		}
	}
	return zones
}

// writeZones is the entry's effective write scope, the twin of readZones. A
// LOCAL service's binding carries an implicit write over its own subtree: the
// door already proved it belongs to this deployment. A machine's binding
// carries no such thing — outside the deployment, position says where it
// sits, never what it may do. That single clause is the whole local/external
// difference. Every explicit write: grant, on either kind, is resolved
// through the node's scope at this moment, exactly as readZones does.
func writeZones(sc Scope, e *Entry) []string {
	var zones []string
	if e.Kind == KindLocal {
		if e.Element == "" {
			// Unplaced: no element ever resolves to the empty local path (a node
			// authors no element for itself), so this is checked on the entry
			// directly rather than routed through zoneOf. Bound to the node
			// itself, so the zone is everything here — not a special default, it
			// is what "attached to this node and nowhere narrower" means.
			zones = append(zones, "#")
		} else if zone, ok := zoneOf(sc, e.Element); ok {
			zones = append(zones, zone)
		}
	}
	for _, g := range e.Grants {
		pg, err := ParseGrant(g)
		if err != nil || pg.Verb != "write" {
			continue
		}
		if zone, ok := zoneOf(sc, pg.Element); ok {
			zones = append(zones, zone)
		}
	}
	return zones
}

// Authorize is the one decision function (§5.3): pure prefix comparison,
// in-memory, no I/O. Invalid grants never reach here — Entry.Validate gates
// enrollment — so ParseGrant errors inside are treated as absent grants.
func Authorize(sc Scope, e *Entry, a Action, topic string) bool {
	switch a {
	case ActReadRecord:
		p, err := Parse(topic)
		if err != nil {
			return false // only uns records exist in the store: fail closed
		}
		for _, z := range readZones(sc, e) {
			if coverPath(z, p.Path) {
				return true
			}
		}
		return false

	case ActSub:
		if isReservedFilter(topic) {
			return false
		}
		if isTimeSyncFilter(topic) {
			// Time-sync design §2.2/§4: every authenticated machine session
			// may subscribe colca/v1/_TimeSync/+, independent of its zone
			// grants — the beacon is node-scoped, not path-scoped, so the
			// normal readZones prefix comparison (which would deny a
			// zone-scoped machine with no read:# grant) does not apply here.
			return true
		}
		fixed, isUns := fixedPathPrefix(topic)
		if !isUns {
			return true // outside colca/# colca is a plain broker (§5.3)
		}
		for _, z := range readZones(sc, e) {
			if z == "#" {
				return true
			}
			if fixed != "" && coverPath(z, fixed) {
				return true
			}
		}
		return false

	case ActCmd:
		p, err := Parse(topic)
		if err != nil || ClassOf(p.Contract) != ClassCmd {
			return false
		}
		class := CmdClass(p.Contract)
		if AuthorizedAtExecutor(p.Contract) {
			// The topic's path routes the command to the node that owns
			// the entities (`{owner-route}/apply`); it names no element,
			// so a prefix comparison against it would refuse every person
			// whose grant is narrower than the owning node. The door asks
			// only whether the person holds a class the editor honours
			// at all — configure, or operate for the annotation intent; the
			// executor authorizes the plan it composes, position by
			// position, with AuthorizeCmdAt (node-side command
			// authorization design §3C).
			return e.HoldsCmdClass(class) || e.HoldsCmdClass("operate")
		}
		return AuthorizeCmdAt(sc, e, class, p.Path)

	case ActPub:
		p, err := Parse(topic)
		if err != nil {
			return false
		}
		for _, z := range writeZones(sc, e) {
			if coverPath(z, p.Path) {
				return true
			}
		}
		return false
	}
	return false
}

// isReservedFilter reports whether a subscribe filter reaches into MQTT's
// reserved "$" space. Every such filter is refused at the door, whatever the
// subscriber's grants.
//
// "$share/<group>/<filter>" is not a topic — it is an ALIAS for <filter>, and
// the broker hands this decision the raw string before it strips the prefix.
// Classifying that string reads "$share" as the first segment, which is not
// "colca", which used to mean "plain-broker traffic, outside grant checking":
// "$share/g/colca/#" therefore granted every record on the node to a subscriber
// scoped to one element. Teaching the classifier to strip the alias would fix
// that one spelling and leave the shape — a second way to spell a filter,
// judged by a second code path — which is what let it happen.
//
// So the rule is one rule: colca publishes nothing under "$", and nothing may
// subscribe there. That also refuses "$SYS/#" (broker internals, never
// authorized by any grant) and shared subscriptions themselves, which a node
// could not honour anyway: mochi never replays retained messages to a shared
// subscription, and "current state arrives on SUBSCRIBE" is the contract live
// values stand on.
func isReservedFilter(filter string) bool { return strings.HasPrefix(filter, "$") }

// isTimeSyncFilter reports whether a subscribe filter names the _TimeSync
// contract exactly at segment 2 (time-sync design §2.2): "colca/v1/_TimeSync",
// "colca/v1/_TimeSync/+" or a concrete node ulid all match. A broader wildcard
// that only INCLUDES _TimeSync in passing (e.g. "colca/#") does NOT match here
// — such a filter is still judged by the normal zone rule below, exactly as
// before this exception existed; only a filter that specifically names
// _TimeSync gets the no-zone-required bypass.
func isTimeSyncFilter(filter string) bool {
	seg := strings.SplitN(filter, "/", 4)
	return len(seg) >= 3 && seg[0] == Root() && seg[1] == Version && seg[2] == "_TimeSync"
}

// fixedPathPrefix decomposes a subscription filter (§5.3 ActSub). isUns
// reports whether the filter could match any topic under colca/ — if not, the
// filter is plain-broker traffic and outside grant checking. For an
// uns-capable filter, fixed is the wildcard-free leading part of the
// HIERARCHY path (segments 5+): the part a read grant must cover. A filter
// whose fixed path is empty (wildcards from the path's first segment on, or a
// filter that never reaches the path region) can only be covered by read:#.
func fixedPathPrefix(filter string) (fixed string, isUns bool) {
	seg := strings.Split(filter, "/")
	if seg[0] != Root() && seg[0] != "+" && seg[0] != "#" {
		return "", false
	}
	var path []string
	for i, s := range seg {
		if s == "#" {
			break // matches everything below: fixed path ends here
		}
		if i < 4 {
			continue // header region: colca/v1/_Contract/{node-id} — wildcards free for readers
		}
		if s == "+" {
			break
		}
		path = append(path, s)
	}
	return strings.Join(path, "/"), true
}

// SubscriptionPath returns the fixed hierarchy path named by an MQTT filter.
// Door audit records use the same decomposition as subscription authorization,
// so a denial can be filtered under the element where it happened.
func SubscriptionPath(filter string) (string, bool) { return fixedPathPrefix(filter) }
