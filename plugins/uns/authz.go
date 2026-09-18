// Infrastructure authorization: the registry entry shape, the grant grammar
// and the Authorize decision. The core owns doors, sessions and persistence
// and never re-implements grant logic.

package uns

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Kind decides which door an identity may use: external services use the MQTT
// and HTTP doors, nodes the replication door.
type Kind string

const (
	// KindExternal is a service outside the node's deployment, such as a
	// machine, a gateway or an integration, using the published doors with a
	// pinned key. Its placement gives it a read zone only; every write needs an
	// explicit grant.
	KindExternal Kind = "external"
	// KindNode is another node of the tree; nodes use the replication door.
	KindNode Kind = "node"
	// KindLocal is a service inside the node's own deployment. It has no
	// pubkey: the local door is unreachable from outside, so reaching it is the
	// proof. It is the only kind that may be unplaced (see Entry.Element).
	KindLocal Kind = "local"
	// KindHuman is an ephemeral kind: TokenEntry turns a verified OIDC token
	// into one. Authorize accepts it, enrollment does not; people are tokens,
	// never registry entries.
	KindHuman Kind = "human"
)

// Entry is one registry entry: the identity triple plus its grants. Its JSON
// form is both the enrollment payload and the stored r/{ulid} value.
type Entry struct {
	ULID   string `json:"ulid"`
	Pubkey string `json:"pubkey"` // hex ed25519 public key, pinned on connect; empty for KindLocal
	Kind   Kind   `json:"kind"`
	// Name identifies a KindLocal entry, which has no pubkey; the local door
	// finds the entry by it.
	Name string `json:"name,omitempty"`
	// Element is the system element this identity binds to, named by id rather
	// than path. "" means bound to the node itself. Only KindLocal may be
	// unplaced; external services and nodes are placed at enrollment. The mount
	// path is resolved when needed, so renaming or moving the element needs no
	// re-enrollment.
	Element string   `json:"element,omitempty"`
	Grants  []string `json:"grants,omitempty"`
	// Groups are the group ids a KindHuman entry's grants came from. They travel
	// with the person's commands as attribution, so a node executing a command
	// after replication can rebuild the same person from its own _Group
	// definitions. Never set on a registry entry.
	Groups []string `json:"groups,omitempty"`
	// Username is a KindHuman entry's preferred_username, set by the door that
	// verified the token. AnnotationSource uses it to tell a Keycloak service
	// account (service-account-<client>) from a person. Never persisted.
	Username string `json:"-"`
	// Status is StatusActive (or empty) or StatusDraining. Only a node entry
	// can drain; an external service's delivery lives in broker session state,
	// so there is nothing to drain.
	Status string `json:"status,omitempty"`
}

// Entry lifecycle states, the values of Entry.Status.
const (
	StatusActive   = "active"
	StatusDraining = "draining"
)

// IsDraining reports whether the entry is moving to a new parent: still
// enrolled and serving reads, but admitting no new commands. A nil entry is
// not draining.
func (e *Entry) IsDraining() bool { return e != nil && e.Status == StatusDraining }

// MarkDraining moves the entry into the draining state. The registry decides
// when; this package defines what draining is.
func (e *Entry) MarkDraining() { e.Status = StatusDraining }

// CanDrain reports whether the entry can drain at all. Only nodes can; a
// machine's delivery lives in broker session state, not in a cursor. A nil
// entry cannot drain.
func (e *Entry) CanDrain() bool { return e != nil && e.Kind == KindNode }

// LocalCursorPrefix namespaces a KindLocal entry's cursors by the name it
// presented. A local caller never learns its minted ULID, and names are unique
// in the registry, so two local services still cannot collide on /ack.
const LocalCursorPrefix = "c/"

// CursorPrefix is the prefix this entry's cursors must carry, which /fetch and
// /ack use to stop one identity moving another's cursor: the ULID for every
// kind except KindLocal, which uses LocalCursorPrefix+name.
//
// Unlike the boolean predicates it is not nil-safe, because "" would prefix
// every cursor. httpapi's ownsCursor rejects a nil entry before calling it.
func (e *Entry) CursorPrefix() string {
	if e.Kind == KindLocal {
		return LocalCursorPrefix + e.Name + "/"
	}
	return e.ULID + "/"
}

// CatalogueName is the last segment of this entry's _DataTags catalogue topic,
// the only place a publisher's identity appears in a topic. A local entry uses
// its name, a keyed entry its ULID. The SDK's Service.external follows the same
// rule, so nobody has to fill in a name at enrollment for autobind to work.
func (e *Entry) CatalogueName() string {
	if e.Name != "" {
		return e.Name
	}
	return e.ULID
}

// Door is one of the ways an identity presents itself to a node. Which kinds
// may use which door is decided here, not at each listener.
type Door int

// Doors an identity can arrive through.
const (
	DoorMQTT Door = iota // the machine-facing broker door
	DoorHTTP             // the machine-facing HTTP door
	DoorRepl             // the node-to-node replication door
	// DoorLocal is unpublished and plaintext, reachable only from inside the
	// node's deployment network; reaching it is the credential.
	DoorLocal
)

// MayUseDoor reports whether this identity may present itself at the door.
// Machines use the MQTT and HTTP doors, nodes the replication door, local
// services the local door. People arrive as tokens and are authorized per
// publish, so they hold no door. A nil entry holds no door.
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

// MayPublishContract reports whether this kind of identity may publish the
// contract at all, before any grant is checked. People configure through
// _CmdEdit only, where every planned write is authorized against them;
// _CmdConfigure runs without a principal and belongs to the admin door and
// placed local services.
func (e *Entry) MayPublishContract(contract string) bool {
	if e == nil {
		return false
	}
	if e.Kind == KindHuman {
		return contract != "_CmdConfigure"
	}
	return true
}

// Registry is the in-memory map the doors consult (ulid to entry). Loading,
// swapping and locking belong to the core's registry manager.
type Registry map[string]*Entry

// Validate checks the entry's own shape. Uniqueness across the registry is
// the registry manager's job.
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
	// No element means bound to the node. Only KindLocal may go unplaced,
	// because the local door already proved it belongs to this deployment.
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
		return fmt.Errorf("entry %s: only kind=%q entries may drain", e.ULID, KindNode)
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

// ValidElementID rejects anything that is not an identity. An element id
// names a thing, not a place, so "/" and wildcards are out, and so is ":",
// which would split a grant ("verb:element:classes") differently than it was
// written. Every door that authors an element id checks here: an element "#"
// would turn any grant on it into "read:#", the whole tree.
func ValidElementID(id string) error {
	if strings.ContainsAny(id, "/+#:") {
		return fmt.Errorf("element %q: an element is named by identity, not by path", id)
	}
	return nil
}

// Namespace resolves an element id to this node's local path, from the
// _SystemElement records the node holds. *ElementIndex implements it. Ask at
// the moment a path is needed instead of storing one.
type Namespace interface {
	PathOf(elementID string) (string, bool)
}

// Scope is what a node knows about where elements are: the ones it holds, and
// whether an element is the node itself or one of its ancestors. A grant may
// name either; the node learns its ancestors on the downlink.
type Scope interface {
	Namespace
	// Reaches reports whether the element is this node or above it. A grant on
	// such an element covers everything here.
	Reaches(elementID string) bool
}

// Grant is one parsed grant. Verb is "read", "write", "cmd" or "admin".
// Element names a system element without the trailing "/#" ("#" alone means
// everything) and covers everything below it. Classes is set only for cmd
// grants; write grants carry no hazard classes. Naming elements instead of
// paths keeps a grant meaning the same subtree at every node and across renames.
type Grant struct {
	Verb    string
	Element string
	Classes []string
}

// Hazard classes. param, operate, maintain and admin rank how dangerous a
// command is to equipment. configure is not on that ladder: it edits the data
// model, so an editor can bind signals without being able to send maintenance
// commands to a PLC.
var cmdClasses = map[string]bool{
	"param": true, "operate": true, "maintain": true, "configure": true, "admin": true,
}

// CmdClasses lists the hazard classes a cmd grant may name, sorted.
func CmdClasses() []string {
	out := make([]string, 0, len(cmdClasses))
	for class := range cmdClasses {
		out = append(out, class)
	}
	sort.Strings(out)
	return out
}

// FormatGrant renders a Grant in the grant grammar, so building and parsing
// grants live in one package. ParseGrant(FormatGrant(g)) == g for every valid
// g. An empty Element means "#".
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

// ParseGrant parses "read:<element>/#" or "cmd:<element>/#:class,...". The
// zone is an element id, or "#" for everything.
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
		// A write grant names a zone and nothing else; hazard classes belong to
		// commands.
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
			// The zone-scoped form is reserved. Rejecting it now keeps adding it
			// later a non-breaking change.
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

// TokenEntry builds the ephemeral entry for a verified OIDC token: the token's
// sub as identity, no element (people own no zone), and the colca_grants claim
// as grants, kept exactly as written. It is validated here, not by
// Entry.Validate, and never persisted. zoneOf resolves the grants when they
// are used.
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

// TokenEntryWithGroups is TokenEntry for a token that names groups: the grants
// are what those groups hold at this node plus any grants on the token itself.
// Membership lives in the identity provider, grants in the tree. A group that
// does not resolve adds nothing and is returned in problems for the caller to
// log.
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

// IsAdmin reports whether the entry holds admin:#. It unlocks the admin routes
// only and never widens read or cmd. A nil entry is not admin.
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

// IsLocal identifies services inside this deployment trust boundary.
func (e *Entry) IsLocal() bool { return e != nil && e.Kind == KindLocal }

// MayPublishAudit reports whether the entry may publish _AuditEvent. Only
// local services may; no grant gives it to anyone else.
func (e *Entry) MayPublishAudit() bool { return e != nil && e.Kind == KindLocal }

// ActorKind maps registry and token kinds to the audit envelope's actor
// vocabulary. Machines are service actors; WrittenBy keeps the keyed/local
// distinction.
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

// MayImplicitlyConfigure reports the one command right implied by local
// placement: an unplaced in-network service may edit its node's model. A placed
// service needs an explicit cmd grant, and no other command is ever implicit.
func (e *Entry) MayImplicitlyConfigure(contract string) bool {
	// Only _CmdConfigure. A _CmdEdit carries a person's intent and is
	// authorized against that person; if the api could send one under its own
	// identity, a missing header would bypass every grant. The api forwards
	// the person's token instead.
	return e != nil && e.Kind == KindLocal && e.Element == "" && contract == "_CmdConfigure"
}

// AuthorizeCmdAt reports whether a cmd grant of the class covers the path. The
// door applies it to a command's topic path and the Edit executor to every
// position of its plan, so both checks use the same comparison.
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

// AuthorizedAtExecutor reports whether a command is authorized against the
// plan its executor composes instead of its topic path. True for _CmdEdit,
// whose path routes to the owning node. The door still checks the class.
func AuthorizedAtExecutor(contract string) bool { return contract == "_CmdEdit" }

// HoldsCmdClass reports whether any of the entry's cmd grants names the class,
// on any element.
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

// IsHuman reports whether this entry is a verified person: a token at a human
// door, or a person rebuilt from attested groups on a replicated command.
func (e *Entry) IsHuman() bool { return e != nil && e.Kind == KindHuman }

// parseZone accepts "#" or one element id, with or without a trailing "/#";
// an element grant always covers the whole subtree. Path-shaped zones are
// refused with a message saying why.
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

// CmdClass maps a command contract to its hazard class. Unknown _Cmd*
// contracts get the highest class.
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

// Action selects which rule Authorize applies.
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

// coverPath reports whether zone covers path: the zone itself or below it. "#"
// covers everything, and "werk10" is not below "werk1".
func coverPath(zone, path string) bool {
	if zone == "#" {
		return true
	}
	return path == zone || strings.HasPrefix(path, zone+"/")
}

// zoneOf resolves an element id to the local coverage it grants:
//
//   - "#" if the grant says so, or if the element is this node or an ancestor;
//   - a local path if the element sits here;
//   - nothing if this node does not know the element. A node that has not yet
//     learned its position therefore honours only "#" grants.
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

// readZones is the entry's effective read scope: its own zone plus every read
// grant, resolved through the node's scope now, so renames and moves apply at
// once. An unplaced local service reads the whole node; an unplaced external
// identity has no default zone.
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

// writeZones is the entry's effective write scope. A local service may write
// its own subtree, since the door proved it belongs to the deployment; an
// external identity's position gives it no write. Explicit write grants
// resolve like read grants.
func writeZones(sc Scope, e *Entry) []string {
	var zones []string
	if e.Kind == KindLocal {
		if e.Element == "" {
			// Unplaced means bound to the node itself, so the zone is everything
			// here. No element resolves to the empty path, so check the entry
			// directly.
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

// Authorize is the one decision function: prefix comparisons in memory, no
// I/O. Entry.Validate keeps invalid grants out, so a grant that fails to parse
// here counts as absent.
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
			// Every authenticated machine session may subscribe to _TimeSync,
			// whatever its zone: the beacon is per node, not per path.
			return true
		}
		fixed, isUns := fixedPathPrefix(topic)
		if !isUns {
			return true // outside colca/# colca is a plain broker
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
			// The topic path routes the command to the owning node and names
			// no element, so a prefix check would refuse anyone with a
			// narrower grant. The door only checks that the person holds a
			// class the editor honours (configure, or operate for annotations);
			// the executor authorizes each position with AuthorizeCmdAt.
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
// reserved "$" space; the door refuses all of them. "$share/g/colca/#" would
// otherwise pass as plain-broker traffic and bypass grants. colca publishes
// nothing under "$", and shared subscriptions never get retained state, which
// live values depend on.
func isReservedFilter(filter string) bool { return strings.HasPrefix(filter, "$") }

// isTimeSyncFilter reports whether a filter names _TimeSync exactly at segment
// 2 ("colca/v1/_TimeSync", "colca/v1/_TimeSync/+" or a node ulid). A broader
// wildcard such as "colca/#" does not count and goes through the zone check.
func isTimeSyncFilter(filter string) bool {
	seg := strings.SplitN(filter, "/", 4)
	return len(seg) >= 3 && seg[0] == Root() && seg[1] == Version && seg[2] == "_TimeSync"
}

// fixedPathPrefix splits a subscription filter. isUns reports whether the
// filter can match anything under the topic root; if not, it is plain-broker
// traffic. fixed is the wildcard-free start of the hierarchy path (segments
// 5+), which a read grant must cover; an empty fixed path needs read:#.
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
			continue // header segments (root/v1/_Contract/node): wildcards are fine for readers
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
