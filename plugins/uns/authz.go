// Infrastructure authorization: the registry entry shape, the grant grammar
// and the Authorize decision (the infra auth design
// §2.1, §5). This file answers every semantic auth question; the core owns
// doors, sessions and persistence and never re-implements grant logic (§8).
package uns

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// Kind gates which door an identity may use: machines connect to the MQTT and
// HTTP doors, nodes to the replication door (§2.1, §6).
type Kind string

const (
	KindMachine Kind = "machine"
	KindNode    Kind = "node"
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
	ULID   string   `json:"ulid"`
	Pubkey string   `json:"pubkey"` // hex ed25519 public key, pinned on connect
	Kind   Kind     `json:"kind"`
	Mount  string   `json:"mount"` // placement; "" = read-only observer (machine only)
	Grants []string `json:"grants,omitempty"`
	// Status is the entry's lifecycle state (move-drain design §3.2):
	// StatusActive or "" (absent ⇒ active, so entries persisted before this
	// field existed need no migration) or StatusDraining. Only a kind=node
	// entry may be StatusDraining — machines are out of scope for move-drain
	// (design §3.2 [delta]: machine delivery rides broker QoS-1 session
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
	if len(e.Pubkey) != 64 {
		return fmt.Errorf("entry %s: pubkey must be 64 hex chars (ed25519), got %d", e.ULID, len(e.Pubkey))
	}
	if _, err := hex.DecodeString(e.Pubkey); err != nil {
		return fmt.Errorf("entry %s: pubkey is not hex: %w", e.ULID, err)
	}
	switch e.Kind {
	case KindMachine:
		// empty mount = read-only observer, allowed
	case KindNode:
		if e.Mount == "" {
			return fmt.Errorf("entry %s: a node needs a mount — only machines may be mountless observers", e.ULID)
		}
	case KindHuman:
		return fmt.Errorf("entry %s: humans are tokens, not registry entries — KindHuman cannot be enrolled", e.ULID)
	default:
		return fmt.Errorf("entry %s: kind must be %q or %q, got %q", e.ULID, KindMachine, KindNode, e.Kind)
	}
	if e.Mount != "" {
		if err := validMount(e.Mount); err != nil {
			return fmt.Errorf("entry %s: %w", e.ULID, err)
		}
	}
	switch e.Status {
	case "", StatusActive, StatusDraining:
	default:
		return fmt.Errorf("entry %s: status must be %q or %q, got %q", e.ULID, StatusActive, StatusDraining, e.Status)
	}
	if e.Status == StatusDraining && e.Kind != KindNode {
		return fmt.Errorf("entry %s: only kind=%q entries may drain — machines are out of scope (move-drain design §3.2)", e.ULID, KindNode)
	}
	for _, g := range e.Grants {
		pg, err := ParseGrant(g)
		if err != nil {
			return fmt.Errorf("entry %s: %w", e.ULID, err)
		}
		if pg.Verb == "admin" {
			// Registry identities may not hold admin: machine provisioning is
			// the (deferred) _CmdAdmin flow, human admin rides in tokens.
			return fmt.Errorf("entry %s: %q — machines and nodes may not hold admin grants", e.ULID, g)
		}
	}
	return nil
}

// validMount rejects mounts that would break the topic grammar or collide with
// reserved segments (the "_"-prefix namespace is reserved: "_observer" is the
// placeholder path segment for mountless entries' _EdgeNode topics, §2.2).
func validMount(m string) error {
	for _, seg := range strings.Split(m, "/") {
		if seg == "" {
			return fmt.Errorf("mount %q: empty path segment", m)
		}
		if strings.HasPrefix(seg, "_") {
			return fmt.Errorf("mount %q: segments starting with %q are reserved", m, "_")
		}
	}
	return nil
}

// Grant is one parsed grant. Verb is "read" or "cmd" — write is not a grant:
// writing is identity (own ULID at level 4, path inside own mount) and never
// widens (§5.1). Prefix is the zone without a trailing "/#" ("#" alone means
// everything). Classes is non-empty exactly for cmd grants.
type Grant struct {
	Verb    string
	Prefix  string
	Classes []string
}

var cmdClasses = map[string]bool{"param": true, "operate": true, "maintain": true, "admin": true}

// ParseGrant parses the §5.1 grammar: "read:zone/#" or "cmd:zone/#:class,...".
func ParseGrant(s string) (Grant, error) {
	parts := strings.SplitN(s, ":", 3)
	switch parts[0] {
	case "read":
		if len(parts) != 2 {
			return Grant{}, fmt.Errorf("grant %q: read grant is read:<zone>", s)
		}
		p, err := parsePrefix(s, parts[1])
		if err != nil {
			return Grant{}, err
		}
		return Grant{Verb: "read", Prefix: p}, nil
	case "admin":
		if len(parts) != 2 {
			return Grant{}, fmt.Errorf("grant %q: admin grant is admin:#", s)
		}
		p, err := parsePrefix(s, parts[1])
		if err != nil {
			return Grant{}, err
		}
		if p != "#" {
			// Reserved grammar (human-authz design §3): the zone-scoped form
			// parses structurally but is not implemented — rejecting it here
			// keeps a later introduction additive instead of breaking.
			return Grant{}, fmt.Errorf("grant %q: zone-scoped admin is reserved and not implemented — use admin:#", s)
		}
		return Grant{Verb: "admin", Prefix: "#"}, nil
	case "cmd":
		if len(parts) != 3 {
			return Grant{}, fmt.Errorf("grant %q: cmd grant is cmd:<zone>:<class,...>", s)
		}
		p, err := parsePrefix(s, parts[1])
		if err != nil {
			return Grant{}, err
		}
		classes := strings.Split(parts[2], ",")
		if parts[2] == "" {
			return Grant{}, fmt.Errorf("grant %q: cmd grant needs at least one class", s)
		}
		for _, c := range classes {
			if !cmdClasses[c] {
				return Grant{}, fmt.Errorf("grant %q: unknown cmd class %q (param|operate|maintain|admin)", s, c)
			}
		}
		return Grant{Verb: "cmd", Prefix: p, Classes: classes}, nil
	default:
		return Grant{}, fmt.Errorf("grant %q: verb must be read, cmd or admin", s)
	}
}

// TokenEntry builds the EPHEMERAL entry a verified OIDC token maps to
// (human-authz design §2.3): identity = the token's sub, no mount (humans own
// no zone — the World-2 rule falls out structurally), grants = the
// colca_grants claim. Grants are authored in ROOT frame (cmdadmin design §3)
// and translated here into the local frame of the verifying node, whose
// root-frame prefix is prefix (prefixKnown=false: never learned — scoped
// grants fail closed). Validated here, never by Entry.Validate (that is the
// enrollment gate and demands a pubkey), never persisted.
func TokenEntry(sub string, grants []string, prefix string, prefixKnown bool) (*Entry, error) {
	if sub == "" {
		return nil, fmt.Errorf("token entry: empty sub")
	}
	for _, g := range grants {
		if _, err := ParseGrant(g); err != nil {
			return nil, fmt.Errorf("token entry %s: %w", sub, err)
		}
	}
	return &Entry{ULID: sub, Kind: KindHuman, Grants: TranslateGrants(prefix, prefixKnown, grants)}, nil
}

// TranslateGrants rewrites ROOT-frame grant strings into the local frame of
// a node whose root-frame prefix is prefix (cmdadmin design §3). known=false
// means the node has never learned its prefix: scoped grants fail closed,
// frame-invariant ones (zone "#", the admin verb) survive. Grants that do
// not reach this node's subtree are dropped; order is preserved. Malformed
// strings are skipped — TokenEntry gates grammar before translation.
func TranslateGrants(prefix string, known bool, grants []string) []string {
	var out []string
	for _, g := range grants {
		pg, err := ParseGrant(g)
		if err != nil {
			continue
		}
		if pg.Verb == "admin" || pg.Prefix == "#" {
			out = append(out, g)
			continue
		}
		if !known {
			continue
		}
		zone, ok := translateZone(prefix, pg.Prefix)
		if !ok {
			continue
		}
		out = append(out, rebuildGrant(pg.Verb, zone, pg.Classes))
	}
	return out
}

// translateZone maps a root-frame zone into the local frame of prefix.
// prefix "" is the root: identity. A zone covering the node collapses to
// "#" (this whole node is inside it); a zone inside the node's subtree is
// stripped to local coordinates; anything else does not apply here. The
// boundary is always the path separator — "site10" is not below "site1".
func translateZone(prefix, zone string) (string, bool) {
	if prefix == "" {
		return zone, true
	}
	if zone == prefix || strings.HasPrefix(prefix, zone+"/") {
		return "#", true
	}
	if strings.HasPrefix(zone, prefix+"/") {
		return zone[len(prefix)+1:], true
	}
	return "", false
}

// rebuildGrant renders a translated grant back into the §5.1 grammar.
func rebuildGrant(verb, zone string, classes []string) string {
	z := zone
	if z != "#" {
		z += "/#"
	}
	if verb == "cmd" {
		return "cmd:" + z + ":" + strings.Join(classes, ",")
	}
	return verb + ":" + z
}

// IsAdmin reports whether the entry carries the admin:# grant — it unlocks
// the ADMIN ROUTES only and never widens read or cmd (§3).
func (e *Entry) IsAdmin() bool {
	for _, g := range e.Grants {
		if pg, err := ParseGrant(g); err == nil && pg.Verb == "admin" {
			return true
		}
	}
	return false
}

func parsePrefix(grant, p string) (string, error) {
	if p == "#" {
		return "#", nil
	}
	p = strings.TrimSuffix(p, "/#")
	if p == "" {
		return "", fmt.Errorf("grant %q: empty zone prefix", grant)
	}
	return p, nil
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
	default:
		return "admin"
	}
}

// Action selects which §5.3 rule Authorize applies. Publishing data/entities
// has no Action: the write rule is identity (level-4 == ULID + mount rewrite),
// enforced structurally by the engine.
type Action int

const (
	ActSub        Action = iota // MQTT subscription filter (may contain wildcards)
	ActReadRecord               // one concrete stored record / KV entry
	ActCmd                      // publishing a _Cmd* contract
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

// readZones is the entry's effective read scope: the default own zone (§5.2,
// mountless observers have none) plus every explicit read grant.
func readZones(e *Entry) []string {
	var zones []string
	if e.Mount != "" {
		zones = append(zones, e.Mount)
	}
	for _, g := range e.Grants {
		if pg, err := ParseGrant(g); err == nil && pg.Verb == "read" {
			zones = append(zones, pg.Prefix)
		}
	}
	return zones
}

// Authorize is the one decision function (§5.3): pure prefix comparison,
// in-memory, no I/O. Invalid grants never reach here — Entry.Validate gates
// enrollment — so ParseGrant errors inside are treated as absent grants.
func Authorize(e *Entry, a Action, topic string) bool {
	switch a {
	case ActReadRecord:
		p, err := Parse(topic)
		if err != nil {
			return false // only uns records exist in the store: fail closed
		}
		for _, z := range readZones(e) {
			if coverPath(z, p.Path) {
				return true
			}
		}
		return false

	case ActSub:
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
		for _, z := range readZones(e) {
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
		for _, g := range e.Grants {
			pg, err := ParseGrant(g)
			if err != nil || pg.Verb != "cmd" || !coverPath(pg.Prefix, p.Path) {
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
	return false
}

// isTimeSyncFilter reports whether a subscribe filter names the _TimeSync
// contract exactly at segment 2 (time-sync design §2.2): "colca/v1/_TimeSync",
// "colca/v1/_TimeSync/+" or a concrete node ulid all match. A broader wildcard
// that only INCLUDES _TimeSync in passing (e.g. "colca/#") does NOT match here
// — such a filter is still judged by the normal zone rule below, exactly as
// before this exception existed; only a filter that specifically names
// _TimeSync gets the no-zone-required bypass.
func isTimeSyncFilter(filter string) bool {
	seg := strings.SplitN(filter, "/", 4)
	return len(seg) >= 3 && seg[0] == "colca" && seg[1] == "v1" && seg[2] == "_TimeSync"
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
	if seg[0] != "colca" && seg[0] != "+" && seg[0] != "#" {
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
