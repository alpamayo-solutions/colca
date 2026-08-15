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
	default:
		return fmt.Errorf("entry %s: kind must be %q or %q, got %q", e.ULID, KindMachine, KindNode, e.Kind)
	}
	if e.Mount != "" {
		if err := validMount(e.Mount); err != nil {
			return fmt.Errorf("entry %s: %w", e.ULID, err)
		}
	}
	for _, g := range e.Grants {
		if _, err := ParseGrant(g); err != nil {
			return fmt.Errorf("entry %s: %w", e.ULID, err)
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
		return Grant{}, fmt.Errorf("grant %q: verb must be read or cmd", s)
	}
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
