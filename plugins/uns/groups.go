package uns

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// GroupIndex resolves a group id to the grants its members hold.
//
// It is a projection of the `_Group` definitions the node holds, which arrived
// down the definition channel from wherever they were authored
// (definition-stream design §8). That is what lets a node authorize a human it
// has never been told about individually, offline, with no read side to consult:
// the token names groups, and the groups are already here.
//
// Two definitions claiming the same id resolve to NOTHING (design §10.3). The
// store keeps both — records are keyed by path and author, so neither
// overwrites the other — and picking one silently would let a node shadow a
// group authored above it and widen its own grants. A collision is a
// provisioning error, and it is reported as one.
type GroupIndex struct {
	store EntityStore
}

func NewGroupIndex(s EntityStore) *GroupIndex { return &GroupIndex{store: s} }

// group is the part of a _Group record this needs.
type group struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Grants []string `json:"grants"`
}

// GrantsOf returns the grants of one group. ok=false when the node holds no
// such group, or holds more than one claiming that id — the reason is in err,
// which is never nil when ok is false.
func (g *GroupIndex) GrantsOf(id string) (grants []string, ok bool, err error) {
	if id == "" {
		return nil, false, fmt.Errorf("group: empty id")
	}
	var found []string // authoring nodes claiming the id
	var match group
	for _, rec := range g.store.KVScanAll("_Group") {
		if rec.Path != id {
			continue
		}
		var candidate group
		if json.Unmarshal(rec.Payload, &candidate) != nil || candidate.ID != id {
			continue
		}
		found = append(found, rec.NodeID)
		match = candidate
	}
	switch len(found) {
	case 0:
		return nil, false, fmt.Errorf("group %s: this node holds no such group", id)
	case 1:
		return match.Grants, true, nil
	default:
		sort.Strings(found)
		return nil, false, fmt.Errorf("group %s is claimed by %s — an id names one thing, and "+
			"resolving it to either of two would let one author shadow the other",
			id, strings.Join(found, " and "))
	}
}

// GrantsFor is the union of the grants of every named group, in the order the
// groups were named and with duplicates dropped.
//
// A group that does not resolve contributes nothing and its reason is returned
// alongside — the caller logs them. Failing the whole token because one group
// is unknown would let a single stale membership lock a human out of everything
// rather than out of that group.
func (g *GroupIndex) GrantsFor(ids []string) (grants []string, problems []error) {
	seen := map[string]bool{}
	for _, id := range ids {
		got, ok, err := g.GrantsOf(id)
		if !ok {
			problems = append(problems, err)
			continue
		}
		for _, grant := range got {
			if !seen[grant] {
				seen[grant] = true
				grants = append(grants, grant)
			}
		}
	}
	return grants, problems
}
