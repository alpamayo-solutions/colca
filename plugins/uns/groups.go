package uns

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// UnknownGroupError is the problem for a group id this node holds no _Group
// for. It is normal, not a fault: an identity provider puts its own roles
// (offline_access, uma_authorization, default-roles-<realm>) and roles meant for
// other systems into the same claim, and each simply grants nothing here.
type UnknownGroupError struct{ ID string }

func (e *UnknownGroupError) Error() string {
	return fmt.Sprintf("group %s: this node holds no such group", e.ID)
}

// maxGroupNotices bounds GroupNotices' memory; past it, nothing is new.
const maxGroupNotices = 4096

// GroupNotices remembers which group ids were already reported, so a caller
// that sees the same token on every request logs each id once.
type GroupNotices struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// First reports whether id has not been seen before, and remembers it.
func (n *GroupNotices) First(id string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.seen[id]; ok {
		return false
	}
	if n.seen == nil {
		n.seen = map[string]struct{}{}
	}
	if len(n.seen) >= maxGroupNotices {
		return false
	}
	n.seen[id] = struct{}{}
	return true
}

// GroupIndex resolves a group id to the grants its members hold. It projects
// the _Group definitions that descended to this node, so a node can authorize a
// person offline from the groups their token names. Two definitions claiming
// one id resolve to nothing, since picking one could let a node shadow a group
// authored above it; the collision is reported.
type GroupIndex struct {
	store  EntityStore
	author string
}

// NewGroupIndex returns an index over the _Group definitions in s.
func NewGroupIndex(s EntityStore) *GroupIndex { return &GroupIndex{store: s} }

// WithAuthority restricts grants to locally authored groups after handover.
func (g *GroupIndex) WithAuthority(author string) *GroupIndex { g.author = author; return g }

// group is the part of a _Group record this needs.
type group struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Grants []string `json:"grants"`
}

// GrantsOf returns one group's grants. ok is false when the node holds no such
// group or more than one definition for it; err then says why.
func (g *GroupIndex) GrantsOf(id string) (grants []string, ok bool, err error) {
	if id == "" {
		return nil, false, fmt.Errorf("group: empty id")
	}
	var found []string // authoring nodes claiming the id
	var match group
	for _, rec := range g.store.KVScanAll("_Group") {
		if rec.Path != id || (g.author != "" && rec.NodeID != g.author) {
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
		return nil, false, &UnknownGroupError{ID: id}
	case 1:
		return match.Grants, true, nil
	default:
		sort.Strings(found)
		return nil, false, fmt.Errorf("group %s is claimed by %s — an id names one thing, and "+
			"resolving it to either of two would let one author shadow the other",
			id, strings.Join(found, " and "))
	}
}

// GrantsFor returns the union of the named groups' grants, in order and
// without duplicates. Groups that do not resolve add nothing and are returned
// as problems; one stale membership must not lock a person out of everything.
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
