package uns

import "strings"

// Ancestor is one position between the root and a node: the element there and
// the name of the position. Element is empty where no element was placed; the
// name still counts toward the path, but no grant can name the position.
type Ancestor struct {
	Element string `json:"element"`
	Name    string `json:"name"`
}

// Ancestry is a node's position in the tree, from the root down to the element
// the node binds to. A node cannot look up its ancestors (their records live
// at the ancestors), so the parent teaches it on the downlink. The path form is
// derived by Prefix, never stored.
type Ancestry []Ancestor

// Prefix renders the ancestry as this node's root-frame path; an empty ancestry
// renders as "".
func (a Ancestry) Prefix() string {
	names := make([]string, 0, len(a))
	for _, step := range a {
		names = append(names, step.Name)
	}
	return strings.Join(names, "/")
}

// Covers reports whether elementID names this node or something above it; a
// grant on such an element reaches the whole node. The empty id never matches,
// or every unplaced segment would grant everything.
func (a Ancestry) Covers(elementID string) bool {
	if elementID == "" {
		return false
	}
	for _, step := range a {
		if step.Element == elementID {
			return true
		}
	}
	return false
}

// Placements says which element sits at a local path, the reverse of
// Namespace. *ElementIndex implements both.
type Placements interface {
	IDAt(path string) (string, bool)
}

// Extend returns the ancestry of a child bound to the element at mount, a path
// in this node's frame. Each mount segment adds a position resolved through
// this node's placements, so the path stays exact even across unplaced
// segments.
func (a Ancestry) Extend(p Placements, mount string) Ancestry {
	out := append(Ancestry{}, a...)
	var local string
	for _, seg := range strings.Split(mount, "/") {
		if seg == "" {
			continue
		}
		if local == "" {
			local = seg
		} else {
			local += "/" + seg
		}
		id, _ := p.IDAt(local)
		out = append(out, Ancestor{Element: id, Name: seg})
	}
	return out
}
