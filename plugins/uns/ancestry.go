package uns

import "strings"

// Ancestor is one position on the way from the root down to a node: the
// element sitting there, and the name that position carries.
//
// Element is empty for a position no element occupies. That is not an error —
// a child may bind to an element nested under a path segment nobody placed an
// element on. The name still contributes to the path; the position simply
// cannot be named in a grant, because there is nothing there to name.
type Ancestor struct {
	Element string `json:"element"`
	Name    string `json:"name"`
}

// Ancestry is a node's position in the tree, from the root down to and
// including the element the node itself binds to (id-grants design §4).
//
// It exists because a node cannot look its own ancestors up: its element index
// holds only the `_SystemElement` records published under its OWN identity, and
// an ancestor's records live at that ancestor. So the parent teaches it, hop by
// hop, on the downlink — the same channel and the same moment the old prefix
// string was taught, carrying identities instead of a path.
//
// The path form is derived, never stored: rendering it is Prefix().
type Ancestry []Ancestor

// Prefix renders the ancestry as this node's root-frame path — exactly the
// string the parent used to hand down directly. The root's ancestry is empty
// and renders as "".
func (a Ancestry) Prefix() string {
	names := make([]string, 0, len(a))
	for _, step := range a {
		names = append(names, step.Name)
	}
	return strings.Join(names, "/")
}

// Covers reports whether elementID names this node or something above it. A
// grant on such an element reaches everything here, so the local frame answer
// is the whole node.
//
// The empty id never matches: a position nobody placed an element on cannot be
// granted, and treating "" as a hit would make every unplaced segment a
// skeleton key.
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

// Placements answers which element sits at a local path — the reverse of
// Namespace, and the direction a parent needs when it is looking at a mount and
// wants the identity there. *ElementIndex implements both.
type Placements interface {
	IDAt(path string) (string, bool)
}

// Extend returns the ancestry of a child that binds to the element at mount,
// where mount is that element's path in THIS node's frame. Every segment of the
// mount becomes one further position, each resolved through the node's own
// placements — so an element nested below a segment nobody placed still arrives
// with its identity intact, and the rendered path stays exact either way.
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
