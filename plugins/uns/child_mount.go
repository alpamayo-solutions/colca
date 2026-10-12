// Where a child node's positions begin. A child node is attached at one of
// this node's elements (its mount), and its records arrive below that element.
// Everything below a mount is the child's to author: position is authority
// and ownership resolves to nodes, so a record this node authored there would
// stand in the child's subtree while the child holds nothing of it. Every
// later edit would be routed to the child, which would refuse it as unknown.
//
// So no command executed here writes a record below a child's mount: not a
// _CmdEdit, which composes its records from an intent, and not a
// _CmdConfigure, which is handed their paths. Both ask refuseChildPositions,
// and this file is the only place the rule lives. Retiring a record (a
// tombstone) is always allowed: that is how one that already stands there is
// moved out or deleted.
//
// A child's own records never pass here. Replication stores them under the
// child's id, and no command executor writes them.

package uns

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ChildPositionError is the refusal of a record that would stand below a
// child node's mount.
type ChildPositionError struct {
	// Code is `cross_node_move` when the record arrives by a move and
	// `foreign_position` otherwise. It leads the message, so a caller can act
	// on it without matching prose; the PREKIT preflight refuses the same
	// plans with the same codes before they reach a node.
	Code string
	// Path is where the record would stand, Mount the element path the child
	// is attached at, Node the child's ULID.
	Path, Mount, Node string
}

func (e *ChildPositionError) Error() string {
	return fmt.Sprintf(
		"%s: %s would stand below %s, where node %s is attached; "+
			"records there are written by that node, not by this one",
		e.Code, e.Path, e.Mount, e.Node,
	)
}

// childMounts maps the path (in this node's frame) of every element a child
// node is attached at to that node's ULID.
func childMounts(store EntityStore) map[string]string {
	mounts := map[string]string{}
	var paths map[string]string
	for _, record := range store.KVScan("_EnrolledIdentity", store.NodeID()) {
		var attachment editNodeAttachment
		if json.Unmarshal(record.Payload, &attachment) != nil ||
			attachment.Kind != "node" || attachment.ULID == "" || attachment.Element == "" {
			continue
		}
		if paths == nil {
			paths = map[string]string{}
			for _, element := range store.KVScan("_SystemElement", store.NodeID()) {
				var held identified
				if json.Unmarshal(element.Payload, &held) == nil && held.ID != "" {
					paths[held.ID] = element.Path
				}
			}
		}
		if path := paths[attachment.Element]; path != "" {
			mounts[path] = attachment.ULID
		}
	}
	return mounts
}

// standsBelow reports whether a record of contract at path stands below
// mount. An element, a signal and a constant are positions themselves. A
// resource stands on an element, at {element path}/{resource id}: one on the
// mount element stands on this node's own record, one on anything below it
// stands in the child's subtree. No other contract has a position in the
// tree.
func standsBelow(contract, path, mount string) bool {
	switch contract {
	case "_SystemElement", "_Signal", "_Constant":
		return strings.HasPrefix(path, mount+"/")
	case "_Resource":
		return strings.HasPrefix(parentPath(path), mount+"/")
	default:
		return false
	}
}

// refuseChildPositions returns the refusal for the first record of a command's
// plan that would stand below a child node's mount, or nil. moved says whether
// a record arrives at its position by a move.
func refuseChildPositions(
	records []StateRecord,
	mounts map[string]string,
	moved func(contract, path string, payload []byte) bool,
) *ChildPositionError {
	if len(mounts) == 0 {
		return nil
	}
	paths := make([]string, 0, len(mounts))
	for path := range mounts {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, record := range records {
		if len(record.Payload) == 0 {
			continue
		}
		parsed, err := Parse(record.Topic)
		if err != nil {
			continue
		}
		for _, mount := range paths {
			if !standsBelow(parsed.Contract, parsed.Path, mount) {
				continue
			}
			code := "foreign_position"
			if moved(parsed.Contract, parsed.Path, record.Payload) {
				code = "cross_node_move"
			}
			return &ChildPositionError{Code: code, Path: parsed.Path, Mount: mount, Node: mounts[mount]}
		}
	}
	return nil
}
