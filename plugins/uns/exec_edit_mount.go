// Where a child node's positions begin. A child node is attached at one of
// this node's elements (its mount), and its records arrive below that element.
// Everything below a mount is the child's to author: position is authority
// and ownership resolves to nodes, so a record this node authored there would
// stand in the child's subtree while the child holds nothing of it. Every
// later edit would be routed to the child, which would refuse it as unknown.
//
// So an edit here never writes a record below a child's mount. Retiring one
// (a tombstone) is always allowed: that is how a record that already stands
// there is moved out or deleted.

package uns

import (
	"fmt"
	"sort"
	"strings"
)

// positionedContracts are the records whose topic path is a position in the
// tree, and so can stand below a mount.
var positionedContracts = map[string]bool{
	"_SystemElement": true,
	"_Signal":        true,
	"_Constant":      true,
}

// childMounts maps the path (in this node's frame) of every element a child
// node is attached at to that node's ULID.
func childMounts(
	entities map[string]editSnapshot,
	attachments map[string]editNodeAttachment,
) map[string]string {
	mounts := map[string]string{}
	for _, attachment := range attachments {
		if attachment.Element == "" {
			continue
		}
		element, ok := entities[entityVersionKey("system-element", attachment.Element)]
		if !ok || element.Record.Path == "" {
			continue
		}
		mounts[element.Record.Path] = attachment.ULID
	}
	return mounts
}

// refuseChildPositions answers 422 when a record of the plan would stand below
// a child node's mount. A move answers `cross_node_move`, everything else
// `foreign_position`; the PREKIT preflight refuses the same plans with the
// same codes before they reach a node.
func refuseChildPositions(
	intentType string,
	records []StateRecord,
	mounts map[string]string,
) (int, string, string) {
	if len(mounts) == 0 {
		return 0, "", ""
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
		if err != nil || !positionedContracts[parsed.Contract] {
			continue
		}
		for _, mount := range paths {
			if !strings.HasPrefix(parsed.Path, mount+"/") {
				continue
			}
			code := "foreign_position"
			if intentType == "placement" {
				code = "cross_node_move"
			}
			return 422, fmt.Sprintf(
				"%s: %s would stand below %s, where node %s is attached; "+
					"records there are written by that node, not by this one",
				code, parsed.Path, mount, mounts[mount],
			), "invalid"
		}
	}
	return 0, "", ""
}
