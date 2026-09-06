package uns

import (
	"fmt"
	"strings"
)

// composeResource composes the records a `resource` Edit intent writes,
// so that a person's resource command is authorized at the node against the
// element the resource sits on — like every other intent, and unlike the
// `_CmdConfigure` path it replaces (node-side command authorization design
// §G).
//
// Until now the api addressed resource writes as ITSELF: `ConfigExec` takes no
// principal, so nothing here knew who was asking and preflight on the far side
// of the door was the only gate. That is the single-gate shape this design
// retires.
//
// A MOVE is one command, not two. `_CmdConfigure` needed an upsert at the new
// position and a separate tombstone at the old one, with two correlation ids
// and a window in between where the resource existed twice or not at all. Here
// both records are composed together and committed in one batch, and BOTH
// positions are authorized before either is written — which is also the only
// way to stop a caller moving a resource out of a zone they hold into one they
// do not.
//
// The blob is deliberately not part of this. Bytes carry no position, so there
// is no element to authorize them against; what a caller needs is the right to
// attach them to THIS resource at THIS position, which is exactly what the
// records below are checked for. The upload door stays where it is and only
// accepts bytes for a resource the node has already accepted a command for.
func (w *EditExec) composeResource(intent editIntent) (int, string, string, []StateRecord) {
	switch intent.Action {
	case "create", "update", "delete":
	default:
		return 422, fmt.Sprintf(
			"resource: action must be create, update or delete, got %q", intent.Action,
		), "invalid", nil
	}
	if intent.Path == "" {
		return 422, "resource: path is required", "invalid", nil
	}
	if err := validatePositionPath(intent.Path); err != nil {
		return 422, "resource: " + err.Error(), "invalid", nil
	}
	topic := w.resourceTopic(intent.Path)

	if intent.Action == "delete" {
		held, ok := w.store.KVGet(topic)
		if !ok {
			return 409, "entity_not_found: resource:" + intent.Entity.ID, "conflict", nil
		}
		if id, ok := ResourceID(held); ok && intent.Entity.ID != "" && id != intent.Entity.ID {
			return 409, fmt.Sprintf(
				"resource: %s holds resource %s, not %s", intent.Path, id, intent.Entity.ID,
			), "conflict", nil
		}
		// An empty payload is the tombstone; the blob is left alone, exactly
		// as `resourceDelete` leaves it — bytes are swept by retention, never
		// by a command, because another resource may name the same digest.
		return 200, "deleted 1", "ok", []StateRecord{{Topic: topic, Payload: nil}}
	}

	if len(intent.Resource) == 0 {
		return 422, "resource: resource payload is required for " + intent.Action, "invalid", nil
	}
	incoming, err := validateResourcePayload(intent.Resource)
	if err != nil {
		return 422, "resource: " + err.Error(), "invalid", nil
	}
	if intent.Entity.ID != "" && incoming.ID != intent.Entity.ID {
		return 422, fmt.Sprintf(
			"resource: payload is resource %s but the intent names %s", incoming.ID, intent.Entity.ID,
		), "invalid", nil
	}
	// One resource per position, judged the same way `resourceUpsert` judges
	// it: a retained record here belonging to a DIFFERENT resource is a
	// conflict, never a silent overwrite that would unaddress the first.
	if held, ok := w.store.KVGet(topic); ok {
		existing, err := validateResourcePayload(held)
		if err != nil || existing.ID != incoming.ID {
			heldID := existing.ID
			if heldID == "" {
				heldID = "an unreadable retained record"
			}
			return 409, fmt.Sprintf(
				"resource: %s is already resource %s — two resources cannot share one position",
				intent.Path, heldID,
			), "conflict", nil
		}
	}
	// Never author a record pointing at bytes this node does not hold. Same
	// invariant, same remedy and the same distinct outcome class as the
	// configure verb: the command was well formed, so a caller can tell a
	// missing blob from a bad request without reading the message.
	if err := w.ensureBlob(incoming.SHA256); err != nil {
		return 422, fmt.Sprintf(
			"blob_unreachable: resource %s: blob %s is not held by this node and could not be fetched: %v",
			incoming.ID, incoming.SHA256, err,
		), "blob_unreachable", nil
	}

	records := []StateRecord{{Topic: topic, Payload: intent.Resource}}
	// A move: vacate the old position in the SAME batch. Composed here rather
	// than left to a second command, so the two positions commit together and
	// are authorized together.
	if intent.FromPath != "" && intent.FromPath != intent.Path {
		if err := validatePositionPath(intent.FromPath); err != nil {
			return 422, "resource: from_path: " + err.Error(), "invalid", nil
		}
		from := w.resourceTopic(intent.FromPath)
		held, ok := w.store.KVGet(from)
		if !ok {
			return 409, "entity_not_found: resource:" + incoming.ID, "conflict", nil
		}
		if id, ok := ResourceID(held); ok && id != incoming.ID {
			return 409, fmt.Sprintf(
				"resource: %s holds resource %s, not %s — a move must vacate its own position",
				intent.FromPath, id, incoming.ID,
			), "conflict", nil
		}
		records = append(records, StateRecord{Topic: from, Payload: nil})
	}
	return 200, fmt.Sprintf("upserted %d", len(records)), "ok", records
}

func (w *EditExec) resourceTopic(path string) string {
	return "colca/v1/_Resource/" + w.store.NodeID() + "/" + path
}

// ensureBlob mirrors `ConfigExec.ensureBlob`: hold the bytes, or pull them
// from an ancestor once, or say plainly that neither worked.
func (w *EditExec) ensureBlob(sha string) error {
	if w.blobs == nil {
		return fmt.Errorf("this node has no blob store")
	}
	if w.blobs.Has(sha) {
		return nil
	}
	if err := w.blobs.Pull(sha); err != nil {
		return err
	}
	if !w.blobs.Has(sha) {
		return fmt.Errorf("the fetch reported success but the blob is still absent")
	}
	return nil
}

// resourcePositions is the resource intent's write-set: every position the
// composed records touch, as element paths.
//
// A create or an in-place update touches one. A move touches two, and both are
// checked — a gate that looked only at the destination would let a person lift
// a resource out of a zone they may not write, which is precisely the hole
// `_preflight_resource` closes on the api side and the reason this returns
// both.
func (w *EditExec) resourcePositions(intent editIntent, records []StateRecord) []editTouched {
	touched := make([]editTouched, 0, len(records))
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		parsed, err := Parse(record.Topic)
		if err != nil {
			continue
		}
		// The record sits AT `{element-path}/{resource-id}`; the element is
		// everything above the last segment.
		element := parsed.Path
		if i := strings.LastIndex(element, "/"); i >= 0 {
			element = element[:i]
		} else {
			element = ""
		}
		if seen[element] {
			continue
		}
		seen[element] = true
		key := "resource:" + intent.Entity.ID
		if key == "resource:" {
			if id, ok := ResourceID(record.Payload); ok {
				key = "resource:" + id
			}
		}
		touched = append(touched, editTouched{path: element, key: key})
	}
	return touched
}
