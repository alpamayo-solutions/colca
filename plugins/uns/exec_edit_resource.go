package uns

import (
	"fmt"
	"strings"
)

// composeResource composes the records a resource Edit intent writes, so the
// person is authorized at the element the resource sits on. A move is one
// command: the new record and the tombstone at the old position commit in one
// batch, and both positions are authorized first. Blobs are not part of this;
// bytes have no position, and the upload door only accepts bytes for a
// resource the node already accepted.
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
		// An empty payload is the tombstone. The blob stays: another resource
		// may use the same digest, and retention sweeps unused bytes.
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
	// One resource per position, as resourceUpsert checks: a record of a
	// different resource here is a conflict, never overwritten.
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
	// Never author a record pointing at bytes this node does not hold. The
	// outcome is blob_unreachable, as for the configure verb, so callers can
	// tell it from a bad request.
	if err := w.ensureBlob(incoming.SHA256); err != nil {
		return 422, fmt.Sprintf(
			"blob_unreachable: resource %s: blob %s is not held by this node and could not be fetched: %v",
			incoming.ID, incoming.SHA256, err,
		), "blob_unreachable", nil
	}

	records := []StateRecord{{Topic: topic, Payload: intent.Resource}}
	// A move vacates the old position in the same batch, so both positions
	// commit and are authorized together.
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
	return Prefix() + "_Resource/" + w.store.NodeID() + "/" + path
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

// resourcePositions returns every position the composed records touch, as
// element paths: one for a create or update, two for a move. Checking only the
// destination would let a person move a resource out of a zone they cannot
// write.
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
