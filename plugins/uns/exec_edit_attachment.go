// The edit intent that changes the registry instead of the entity store:
// attaching, remounting or draining a node. It writes through the
// NodeAttachmentWriter seam, so this package never imports the registry. A
// remount that already happened is recognised from the attachment snapshot,
// since the registry write is not part of the entity batch.

package uns

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

func nodeAttachmentAlreadyApplied(
	intent editIntent,
	expected map[string]uint64,
	attachments map[string]editNodeAttachment,
	versions map[string]uint64,
) bool {
	if intent.Entity.Kind != "colca-node" || intent.Entity.ID == "" {
		return false
	}
	attachment, ok := attachments[intent.Entity.ID]
	if !ok {
		return false
	}
	attachmentKey := "node-attachment:" + intent.Entity.ID
	expectedAttachment, ok := expected[attachmentKey]
	if !ok || expectedAttachment > editRecordVersion(attachment.Record) {
		return false
	}
	for key, wanted := range expected {
		if key == attachmentKey {
			continue
		}
		if versions[key] != wanted {
			return false
		}
	}
	switch intent.Action {
	case "drain":
		return intent.MountSystemElement == "" && attachment.Status == StatusDraining
	case "remount":
		targetKey := entityVersionKey("system-element", intent.MountSystemElement)
		_, targetExpected := expected[targetKey]
		return targetExpected && intent.MountSystemElement != "" &&
			attachment.Status != StatusDraining &&
			attachment.Element == intent.MountSystemElement
	default:
		return false
	}
}

func (w *EditExec) rememberAttachmentReplay(
	ctx CommandContext,
	operationID string,
	digest [sha256.Size]byte,
	intent editIntent,
	attachments map[string]editNodeAttachment,
) (int, string, string, []StateWrite) {
	attachment := attachments[intent.Entity.ID]
	write := StateWrite{
		Stream: "entities", Offset: attachment.Record.Offset, Topic: attachment.Record.Topic,
	}
	message := fmt.Sprintf("node attachment %s already applied", intent.Action)
	if err := w.persistStandaloneReceipt(
		ctx, operationID, digest, message, "ok", []StateWrite{write},
	); err != nil {
		return 500, "edit receipt failed: " + err.Error(), "error", nil
	}
	return w.remember(operationID, digest, 200, message, "ok", []StateWrite{write})
}

func (w *EditExec) executeNodeAttachment(
	ctx CommandContext,
	operationID string,
	digest [sha256.Size]byte,
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
	attachments map[string]editNodeAttachment,
) (int, string, string, []StateWrite) {
	if intent.Entity.Kind != "colca-node" || intent.Entity.ID == "" {
		return w.remember(
			operationID, digest, 422,
			"node_attachment: entity must identify a colca-node", "invalid", nil,
		)
	}
	attachmentKey := "node-attachment:" + intent.Entity.ID
	if message := requireExpected(expected, attachmentKey); message != "" {
		return w.remember(operationID, digest, 422, "node_attachment: "+message, "invalid", nil)
	}
	attachment, ok := attachments[intent.Entity.ID]
	if !ok {
		return w.remember(
			operationID, digest, 409,
			"node_attachment_not_found: "+intent.Entity.ID, "conflict", nil,
		)
	}
	if w.attachments == nil {
		return w.remember(
			operationID, digest, 500, "node attachment writer is not configured", "error", nil,
		)
	}

	// The plan: the node's current element and, for a remount, its new
	// element. Both must be covered.
	touched := []editTouched{touchedEntity(entities, "system-element", attachment.Element)}
	if intent.MountSystemElement != "" {
		touched = append(touched, touchedEntity(entities, "system-element", intent.MountSystemElement))
	}
	if code, message, result := w.authorizeTouched(ctx, touched); code != 0 {
		return w.remember(operationID, digest, code, message, result, nil)
	}

	var (
		offset  uint64
		code    int
		message string
	)
	switch intent.Action {
	case "drain":
		if intent.MountSystemElement != "" {
			return w.remember(
				operationID, digest, 422,
				"node_attachment: drain does not accept mount_system_element_id", "invalid", nil,
			)
		}
		offset, code, message = w.attachments.DrainNode(intent.Entity.ID)
	case "remount":
		if attachment.Status == StatusDraining {
			return w.remember(
				operationID, digest, 409,
				"node_attachment_draining: "+intent.Entity.ID, "conflict", nil,
			)
		}
		if intent.MountSystemElement == "" {
			return w.remember(
				operationID, digest, 422,
				"node_attachment: remount requires mount_system_element_id", "invalid", nil,
			)
		}
		if _, targetCode, targetMessage := requireEntity(
			expected, entities, "system-element", intent.MountSystemElement,
		); targetCode != 0 {
			return w.remember(
				operationID, digest, targetCode,
				"node_attachment: "+targetMessage, resultFor(targetCode), nil,
			)
		}
		updated := attachment
		updated.Record = KVRecord{}
		updated.Element = intent.MountSystemElement
		encoded, err := json.Marshal(updated)
		if err != nil {
			return w.remember(
				operationID, digest, 422,
				"node_attachment: attachment is not encodable", "invalid", nil,
			)
		}
		offset, code, message = w.attachments.RemountNode(encoded)
	default:
		return w.remember(
			operationID, digest, 422,
			fmt.Sprintf("node_attachment: unknown action %q", intent.Action), "invalid", nil,
		)
	}
	if code != 200 {
		if code == 0 {
			code = 500
		}
		if message == "" {
			message = "node attachment mutation failed"
		}
		return w.remember(operationID, digest, code, message, resultFor(code), nil)
	}
	if offset == 0 {
		return 500, "node attachment mutation returned no state coordinate", "error", nil
	}
	write := StateWrite{Stream: "entities", Offset: offset, Topic: attachment.Record.Topic}
	if err := w.persistStandaloneReceipt(
		ctx, operationID, digest, message, "ok", []StateWrite{write},
	); err != nil {
		return 500, "edit receipt failed: " + err.Error(), "error", nil
	}
	return w.remember(operationID, digest, 200, message, "ok", []StateWrite{write})
}
