// The annotation intent is the one Edit command whose record is an event, not
// state. Its composer still only composes, but ExecuteWithWrites commits the
// record through PublishEvent and executeAnnotationWrite records the replay
// receipt in a second commit, as node attachments do: an annotation and its
// receipt live on different streams.
//
// The annotation id is defined by colca_data_contracts.derive_annotation_id.
// deriveAnnotationID below is the Go copy (this package is stdlib-only), and
// both are tested against contracts/src/colca_data_contracts/vectors/annotation_id.json.

package uns

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// composeAnnotation validates and composes the one _Annotation record an
// annotation intent produces. It takes no entity snapshot: annotations are
// never KV-projected, so there is no version to compare.
func (w *EditExec) composeAnnotation(intent editIntent) (int, string, string, []StateRecord) {
	switch intent.Action {
	case "create", "update", "delete":
	default:
		return 422, fmt.Sprintf("annotation: action must be create, update or delete, got %q", intent.Action), "invalid", nil
	}
	if intent.AnnotationTypeID == "" {
		return 422, "annotation: annotation_type_id is required", "invalid", nil
	}
	if intent.TimeStart == nil {
		return 422, "annotation: time_start is required", "invalid", nil
	}
	if intent.Source == "" {
		return 422, "annotation: source is required", "invalid", nil
	}

	var id string
	switch intent.Action {
	case "create":
		// A create must not name an existing annotation's id; refuse instead
		// of ignoring the value, so the caller sees the conflict.
		if intent.AnnotationID != "" {
			return 409, "annotation_id_supplied_on_create: " + intent.AnnotationID, "conflict", nil
		}
		id = deriveAnnotationID(intent.AnnotationTypeID, intent.Source, *intent.TimeStart, intent.SignalIDs)
	case "update", "delete":
		if intent.AnnotationID == "" {
			return 422, fmt.Sprintf("annotation: annotation_id is required for %s", intent.Action), "invalid", nil
		}
		id = intent.AnnotationID
	}

	payload := map[string]json.RawMessage{
		"annotation_id":      rawJSON(id),
		"annotation_type_id": rawJSON(intent.AnnotationTypeID),
		"time_start":         rawJSON(*intent.TimeStart),
		"source":             rawJSON(intent.Source),
		"deleted":            rawJSON(intent.Action == "delete"),
	}
	if intent.TimeEnd != nil {
		payload["time_end"] = rawJSON(*intent.TimeEnd)
	}
	if len(intent.Value) > 0 && !bytes.Equal(intent.Value, []byte("null")) {
		payload["value"] = intent.Value
	}
	signalIDs := intent.SignalIDs
	if signalIDs == nil {
		signalIDs = []string{}
	}
	payload["signal_ids"] = rawJSON(signalIDs)

	encoded, err := json.Marshal(payload)
	if err != nil {
		return 422, "annotation: payload is not encodable", "invalid", nil
	}
	// Reserved namespace, like _colca/alarm-events/{id}: an annotation can
	// cover several signals, so it has no natural position in the tree.
	topic := editTopic("_Annotation", w.store.NodeID(), "_colca/annotations/"+id)
	return 200, fmt.Sprintf("annotation %sd: %s", intent.Action, id), "ok", []StateRecord{{Topic: topic, Payload: encoded}}
}

// executeAnnotationWrite commits composeAnnotation's record through
// PublishEvent, then the replay receipt in a separate PublishBatch. The two
// live on different streams, so they cannot be one transition, but the
// operation_id replay guarantee is the same as for every other intent.
func (w *EditExec) executeAnnotationWrite(
	ctx CommandContext,
	operationID string,
	digest [sha256.Size]byte,
	message string,
	records []StateRecord,
) (int, string, string, []StateWrite) {
	if len(records) != 1 {
		return 500, "annotation: composer must produce exactly one record", "error", nil
	}
	write, err := w.store.PublishEvent(ctx, records[0])
	if err != nil {
		return 500, "edit commit failed: " + err.Error(), "error", nil
	}
	if err := w.persistStandaloneReceipt(
		ctx, operationID, digest, message, "ok", []StateWrite{write},
	); err != nil {
		return 500, "edit receipt failed: " + err.Error(), "error", nil
	}
	return w.remember(operationID, digest, 200, message, "ok", []StateWrite{write})
}

// crockford32 is the ULID alphabet (Crockford's Base32). The encoding below is
// the standard ULID layout, written out here because plugins/uns is
// stdlib-only; the golden vectors prove it matches the Python side.
const crockford32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// ulidFromBytes encodes exactly 16 bytes as a 26-character ULID string. The
// bytes are not read as timestamp and randomness; only the encoding matters.
func ulidFromBytes(id []byte) string {
	var dst [26]byte
	dst[0] = crockford32[(id[0]&224)>>5]
	dst[1] = crockford32[id[0]&31]
	dst[2] = crockford32[(id[1]&248)>>3]
	dst[3] = crockford32[((id[1]&7)<<2)|((id[2]&192)>>6)]
	dst[4] = crockford32[(id[2]&62)>>1]
	dst[5] = crockford32[((id[2]&1)<<4)|((id[3]&240)>>4)]
	dst[6] = crockford32[((id[3]&15)<<1)|((id[4]&128)>>7)]
	dst[7] = crockford32[(id[4]&124)>>2]
	dst[8] = crockford32[((id[4]&3)<<3)|((id[5]&224)>>5)]
	dst[9] = crockford32[id[5]&31]
	dst[10] = crockford32[(id[6]&248)>>3]
	dst[11] = crockford32[((id[6]&7)<<2)|((id[7]&192)>>6)]
	dst[12] = crockford32[(id[7]&62)>>1]
	dst[13] = crockford32[((id[7]&1)<<4)|((id[8]&240)>>4)]
	dst[14] = crockford32[((id[8]&15)<<1)|((id[9]&128)>>7)]
	dst[15] = crockford32[(id[9]&124)>>2]
	dst[16] = crockford32[((id[9]&3)<<3)|((id[10]&224)>>5)]
	dst[17] = crockford32[id[10]&31]
	dst[18] = crockford32[(id[11]&248)>>3]
	dst[19] = crockford32[((id[11]&7)<<2)|((id[12]&192)>>6)]
	dst[20] = crockford32[(id[12]&62)>>1]
	dst[21] = crockford32[((id[12]&1)<<4)|((id[13]&240)>>4)]
	dst[22] = crockford32[((id[13]&15)<<1)|((id[14]&128)>>7)]
	dst[23] = crockford32[(id[14]&124)>>2]
	dst[24] = crockford32[((id[14]&3)<<3)|((id[15]&224)>>5)]
	dst[25] = crockford32[id[15]&31]
	return string(dst[:])
}

// deriveAnnotationID is the Go copy of colca_data_contracts.derive_annotation_id:
// the ULID encoding of the first 16 bytes of
// SHA-256(f"{annotation_type_id}|{source}|{time_start:.6f}|{','.join(sorted(signal_ids))}").
// Create, update and delete of one annotation therefore share an id, whether a
// person or a producer authors it. The signal set is part of the id and is
// sorted on a copy; no signals leaves the empty string after the last "|".
func deriveAnnotationID(annotationTypeID, source string, timeStart float64, signalIDs []string) string {
	sorted := append([]string(nil), signalIDs...)
	sort.Strings(sorted)
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"%s|%s|%.6f|%s", annotationTypeID, source, timeStart, strings.Join(sorted, ","),
	)))
	return ulidFromBytes(digest[:16])
}
