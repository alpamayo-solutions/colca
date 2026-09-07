// The `annotation` intent: the one Edit command whose composed record is
// an EVENT, not entity/definition state (annotation-cutover design D1).
//
// Every other composer in this package decides about KV-projected state and
// hands its result to the shared batch-plus-receipt commit
// (ExecuteWithWrites -> withDurableReceipt -> PublishBatch). An annotation
// cannot: IsCommandAuthoredState(ClassAnnotation) is deliberately false
// (dataops-evaluator design §8, pinned by TestAnnotationIsAnEventNotState) —
// an annotation is never state, so PublishBatch refuses it, and even if it
// did not, its batch always lands on ONE stream together with the
// `_EditOperation` receipt every intent produces; an annotation record
// (stream "annotations") and that receipt (stream "entities") can never
// share one. So composeAnnotation still composes (never writes, exactly like
// every sibling composer), but ExecuteWithWrites commits its one record
// through PublishEvent — a separate write door — and executeAnnotationWrite
// below records the durable replay receipt as its own follow-up commit,
// mirroring exec_edit_attachment.go's persistStandaloneReceipt use for
// the same reason: its write does not live in the entity batch either.
//
// Id derivation is the contract's fact, not the command's (architecture
// principle 2): colca_data_contracts.derive_annotation_id
// (payload.py) is the one definition, and deriveAnnotationID below is
// the necessary Go-native duplicate a human-authored command needs (Go
// cannot import Python, and plugins/uns is stdlib-only — arch_test.go — so it
// cannot pull in a ULID library either). Both sides are pinned against the
// SAME checked-in golden vectors
// (contracts/src/colca_data_contracts/vectors/annotation_id.json,
// the same shared-file mechanism sanitize.json and data_model_vocabulary.json
// already use) so a silent divergence fails a test on both sides instead of
// producing two different ids for what should be the same annotation; see
// exec_edit_annotation_test.go on this side and
// colca-data-contracts/tests/test_annotation_payload.py on the other.
package uns

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// composeAnnotation validates and composes the ONE `_Annotation` record a
// create/update/delete annotation intent produces. It deliberately takes no
// `expected`/`entities` snapshot, unlike every sibling composer: an
// annotation is never KV-projected, so there is no current version anywhere
// to compare against, and nothing here reads the entity graph.
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
		// The same class of hole a caller-supplied create id opens everywhere
		// else in this package (composeModel's claimID, composeCreate's
		// entity_already_exists check): naming an id the caller does not own
		// would let a create slot target an EXISTING annotation's identity.
		// Refusing outright — rather than silently ignoring the caller's
		// value — is what makes this a conflict a caller can see, not state
		// they quietly failed to set.
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
	// Reserved namespace, the same shape _colca/alarm-events/{id} and
	// _colca/external-references/{id} already use for a record that names
	// itself by id rather than by a position in the system-element tree — an
	// annotation may cover several signal_ids at once, so it has no single
	// natural path to sit at.
	topic := editTopic("_Annotation", w.store.NodeID(), "_colca/annotations/"+id)
	return 200, fmt.Sprintf("annotation %sd: %s", intent.Action, id), "ok", []StateRecord{{Topic: topic, Payload: encoded}}
}

// executeAnnotationWrite commits composeAnnotation's one record through
// PublishEvent and then records the durable replay receipt as a SEPARATE
// PublishBatch call, mirroring exec_edit_attachment.go's
// persistStandaloneReceipt use: the event append and the receipt cannot be
// one atomic transition (different streams), so this is two commits instead
// of withDurableReceipt's one, in exchange for keeping the exact same
// operation_id replay guarantee — the in-memory cache and the durable
// `_EditOperation` receipt — every other intent gets.
func (w *EditExec) executeAnnotationWrite(
	operationID string,
	digest [sha256.Size]byte,
	message string,
	records []StateRecord,
) (int, string, string, []StateWrite) {
	if len(records) != 1 {
		return 500, "annotation: composer must produce exactly one record", "error", nil
	}
	write, err := w.store.PublishEvent(records[0])
	if err != nil {
		return 500, "edit commit failed: " + err.Error(), "error", nil
	}
	if err := w.persistStandaloneReceipt(
		operationID, digest, message, "ok", []StateWrite{write},
	); err != nil {
		return 500, "edit receipt failed: " + err.Error(), "error", nil
	}
	return w.remember(operationID, digest, 200, message, "ok", []StateWrite{write})
}

// crockford32 is the ULID alphabet (Crockford's Base32): digits and
// uppercase letters, excluding I, L, O and U to avoid confusion with 1, 1, 0
// and V. Encoding logic below is the standard ULID bit layout applied to an
// arbitrary 16-byte value — the same encoding
// colca_data_contracts.derive_annotation_id gets from Python's `ulid`
// package (`ulid.from_bytes(...)`) and colca's own minted ids get from
// `github.com/oklog/ulid/v2` (exec_configure.go's `newID` port) — but
// reimplemented natively here rather than imported, because plugins/uns is
// stdlib-only (architecture principle 4, arch_test.go) and a third-party ULID
// library cannot cross that boundary. See exec_edit_annotation_test.go:
// the golden vectors prove this produces byte-identical output to the Python
// side.
const crockford32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// ulidFromBytes Crockford-base32-encodes exactly 16 bytes into the canonical
// 26-character ULID string. It does not interpret the bytes as a
// timestamp-plus-randomness pair — nothing here needs monotonicity or a wall
// clock — it only needs the same bit-for-bit encoding of an arbitrary 128-bit
// value the ULID spec defines, because that is what both language's minting
// functions already produce for derive_annotation_id's SHA-256 digest.
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

// deriveAnnotationID is the Go-native copy of
// colca_data_contracts.derive_annotation_id (payload.py): ULID-encode the
// first 16 bytes of
// SHA-256(f"{annotation_type_id}|{source}|{time_start:.6f}|{','.join(sorted(signal_ids))}").
// Folding the producer's own idempotency into the identity itself is what
// lets create, update (setting time_end) and delete of the same logical
// annotation all be appends carrying the SAME id on the append-only
// `annotations` stream (design D1/D6b) — a re-derivation here for a human
// create intent must land on the exact same id a dataops producer would have
// derived for the identical (type, source, time_start, signal set), or the
// two authors (design D4) would not actually be one act.
//
// The signal set is part of the identity (design §8): one
// author opening two annotations of the same type at the same instant on two
// different machines is two annotations, and before this rule the second
// was refused as a collision with the first. The set is sorted on a copy —
// the caller's order is not identity, and the intent's own slice is left as
// it arrived so the composed record still carries the caller's order. No
// signals contributes the empty string after the final `|`.
func deriveAnnotationID(annotationTypeID, source string, timeStart float64, signalIDs []string) string {
	sorted := append([]string(nil), signalIDs...)
	sort.Strings(sorted)
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"%s|%s|%.6f|%s", annotationTypeID, source, timeStart, strings.Join(sorted, ","),
	)))
	return ulidFromBytes(digest[:16])
}
