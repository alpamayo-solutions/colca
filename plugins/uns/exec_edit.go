// The `_CmdEdit` executor: what a edit command IS, and how one is
// dispatched. The work itself lives in six sibling files, split along the
// seams ExecuteWithWrites below calls through in order:
//
//	exec_edit_receipt.go     idempotency — the replay cache and the
//	                              durable receipt that outlives it
//	exec_edit_snapshot.go    the read half — one consistent view, and the
//	                              optimistic-version check against it
//	exec_edit_entity.go      composing create / update / delete and the
//	                              external references hanging off them
//	exec_edit_placement.go   composing the two POSITION intents: moving an
//	                              entity, and binding a signal to a tag
//	exec_edit_model.go       composing the one intent that spans a
//	                              SUBTREE: assigning a data model to an
//	                              element, its child elements and their models
//	exec_edit_attachment.go  the one intent that writes the REGISTRY
//	                              rather than the entity store
//	exec_edit_annotation.go  the one intent whose class is an EVENT, not
//	                              entity/definition state: it derives its own
//	                              id and commits through PublishEvent, a
//	                              separate write door, because
//	                              IsCommandAuthoredState refuses it in
//	                              PublishBatch's batch
//
// What stays here is what all of them share: the envelope and intent types,
// the executor struct, and the small helpers more than one composer needs.
package uns

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
)

const editMutationLimit = 200

// EditExec turns one typed UI intent into one atomic retained-state
// transition. Paths, topics and state records are derived here, never supplied
// by the browser or API transport.
type EditExec struct {
	store EntityStore
	// bound answers which identities stand on an element. A delete intent
	// retires positions exactly as the `_CmdConfigure` element/delete verb
	// does, so it needs the same port to judge the same occupancy rule; two
	// doors retiring elements under two rules is how an Edit cascade came
	// to cut off a child node the configure verb would have refused to strand.
	bound       Bindings
	attachments NodeAttachmentWriter
	// scope resolves a grant's element to the zone it covers here — the
	// node's own element index in production (SetScope). Nil resolves only
	// realm-wide grants.
	scope Scope
	// blobs is the resource intent's port onto this node's content-addressed
	// store (SetBlobs); nil in unit tests that compose no resource records.
	blobs Blobs
	mu    sync.Mutex

	replays     map[string]editReplay
	replayOrder []string
}

// NodeAttachmentWriter is the registry-owned mutation seam used by the
// Edit executor. The core adapter classifies registry errors; the domain
// plugin never imports the core registry package.
type NodeAttachmentWriter interface {
	RemountNode(entryJSON []byte) (offset uint64, status int, message string)
	DrainNode(ulid string) (offset uint64, status int, message string)
}

type editReplay struct {
	digest  [sha256.Size]byte
	code    int
	message string
	result  string
	writes  []StateWrite
}

type editEnvelope struct {
	OperationID      string                     `json:"operation_id"`
	CorrelationID    string                     `json:"correlation_id"`
	ExpiresAt        int64                      `json:"expires_at"`
	ExpectedVersions map[string]json.RawMessage `json:"expected_versions"`
	Intent           json.RawMessage            `json:"intent"`
}

type editEntityKey struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

type editIntent struct {
	Type               string                     `json:"type"`
	Entity             editEntityKey              `json:"entity"`
	ParentID           string                     `json:"parent_id"`
	TargetParentID     string                     `json:"target_parent_id"`
	Segment            string                     `json:"segment"`
	Attributes         map[string]json.RawMessage `json:"attributes"`
	ExternalReferences *[]editExternalReference   `json:"external_references"`
	Cascade            bool                       `json:"cascade"`
	ConnectorID        string                     `json:"connector_id"`
	Operations         []editBindingOperation     `json:"operations"`
	Action             string                     `json:"action"`
	MountSystemElement string                     `json:"mount_system_element_id"`
	Models             []string                   `json:"models"`
	Creates            map[string]string          `json:"creates"`
	// The fields below are the `annotation` intent's own — see
	// exec_edit_annotation.go. Action is shared (create/update/delete,
	// the same vocabulary "model" uses for assign/unassign): an annotation
	// intent is the only one whose composer never reads `entities`/`expected`
	// at all, because its record is never KV-projected and there is nothing
	// to compare a version against.
	AnnotationID     string          `json:"annotation_id"`
	AnnotationTypeID string          `json:"annotation_type_id"`
	TimeStart        *float64        `json:"time_start"`
	TimeEnd          *float64        `json:"time_end"`
	Value            json.RawMessage `json:"value"`
	SignalIDs        []string        `json:"signal_ids"`
	Source           string          `json:"source"`
	// The `resource` intent's own — see exec_edit_resource.go. `Path` is
	// where the resource sits (element path plus the resource id); `FromPath`
	// is set only by a move, and makes the vacated position part of the same
	// batch and the same authorization decision.
	Path     string          `json:"path"`
	FromPath string          `json:"from_path"`
	Resource json.RawMessage `json:"resource"`
	// The alarm family's own — see exec_edit_alarm.go. The snapshot IS
	// the `_AlarmNotificationConfig` record; an acknowledgement carries none,
	// because current alarm status is the evaluator's, not KV state.
	Snapshot json.RawMessage `json:"snapshot"`
}

type editNodeAttachment struct {
	ULID    string   `json:"ulid"`
	Pubkey  string   `json:"pubkey"`
	Kind    string   `json:"kind"`
	Name    string   `json:"name,omitempty"`
	Element string   `json:"element,omitempty"`
	Grants  []string `json:"grants,omitempty"`
	Status  string   `json:"status,omitempty"`
	Record  KVRecord `json:"-"`
}

type editExternalReference struct {
	ClientID         string `json:"client_id"`
	ID               string `json:"id"`
	Version          string `json:"version"`
	SourceEntity     string `json:"source_entity"`
	SourceObjectID   string `json:"source_object_id"`
	RelationshipType string `json:"relationship_type"`
	ExternalSystemID string `json:"external_system_id"`
	ExternalTable    string `json:"external_table"`
	ExternalColumn   string `json:"external_column"`
	ExternalRowID    string `json:"external_row_id"`
	Description      string `json:"description"`
}

type editBindingOperation struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	TagID    string `json:"tag_id"`
	SignalID string `json:"signal_id"`
	ParentID string `json:"parent_id"`
	Name     string `json:"name"`
}

type editSnapshot struct {
	Key     string
	Kind    string
	Record  KVRecord
	Payload map[string]json.RawMessage
}

type editCatalogue struct {
	Connector string `json:"connector"`
	DataTags  []struct {
		ID       string `json:"id"`
		DataType string `json:"data_type"`
		IsStale  bool   `json:"is_stale"`
	} `json:"data_tags"`
}

type editCatalogueSnapshot struct {
	Record    KVRecord
	Catalogue editCatalogue
}

func NewEditExec(
	store EntityStore, bound Bindings, attachmentWriters ...NodeAttachmentWriter,
) *EditExec {
	var attachments NodeAttachmentWriter
	if len(attachmentWriters) > 0 {
		attachments = attachmentWriters[0]
	}
	return &EditExec{
		store: store, bound: bound, attachments: attachments, replays: map[string]editReplay{},
	}
}

// SetBlobs gives the executor the blob store the `resource` intent needs, so
// it can hold the same invariant `ConfigExec` does: never author a record
// pointing at bytes this node cannot produce. Nil leaves resource writes
// refusing with `blob_unreachable`, which is the honest answer for a node with
// no blob store.
func (w *EditExec) SetBlobs(blobs Blobs) { w.blobs = blobs }

func (w *EditExec) Handles(contract string) bool { return contract == "_CmdEdit" }

func (w *EditExec) Execute(ctx CommandContext, contract, verb string, payload []byte) (int, string, string) {
	code, message, result, _ := w.ExecuteWithWrites(ctx, contract, verb, payload)
	return code, message, result
}

func (w *EditExec) ExecuteWithWrites(
	ctx CommandContext,
	contract, verb string,
	payload []byte,
) (int, string, string, []StateWrite) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if contract != "_CmdEdit" {
		return 422, "unsupported edit contract", "invalid", nil
	}
	// An Edit command is a person's intent and is authorized against that
	// person (node-side command authorization design §3A). Refused before
	// the replay lookup: a non-human never earns a receipt, so a later replay
	// cannot return an outcome nobody was authorized to produce.
	if !ctx.Actor.IsHuman() {
		return 403, "_CmdEdit requires a human actor: forward the person's token, or carry their attested groups on the record", "denied", nil
	}
	if verb != "apply" {
		return 422, fmt.Sprintf("unknown edit verb %q", verb), "invalid", nil
	}

	var envelope editEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return 422, "unreadable edit command: " + err.Error(), "invalid", nil
	}
	if envelope.OperationID == "" {
		return 422, "operation_id is required", "invalid", nil
	}
	if err := validateOperationID(envelope.OperationID); err != nil {
		return 422, err.Error(), "invalid", nil
	}
	if envelope.ExpectedVersions == nil {
		return 422, "expected_versions is required", "invalid", nil
	}
	expectedVersions, err := parseExpectedVersions(envelope.ExpectedVersions)
	if err != nil {
		return 422, "expected_versions: " + err.Error(), "invalid", nil
	}
	if len(envelope.Intent) == 0 || bytes.Equal(envelope.Intent, []byte("null")) {
		return 422, "intent is required", "invalid", nil
	}
	digest, err := canonicalJSONDigest(payload)
	if err != nil {
		return 422, "unreadable edit command: " + err.Error(), "invalid", nil
	}
	if replay, ok := w.replays[envelope.OperationID]; ok {
		if replay.digest != digest {
			return 409, "idempotency_conflict", "conflict", nil
		}
		return replay.code, replay.message, replay.result, cloneStateWrites(replay.writes)
	}
	if replay, found, err := w.durableReplay(envelope.OperationID); err != nil {
		return 409, "edit replay receipt is unreadable: " + err.Error(), "conflict", nil
	} else if found {
		w.replays[envelope.OperationID] = replay
		if replay.digest != digest {
			return 409, "idempotency_conflict", "conflict", nil
		}
		return replay.code, replay.message, replay.result, cloneStateWrites(replay.writes)
	}

	var intent editIntent
	if err := json.Unmarshal(envelope.Intent, &intent); err != nil {
		return w.remember(envelope.OperationID, digest, 422, "intent must be an object: "+err.Error(), "invalid", nil)
	}
	if intent.Type == "" {
		return w.remember(envelope.OperationID, digest, 422, "intent.type is required", "invalid", nil)
	}

	entities, attachments, catalogues, externalSystems, versions, snapshotErr := w.snapshot()
	if snapshotErr != nil {
		return 409, snapshotErr.Error(), "conflict", nil
	}
	if intent.Type == "node_attachment" && nodeAttachmentAlreadyApplied(
		intent, expectedVersions, attachments, versions,
	) {
		return w.rememberAttachmentReplay(
			envelope.OperationID, digest, intent, attachments,
		)
	}
	if message := validateSuppliedVersions(expectedVersions, versions); message != "" {
		return w.remember(envelope.OperationID, digest, 409, message, "conflict", nil)
	}
	if intent.Type == "node_attachment" {
		return w.executeNodeAttachment(
			ctx, envelope.OperationID, digest, intent, expectedVersions, entities, attachments,
		)
	}

	code, message, result, records := w.compose(
		intent, expectedVersions, entities, catalogues, externalSystems,
	)
	if code != 200 {
		return w.remember(envelope.OperationID, digest, code, message, result, nil)
	}
	// The plan is composed; nothing is written yet. Every position it
	// touches must be covered by the person's configure grants, or the whole
	// command is refused with zero writes (design §3C).
	if code, message, result := w.authorizeTouched(ctx, w.planFor(ctx, intent, records, entities, catalogues)); code != 0 {
		return w.remember(envelope.OperationID, digest, code, message, result, nil)
	}
	if intent.Type == "annotation" {
		return w.executeAnnotationWrite(envelope.OperationID, digest, message, records)
	}
	batch, stateStart, err := w.withDurableReceipt(
		envelope.OperationID, digest, message, "ok", records,
	)
	if err != nil {
		return 500, "edit receipt failed: " + err.Error(), "error", nil
	}
	batchWrites, err := w.store.PublishBatch(batch)
	if err != nil {
		return 500, "edit commit failed: " + err.Error(), "error", nil
	}
	if len(batchWrites) < stateStart+len(records) {
		return 500, "edit commit returned incomplete state coordinates", "error", nil
	}
	writes := batchWrites[stateStart : stateStart+len(records)]
	return w.remember(envelope.OperationID, digest, 200, message, "ok", writes)
}

func requireEntity(
	expected map[string]uint64,
	entities map[string]editSnapshot,
	kind, id string,
) (editSnapshot, int, string) {
	if kind == "" || id == "" {
		return editSnapshot{}, 422, "entity kind and id are required"
	}
	key := entityVersionKey(kind, id)
	if message := requireExpected(expected, key); message != "" {
		return editSnapshot{}, 422, message
	}
	entity, ok := entities[key]
	if !ok {
		return editSnapshot{}, 409, "entity_not_found: " + key
	}
	return entity, 0, ""
}

func requireExpected(expected map[string]uint64, key string) string {
	if _, ok := expected[key]; !ok {
		return "missing_expected_version: " + key
	}
	return ""
}

func resultFor(code int) string {
	if code == 409 {
		return "conflict"
	}
	if code >= 500 {
		return "error"
	}
	return "invalid"
}

func entityVersionKey(kind, id string) string { return kind + ":" + id }

func editTopic(contract, nodeID, path string) string {
	return "colca/v1/" + contract + "/" + nodeID + "/" + path
}

func cloneRawMap(source map[string]json.RawMessage) map[string]json.RawMessage {
	clone := make(map[string]json.RawMessage, len(source))
	for key, value := range source {
		clone[key] = append(json.RawMessage(nil), value...)
	}
	return clone
}

func rawJSON(value any) json.RawMessage {
	encoded, _ := json.Marshal(value)
	return encoded
}

func rawString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}
