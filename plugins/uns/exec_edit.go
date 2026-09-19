// The _CmdEdit executor: the command types, the dispatch and the helpers the
// composers share. The composers live in sibling files:
//
//	exec_edit_receipt.go     replay cache and durable receipt
//	exec_edit_snapshot.go    consistent snapshot and version checks
//	exec_edit_entity.go      create, update, delete, external references
//	exec_edit_placement.go   moving entities, binding signals to tags
//	exec_edit_model.go       assigning data models to a subtree
//	exec_edit_attachment.go  node attachments in the registry
//	exec_edit_annotation.go  annotations, committed as events
//	exec_edit_alarm.go       alarm and notification configuration
//	exec_edit_resource.go    resources
//	exec_edit_authz.go       authorizing the composed plan

package uns

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
)

const editMutationLimit = 200

// EditExec turns one UI intent into one atomic state transition. Paths, topics
// and records are derived here, never taken from the browser or the API.
type EditExec struct {
	store EntityStore
	// bound says which identities stand on an element. A delete intent
	// applies the same occupancy rule as element/delete, so both doors
	// refuse the same retirements.
	bound       Bindings
	attachments NodeAttachmentWriter
	// scope resolves a grant's element to the zone it covers here (the
	// element index, see SetScope). Nil resolves only realm-wide grants.
	scope Scope
	// blobs is the resource intent's port onto this node's content-addressed
	// store (SetBlobs); nil in unit tests that compose no resource records.
	blobs Blobs
	mu    sync.Mutex

	replays     map[string]editReplay
	replayOrder []string
}

// NodeAttachmentWriter is how the Edit executor changes the registry. The core
// adapter classifies registry errors, so this package never imports the
// registry.
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
	// The fields below belong to the annotation intent (see
	// exec_edit_annotation.go). Action is shared with the model intent.
	AnnotationID     string          `json:"annotation_id"`
	AnnotationTypeID string          `json:"annotation_type_id"`
	TimeStart        *float64        `json:"time_start"`
	TimeEnd          *float64        `json:"time_end"`
	Value            json.RawMessage `json:"value"`
	SignalIDs        []string        `json:"signal_ids"`
	Source           string          `json:"source"`
	// The resource intent's fields (see exec_edit_resource.go). Path is
	// where the resource sits; FromPath is set only by a move, which makes
	// the old position part of the same batch and authorization.
	Path     string          `json:"path"`
	FromPath string          `json:"from_path"`
	Resource json.RawMessage `json:"resource"`
	// The alarm intents' field (see exec_edit_alarm.go): the
	// _AlarmNotificationConfig record.
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

// NewEditExec returns the _CmdEdit executor. The attachment writer is optional.
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

// SetBlobs gives the executor the blob store the resource intent needs to
// avoid authoring records for bytes the node does not have. Without it,
// resource writes answer blob_unreachable.
func (w *EditExec) SetBlobs(blobs Blobs) { w.blobs = blobs }

// Handles reports whether this executor runs the given contract.
func (w *EditExec) Handles(contract string) bool { return contract == "_CmdEdit" }

// Execute runs one _CmdEdit command and returns its status, message and result.
func (w *EditExec) Execute(ctx CommandContext, contract, verb string, payload []byte) (int, string, string) {
	code, message, result, _ := w.ExecuteWithWrites(ctx, contract, verb, payload)
	return code, message, result
}

// ExecuteWithWrites is Execute that also returns the state writes it committed.
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
	// An Edit command is authorized against a person. Refuse others
	// before the replay lookup, so no receipt is ever written for them.
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
	// Nothing is written yet. Every position the plan touches must be
	// covered by the person's grants, or the whole command is refused.
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
	return Prefix() + contract + "/" + nodeID + "/" + path
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
