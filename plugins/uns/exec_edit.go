package uns

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const editReplayLimit = 1024
const editMutationLimit = 200

// EditExec turns one typed UI intent into one atomic retained-state
// transition. Paths, topics and state records are derived here, never supplied
// by the browser or API transport.
type EditExec struct {
	store EntityStore
	mu    sync.Mutex

	replays     map[string]editReplay
	replayOrder []string
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
	Type               string                        `json:"type"`
	Entity             editEntityKey            `json:"entity"`
	ParentID           string                        `json:"parent_id"`
	TargetParentID     string                        `json:"target_parent_id"`
	Segment            string                        `json:"segment"`
	Attributes         map[string]json.RawMessage    `json:"attributes"`
	ExternalReferences *[]editExternalReference `json:"external_references"`
	Cascade            bool                          `json:"cascade"`
	ConnectorID        string                        `json:"connector_id"`
	Operations         []editBindingOperation   `json:"operations"`
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

type editOperationReceipt struct {
	ID      string   `json:"id"`
	Digest  string   `json:"digest"`
	Message string   `json:"message"`
	Result  string   `json:"result"`
	Topics  []string `json:"topics"`
}

var editContracts = map[string]string{
	"system-element":     "_SystemElement",
	"signal":             "_Signal",
	"constant":           "_Constant",
	"colca-node":        "_Node",
	"external-reference": "_ExternalReference",
}

var editKinds = map[string]string{
	"_SystemElement":     "system-element",
	"_Signal":            "signal",
	"_Constant":          "constant",
	"_Node":        "colca-node",
	"_ExternalReference": "external-reference",
}

func NewEditExec(store EntityStore) *EditExec {
	return &EditExec{store: store, replays: map[string]editReplay{}}
}

func (w *EditExec) Handles(contract string) bool { return contract == "_CmdEdit" }

func (w *EditExec) Execute(contract, verb string, payload []byte) (int, string, string) {
	code, message, result, _ := w.ExecuteWithWrites(contract, verb, payload)
	return code, message, result
}

func (w *EditExec) ExecuteWithWrites(
	contract, verb string,
	payload []byte,
) (int, string, string, []StateWrite) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if contract != "_CmdEdit" {
		return 422, "unsupported edit contract", "invalid", nil
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

	entities, catalogues, externalSystems, versions, snapshotErr := w.snapshot()
	if snapshotErr != nil {
		return 409, snapshotErr.Error(), "conflict", nil
	}
	if message := validateSuppliedVersions(expectedVersions, versions); message != "" {
		return w.remember(envelope.OperationID, digest, 409, message, "conflict", nil)
	}

	code, message, result, records := w.compose(
		intent, expectedVersions, entities, catalogues, externalSystems,
	)
	if code != 200 {
		return w.remember(envelope.OperationID, digest, code, message, result, nil)
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

func (w *EditExec) remember(
	operationID string,
	digest [sha256.Size]byte,
	code int,
	message, result string,
	writes []StateWrite,
) (int, string, string, []StateWrite) {
	writes = cloneStateWrites(writes)
	w.replays[operationID] = editReplay{
		digest: digest, code: code, message: message, result: result, writes: writes,
	}
	w.replayOrder = append(w.replayOrder, operationID)
	if len(w.replayOrder) > editReplayLimit {
		oldest := w.replayOrder[0]
		w.replayOrder = w.replayOrder[1:]
		delete(w.replays, oldest)
	}
	return code, message, result, cloneStateWrites(writes)
}

func cloneStateWrites(in []StateWrite) []StateWrite {
	return append([]StateWrite(nil), in...)
}

func canonicalJSONDigest(payload []byte) ([sha256.Size]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return [sha256.Size]byte{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return [sha256.Size]byte{}, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(canonical), nil
}

func validateOperationID(operationID string) error {
	if len(operationID) > 128 {
		return fmt.Errorf("operation_id is longer than 128 characters")
	}
	if strings.ContainsAny(operationID, "/+#") {
		return fmt.Errorf("operation_id must be one canonical path segment")
	}
	for _, char := range operationID {
		if char < 0x20 || char == 0x7f {
			return fmt.Errorf("operation_id contains a control character")
		}
	}
	return nil
}

func (w *EditExec) operationRecords() []KVRecord {
	records := w.store.KVScan("_EditOperation", w.store.NodeID())
	sort.Slice(records, func(i, j int) bool { return records[i].Offset < records[j].Offset })
	return records
}

func (w *EditExec) durableReplay(operationID string) (editReplay, bool, error) {
	wantTopic := editOperationTopic(w.store.NodeID(), operationID)
	for _, record := range w.operationRecords() {
		if record.Topic != wantTopic {
			continue
		}
		var receipt editOperationReceipt
		if err := json.Unmarshal(record.Payload, &receipt); err != nil {
			return editReplay{}, false, err
		}
		if receipt.ID != operationID || receipt.Digest == "" {
			return editReplay{}, false, fmt.Errorf("receipt identity or digest does not match its topic")
		}
		if record.Offset <= uint64(len(receipt.Topics)) {
			return editReplay{}, false, fmt.Errorf("receipt offset cannot reconstruct its state writes")
		}
		digestBytes, err := parseHexDigest(receipt.Digest)
		if err != nil {
			return editReplay{}, false, err
		}
		first := record.Offset - uint64(len(receipt.Topics))
		writes := make([]StateWrite, len(receipt.Topics))
		for index, topic := range receipt.Topics {
			writes[index] = StateWrite{Stream: "entities", Offset: first + uint64(index), Topic: topic}
		}
		return editReplay{
			digest: digestBytes, code: 200, message: receipt.Message, result: receipt.Result, writes: writes,
		}, true, nil
	}
	return editReplay{}, false, nil
}

func parseHexDigest(value string) ([sha256.Size]byte, error) {
	if len(value) != sha256.Size*2 {
		return [sha256.Size]byte{}, fmt.Errorf("receipt digest has invalid length")
	}
	var digest [sha256.Size]byte
	for index := range digest {
		parsed, err := strconv.ParseUint(value[index*2:index*2+2], 16, 8)
		if err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("receipt digest is not hexadecimal")
		}
		digest[index] = byte(parsed)
	}
	return digest, nil
}

func (w *EditExec) withDurableReceipt(
	operationID string,
	digest [sha256.Size]byte,
	message, result string,
	records []StateRecord,
) ([]StateRecord, int, error) {
	if len(records) == 0 {
		return nil, 0, fmt.Errorf("a successful operation must produce state")
	}
	batch := make([]StateRecord, 0, len(records)+2)
	operationRecords := w.operationRecords()
	prune := len(operationRecords) - editReplayLimit + 1
	if prune > 0 {
		for _, record := range operationRecords[:prune] {
			batch = append(batch, StateRecord{Topic: record.Topic})
		}
	}
	stateStart := len(batch)
	batch = append(batch, records...)
	topics := make([]string, len(records))
	for index, record := range records {
		topics[index] = record.Topic
	}
	receipt := editOperationReceipt{
		ID: operationID, Digest: fmt.Sprintf("%x", digest), Message: message, Result: result, Topics: topics,
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return nil, 0, err
	}
	batch = append(batch, StateRecord{
		Topic: editOperationTopic(w.store.NodeID(), operationID), Payload: payload,
	})
	return batch, stateStart, nil
}

func editOperationTopic(nodeID, operationID string) string {
	return "colca/v1/_EditOperation/" + nodeID + "/_colca/edit/operations/" + operationID
}

func parseExpectedVersions(raw map[string]json.RawMessage) (map[string]uint64, error) {
	versions := make(map[string]uint64, len(raw))
	for key, encoded := range raw {
		if key == "" {
			return nil, fmt.Errorf("keys must be non-empty")
		}
		var decimal string
		if err := json.Unmarshal(encoded, &decimal); err != nil {
			// Accept integral JSON numbers defensively for non-JavaScript callers;
			// generated clients use decimal strings so uint64 remains exact.
			decimal = string(encoded)
		}
		version, err := strconv.ParseUint(decimal, 10, 64)
		if err != nil || version == 0 {
			return nil, fmt.Errorf("%s must be a positive decimal stream offset", key)
		}
		versions[key] = version
	}
	return versions, nil
}

func (w *EditExec) snapshot() (
	map[string]editSnapshot,
	map[string]editCatalogueSnapshot,
	map[string]bool,
	map[string]uint64,
	error,
) {
	entities := map[string]editSnapshot{}
	versions := map[string]uint64{}
	for contract, kind := range editKinds {
		for _, record := range w.store.KVScan(contract, w.store.NodeID()) {
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(record.Payload, &payload); err != nil {
				return nil, nil, nil, nil, fmt.Errorf("retained %s at %s is unreadable", contract, record.Path)
			}
			id, err := rawString(payload["id"])
			if err != nil || id == "" {
				return nil, nil, nil, nil, fmt.Errorf("retained %s at %s has no identity", contract, record.Path)
			}
			key := entityVersionKey(kind, id)
			if held, exists := entities[key]; exists && held.Record.Topic != record.Topic {
				return nil, nil, nil, nil, fmt.Errorf("duplicate retained identity %s", key)
			}
			entities[key] = editSnapshot{Key: key, Kind: kind, Record: record, Payload: payload}
			versions[key] = editRecordVersion(record)
		}
	}

	catalogues := map[string]editCatalogueSnapshot{}
	for _, record := range w.store.KVScan("_DataTags", w.store.NodeID()) {
		var catalogue editCatalogue
		if err := json.Unmarshal(record.Payload, &catalogue); err != nil || catalogue.Connector == "" {
			return nil, nil, nil, nil, fmt.Errorf("retained _DataTags at %s is unreadable", record.Path)
		}
		if held, exists := catalogues[catalogue.Connector]; exists && held.Record.Topic != record.Topic {
			return nil, nil, nil, nil, fmt.Errorf("duplicate retained catalogue %s", catalogue.Connector)
		}
		catalogues[catalogue.Connector] = editCatalogueSnapshot{Record: record, Catalogue: catalogue}
		versions["catalogue:"+catalogue.Connector] = editRecordVersion(record)
	}
	externalSystems := map[string]bool{}
	for _, record := range w.store.KVScanAll("_ExternalSystem") {
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("retained _ExternalSystem at %s is unreadable", record.Path)
		}
		id, err := rawString(payload["id"])
		if err != nil || id == "" {
			return nil, nil, nil, nil, fmt.Errorf("retained _ExternalSystem at %s has no identity", record.Path)
		}
		externalSystems[id] = true
	}
	return entities, catalogues, externalSystems, versions, nil
}

func editRecordVersion(record KVRecord) uint64 {
	if record.OriginOffset != 0 {
		return record.OriginOffset
	}
	// Compatibility for test stores and pre-origin-offset retained entries.
	return record.Offset
}

func validateSuppliedVersions(expected, actual map[string]uint64) string {
	keys := make([]string, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		got, ok := actual[key]
		if !ok {
			return "stale_version: " + key + " no longer exists"
		}
		if got != expected[key] {
			return fmt.Sprintf("stale_version: %s expected %d got %d", key, expected[key], got)
		}
	}
	return ""
}

func (w *EditExec) compose(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
	catalogues map[string]editCatalogueSnapshot,
	externalSystems map[string]bool,
) (int, string, string, []StateRecord) {
	switch intent.Type {
	case "create":
		return w.composeCreate(intent, expected, entities)
	case "update":
		return w.composeUpdate(intent, expected, entities, externalSystems)
	case "delete":
		return w.composeDelete(intent, expected, entities)
	case "placement":
		return w.composePlacement(intent, expected, entities)
	case "binding":
		return w.composeBinding(intent, expected, entities, catalogues)
	default:
		return 422, fmt.Sprintf("unknown edit intent %q", intent.Type), "invalid", nil
	}
}

func (w *EditExec) composeCreate(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
) (int, string, string, []StateRecord) {
	contract, key, err := validateEntityKey(intent.Entity)
	if err != nil {
		return 422, "create: " + err.Error(), "invalid", nil
	}
	if _, exists := entities[key]; exists {
		return 409, "entity_already_exists: " + key, "conflict", nil
	}
	if len(intent.Attributes) == 0 {
		return 422, "create: attributes are required", "invalid", nil
	}
	attributes := cloneRawMap(intent.Attributes)
	attributes["id"] = rawJSON(intent.Entity.ID)

	var path string
	switch intent.Entity.Kind {
	case "system-element", "signal", "constant":
		parent, code, message := requireEntity(expected, entities, "system-element", intent.ParentID)
		if code != 0 {
			return code, "create: " + message, resultFor(code), nil
		}
		name, nameErr := rawString(attributes["name"])
		if nameErr != nil || name == "" {
			return 422, "create: attributes.name is required", "invalid", nil
		}
		segment := intent.Segment
		if segment == "" {
			segment = sanitize(name)
		}
		path = joinPath(parent.Record.Path, segment)
		if intent.Entity.Kind == "system-element" {
			attributes["parent_id"] = rawJSON(intent.ParentID)
		} else {
			attributes["system_element_id"] = rawJSON(intent.ParentID)
		}
	case "colca-node":
		path = "_colca/nodes/" + intent.Entity.ID
	case "external-reference":
		path = "_colca/external-references/" + intent.Entity.ID
	default:
		return 422, "create: unsupported entity kind", "invalid", nil
	}
	if err := validatePositionPath(path); err != nil {
		return 422, "create: " + err.Error(), "invalid", nil
	}
	topic := editTopic(contract, w.store.NodeID(), path)
	if _, occupied := w.store.KVGet(topic); occupied {
		return 409, "duplicate_name: position is already occupied", "conflict", nil
	}
	payload, err := json.Marshal(attributes)
	if err != nil {
		return 422, "create: attributes are not encodable", "invalid", nil
	}
	return 200, "created " + key, "ok", []StateRecord{{Topic: topic, Payload: payload}}
}

func (w *EditExec) composeUpdate(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
	externalSystems map[string]bool,
) (int, string, string, []StateRecord) {
	_, key, err := validateEntityKey(intent.Entity)
	if err != nil {
		return 422, "update: " + err.Error(), "invalid", nil
	}
	current, code, message := requireEntity(expected, entities, intent.Entity.Kind, intent.Entity.ID)
	if code != 0 {
		return code, "update: " + message, resultFor(code), nil
	}
	if len(intent.Attributes) == 0 && intent.ExternalReferences == nil {
		return 422, "update: attributes or external_references are required", "invalid", nil
	}
	records := []StateRecord{}
	merged := cloneRawMap(current.Payload)
	for name, value := range intent.Attributes {
		merged[name] = append(json.RawMessage(nil), value...)
	}
	merged["id"] = rawJSON(intent.Entity.ID)
	if current.Kind == "system-element" {
		delete(merged, "parent_id")
		if parent, ok := current.Payload["parent_id"]; ok {
			merged["parent_id"] = append(json.RawMessage(nil), parent...)
		}
	}
	if current.Kind == "signal" || current.Kind == "constant" {
		delete(merged, "system_element_id")
		if parent, ok := current.Payload["system_element_id"]; ok {
			merged["system_element_id"] = append(json.RawMessage(nil), parent...)
		}
	}
	if len(intent.Attributes) > 0 && !rawMapsEqual(merged, current.Payload) {
		payload, err := json.Marshal(merged)
		if err != nil {
			return 422, "update: attributes are not encodable", "invalid", nil
		}
		records = append(records, StateRecord{Topic: current.Record.Topic, Payload: payload})
	}
	if intent.ExternalReferences != nil {
		code, message, referenceRecords := w.composeExternalReferences(
			intent, expected, entities, externalSystems,
		)
		if code != 200 {
			return code, "update: " + message, resultFor(code), nil
		}
		records = append(records, referenceRecords...)
	}
	if len(records) == 0 {
		return 409, "update_unchanged: " + key, "conflict", nil
	}
	if len(records) > editMutationLimit {
		return 422, fmt.Sprintf("update: at most %d entities may be changed atomically", editMutationLimit), "invalid", nil
	}
	return 200, fmt.Sprintf("updated %s with %d state changes", key, len(records)), "ok", records
}

func (w *EditExec) composeExternalReferences(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
	externalSystems map[string]bool,
) (int, string, []StateRecord) {
	sourceEntity := editSourceEntity(intent.Entity.Kind)
	if sourceEntity == "" {
		return 422, "external references are unsupported for this entity kind", nil
	}
	current := map[string]editSnapshot{}
	for _, candidate := range entities {
		if candidate.Kind != "external-reference" {
			continue
		}
		heldSource, _ := rawString(candidate.Payload["source_entity"])
		heldObject, _ := rawString(candidate.Payload["source_object_id"])
		if heldSource == sourceEntity && heldObject == intent.Entity.ID {
			current[strings.TrimPrefix(candidate.Key, "external-reference:")] = candidate
			if message := requireExpected(expected, candidate.Key); message != "" {
				return 422, message, nil
			}
		}
	}

	desiredIDs := map[string]bool{}
	clientIDs := map[string]bool{}
	semanticKeys := map[string]bool{}
	newRecords := map[string]StateRecord{}
	for index, reference := range *intent.ExternalReferences {
		if reference.ClientID == "" || clientIDs[reference.ClientID] {
			return 422, fmt.Sprintf("external reference %d has a missing or duplicate client_id", index), nil
		}
		clientIDs[reference.ClientID] = true
		if reference.ID == "" || desiredIDs[reference.ID] {
			return 422, fmt.Sprintf("external reference %d has a missing or duplicate id", index), nil
		}
		desiredIDs[reference.ID] = true
		if reference.SourceEntity != sourceEntity || reference.SourceObjectID != intent.Entity.ID {
			return 422, fmt.Sprintf("foreign_reference: %s has invalid source ownership", reference.ID), nil
		}
		if reference.RelationshipType == "" || reference.ExternalSystemID == "" || reference.ExternalTable == "" || reference.ExternalRowID == "" {
			return 422, fmt.Sprintf("external reference %s is incomplete", reference.ID), nil
		}
		if !externalSystems[reference.ExternalSystemID] {
			return 422, "external_system_not_found: " + reference.ExternalSystemID, nil
		}
		semanticKey := strings.Join([]string{
			reference.RelationshipType, reference.ExternalSystemID,
			reference.ExternalTable, reference.ExternalRowID,
		}, "\x00")
		if semanticKeys[semanticKey] {
			return 422, "duplicate_external_reference", nil
		}
		semanticKeys[semanticKey] = true

		key := entityVersionKey("external-reference", reference.ID)
		existing, belongsToSource := current[reference.ID]
		if held, exists := entities[key]; exists && !belongsToSource {
			return 422, "foreign_reference: " + held.Key, nil
		}
		if belongsToSource {
			if reference.Version == "" || reference.Version != strconv.FormatUint(editRecordVersion(existing.Record), 10) {
				return 409, "stale_version: " + key, nil
			}
		} else if reference.Version != "" {
			return 422, "foreign_reference: " + key, nil
		}

		payload := externalReferencePayload(reference)
		if belongsToSource && externalReferenceMatches(existing.Payload, payload) {
			continue
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 422, "external reference payload is not encodable", nil
		}
		topic := editTopic(
			"_ExternalReference", w.store.NodeID(), "_colca/external-references/"+reference.ID,
		)
		newRecords[topic] = StateRecord{Topic: topic, Payload: encoded}
	}

	records := []StateRecord{}
	removedTopics := []string{}
	for id, existing := range current {
		if !desiredIDs[id] {
			removedTopics = append(removedTopics, existing.Record.Topic)
		}
	}
	sort.Strings(removedTopics)
	for _, topic := range removedTopics {
		records = append(records, StateRecord{Topic: topic})
	}
	newTopics := make([]string, 0, len(newRecords))
	for topic := range newRecords {
		newTopics = append(newTopics, topic)
	}
	sort.Strings(newTopics)
	for _, topic := range newTopics {
		records = append(records, newRecords[topic])
	}
	return 200, "external references validated", records
}

func (w *EditExec) composeDelete(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
) (int, string, string, []StateRecord) {
	_, key, err := validateEntityKey(intent.Entity)
	if err != nil {
		return 422, "delete: " + err.Error(), "invalid", nil
	}
	current, code, message := requireEntity(expected, entities, intent.Entity.Kind, intent.Entity.ID)
	if code != 0 {
		return code, "delete: " + message, resultFor(code), nil
	}
	selected := []editSnapshot{current}
	if intent.Entity.Kind == "system-element" {
		selected = selected[:0]
		for _, candidate := range entities {
			if candidate.Record.Path == current.Record.Path || strings.HasPrefix(candidate.Record.Path, current.Record.Path+"/") {
				selected = append(selected, candidate)
			}
		}
		if len(selected) > 1 && !intent.Cascade {
			return 409, fmt.Sprintf("delete_impact: %s still contains %d entities", key, len(selected)-1), "conflict", nil
		}
	}
	ownedSources := map[string]bool{}
	for _, candidate := range selected {
		sourceEntity := editSourceEntity(candidate.Kind)
		sourceID, _ := rawString(candidate.Payload["id"])
		if sourceEntity != "" && sourceID != "" {
			ownedSources[sourceEntity+"\x00"+sourceID] = true
		}
	}
	for _, candidate := range entities {
		if candidate.Kind != "external-reference" {
			continue
		}
		sourceEntity, _ := rawString(candidate.Payload["source_entity"])
		sourceID, _ := rawString(candidate.Payload["source_object_id"])
		if ownedSources[sourceEntity+"\x00"+sourceID] {
			selected = append(selected, candidate)
		}
	}
	for _, candidate := range selected {
		if message := requireExpected(expected, candidate.Key); message != "" {
			return 422, "delete: " + message, "invalid", nil
		}
	}
	if len(selected) > editMutationLimit {
		return 422, fmt.Sprintf("delete: at most %d entities may be changed atomically", editMutationLimit), "invalid", nil
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Record.Topic < selected[j].Record.Topic })
	records := make([]StateRecord, len(selected))
	for i, candidate := range selected {
		records[i] = StateRecord{Topic: candidate.Record.Topic}
	}
	return 200, fmt.Sprintf("deleted %d entities", len(records)), "ok", records
}

func (w *EditExec) composePlacement(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
) (int, string, string, []StateRecord) {
	if intent.Entity.Kind != "system-element" && intent.Entity.Kind != "signal" && intent.Entity.Kind != "constant" {
		return 422, "placement: entity kind is not positionable", "invalid", nil
	}
	current, code, message := requireEntity(expected, entities, intent.Entity.Kind, intent.Entity.ID)
	if code != 0 {
		return code, "placement: " + message, resultFor(code), nil
	}
	target, code, message := requireEntity(expected, entities, "system-element", intent.TargetParentID)
	if code != 0 {
		return code, "placement: " + message, resultFor(code), nil
	}
	if current.Kind == "system-element" &&
		(target.Record.Path == current.Record.Path || strings.HasPrefix(target.Record.Path, current.Record.Path+"/")) {
		return 409, "descendant_cycle", "conflict", nil
	}
	leaf := current.Record.Path
	if index := strings.LastIndexByte(leaf, '/'); index >= 0 {
		leaf = leaf[index+1:]
	}
	newRoot := joinPath(target.Record.Path, leaf)
	if newRoot == current.Record.Path {
		return 409, "placement_unchanged", "conflict", nil
	}

	selected := []editSnapshot{current}
	if current.Kind == "system-element" {
		selected = selected[:0]
		for _, candidate := range entities {
			if candidate.Record.Path == current.Record.Path || strings.HasPrefix(candidate.Record.Path, current.Record.Path+"/") {
				selected = append(selected, candidate)
			}
		}
	}
	for _, candidate := range selected {
		if message := requireExpected(expected, candidate.Key); message != "" {
			return 422, "placement: " + message, "invalid", nil
		}
	}
	if len(selected) > editMutationLimit {
		return 422, fmt.Sprintf("placement: at most %d entities may be changed atomically", editMutationLimit), "invalid", nil
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Record.Topic < selected[j].Record.Topic })
	movingTopics := map[string]bool{}
	for _, candidate := range selected {
		movingTopics[candidate.Record.Topic] = true
	}

	records := make([]StateRecord, 0, len(selected)*2)
	for _, candidate := range selected {
		records = append(records, StateRecord{Topic: candidate.Record.Topic})
	}
	for _, candidate := range selected {
		suffix := strings.TrimPrefix(candidate.Record.Path, current.Record.Path)
		newPath := newRoot + suffix
		parsed, err := Parse(candidate.Record.Topic)
		if err != nil {
			return 409, "placement: retained topic is invalid", "conflict", nil
		}
		newTopic := editTopic(parsed.Contract, w.store.NodeID(), newPath)
		if _, occupied := w.store.KVGet(newTopic); occupied && !movingTopics[newTopic] {
			return 409, "duplicate_name: destination is occupied", "conflict", nil
		}
		payload := cloneRawMap(candidate.Payload)
		if candidate.Record.Topic == current.Record.Topic {
			if current.Kind == "system-element" {
				payload["parent_id"] = rawJSON(intent.TargetParentID)
			} else {
				payload["system_element_id"] = rawJSON(intent.TargetParentID)
			}
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 422, "placement: payload is not encodable", "invalid", nil
		}
		records = append(records, StateRecord{Topic: newTopic, Payload: encoded})
	}
	return 200, fmt.Sprintf("moved %d entities", len(selected)), "ok", records
}

func (w *EditExec) composeBinding(
	intent editIntent,
	expected map[string]uint64,
	entities map[string]editSnapshot,
	catalogues map[string]editCatalogueSnapshot,
) (int, string, string, []StateRecord) {
	if intent.ConnectorID == "" {
		return 422, "binding: connector_id is required", "invalid", nil
	}
	if len(intent.Operations) == 0 {
		return 422, "binding: operations are required", "invalid", nil
	}
	if len(intent.Operations) > editMutationLimit {
		return 422, fmt.Sprintf("binding: at most %d operations are allowed", editMutationLimit), "invalid", nil
	}
	catalogue, ok := catalogues[intent.ConnectorID]
	if !ok {
		return 409, "binding: connector catalogue not found", "conflict", nil
	}
	if message := requireExpected(expected, "catalogue:"+intent.ConnectorID); message != "" {
		return 422, "binding: " + message, "invalid", nil
	}

	type bindingTag struct {
		dataType string
		stale    bool
	}
	tags := map[string]bindingTag{}
	for _, tag := range catalogue.Catalogue.DataTags {
		tags[tag.ID] = bindingTag{dataType: tag.DataType, stale: tag.IsStale}
	}
	signals := map[string]editSnapshot{}
	boundTags := map[string]string{}
	takenPaths := map[string]bool{}
	for key, entity := range entities {
		if entity.Kind != "signal" {
			continue
		}
		id := strings.TrimPrefix(key, "signal:")
		signals[id] = entity
		takenPaths[entity.Record.Path] = true
		if tagID, _ := rawString(entity.Payload["data_tag"]); tagID != "" {
			boundTags[tagID] = id
		}
	}

	pending := map[string]StateRecord{}
	order := []string{}
	operationIDs := map[string]bool{}
	queue := func(topic string, payload map[string]json.RawMessage) error {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, exists := pending[topic]; !exists {
			order = append(order, topic)
		}
		pending[topic] = StateRecord{Topic: topic, Payload: encoded}
		return nil
	}

	for index, operation := range intent.Operations {
		if operation.ID == "" || operationIDs[operation.ID] {
			return 422, fmt.Sprintf("binding: operation %d has a missing or duplicate id", index), "invalid", nil
		}
		operationIDs[operation.ID] = true
		tag, found := tags[operation.TagID]
		if !found {
			return 422, fmt.Sprintf("binding: operation %s names unknown tag %s", operation.ID, operation.TagID), "invalid", nil
		}
		if tag.stale {
			return 409, fmt.Sprintf("binding: tag %s is stale", operation.TagID), "conflict", nil
		}

		switch operation.Kind {
		case "bind":
			signal, found := signals[operation.SignalID]
			if !found {
				return 422, fmt.Sprintf("binding: operation %s names unknown signal %s", operation.ID, operation.SignalID), "invalid", nil
			}
			if message := requireExpected(expected, entityVersionKey("signal", operation.SignalID)); message != "" {
				return 422, "binding: " + message, "invalid", nil
			}
			if held := boundTags[operation.TagID]; held != "" && held != operation.SignalID {
				return 409, fmt.Sprintf("binding: tag %s is already bound to %s", operation.TagID, held), "conflict", nil
			}
			if held, _ := rawString(signal.Payload["data_tag"]); held != "" && held != operation.TagID {
				return 409, fmt.Sprintf("binding: signal %s is already bound to %s", operation.SignalID, held), "conflict", nil
			}
			signalType, _ := rawString(signal.Payload["data_type"])
			if signalType != "" && tag.dataType != "" && !compatibleDataTypes(signalType, tag.dataType) {
				return 409, fmt.Sprintf("binding: datatype mismatch for operation %s", operation.ID), "conflict", nil
			}
			payload := cloneRawMap(signal.Payload)
			payload["data_tag"] = rawJSON(operation.TagID)
			if err := queue(signal.Record.Topic, payload); err != nil {
				return 422, "binding: payload is not encodable", "invalid", nil
			}
			boundTags[operation.TagID] = operation.SignalID
			signal.Payload = payload
			signals[operation.SignalID] = signal

		case "unbind":
			signal, found := signals[operation.SignalID]
			if !found {
				return 422, fmt.Sprintf("binding: operation %s names unknown signal %s", operation.ID, operation.SignalID), "invalid", nil
			}
			if message := requireExpected(expected, entityVersionKey("signal", operation.SignalID)); message != "" {
				return 422, "binding: " + message, "invalid", nil
			}
			held, _ := rawString(signal.Payload["data_tag"])
			if held != operation.TagID {
				return 409, fmt.Sprintf("binding: signal %s is not bound to tag %s", operation.SignalID, operation.TagID), "conflict", nil
			}
			payload := cloneRawMap(signal.Payload)
			delete(payload, "data_tag")
			if err := queue(signal.Record.Topic, payload); err != nil {
				return 422, "binding: payload is not encodable", "invalid", nil
			}
			delete(boundTags, operation.TagID)
			signal.Payload = payload
			signals[operation.SignalID] = signal

		case "create_signal_and_bind":
			if operation.SignalID == "" || operation.ParentID == "" || operation.Name == "" {
				return 422, fmt.Sprintf("binding: operation %s requires signal_id, parent_id and name", operation.ID), "invalid", nil
			}
			if _, exists := signals[operation.SignalID]; exists {
				return 409, fmt.Sprintf("binding: signal %s already exists", operation.SignalID), "conflict", nil
			}
			if held := boundTags[operation.TagID]; held != "" {
				return 409, fmt.Sprintf("binding: tag %s is already bound to %s", operation.TagID, held), "conflict", nil
			}
			parent, code, message := requireEntity(expected, entities, "system-element", operation.ParentID)
			if code != 0 {
				return code, "binding: " + message, resultFor(code), nil
			}
			path := joinPath(parent.Record.Path, sanitize(operation.Name))
			if takenPaths[path] {
				return 409, fmt.Sprintf("binding: signal name %q collides below target", operation.Name), "conflict", nil
			}
			payload := map[string]json.RawMessage{
				"id": rawJSON(operation.SignalID), "name": rawJSON(operation.Name),
				"system_element_id": rawJSON(operation.ParentID), "data_tag": rawJSON(operation.TagID),
				"is_published": rawJSON(true), "index_type": rawJSON("time"),
			}
			if tag.dataType != "" {
				payload["data_type"] = rawJSON(tag.dataType)
			}
			topic := editTopic("_Signal", w.store.NodeID(), path)
			if err := queue(topic, payload); err != nil {
				return 422, "binding: payload is not encodable", "invalid", nil
			}
			boundTags[operation.TagID] = operation.SignalID
			takenPaths[path] = true
			signals[operation.SignalID] = editSnapshot{
				Key: entityVersionKey("signal", operation.SignalID), Kind: "signal",
				Record: KVRecord{Topic: topic, Path: path, NodeID: w.store.NodeID()}, Payload: payload,
			}

		default:
			return 422, fmt.Sprintf("binding: operation %s has unknown kind %q", operation.ID, operation.Kind), "invalid", nil
		}
	}

	records := make([]StateRecord, 0, len(order))
	for _, topic := range order {
		records = append(records, pending[topic])
	}
	return 200, fmt.Sprintf("applied %d binding operations", len(intent.Operations)), "ok", records
}

func validateEntityKey(entity editEntityKey) (contract, key string, err error) {
	if entity.Kind == "" || entity.ID == "" {
		return "", "", fmt.Errorf("entity kind and id are required")
	}
	contract, ok := editContracts[entity.Kind]
	if !ok {
		return "", "", fmt.Errorf("unknown entity kind %q", entity.Kind)
	}
	return contract, entityVersionKey(entity.Kind, entity.ID), nil
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

func rawMapsEqual(left, right map[string]json.RawMessage) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func editSourceEntity(kind string) string {
	return map[string]string{
		"system-element": "SystemElement",
		"signal":         "Signal",
		"constant":       "Constant",
		"colca-node":    "Node",
	}[kind]
}

func externalReferencePayload(reference editExternalReference) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"id":                 rawJSON(reference.ID),
		"source_entity":      rawJSON(reference.SourceEntity),
		"source_object_id":   rawJSON(reference.SourceObjectID),
		"relationship_type":  rawJSON(reference.RelationshipType),
		"external_system_id": rawJSON(reference.ExternalSystemID),
		"external_table":     rawJSON(reference.ExternalTable),
		"external_column":    rawJSON(reference.ExternalColumn),
		"external_row_id":    rawJSON(reference.ExternalRowID),
		"description":        rawJSON(reference.Description),
	}
}

func externalReferenceMatches(
	current map[string]json.RawMessage,
	desired map[string]json.RawMessage,
) bool {
	for _, field := range []string{
		"id", "source_entity", "source_object_id", "relationship_type",
		"external_system_id", "external_table", "external_column",
		"external_row_id", "description",
	} {
		currentValue, currentErr := rawString(current[field])
		desiredValue, desiredErr := rawString(desired[field])
		if currentErr != nil || desiredErr != nil || currentValue != desiredValue {
			return false
		}
	}
	return true
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

func compatibleDataTypes(left, right string) bool {
	normalize := func(value string) string {
		value = strings.ToLower(strings.TrimSpace(value))
		value = strings.NewReplacer("_", "", "-", "", " ", "").Replace(value)
		switch value {
		case "int", "int64", "integer":
			return "integer"
		case "float", "float64", "double":
			return "float"
		case "bool", "boolean":
			return "boolean"
		case "str", "string", "text":
			return "string"
		default:
			return value
		}
	}
	return normalize(left) != "" && normalize(left) == normalize(right)
}
