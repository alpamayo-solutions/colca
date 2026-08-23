// The read half of a edit command: one consistent view of everything an
// intent may touch, plus the optimistic-concurrency check against it.
//
// Every edit command carries expected_versions, and every version is
// checked against this single snapshot before anything is composed. That is
// what makes a edit apply atomic in the sense the UI needs: the versions a
// browser saw are the versions the command is judged against, and a concurrent
// edit is a 409 rather than a lost update.
package uns

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

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
	map[string]editNodeAttachment,
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
				return nil, nil, nil, nil, nil, fmt.Errorf("retained %s at %s is unreadable", contract, record.Path)
			}
			id, err := rawString(payload["id"])
			if err != nil || id == "" {
				return nil, nil, nil, nil, nil, fmt.Errorf("retained %s at %s has no identity", contract, record.Path)
			}
			key := entityVersionKey(kind, id)
			if held, exists := entities[key]; exists && held.Record.Topic != record.Topic {
				return nil, nil, nil, nil, nil, fmt.Errorf("duplicate retained identity %s", key)
			}
			entities[key] = editSnapshot{Key: key, Kind: kind, Record: record, Payload: payload}
			versions[key] = editRecordVersion(record)
		}
	}

	attachments := map[string]editNodeAttachment{}
	for _, record := range w.store.KVScan("_EnrolledIdentity", w.store.NodeID()) {
		var attachment editNodeAttachment
		if err := json.Unmarshal(record.Payload, &attachment); err != nil || attachment.ULID == "" {
			return nil, nil, nil, nil, nil, fmt.Errorf(
				"retained _EnrolledIdentity at %s is unreadable", record.Path,
			)
		}
		if attachment.Kind != "node" {
			continue
		}
		attachment.Record = record
		attachments[attachment.ULID] = attachment
		versions["node-attachment:"+attachment.ULID] = editRecordVersion(record)
	}

	catalogues := map[string]editCatalogueSnapshot{}
	for _, record := range w.store.KVScan("_DataTags", w.store.NodeID()) {
		var catalogue editCatalogue
		if err := json.Unmarshal(record.Payload, &catalogue); err != nil || catalogue.Connector == "" {
			return nil, nil, nil, nil, nil, fmt.Errorf("retained _DataTags at %s is unreadable", record.Path)
		}
		if held, exists := catalogues[catalogue.Connector]; exists && held.Record.Topic != record.Topic {
			return nil, nil, nil, nil, nil, fmt.Errorf("duplicate retained catalogue %s", catalogue.Connector)
		}
		catalogues[catalogue.Connector] = editCatalogueSnapshot{Record: record, Catalogue: catalogue}
		versions["catalogue:"+catalogue.Connector] = editRecordVersion(record)
	}
	externalSystems := map[string]bool{}
	for _, record := range w.store.KVScanAll("_ExternalSystem") {
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("retained _ExternalSystem at %s is unreadable", record.Path)
		}
		id, err := rawString(payload["id"])
		if err != nil || id == "" {
			return nil, nil, nil, nil, nil, fmt.Errorf("retained _ExternalSystem at %s has no identity", record.Path)
		}
		externalSystems[id] = true
	}
	return entities, attachments, catalogues, externalSystems, versions, nil
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

var editKinds = map[string]string{
	"_SystemElement":     "system-element",
	"_Signal":            "signal",
	"_Constant":          "constant",
	"_Node":        "colca-node",
	"_ExternalReference": "external-reference",
}
