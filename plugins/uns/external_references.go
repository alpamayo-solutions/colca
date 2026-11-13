package uns

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// referenceIdentity is what makes an external reference the same reference:
// the entity it belongs to, the relationship, and the external row it points
// at. The record's id addresses it; the identity says what it means. A node
// holds at most one record per identity, because a reader that keys by the
// identity cannot hold a second one and would hide it from the people who
// then have to version it in every edit of the entity.
//
// external_column and description are not part of it: they describe the
// same link, and changing them does not make another one.
//
// The contracts package states the same rule (ExternalReference.identity),
// and vectors/external_reference_identity.json pins it for both sides.
type referenceIdentity struct {
	SourceEntity     string `json:"source_entity"`
	SourceObjectID   string `json:"source_object_id"`
	RelationshipType string `json:"relationship_type"`
	ExternalSystemID string `json:"external_system_id"`
	ExternalTable    string `json:"external_table"`
	ExternalRowID    string `json:"external_row_id"`
}

// key is the identity as one comparable value, "" for a record that names no
// complete identity and so cannot be the duplicate of anything.
func (r referenceIdentity) key() string {
	parts := []string{
		r.SourceEntity, r.SourceObjectID, r.RelationshipType,
		r.ExternalSystemID, r.ExternalTable, r.ExternalRowID,
	}
	for _, part := range parts {
		if part == "" {
			return ""
		}
	}
	return strings.Join(parts, "\x00")
}

// externalReferenceIdentity reads the identity of an _ExternalReference
// payload. An unreadable payload has none.
func externalReferenceIdentity(payload []byte) string {
	var reference referenceIdentity
	if json.Unmarshal(payload, &reference) != nil {
		return ""
	}
	return reference.key()
}

// ReferenceHeldError refuses a write that would give a node a second
// external reference with the identity another record already holds.
// Executors answer it with a 409.
type ReferenceHeldError struct {
	Reference        string // the record the write would add
	Holder           string // the record that holds the identity
	SourceEntity     string
	SourceObjectID   string
	RelationshipType string
	ExternalTable    string
	ExternalRowID    string
}

func (e *ReferenceHeldError) Error() string {
	return fmt.Sprintf(
		"duplicate_external_reference: %s %s already holds reference %s as %s to row %s of %s — "+
			"one reference per entity, relationship and row",
		e.SourceEntity, e.SourceObjectID, e.Holder, e.RelationshipType, e.ExternalRowID, e.ExternalTable,
	)
}

// CheckExternalReferences applies the rule to a batch about to commit at one
// node. current is the node's own _ExternalReference records, batch the
// records the command writes. A record that takes an identity it did not hold
// before is refused when, once the batch is applied, another record holds the
// same identity. A record that keeps its identity is left alone, including
// two records on one identity that a node held before the rule was enforced:
// the command that removes one of them has to be able to commit.
//
// The engine runs it under one lock with the commit, so two commands that
// write the same reference at the same time cannot both pass.
func CheckExternalReferences(current []KVRecord, batch []StateRecord) error {
	type standing struct{ id, identity string }
	recordID := func(payload []byte, path string) string {
		var held identified
		if json.Unmarshal(payload, &held) == nil && held.ID != "" {
			return held.ID
		}
		return strings.TrimPrefix(path, "_colca/external-references/")
	}
	held := map[string]string{}    // record id → the identity it held before the batch
	after := map[string]standing{} // topic → the reference standing there after it
	for _, rec := range current {
		id := recordID(rec.Payload, rec.Path)
		identity := externalReferenceIdentity(rec.Payload)
		after[rec.Topic] = standing{id: id, identity: identity}
		held[id] = identity
	}
	type write struct {
		standing
		payload []byte
	}
	var fresh []write
	for _, rec := range batch {
		parsed, err := Parse(rec.Topic)
		if err != nil || parsed.Contract != "_ExternalReference" {
			continue
		}
		if len(rec.Payload) == 0 {
			delete(after, rec.Topic)
			continue
		}
		id := recordID(rec.Payload, parsed.Path)
		identity := externalReferenceIdentity(rec.Payload)
		after[rec.Topic] = standing{id: id, identity: identity}
		if identity != "" && held[id] != identity {
			fresh = append(fresh, write{standing{id: id, identity: identity}, rec.Payload})
		}
	}
	if len(fresh) == 0 {
		return nil
	}
	topics := make([]string, 0, len(after))
	for topic := range after {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	for _, taken := range fresh {
		for _, topic := range topics {
			other := after[topic]
			if other.identity != taken.identity || other.id == taken.id {
				continue
			}
			var named referenceIdentity
			_ = json.Unmarshal(taken.payload, &named)
			return &ReferenceHeldError{
				Reference: taken.id, Holder: other.id,
				SourceEntity: named.SourceEntity, SourceObjectID: named.SourceObjectID,
				RelationshipType: named.RelationshipType,
				ExternalTable:    named.ExternalTable, ExternalRowID: named.ExternalRowID,
			}
		}
	}
	return nil
}
