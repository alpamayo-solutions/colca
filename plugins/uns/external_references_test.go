package uns

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
)

// The contracts package and the node agree on what makes two records the
// same reference; the vector is the answer both read.
func TestExternalReferenceIdentityVector(t *testing.T) {
	data, err := os.ReadFile("../../contracts/src/colca_data_contracts/vectors/external_reference_identity.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Base  map[string]any `json:"base"`
		Cases []struct {
			Name   string         `json:"name"`
			Change map[string]any `json:"change"`
			Same   bool           `json:"same"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &vector); err != nil {
		t.Fatal(err)
	}
	base := externalReferenceIdentity(mustJSON(vector.Base))
	if base == "" {
		t.Fatal("the base record names no identity")
	}
	for _, c := range vector.Cases {
		changed := map[string]any{}
		for name, value := range vector.Base {
			changed[name] = value
		}
		for name, value := range c.Change {
			changed[name] = value
		}
		if got := externalReferenceIdentity(mustJSON(changed)) == base; got != c.Same {
			t.Errorf("%s: same identity = %v, want %v", c.Name, got, c.Same)
		}
	}
}

// reference is one _ExternalReference payload of el-line1; row and relation
// are what the tests vary.
func reference(id, relation, row string) map[string]any {
	return map[string]any{
		"id": id, "source_entity": "SystemElement", "source_object_id": "el-line1",
		"relationship_type": relation, "external_system_id": "ext-tcdb",
		"external_table": "equipment", "external_column": "id", "external_row_id": row,
	}
}

func referenceTopic(node, id string) string {
	return "colca/v1/_ExternalReference/" + node + "/_colca/external-references/" + id
}

func referenceWrite(node, id string, payload map[string]any) StateRecord {
	if payload == nil {
		return StateRecord{Topic: referenceTopic(node, id)}
	}
	return StateRecord{Topic: referenceTopic(node, id), Payload: mustJSON(payload)}
}

func TestCheckExternalReferences(t *testing.T) {
	held := func(id string, payload map[string]any) KVRecord {
		return KVRecord{
			Topic: referenceTopic("n1", id), Path: "_colca/external-references/" + id,
			NodeID: "n1", Payload: mustJSON(payload),
		}
	}
	current := []KVRecord{
		held("ra", reference("ra", "access:equipment", "7")),
		// Two records on one identity, from before the rule.
		held("rc", reference("rc", "access:equipment", "9")),
		held("rd", reference("rd", "access:equipment", "9")),
	}
	described := reference("rd", "access:equipment", "9")
	described["description"] = "press"
	for name, tc := range map[string]struct {
		batch  []StateRecord
		holder string
	}{
		"another row is another reference": {
			batch: []StateRecord{referenceWrite("n1", "rb", reference("rb", "access:equipment", "8"))},
		},
		"another relationship to the same row is another reference": {
			batch: []StateRecord{referenceWrite("n1", "rb", reference("rb", "access:location", "7"))},
		},
		"a second record for a held identity is refused": {
			batch:  []StateRecord{referenceWrite("n1", "rb", reference("rb", "access:equipment", "7"))},
			holder: "ra",
		},
		"the holder rewritten keeps its identity": {
			batch: []StateRecord{referenceWrite("n1", "ra", reference("ra", "access:equipment", "7"))},
		},
		"retiring the holder in the same batch frees the identity": {
			batch: []StateRecord{
				referenceWrite("n1", "ra", nil),
				referenceWrite("n1", "rb", reference("rb", "access:equipment", "7")),
			},
		},
		"two records for one identity in one batch are refused": {
			batch: []StateRecord{
				referenceWrite("n1", "rb", reference("rb", "access:equipment", "5")),
				referenceWrite("n1", "re", reference("re", "access:equipment", "5")),
			},
			holder: "re",
		},
		"a record retargeted onto a held identity is refused": {
			batch:  []StateRecord{referenceWrite("n1", "ra", reference("ra", "access:equipment", "9"))},
			holder: "rc",
		},
		"a standing duplicate may be described": {
			batch: []StateRecord{referenceWrite("n1", "rd", described)},
		},
		"a standing duplicate may be removed": {
			batch: []StateRecord{referenceWrite("n1", "rd", nil)},
		},
		"a third record on a duplicated identity is refused": {
			batch:  []StateRecord{referenceWrite("n1", "rb", reference("rb", "access:equipment", "9"))},
			holder: "rc",
		},
		"a record that names no identity is the contract's to refuse": {
			batch: []StateRecord{referenceWrite("n1", "rb", map[string]any{"id": "rb"})},
		},
		"other contracts are not references": {
			batch: []StateRecord{{
				Topic:   "colca/v1/_Node/n1/_colca/nodes/n1",
				Payload: mustJSON(reference("rb", "access:equipment", "7")),
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := CheckExternalReferences(current, tc.batch)
			if tc.holder == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			var duplicate *ReferenceHeldError
			if !errors.As(err, &duplicate) || duplicate.Holder != tc.holder {
				t.Fatalf("err = %v, want the identity held by %s", err, tc.holder)
			}
		})
	}
}

func upsertReference(t *testing.T, c *ConfigExec, payload map[string]any) (int, string) {
	t.Helper()
	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "entity/upsert", body(t, map[string]any{
		"entities": []map[string]any{{"contract": "_ExternalReference", "entity": payload}},
	}))
	return code, msg
}

func referencesHeld(f *fakeStore) []string {
	var ids []string
	for _, rec := range f.KVScan("_ExternalReference", f.node) {
		var held identified
		if json.Unmarshal(rec.Payload, &held) == nil {
			ids = append(ids, held.ID)
		}
	}
	return ids
}

// The plant's two reference runs: the second one names the same entity,
// relationship and row under a new record id, because the caller had not yet
// seen the first. It is refused and names the record that stands.
func TestEntityUpsertRefusesASecondRecordForOneReference(t *testing.T) {
	c := newConfigExec(t)
	f := c.store.(*fakeStore)
	if code, msg := upsertReference(t, c, reference("ref-first", "access:equipment", "7")); code != 200 {
		t.Fatalf("first reference = %d %q", code, msg)
	}
	before := f.offset

	code, msg := upsertReference(t, c, reference("ref-second", "access:equipment", "7"))

	if code != 409 || msg != "entity/upsert: duplicate_external_reference: SystemElement el-line1 already holds "+
		"reference ref-first as access:equipment to row 7 of equipment — one reference per entity, relationship and row" {
		t.Fatalf("second reference = %d %q, want the 409 naming ref-first", code, msg)
	}
	if f.offset != before || len(referencesHeld(f)) != 1 {
		t.Fatalf("the refused reference wrote: offset %d from %d, held %v", f.offset, before, referencesHeld(f))
	}
	// The same record sent again is a rewrite, not a second reference.
	if code, msg := upsertReference(t, c, reference("ref-first", "access:equipment", "7")); code != 200 {
		t.Fatalf("re-sent reference = %d %q", code, msg)
	}
}

// However two writers of one reference interleave, the node ends with one
// record and tells every other writer so.
func TestConcurrentUpsertsOfOneReferenceLeaveOneRecord(t *testing.T) {
	c := newConfigExec(t)
	f := c.store.(*fakeStore)
	const n = 8
	var wg sync.WaitGroup
	codes := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], _, _ = c.Execute(asHuman, "_CmdConfigure", "entity/upsert", mustJSON(map[string]any{
				"entities": []map[string]any{{
					"contract": "_ExternalReference",
					"entity":   reference(fmt.Sprintf("ref-%d", i), "access:equipment", "7"),
				}},
			}))
		}(i)
	}
	wg.Wait()
	won := 0
	for _, code := range codes {
		switch code {
		case 200:
			won++
		case 409:
		default:
			t.Fatalf("codes = %v, want only 200 and 409", codes)
		}
	}
	if won != 1 || len(referencesHeld(f)) != 1 {
		t.Fatalf("codes = %v, held = %v, want exactly one record", codes, referencesHeld(f))
	}
}

// The Edit door's create writes the same record, so it is held to the same
// rule: a create for a reference another record already is answers 409.
func TestEditCreateRefusesASecondRecordForOneReference(t *testing.T) {
	f := newStore("n-edge1")
	seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{"id": "el-line1", "name": "Line 1"})
	seedEditEntity(t, f, "_ExternalReference", "_colca/external-references/ref-first",
		reference("ref-first", "access:equipment", "7"))
	exec := NewEditExec(f, nil)

	attributes := reference("ref-second", "access:equipment", "7")
	delete(attributes, "id")
	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", editBody(
		t, "op-create-duplicate", map[string]uint64{}, map[string]any{
			"type":       "create",
			"entity":     map[string]any{"kind": "external-reference", "id": "ref-second"},
			"attributes": attributes,
		},
	))

	if code != 409 || len(writes) != 0 || len(referencesHeld(f)) != 1 {
		t.Fatalf("duplicate create = %d %q writes=%+v held=%v", code, msg, writes, referencesHeld(f))
	}
	if !strings.HasPrefix(msg, "duplicate_external_reference: SystemElement el-line1 already holds reference ref-first") {
		t.Fatalf("duplicate create said %q, want it to name ref-first", msg)
	}
}

// A node that already holds two records for one reference: every edit of the
// entity has to version both, and the edit that keeps one removes the other.
func TestEditUpdateRemovesAStandingDuplicateReference(t *testing.T) {
	f := newStore("n-edge1")
	entityVersion := seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{"id": "el-line1", "name": "Line 1"})
	seedEditEntity(t, f, "_ExternalSystem", "tcdb", map[string]any{
		"id": "ext-tcdb", "key": "tcdb", "name": "TCDB", "system_type": "database",
	})
	keepVersion := seedEditEntity(t, f, "_ExternalReference", "_colca/external-references/ref-keep",
		reference("ref-keep", "access:equipment", "7"))
	duplicateVersion := seedEditEntity(t, f, "_ExternalReference", "_colca/external-references/ref-duplicate",
		reference("ref-duplicate", "access:equipment", "7"))
	exec := NewEditExec(f, nil)
	kept := reference("ref-keep", "access:equipment", "7")
	kept["client_id"], kept["version"] = "keep", fmt.Sprint(keepVersion)
	intent := map[string]any{
		"type": "update", "entity": map[string]any{"kind": "system-element", "id": "el-line1"},
		"external_references": []map[string]any{kept},
	}

	code, msg, _, _ := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", editBody(
		t, "op-unversioned-duplicate", map[string]uint64{
			"system-element:el-line1":     entityVersion,
			"external-reference:ref-keep": keepVersion,
		}, intent,
	))
	if code != 422 || msg != "update: missing_expected_version: external-reference:ref-duplicate" {
		t.Fatalf("edit without the duplicate's version = %d %q", code, msg)
	}

	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", editBody(
		t, "op-remove-duplicate", map[string]uint64{
			"system-element:el-line1":          entityVersion,
			"external-reference:ref-keep":      keepVersion,
			"external-reference:ref-duplicate": duplicateVersion,
		}, intent,
	))
	if code != 200 || len(writes) != 1 {
		t.Fatalf("remove duplicate = %d %q writes=%+v", code, msg, writes)
	}
	if held := referencesHeld(f); len(held) != 1 || held[0] != "ref-keep" {
		t.Fatalf("held = %v, want only ref-keep", held)
	}
}
