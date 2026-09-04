package uns

import (
	"encoding/json"
	"strings"
	"testing"
)

// autobindOneTag provisions a single-tag catalogue through the real
// `signal/autobind` verb and reports what it did with that tag. When heldBy is
// given, a curated signal of that id already holds the tag before the run.
func autobindOneTag(t *testing.T, heldBy string) (created, skipped int) {
	t.Helper()
	c := newConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-1", "")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/opcua-1", tags("t1"))
	if heldBy != "" {
		f, ok := c.store.(*fakeStore)
		if !ok {
			t.Fatalf("autobindOneTag: %T is not a fakeStore", c.store)
		}
		f.records["colca/v1/_Signal/n1/curated"] = mustJSON(map[string]any{
			"id": heldBy, "name": "curated", "data_tag": "t1", "is_published": true,
		})
	}

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind",
		body(t, map[string]any{"connector": "01JCONN"}))
	if code != 200 {
		t.Fatalf("autobind = %d %q, want 200 — provisioning never refuses over a binding", code, msg)
	}
	var outcome struct {
		Created int `json:"created"`
		Skipped int `json:"skipped"`
	}
	if err := json.Unmarshal([]byte(msg), &outcome); err != nil {
		t.Fatalf("autobind outcome %q is unreadable: %v", msg, err)
	}
	return outcome.Created, outcome.Skipped
}

// editBindOneTag curates a single binding of the same tag through the real
// `_CmdEdit apply` door. When heldBy is given, a curated signal of that id
// already holds the tag before the edit.
func editBindOneTag(t *testing.T, heldBy string) (code int, message string) {
	t.Helper()
	f := newStore("n-edge1")
	parentVersion := seedEditEntity(t, f, "_SystemElement", "line1", map[string]any{
		"id": "el-line1", "name": "Line 1",
	})
	catalogueVersion := seedEditEntity(t, f, "_DataTags", "connector-1", map[string]any{
		"connector": "connector-1",
		"data_tags": []map[string]any{
			{"id": "t1", "name": "Temperature", "data_type": "float", "is_stale": false},
		},
	})
	expected := map[string]uint64{
		"catalogue:connector-1":   catalogueVersion,
		"system-element:el-line1": parentVersion,
	}
	if heldBy != "" {
		expected["signal:"+heldBy] = seedEditEntity(t, f, "_Signal", "line1/curated", map[string]any{
			"id": heldBy, "name": "Curated", "system_element_id": "el-line1",
			"data_tag": "t1", "data_type": "float",
		})
	}

	payload := editBody(t, "op-binding-agreement", expected, map[string]any{
		"type": "binding", "connector_id": "connector-1",
		"operations": []map[string]any{{
			"id": "bind-t1", "kind": "create_signal_and_bind", "tag_id": "t1",
			"signal_id": "sig-new", "parent_id": "el-line1", "name": "Temperature",
		}},
	})
	code, message, _, _ = NewEditExec(f, nil).ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	return code, message
}

// The tag↔signal binding invariant has ONE owner — signalBindings — and two
// operations that react to its verdict differently on purpose: `signal/autobind`
// provisions a whole catalogue and SKIPS what it may not bind, so a replay or a
// lifecycle trigger changes nothing; the edit curates one edit and REFUSES
// it with a 409, so the person is told what stands in the way.
//
// This poses the same situations to both real doors and asserts each one's
// outcome against the single verdict the owner gives. Nothing here restates the
// rule: the expectation for both paths is derived from propose. If either path
// stops following the owner — or starts spelling a rule of its own again — the
// two answers diverge and this goes red.
func TestBothBindingPathsFollowTheOneBindingRule(t *testing.T) {
	for _, situation := range []struct {
		name   string
		heldBy string // the signal already holding tag t1, "" if nothing does
	}{
		{name: "tag is free", heldBy: ""},
		{name: "tag is already held by another signal", heldBy: "sig-curated"},
	} {
		t.Run(situation.name, func(t *testing.T) {
			// The owner's verdict for this situation, asked exactly once. Both
			// operations propose a signal that does not hold anything yet — a
			// freshly minted one for autobind, "sig-new" for the edit — so
			// the situation, and therefore the verdict, is the same for both.
			bindings := newSignalBindings()
			bindings.bind("t1", situation.heldBy)
			mayBind := bindings.propose("t1", "sig-new") == bindFree

			created, skipped := autobindOneTag(t, situation.heldBy)
			if mayBind && (created != 1 || skipped != 0) {
				t.Errorf("autobind created=%d skipped=%d, but the binding rule says t1 may bind"+
					" — provisioning disagrees with the invariant's owner", created, skipped)
			}
			if !mayBind && (created != 0 || skipped != 1) {
				t.Errorf("autobind created=%d skipped=%d, but the binding rule says t1 may NOT bind"+
					" — provisioning overwrote a binding the edit would refuse", created, skipped)
			}

			code, message := editBindOneTag(t, situation.heldBy)
			if mayBind && code != 200 {
				t.Errorf("edit binding = %d %q, but the binding rule says t1 may bind"+
					" — curating refuses what provisioning would create", code, message)
			}
			if !mayBind {
				if code != 409 {
					t.Errorf("edit binding = %d %q, but the binding rule says t1 may NOT bind"+
						" — curating accepted what provisioning skips", code, message)
				} else if !strings.Contains(message, situation.heldBy) {
					t.Errorf("edit refusal %q does not name the signal holding the tag", message)
				}
			}
		})
	}
}

// The extra questions the edit asks — is the tag stale, does its datatype
// fit the signal — are that operation's own, not the shared invariant. They must
// not leak into provisioning, which has no person to tell and must stay
// idempotent: a stale tag with an unrelated datatype is still bound.
func TestProvisioningDoesNotInheritTheEditOnlyChecks(t *testing.T) {
	c := newConfigExec(t)
	bindEntry(t, c, "01JCONN", "opcua-1", "")
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/opcua-1", []map[string]any{
		{"id": "t1", "name": "Temperature", "data_type": "string", "is_stale": true},
	})

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind",
		body(t, map[string]any{"connector": "01JCONN"}))

	if code != 200 || !strings.Contains(msg, `"created":1`) {
		t.Fatalf("autobind = %d %q, want the stale tag bound — staleness is a edit"+
			" concern and must not have leaked into provisioning", code, msg)
	}
}
