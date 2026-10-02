package uns

import (
	"strings"
	"testing"
)

// A caller that supplies a document's version gets it checked, like every
// other thing a command can destroy.
//
// `editKinds` — what `snapshot()` reads to build the version map — lists
// elements, signals, constants, nodes and external references. Not resources:
// a resource intent addresses its record by path and reads the store itself,
// so nothing here needed them. But a caller about to destroy a document sends
// `resource:<id>` among its expected versions anyway, and a cascade delete of
// an element does exactly that. With no entry in the map, every such key
// failed validation as "no longer exists" — while the record sat in the
// store, readable, at the path the message did not mention.
//
// The effect was that a subtree holding a single document could not be
// deleted, on any node, ever, and the operator was told the document was
// already gone.

func seedResource(t *testing.T, f *fakeStore, path, id string) uint64 {
	t.Helper()
	return seedEditEntity(t, f, "_Resource", path, resourcePayload(id, "el-line1"))
}

func TestASuppliedResourceVersionResolves(t *testing.T) {
	f, exec := resourceExec(t)
	version := seedResource(t, f, "line1/res-9", "res-9")

	// Delete the document, naming the version the caller believes it has.
	intent := editBody(t, "op-res-ver", map[string]uint64{"resource:res-9": version}, map[string]any{
		"type": "resource", "action": "delete", "path": "line1/res-9",
		"entity": map[string]any{"kind": "resource", "id": "res-9"},
	})
	code, message, result, writes := exec.ExecuteWithWrites(scopedTo("el-line1"), "_CmdEdit", "apply", intent)
	if code != 200 {
		t.Fatalf("delete with the right version = %d %q %q, want 200 (the record is in the store)",
			code, message, result)
	}
	if len(writes) != 1 {
		t.Fatalf("writes = %d, want the one tombstone", len(writes))
	}
}

func TestAStaleResourceVersionIsStillAConflict(t *testing.T) {
	// The denominator. If the version simply stopped being checked, the test
	// above would pass for the wrong reason and a document replaced under the
	// operator would be destroyed silently.
	f, exec := resourceExec(t)
	version := seedResource(t, f, "line1/res-8", "res-8")

	intent := editBody(t, "op-res-stale", map[string]uint64{"resource:res-8": version + 1}, map[string]any{
		"type": "resource", "action": "delete", "path": "line1/res-8",
		"entity": map[string]any{"kind": "resource", "id": "res-8"},
	})
	code, message, _, writes := exec.ExecuteWithWrites(scopedTo("el-line1"), "_CmdEdit", "apply", intent)
	if code != 409 || len(writes) != 0 {
		t.Fatalf("delete with a wrong version = %d %q writes=%d, want a refusal with nothing written",
			code, message, len(writes))
	}
	if !strings.Contains(message, "expected") {
		t.Fatalf("refusal = %q, want the mismatched-version wording, not the missing-record one", message)
	}
}

func TestAResourceVersionForSomethingAbsentIsStillAConflict(t *testing.T) {
	// The other denominator: "no longer exists" must keep meaning that.
	_, exec := resourceExec(t)

	intent := editBody(t, "op-res-gone", map[string]uint64{"resource:res-none": 42}, map[string]any{
		"type": "resource", "action": "delete", "path": "line1/res-none",
		"entity": map[string]any{"kind": "resource", "id": "res-none"},
	})
	code, message, _, writes := exec.ExecuteWithWrites(scopedTo("el-line1"), "_CmdEdit", "apply", intent)
	if code != 409 || len(writes) != 0 {
		t.Fatalf("delete of an absent resource = %d %q writes=%d, want a refusal", code, message, len(writes))
	}
	if !strings.Contains(message, "no longer exists") {
		t.Fatalf("refusal = %q, want the missing-record wording", message)
	}
}
