package uns

import (
	"strings"
	"testing"
)

// heldBlobs is a node that already has every blob asked of it. The resource
// intent's blob invariant has its own tests below; the authorization tests
// must not fail for want of bytes.
type heldBlobs struct {
	pulled []string
	absent bool
}

func (b *heldBlobs) Has(string) bool { return !b.absent }
func (b *heldBlobs) Pull(sha string) error {
	b.pulled = append(b.pulled, sha)
	return nil
}

func resourcePayload(id, element string) map[string]any {
	return map[string]any{
		"id": id, "system_element_id": element, "filename": "datasheet.pdf",
		"content_type": "application/pdf", "sha256": testSHA, "size_bytes": 12,
	}
}

func resourceIntent(t *testing.T, op, action, path string, extra map[string]any) []byte {
	t.Helper()
	intent := map[string]any{
		"type": "resource", "action": action, "path": path,
		"entity": map[string]any{"kind": "resource", "id": "res-1"},
	}
	for k, v := range extra {
		intent[k] = v
	}
	return editBody(t, op, map[string]uint64{}, intent)
}

func resourceExec(t *testing.T) (*fakeStore, *EditExec) {
	t.Helper()
	f, exec, _ := twoLines(t)
	exec.SetBlobs(&heldBlobs{})
	return f, exec
}

// The claim §G exists for: a resource command is authorized at the element the
// resource sits on, as the person, before anything is written.
func TestEditResourceIsAuthorizedAtItsElement(t *testing.T) {
	f, exec := resourceExec(t)
	anna := scopedTo("el-line1")
	before := f.offset

	outside := resourceIntent(t, "op-res-out", "create", "line2/res-1", map[string]any{
		"resource": resourcePayload("res-1", "el-line2"),
	})
	code, message, result, writes := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", outside)
	if code != 409 || result != "conflict" || len(writes) != 0 {
		t.Fatalf("create outside the grant = %d %q %q writes=%d, want a refusal with nothing written",
			code, message, result, len(writes))
	}
	if !strings.HasPrefix(message, "entity_not_found:") {
		t.Fatalf("refusal = %q, want the not-found shape that keeps the existence oracle closed", message)
	}
	if f.offset != before {
		t.Fatalf("a refused resource plan wrote: offset %d → %d", before, f.offset)
	}

	inside := resourceIntent(t, "op-res-in", "create", "line1/res-1", map[string]any{
		"resource": resourcePayload("res-1", "el-line1"),
	})
	if code, message, _, writes = exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", inside); code != 200 || len(writes) != 1 {
		t.Fatalf("create inside the grant = %d %q writes=%d", code, message, len(writes))
	}
}

// A move is one command over two positions, and BOTH are authorized. Checking
// only the destination would let a person lift a resource out of a zone they
// may not write.
func TestEditResourceMoveIsAuthorizedAtBothPositions(t *testing.T) {
	f, exec := resourceExec(t)
	full := CommandContext{Actor: &Entry{ULID: "kc-admin", Kind: KindHuman, Grants: []string{"cmd:#:configure"}}}
	seed := resourceIntent(t, "op-seed", "create", "line2/res-1", map[string]any{
		"resource": resourcePayload("res-1", "el-line2"),
	})
	if code, message, _, _ := exec.ExecuteWithWrites(full, "_CmdEdit", "apply", seed); code != 200 {
		t.Fatalf("seeding the resource under line2 = %d %q", code, message)
	}

	// anna holds line1 (the destination) and not line2 (the origin).
	anna := scopedTo("el-line1")
	before := f.offset
	move := resourceIntent(t, "op-res-move", "update", "line1/res-1", map[string]any{
		"resource": resourcePayload("res-1", "el-line1"), "from_path": "line2/res-1",
	})
	code, message, _, writes := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", move)
	if code != 409 || len(writes) != 0 {
		t.Fatalf("move out of a zone she may not write = %d %q writes=%d, want a refusal", code, message, len(writes))
	}
	if f.offset != before {
		t.Fatalf("a refused move wrote: offset %d → %d", before, f.offset)
	}

	// The same move, by someone who holds both, writes both records at once:
	// the new position and the tombstone at the old one.
	code, message, _, writes = exec.ExecuteWithWrites(full, "_CmdEdit", "apply",
		resourceIntent(t, "op-res-move-ok", "update", "line1/res-1", map[string]any{
			"resource": resourcePayload("res-1", "el-line1"), "from_path": "line2/res-1",
		}))
	if code != 200 || len(writes) != 2 {
		t.Fatalf("move by a fully scoped person = %d %q writes=%d, want both positions in one batch",
			code, message, len(writes))
	}
}

// The invariant `ConfigExec` holds, held here too: never author a record
// pointing at bytes this node cannot produce.
func TestEditResourceRefusesWhenTheBlobIsUnreachable(t *testing.T) {
	f, exec := resourceExec(t)
	blobs := &heldBlobs{absent: true}
	exec.SetBlobs(blobs)
	full := CommandContext{Actor: &Entry{ULID: "kc-admin", Kind: KindHuman, Grants: []string{"cmd:#:configure"}}}
	before := f.offset

	code, message, result, writes := exec.ExecuteWithWrites(full, "_CmdEdit", "apply",
		resourceIntent(t, "op-res-blob", "create", "line1/res-1", map[string]any{
			"resource": resourcePayload("res-1", "el-line1"),
		}))

	if result != "blob_unreachable" || len(writes) != 0 {
		t.Fatalf("create with no blob = %d %q %q writes=%d, want blob_unreachable and nothing written",
			code, message, result, len(writes))
	}
	if f.offset != before {
		t.Fatalf("a blob-less create wrote: offset %d → %d", before, f.offset)
	}
	if len(blobs.pulled) != 1 || blobs.pulled[0] != testSHA {
		t.Fatalf("pulled = %v, want exactly one attempt to fetch %s from an ancestor", blobs.pulled, testSHA)
	}
}

// Two resources cannot share one position — the same rule the configure verb
// enforces, so the two doors cannot disagree about what a position holds.
func TestEditResourceRefusesASecondResourceAtOnePosition(t *testing.T) {
	_, exec := resourceExec(t)
	full := CommandContext{Actor: &Entry{ULID: "kc-admin", Kind: KindHuman, Grants: []string{"cmd:#:configure"}}}
	first := resourceIntent(t, "op-res-a", "create", "line1/res-1", map[string]any{
		"resource": resourcePayload("res-1", "el-line1"),
	})
	if code, message, _, _ := exec.ExecuteWithWrites(full, "_CmdEdit", "apply", first); code != 200 {
		t.Fatalf("first resource = %d %q", code, message)
	}

	second := editBody(t, "op-res-b", map[string]uint64{}, map[string]any{
		"type": "resource", "action": "create", "path": "line1/res-1",
		"entity":   map[string]any{"kind": "resource", "id": "res-2"},
		"resource": resourcePayload("res-2", "el-line1"),
	})
	code, message, result, writes := exec.ExecuteWithWrites(full, "_CmdEdit", "apply", second)
	if code != 409 || result != "conflict" || len(writes) != 0 {
		t.Fatalf("second resource at one position = %d %q %q writes=%d, want a conflict",
			code, message, result, len(writes))
	}
	if !strings.Contains(message, "res-1") {
		t.Fatalf("refusal = %q, want it to name the resource already there", message)
	}
}

// A delete tombstones the record and is authorized at the position it clears.
func TestEditResourceDeleteIsAuthorizedAndTombstones(t *testing.T) {
	f, exec := resourceExec(t)
	full := CommandContext{Actor: &Entry{ULID: "kc-admin", Kind: KindHuman, Grants: []string{"cmd:#:configure"}}}
	if code, message, _, _ := exec.ExecuteWithWrites(full, "_CmdEdit", "apply",
		resourceIntent(t, "op-res-seed", "create", "line2/res-1", map[string]any{
			"resource": resourcePayload("res-1", "el-line2"),
		})); code != 200 {
		t.Fatalf("seed = %d %q", code, message)
	}

	anna := scopedTo("el-line1")
	del := resourceIntent(t, "op-res-del-out", "delete", "line2/res-1", nil)
	if code, message, _, writes := exec.ExecuteWithWrites(anna, "_CmdEdit", "apply", del); code != 409 || len(writes) != 0 {
		t.Fatalf("delete outside the grant = %d %q writes=%d", code, message, len(writes))
	}

	code, message, _, writes := exec.ExecuteWithWrites(full, "_CmdEdit", "apply",
		resourceIntent(t, "op-res-del", "delete", "line2/res-1", nil))
	if code != 200 || len(writes) != 1 {
		t.Fatalf("delete = %d %q writes=%d", code, message, len(writes))
	}
	// The tombstone lands at the resource's own position — a StateWrite
	// carries where it went, and the empty payload is read back from the store.
	if held, ok := f.KVGet(writes[0].Topic); ok && len(held) != 0 {
		t.Fatalf("delete left %d bytes at %s, want the position empty", len(held), writes[0].Topic)
	}
}
