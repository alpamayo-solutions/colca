package blobgc

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// newBody returns fresh content for blobs.Put — distinct literal text per
// call is enough to give each test blob its own digest.
func newBody(t *testing.T, content string) io.Reader {
	t.Helper()
	return bytes.NewReader([]byte(content))
}

const nodeULID = "n1"

// sweepParts builds a sweeper on a real store, engine and blob store. grace is
// the sweeper's grace period; tests move the sweeper's now to age blobs without
// sleeping.
func sweepParts(t *testing.T, grace time.Duration) (*engine.Engine, *blobstore.Store, *Sweeper) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg, err := registry.New(st, nodeULID)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{ULID: nodeULID}
	eng := engine.New(st, cfg, reg, nil, nil, nil)
	reg.SetNamespace(eng.Elements())

	blobs, err := blobstore.Open(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}

	g := config.Duration(grace)
	sweeper := NewSweeper(blobs, eng, config.BlobGC{Grace: &g}, nil, nodeULID)
	return eng, blobs, sweeper
}

// authorResource places an element at elementPath and publishes a _Resource
// record naming id, sha and size there, on the topic ConfigExec.resourceTopic
// builds.
func authorResource(t *testing.T, eng *engine.Engine, elementPath, id, sha string, size int64) {
	t.Helper()
	elementID := authtest.Place(t, eng, elementPath)
	topic := "colca/v1/_Resource/" + nodeULID + "/" + elementPath + "/" + id
	payload, err := json.Marshal(map[string]any{
		"id":                id,
		"system_element_id": elementID,
		"filename":          "m.pdf",
		"content_type":      "application/pdf",
		"size_bytes":        size,
		"sha256":            sha,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.IngestAdmin(topic, payload); err != nil {
		t.Fatalf("author resource at %s: %v", topic, err)
	}
}

// tombstoneResource retires the resource authorResource published at
// elementPath/id with an empty payload on the same topic.
func tombstoneResource(t *testing.T, eng *engine.Engine, elementPath, id string) {
	t.Helper()
	topic := "colca/v1/_Resource/" + nodeULID + "/" + elementPath + "/" + id
	if _, err := eng.IngestAdmin(topic, nil); err != nil {
		t.Fatalf("tombstone resource at %s: %v", topic, err)
	}
}

func mustHave(t *testing.T, blobs *blobstore.Store, sha string, want bool) {
	t.Helper()
	if _, got := blobs.Has(sha); got != want {
		t.Fatalf("blobs.Has(%s) = %v, want %v", sha, got, want)
	}
}

// TestSweepKeepsAReferencedBlob: a blob a live _Resource names survives whatever
// its age; grace only matters for unreferenced blobs.
func TestSweepKeepsAReferencedBlob(t *testing.T) {
	eng, blobs, sweeper := sweepParts(t, time.Hour)
	sha, size, err := blobs.Put(newBody(t, "referenced"), "")
	if err != nil {
		t.Fatal(err)
	}
	authorResource(t, eng, "press3", "r1", sha, size)

	// Age well past the grace so a bug that only checks age (and not
	// liveness) would wrongly delete this blob too.
	sweeper.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	sweeper.runOnce()

	mustHave(t, blobs, sha, true)
}

// TestSweepDeletesAnUnreferencedBlobPastTheGrace: a referenced blob in the same
// sweep survives, so the deletion is not the sweep deleting everything.
func TestSweepDeletesAnUnreferencedBlobPastTheGrace(t *testing.T) {
	eng, blobs, sweeper := sweepParts(t, time.Hour)
	liveSHA, liveSize, err := blobs.Put(newBody(t, "referenced"), "")
	if err != nil {
		t.Fatal(err)
	}
	authorResource(t, eng, "press3", "r1", liveSHA, liveSize)

	orphanSHA, _, err := blobs.Put(newBody(t, "nothing points at this"), "")
	if err != nil {
		t.Fatal(err)
	}

	sweeper.now = func() time.Time { return time.Now().Add(2 * time.Hour) } // past the 1h grace
	sweeper.runOnce()

	mustHave(t, blobs, orphanSHA, false)
	mustHave(t, blobs, liveSHA, true) // denominator: the sweep did not delete everything
}

// TestSweepSparesAYoungUnreferencedBlob: an unreferenced blob newer than the
// grace survives, since it may still be waiting for its record. The same blob
// aged past the grace is deleted, so the survival was the grace window.
func TestSweepSparesAYoungUnreferencedBlob(t *testing.T) {
	// No resource is ever authored for this blob, so only the grace window explains
	// its survival.
	_, blobs, sweeper := sweepParts(t, time.Hour)
	sha, _, err := blobs.Put(newBody(t, "just uploaded"), "")
	if err != nil {
		t.Fatal(err)
	}

	sweeper.now = time.Now // no aging at all: well inside the 1h grace
	sweeper.runOnce()
	mustHave(t, blobs, sha, true)

	sweeper.now = func() time.Time { return time.Now().Add(2 * time.Hour) } // now past the grace
	sweeper.runOnce()
	mustHave(t, blobs, sha, false)
}

// TestSweepReclaimsAfterATombstone: a retired resource gives its blob back once
// nothing live references it and the grace has passed.
func TestSweepReclaimsAfterATombstone(t *testing.T) {
	eng, blobs, sweeper := sweepParts(t, time.Hour)
	sha, size, err := blobs.Put(newBody(t, "will be retired"), "")
	if err != nil {
		t.Fatal(err)
	}
	authorResource(t, eng, "press3", "r1", sha, size)

	// Still referenced: even aged past the grace, the blob survives.
	sweeper.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	sweeper.runOnce()
	mustHave(t, blobs, sha, true)

	tombstoneResource(t, eng, "press3", "r1")

	// No longer referenced, and already past the grace: reclaimed.
	sweeper.runOnce()
	mustHave(t, blobs, sha, false)
}

// TestSweepNeverDeletesOnAFailedScan: a records() failure must never read as
// "nothing is referenced", which would delete blobs a working scan shows as
// live. With a working read swapped in, the same orphan past its grace is
// deleted, so the sweeper is not simply broken.
func TestSweepNeverDeletesOnAFailedScan(t *testing.T) {
	_, blobs, sweeper := sweepParts(t, time.Hour)
	sha, _, err := blobs.Put(newBody(t, "orphan, well past the grace"), "")
	if err != nil {
		t.Fatal(err)
	}
	sweeper.now = func() time.Time { return time.Now().Add(2 * time.Hour) } // past the 1h grace throughout

	sweeper.records = func() ([]uns.KVRecord, error) {
		return nil, errors.New("simulated KV scan failure")
	}
	sweeper.runOnce()
	mustHave(t, blobs, sha, true) // a failed scan must never be read as "nothing referenced this"

	sweeper.records = func() ([]uns.KVRecord, error) { return nil, nil } // working read: genuinely nothing live
	sweeper.runOnce()
	mustHave(t, blobs, sha, false) // denominator: the same orphan IS deleted once the scan actually works
}
