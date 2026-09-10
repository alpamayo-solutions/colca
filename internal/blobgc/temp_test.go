package blobgc

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// plantTemp writes an unfinished-upload file of the shape blobstore.Put uses
// and backdates it, standing in for a process killed mid-transfer.
func plantTemp(t *testing.T, dir, name string, age time.Duration) string {
	t.Helper()
	path := filepath.Join(dir, ".incoming-"+name+".tmp")
	if err := os.WriteFile(path, []byte("half an upload"), 0o600); err != nil {
		t.Fatal(err)
	}
	when := time.Now().Add(-age)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatal(err)
	}
	return path
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	return err == nil
}

// A process killed mid-Put leaves a .incoming-*.tmp file with no digest, which
// nothing else would ever reclaim. The grace is 0 on purpose: an unfinished
// upload must not inherit the blob grace, or every transfer in flight would be
// deleted, and the fresh temp file surviving pins that. The two finished blobs
// show the sweep actually ran.
func TestSweepReclaimsAbandonedUploadsWithoutTouchingLiveTransfers(t *testing.T) {
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

	blobDir := t.TempDir()
	blobs, err := blobstore.Open(blobDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	// grace 0: "sweep unreferenced blobs immediately", the operator setting
	// that makes the temp-file rule's independence observable.
	grace := config.Duration(0)
	sweeper := NewSweeper(blobs, eng, config.BlobGC{Grace: &grace}, nil, nodeULID)

	liveSHA, liveSize, err := blobs.Put(newBody(t, "referenced content"), "")
	if err != nil {
		t.Fatal(err)
	}
	authorResource(t, eng, "line1", "r-live", liveSHA, liveSize)
	deadSHA, _, err := blobs.Put(newBody(t, "unreferenced content"), "")
	if err != nil {
		t.Fatal(err)
	}

	stale := plantTemp(t, blobDir, "stale", 2*time.Hour)
	inflight := plantTemp(t, blobDir, "inflight", 0)

	sweeper.runOnce()

	if exists(t, stale) {
		t.Errorf("an upload abandoned 2h ago survived the sweep (%s) — nothing else can ever "+
			"see it, so it stays until the disk fills", stale)
	}
	if !exists(t, inflight) {
		t.Errorf("the sweep deleted a temp file being written right now (%s) — an unfinished "+
			"upload must not inherit the blob grace period", inflight)
	}
	if _, ok := blobs.Has(liveSHA); !ok {
		t.Errorf("the sweep deleted a referenced blob %s", liveSHA)
	}
	if _, ok := blobs.Has(deadSHA); ok {
		t.Errorf("the sweep left the unreferenced blob %s behind — it swept nothing, so the "+
			"assertions above prove nothing", deadSHA)
	}
}
