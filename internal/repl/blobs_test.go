package repl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/config"
)

func TestChildPushesABlobToItsParent(t *testing.T) {
	parent, child := newReplPair(t) // parent *Server, child *Client (pinned, enrolled)

	content := []byte("press 3 manual")
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])

	has, err := child.BlobHas(sha)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("parent reports a blob it was never given")
	}

	if err := child.BlobPut(sha, bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatal(err)
	}

	has, err = child.BlobHas(sha)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("parent does not report the blob it was just given")
	}
	if _, ok := parent.blobs.Has(sha); !ok {
		t.Fatal("the blob is not in the parent's store")
	}
}

func TestParentRefusesABlobWhoseContentDoesNotMatchItsDigest(t *testing.T) {
	parent, child := newReplPair(t)
	claimed := sha256.Sum256([]byte("claimed"))
	sha := hex.EncodeToString(claimed[:])

	err := child.BlobPut(sha, bytes.NewReader([]byte("actually something else")), 23)
	if err == nil {
		t.Fatal("want an error for mismatched content")
	}
	if _, ok := parent.blobs.Has(sha); ok {
		t.Fatal("the parent stored content that did not match its digest")
	}
	// Denominator: a correct push through the same path IS stored.
	good := []byte("correct")
	sum := sha256.Sum256(good)
	goodSHA := hex.EncodeToString(sum[:])
	if err := child.BlobPut(goodSHA, bytes.NewReader(good), int64(len(good))); err != nil {
		t.Fatal(err)
	}
	if _, ok := parent.blobs.Has(goodSHA); !ok {
		t.Fatal("parent.blobs.Has cannot see a stored blob")
	}
}

func TestParentRefusesAnUnenrolledChild(t *testing.T) {
	parent, addr := newReplServer(t)
	stranger := newUnenrolledClient(t, parent, addr) // valid TLS, not in the registry
	if _, err := stranger.BlobHas(strings.Repeat("a", 64)); err == nil {
		t.Fatal("an unenrolled child must not reach the blob door")
	}
}

// --- helpers -----------------------------------------------------------

// mustBlobStore opens a blob store under dir with the default cap.
func mustBlobStore(t *testing.T, dir string) *blobstore.Store {
	t.Helper()
	bs, err := blobstore.Open(dir, config.Limits{}.EffectiveMaxBlobBytes())
	if err != nil {
		t.Fatalf("open blob store %s: %v", dir, err)
	}
	return bs
}

// newReplServer brings up a parent repl server with a real blob store and one
// enrolled child (n-child, mount child1), returning the server and its
// resolved address.
func newReplServer(t *testing.T) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	blobs := mustBlobStore(t, filepath.Join(dir, "blobs"))

	srv, err := NewServer(pcfg, peng, parentID, preg, blobs, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	addr, err := srv.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(srv.Stop)
	return srv, addr
}

// newReplPair brings up a parent (as newReplServer) and returns a client
// pinned to it, presenting the enrolled child's identity.
func newReplPair(t *testing.T) (*Server, *Client) {
	t.Helper()
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	blobs := mustBlobStore(t, filepath.Join(dir, "blobs"))

	srv, err := NewServer(pcfg, peng, parentID, preg, blobs, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	addr, err := srv.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(srv.Stop)

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	return srv, cl
}

// newUnenrolledClient presents a fresh identity that was never enrolled in
// parent's registry — valid TLS (a self-signed cert pinning parent's key),
// but no registry entry to authorize it.
func newUnenrolledClient(t *testing.T, parent *Server, addr string) *Client {
	t.Helper()
	dir := t.TempDir()
	strangerID := mustIdentity(t, filepath.Join(dir, "stranger.key"))
	return mustClient(t, addr, parent.id.PublicHex(), strangerID)
}
