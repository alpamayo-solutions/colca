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
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
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

// The 413 path in handleBlobPut used to write the response and
// return before the shared BlobTransfer("receive","error") call below the
// switch, so a real oversize rejection — the only reachable "too large"
// outcome on this door, since http.MaxBytesReader always trips before
// blobstore.ErrTooLarge ever could — was counted nowhere. This pins that both
// counters move, with a denominator on each side: an in-cap push first (so a
// silently-never-firing metric cannot pass by looking unchanged), then the
// rejected push, then a check that the "ok" counter did NOT also move.
func TestParentRefusesAnOversizeBlobAndCountsIt(t *testing.T) {
	const cap = 16 // bytes
	parent, child, pm := newReplPairWithCap(t, cap)

	good := []byte("small enough") // 12 bytes, under cap
	sum := sha256.Sum256(good)
	goodSHA := hex.EncodeToString(sum[:])
	if err := child.BlobPut(goodSHA, bytes.NewReader(good), int64(len(good))); err != nil {
		t.Fatalf("a within-cap push must succeed: %v", err)
	}
	if _, ok := parent.blobs.Has(goodSHA); !ok {
		t.Fatal("the within-cap blob was not stored")
	}
	if got := metricstest.Value(t, pm, `colca_blob_transfers_total{direction="receive",result="ok"}`); got != 1 {
		t.Fatalf(`colca_blob_transfers_total{direction="receive",result="ok"} = %v, want 1`, got)
	}

	big := bytes.Repeat([]byte("x"), cap*4)
	sumBig := sha256.Sum256(big)
	bigSHA := hex.EncodeToString(sumBig[:])
	err := child.BlobPut(bigSHA, bytes.NewReader(big), int64(len(big)))
	if err == nil {
		t.Fatal("want an error for an over-cap push")
	}
	if !strings.Contains(err.Error(), "413") {
		t.Fatalf("want a 413-shaped error, got: %v", err)
	}
	if _, ok := parent.blobs.Has(bigSHA); ok {
		t.Fatal("the parent stored an over-cap blob")
	}
	if got := metricstest.Value(t, pm, `colca_blob_transfers_total{direction="receive",result="error"}`); got != 1 {
		t.Fatalf(`colca_blob_transfers_total{direction="receive",result="error"} = %v, want 1 — the 413 path must count`, got)
	}
	if got := metricstest.Value(t, pm, `colca_blob_rejects_total{reason="too_large"}`); got != 1 {
		t.Fatalf(`colca_blob_rejects_total{reason="too_large"} = %v, want 1`, got)
	}
	// The "ok" counter from the earlier push must not have moved.
	if got := metricstest.Value(t, pm, `colca_blob_transfers_total{direction="receive",result="ok"}`); got != 1 {
		t.Fatalf(`colca_blob_transfers_total{direction="receive",result="ok"} = %v after the rejected push, want unchanged at 1`, got)
	}
}

// --- helpers -----------------------------------------------------------

// mustBlobStore opens a blob store under dir with the given cap.
func mustBlobStore(t *testing.T, dir string, maxBytes uint64) *blobstore.Store {
	t.Helper()
	bs, err := blobstore.Open(dir, maxBytes)
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
	blobs := mustBlobStore(t, filepath.Join(dir, "blobs"), pcfg.Limits.EffectiveMaxBlobBytes())

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
	blobs := mustBlobStore(t, filepath.Join(dir, "blobs"), pcfg.Limits.EffectiveMaxBlobBytes())

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

// newReplPairWithCap is newReplPair with an explicit max_blob_bytes and a
// real *metrics.Metrics wired in, so a caller can read counters back with
// metricstest.Value. The blob store is opened from the SAME config value the
// door computes its own cap from — exactly like node.Start wires a real
// deployment (cfg.Limits.EffectiveMaxBlobBytes() feeds both blobstore.Open and,
// via handleBlobPut, http.MaxBytesReader) — so the 413 path is reachable here
// the same way it is in production.
func newReplPairWithCap(t *testing.T, maxBlobBytes uint64) (*Server, *Client, *metrics.Metrics) {
	t.Helper()
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{
		ULID:   "n-parent",
		Repl:   config.Endpoint{Addr: "127.0.0.1:0"},
		Limits: config.Limits{MaxBlobBytes: config.ByteSize(maxBlobBytes)},
	}
	preg, peng := nodeParts(t, ps, pcfg, nil, nil, nil, childSpec{"n-child", childID.PublicHex(), "child1"})
	blobs := mustBlobStore(t, filepath.Join(dir, "blobs"), pcfg.Limits.EffectiveMaxBlobBytes())
	pm := metrics.New(ps, config.Retention{}, nil)

	srv, err := NewServer(pcfg, peng, parentID, preg, blobs, pm)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	addr, err := srv.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(srv.Stop)

	cl := mustClient(t, addr, parentID.PublicHex(), childID)
	return srv, cl, pm
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
