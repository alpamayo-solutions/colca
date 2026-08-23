package repl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

func TestSyncPushesOnlyWhatTheParentLacks(t *testing.T) {
	parent, child := newReplPair(t)
	childBlobs := newBlobStore(t)

	first, _, err := childBlobs.Put(bytes.NewReader([]byte("one")), "")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := childBlobs.Put(bytes.NewReader([]byte("two")), "")
	if err != nil {
		t.Fatal(err)
	}

	confirmed := map[string]bool{}
	if pushed := syncBlobs(child, childBlobs, nil, confirmed); pushed != 2 {
		t.Fatalf("first pass pushed %d, want 2", pushed)
	}
	if _, ok := parent.blobs.Has(first); !ok {
		t.Fatal("parent is missing the first blob")
	}
	if _, ok := parent.blobs.Has(second); !ok {
		t.Fatal("parent is missing the second blob")
	}

	if pushed := syncBlobs(child, childBlobs, nil, confirmed); pushed != 0 {
		t.Fatalf("second pass pushed %d, want 0 — confirmed blobs must not be re-sent", pushed)
	}

	third, _, err := childBlobs.Put(bytes.NewReader([]byte("three")), "")
	if err != nil {
		t.Fatal(err)
	}
	if pushed := syncBlobs(child, childBlobs, nil, confirmed); pushed != 1 {
		t.Fatalf("third pass pushed %d, want 1 — a new blob must be sent", pushed)
	}
	if _, ok := parent.blobs.Has(third); !ok {
		t.Fatal("parent is missing the third blob")
	}
}

func TestSyncSkipsWhatTheParentAlreadyHas(t *testing.T) {
	parent, child := newReplPair(t)
	childBlobs := newBlobStore(t)
	content := []byte("a sibling already sent this")
	sha, _, err := childBlobs.Put(bytes.NewReader(content), "")
	if err != nil {
		t.Fatal(err)
	}
	// The parent got it from elsewhere — content addressing makes it the same blob.
	if _, _, err := parent.blobs.Put(bytes.NewReader(content), ""); err != nil {
		t.Fatal(err)
	}
	if pushed := syncBlobs(child, childBlobs, nil, map[string]bool{}); pushed != 0 {
		t.Fatalf("pushed %d, want 0", pushed)
	}
	if _, ok := parent.blobs.Has(sha); !ok {
		t.Fatal("the parent lost the blob it already had")
	}
}

func TestSyncSurvivesAnUnreachableParent(t *testing.T) {
	parent, child := newReplPair(t)
	childBlobs := newBlobStore(t)
	first, _, err := childBlobs.Put(bytes.NewReader([]byte("pending")), "")
	if err != nil {
		t.Fatal(err)
	}

	// Denominator, in this same test: prove the parent is reachable and
	// syncBlobs actually pushes over this wiring BEFORE it goes away. Without
	// this, a zero-pushed / zero-confirmed result below is equally consistent
	// with a parent that was never reachable in the first place, or with
	// syncBlobs silently never running at all.
	confirmed := map[string]bool{}
	if pushed := syncBlobs(child, childBlobs, nil, confirmed); pushed != 1 {
		t.Fatalf("first pass (parent up) pushed %d, want 1", pushed)
	}
	if !confirmed[first] {
		t.Fatal("first pass (parent up) did not confirm the blob it pushed")
	}
	if _, ok := parent.blobs.Has(first); !ok {
		t.Fatal("parent is missing the blob from the first pass")
	}

	parent.Stop() // close the parent listener: the child now has a dead parent
	second, _, err := childBlobs.Put(bytes.NewReader([]byte("added after the parent died")), "")
	if err != nil {
		t.Fatal(err)
	}
	if pushed := syncBlobs(child, childBlobs, nil, confirmed); pushed != 0 {
		t.Fatalf("pushed %d against a dead parent, want 0", pushed)
	}
	if confirmed[second] {
		t.Fatal("a failed push must not be recorded as confirmed")
	}
	if len(confirmed) != 1 {
		t.Fatalf("confirmed = %v, want only the pre-outage blob still confirmed", confirmed)
	}
}

// A blob the parent will NEVER accept as-is (here: permanently over its
// cap) must not block every blob behind it in blobstore.List()'s order,
// forever. This is a regression test for exactly that bug: the earlier
// implementation returned on the first BlobPut failure of any kind, so a
// persistently-rejected blob listed before a good one starved the good one
// on every single pass.
//
// blobstore.List() is sorted by hex digest (blobstore.go: shard = sha[:2],
// then filename = the full sha, both walked via sorted os.ReadDir). The
// over-cap content is searched until its digest sorts before the valid
// blob's, so this test actually exercises "rejected first, valid second" —
// not an order the old code happened to get lucky on.
func TestSyncSkipsAPersistentlyRejectedBlobAndContinues(t *testing.T) {
	const cap = 16 // bytes — deliberately tiny so an over-cap push is realistic
	parent, child, pm := newReplPairWithCap(t, cap)
	childBlobs := newBlobStore(t)

	valid := []byte("small enough") // 12 bytes, under cap
	validSHA, _, err := childBlobs.Put(bytes.NewReader(valid), "")
	if err != nil {
		t.Fatal(err)
	}

	var rejectedSHA string
	for i := 0; rejectedSHA == ""; i++ {
		candidate := []byte(fmt.Sprintf("way too big for the sixteen byte cap #%d", i))
		sum := sha256.Sum256(candidate)
		sha := hex.EncodeToString(sum[:])
		if sha >= validSHA {
			continue // keep searching for a digest that lists before validSHA
		}
		if _, _, err := childBlobs.Put(bytes.NewReader(candidate), ""); err != nil {
			t.Fatal(err)
		}
		rejectedSHA = sha
	}

	confirmed := map[string]bool{}
	pushed := syncBlobs(child, childBlobs, pm, confirmed)

	// Denominator: the valid blob actually landed and was confirmed in this
	// SAME pass — proves the loop did not stop dead at the rejected entry
	// ahead of it. Under the old return-on-first-failure code this would be 0.
	if pushed != 1 {
		t.Fatalf("pushed %d, want 1 (only the valid blob)", pushed)
	}
	if !confirmed[validSHA] {
		t.Fatal("the valid blob was not confirmed")
	}
	if _, ok := parent.blobs.Has(validSHA); !ok {
		t.Fatal("parent is missing the valid blob")
	}

	if confirmed[rejectedSHA] {
		t.Fatal("a rejected blob must not be marked confirmed")
	}
	if _, ok := parent.blobs.Has(rejectedSHA); ok {
		t.Fatal("the parent stored an over-cap blob")
	}
}

// --- helpers -----------------------------------------------------------

// newBlobStore opens a fresh, empty blob store in a temp dir with a 1 MiB cap
// — for tests that need a child-side store distinct from the parent's.
func newBlobStore(t *testing.T) *blobstore.Store {
	t.Helper()
	return mustBlobStore(t, t.TempDir(), 1<<20)
}

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
