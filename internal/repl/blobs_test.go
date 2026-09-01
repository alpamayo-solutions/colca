package repl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

	confirmed := map[string]time.Time{}
	rejected := map[string]bool{}
	if pushed := syncBlobs(child, childBlobs, nil, confirmed, rejected); pushed != 2 {
		t.Fatalf("first pass pushed %d, want 2", pushed)
	}
	if _, ok := parent.blobs.Has(first); !ok {
		t.Fatal("parent is missing the first blob")
	}
	if _, ok := parent.blobs.Has(second); !ok {
		t.Fatal("parent is missing the second blob")
	}

	if pushed := syncBlobs(child, childBlobs, nil, confirmed, rejected); pushed != 0 {
		t.Fatalf("second pass pushed %d, want 0 — confirmed blobs must not be re-sent", pushed)
	}

	third, _, err := childBlobs.Put(bytes.NewReader([]byte("three")), "")
	if err != nil {
		t.Fatal(err)
	}
	if pushed := syncBlobs(child, childBlobs, nil, confirmed, rejected); pushed != 1 {
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
	if pushed := syncBlobs(child, childBlobs, nil, map[string]time.Time{}, map[string]bool{}); pushed != 0 {
		t.Fatalf("pushed %d, want 0", pushed)
	}
	if _, ok := parent.blobs.Has(sha); !ok {
		t.Fatal("the parent lost the blob it already had")
	}
}

// TestSyncReoffersABlobSweptThenRecreatedAtTheParent pins the critical fix:
// a digest that was confirmed, then swept away at the parent (blobgc reclaims
// anything no live _Resource references, once nothing does and the grace has
// passed), then re-staged at the child under the SAME still-running Client —
// an ordinary sequence for an edge node (delete a resource, wait out the
// grace, re-attach the identical file) — must be re-offered. A permanent
// confirmed[sha]=true would skip it forever, leaving the parent holding a
// record with no way to ever receive its bytes (409 blob_pending, unrecoverable
// short of a child restart).
func TestSyncReoffersABlobSweptThenRecreatedAtTheParent(t *testing.T) {
	parent, child := newReplPair(t)
	childBlobs := newBlobStore(t)
	content := []byte("press 3 operator manual, revision 2")

	sha, _, err := childBlobs.Put(bytes.NewReader(content), "")
	if err != nil {
		t.Fatal(err)
	}

	confirmed := map[string]time.Time{}
	rejected := map[string]bool{}
	if pushed := syncBlobs(child, childBlobs, nil, confirmed, rejected); pushed != 1 {
		t.Fatalf("first pass pushed %d, want 1", pushed)
	}
	if _, ok := confirmed[sha]; !ok {
		t.Fatal("first pass did not confirm the blob it pushed")
	}
	if _, ok := parent.blobs.Has(sha); !ok {
		t.Fatal("parent is missing the blob from the first pass")
	}

	// Simulate the parent's own sweeper reclaiming it once nothing referenced
	// it any more and the grace elapsed. confirmed[sha] is untouched — this
	// client has no way to learn the parent no longer holds it.
	if err := parent.blobs.Delete(sha); err != nil {
		t.Fatal(err)
	}

	// Denominator: WITHOUT the re-Put below, a pass against the still-stale
	// confirmed entry pushes 0 — proving the parent-side delete alone is not
	// what makes the next pass push. If this assertion started failing, it
	// would mean this test no longer measures what it claims to.
	if pushed := syncBlobs(child, childBlobs, nil, confirmed, rejected); pushed != 0 {
		t.Fatalf("pass against a stale confirmed entry pushed %d, want 0 (this is the denominator, not the fix)", pushed)
	}
	if _, ok := parent.blobs.Has(sha); ok {
		t.Fatal("precondition broken: the parent must not hold the blob at this point")
	}

	// The operator re-attaches the identical file. Put always renames a fresh
	// temp file onto the target, so this bumps the stored blob's mtime even
	// though the content — and therefore the digest — is unchanged. A short
	// sleep guarantees the new mtime is observably later regardless of the
	// filesystem's timestamp resolution.
	time.Sleep(5 * time.Millisecond)
	reSHA, _, err := childBlobs.Put(bytes.NewReader(content), "")
	if err != nil {
		t.Fatal(err)
	}
	if reSHA != sha {
		t.Fatalf("re-Put of identical content produced a different digest: %s vs %s", reSHA, sha)
	}

	if pushed := syncBlobs(child, childBlobs, nil, confirmed, rejected); pushed != 1 {
		t.Fatalf("pass after the re-Put pushed %d, want 1 — a swept-then-recreated blob must be re-offered", pushed)
	}
	if _, ok := parent.blobs.Has(sha); !ok {
		t.Fatal("parent still missing the blob after it was recreated and re-synced")
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
	confirmed := map[string]time.Time{}
	rejected := map[string]bool{}
	if pushed := syncBlobs(child, childBlobs, nil, confirmed, rejected); pushed != 1 {
		t.Fatalf("first pass (parent up) pushed %d, want 1", pushed)
	}
	if _, ok := confirmed[first]; !ok {
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
	if pushed := syncBlobs(child, childBlobs, nil, confirmed, rejected); pushed != 0 {
		t.Fatalf("pushed %d against a dead parent, want 0", pushed)
	}
	if _, ok := confirmed[second]; ok {
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

	confirmed := map[string]time.Time{}
	rejected := map[string]bool{}
	pushed := syncBlobs(child, childBlobs, pm, confirmed, rejected)

	// Denominator: the valid blob actually landed and was confirmed in this
	// SAME pass — proves the loop did not stop dead at the rejected entry
	// ahead of it. Under the old return-on-first-failure code this would be 0.
	if pushed != 1 {
		t.Fatalf("pushed %d, want 1 (only the valid blob)", pushed)
	}
	if _, ok := confirmed[validSHA]; !ok {
		t.Fatal("the valid blob was not confirmed")
	}
	if _, ok := parent.blobs.Has(validSHA); !ok {
		t.Fatal("parent is missing the valid blob")
	}

	if _, ok := confirmed[rejectedSHA]; ok {
		t.Fatal("a rejected blob must not be marked confirmed")
	}
	if _, ok := parent.blobs.Has(rejectedSHA); ok {
		t.Fatal("the parent stored an over-cap blob")
	}
	if !rejected[rejectedSHA] {
		t.Fatal("the rejected blob was not recorded so a later pass can skip it")
	}

	// Denominator for what follows: the first pass actually attempted the PUT
	// and the parent actually counted the rejection — proves this counter can
	// move at all, so an unchanged reading after the second pass means the
	// PUT was skipped rather than the counter being dead.
	errAfterFirstPass := metricstest.Value(t, pm, `colca_blob_transfers_total{direction="receive",result="error"}`)
	if errAfterFirstPass != 1 {
		t.Fatalf(`colca_blob_transfers_total{direction="receive",result="error"} = %v after the first pass, want 1`, errAfterFirstPass)
	}

	// Without the rejected set, this second pass would HEAD, GET, and
	// full-body PUT the same over-cap blob all over again — forever. With it,
	// the rejected sha is skipped before any of that reaches the parent.
	if pushed := syncBlobs(child, childBlobs, pm, confirmed, rejected); pushed != 0 {
		t.Fatalf("second pass pushed %d, want 0", pushed)
	}
	if got := metricstest.Value(t, pm, `colca_blob_transfers_total{direction="receive",result="error"}`); got != errAfterFirstPass {
		t.Fatalf(`colca_blob_transfers_total{direction="receive",result="error"} = %v after a second pass, want unchanged at %v — a permanently rejected blob must not be re-uploaded`, got, errAfterFirstPass)
	}
}

func TestPullThroughFetchesFromTheGrandparent(t *testing.T) {
	root, mid, leaf := newReplChain(t) // helper: three servers, leaf→mid→root, each pinned

	content := []byte("recipe staged at the root")
	sha, _, err := root.blobs.Put(bytes.NewReader(content), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := mid.blobs.Has(sha); ok {
		t.Fatal("precondition: mid must not hold the blob yet")
	}

	rc, size, err := leaf.client.BlobGet(sha, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, content) {
		t.Fatal("pulled content differs")
	}
	if size != int64(len(content)) {
		t.Fatalf("size = %d, want %d", size, len(content))
	}
	if _, ok := mid.blobs.Has(sha); !ok {
		t.Fatal("the intermediate node did not cache what it relayed")
	}
}

func TestPullThroughReportsAnAbsentBlob(t *testing.T) {
	root, _, leaf := newReplChain(t)
	absent := strings.Repeat("b", 64)
	if _, _, err := leaf.client.BlobGet(absent, 4); err == nil {
		t.Fatal("want an error when no ancestor holds the blob")
	}
	// Denominator: the same call succeeds for a blob the root does hold.
	sha, _, err := root.blobs.Put(bytes.NewReader([]byte("present")), "")
	if err != nil {
		t.Fatal(err)
	}
	rc, _, err := leaf.client.BlobGet(sha, 4)
	if err != nil {
		t.Fatalf("BlobGet on a present blob failed: %v — the error above proves nothing", err)
	}
	rc.Close()
}

func TestPullThroughStopsAtTheHopLimit(t *testing.T) {
	root, _, leaf := newReplChain(t)
	sha, _, err := root.blobs.Put(bytes.NewReader([]byte("two hops away")), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := leaf.client.BlobGet(sha, 1); err == nil {
		t.Fatal("a one-hop budget must not reach the grandparent")
	}
}

// A transfer is bounded by what it CARRIES, not by one fixed cap.
//
// The shared 30s http.Client.Timeout covered the whole exchange, body
// included, so a blob needed size/30s bytes per second to finish at all: at
// the default 32 MiB cap, better than ~9 Mbit/s sustained. On a real edge
// uplink every PUT aborted mid-body, was not marked rejected (a transport
// failure says nothing about the blob), and was retried on the next uplink
// pass — a 30s upload burst every 30s forever, while every ancestor answered
// 409 blob_pending and the resource never resolved.
func TestBlobTransfersAreBoundedBySizeNotByAFixedCap(t *testing.T) {
	_, child := newReplPair(t)

	if child.http.Timeout != 0 {
		t.Fatalf("the repl client caps the whole exchange at %s — a blob bigger than that cap times the "+
			"link speed can never be transferred, at any retry count", child.http.Timeout)
	}
	// The largest blob the default config accepts, on a link this product
	// runs on. Nothing makes those two numbers meet unless the deadline is
	// derived from the size.
	const maxBlob = 32 << 20    // the default max_blob_bytes
	const slowLinkBPS = 1 << 17 // 1 Mbit/s
	need := time.Duration(maxBlob/slowLinkBPS) * time.Second
	if got := transferDeadline(maxBlob); got < need {
		t.Fatalf("a %d-byte blob gets %s but needs %s at %d bit/s — it can never finish on that link",
			maxBlob, got, need, slowLinkBPS*8)
	}
	// The other direction: a small request must not be allowed to hang for
	// minutes just because a large one may.
	if got := transferDeadline(2048); got > 2*transferGrace {
		t.Fatalf("a 2KiB request gets %s — a stalled poll would hold its lane far past the point of usefulness", got)
	}
	// Getting a CONNECTION stays bounded by time: that cost does not depend
	// on how many bytes follow it, and a parent that accepts a connection and
	// then says nothing must not hold a transfer open forever.
	tr, ok := child.http.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, so the connection bounds below cannot be checked", child.http.Transport)
	}
	if tr.TLSHandshakeTimeout == 0 || tr.DialContext == nil {
		t.Fatal("dial and TLS handshake are unbounded: with no whole-exchange timeout either, " +
			"a parent that never answers holds every transfer to it open forever")
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

// chainNode pairs a running repl server with the client this node uses to
// reach ITS OWN parent (nil at the root). Embedding *Server promotes fields
// such as .blobs directly, matching how the single-hop tests above already
// read parent.blobs / child... through the plain *Server they hold.
type chainNode struct {
	*Server
	client *Client
}

// newReplChain brings up three repl servers wired leaf→mid→root, exactly the
// way real enrollment does it one hop at a time: root enrolls mid as its
// child, mid enrolls leaf as its child, and each non-root node is pinned to
// its parent AND has SetUpstream called on its own server — the same two
// steps node.Start performs for a real parent link. root gets no upstream,
// which is what lets an absent blob or an exhausted hop budget terminate
// instead of walking off the top of the tree.
func newReplChain(t *testing.T) (root, mid, leaf *chainNode) {
	t.Helper()
	dir := t.TempDir()

	rootID := mustIdentity(t, filepath.Join(dir, "root.key"))
	midID := mustIdentity(t, filepath.Join(dir, "mid.key"))
	leafID := mustIdentity(t, filepath.Join(dir, "leaf.key"))

	// root: enrolls mid as its child, no upstream of its own.
	rs := mustStore(t, filepath.Join(dir, "rootdata"))
	rcfg := &config.Config{ULID: "n-root", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	rreg, reng := nodeParts(t, rs, rcfg, nil, nil, nil, childSpec{"n-mid", midID.PublicHex(), "mid1"})
	rblobs := mustBlobStore(t, filepath.Join(dir, "rootblobs"), rcfg.Limits.EffectiveMaxBlobBytes())
	rootSrv, err := NewServer(rcfg, reng, rootID, rreg, rblobs, nil)
	if err != nil {
		t.Fatalf("NewServer(root): %v", err)
	}
	raddr, err := rootSrv.Start()
	if err != nil {
		t.Fatalf("Start(root): %v", err)
	}
	t.Cleanup(rootSrv.Stop)

	// mid: enrolls leaf as its child, pinned to root as its parent.
	ms := mustStore(t, filepath.Join(dir, "middata"))
	mcfg := &config.Config{ULID: "n-mid", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	mreg, meng := nodeParts(t, ms, mcfg, nil, nil, nil, childSpec{"n-leaf", leafID.PublicHex(), "leaf1"})
	mblobs := mustBlobStore(t, filepath.Join(dir, "midblobs"), mcfg.Limits.EffectiveMaxBlobBytes())
	midSrv, err := NewServer(mcfg, meng, midID, mreg, mblobs, nil)
	if err != nil {
		t.Fatalf("NewServer(mid): %v", err)
	}
	maddr, err := midSrv.Start()
	if err != nil {
		t.Fatalf("Start(mid): %v", err)
	}
	t.Cleanup(midSrv.Stop)
	midToRoot := mustClient(t, raddr, rootID.PublicHex(), midID)
	midSrv.SetUpstream(midToRoot)

	// leaf: no children of its own, pinned to mid as its parent.
	ls := mustStore(t, filepath.Join(dir, "leafdata"))
	lcfg := &config.Config{ULID: "n-leaf", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	lreg, leng := nodeParts(t, ls, lcfg, nil, nil, nil)
	lblobs := mustBlobStore(t, filepath.Join(dir, "leafblobs"), lcfg.Limits.EffectiveMaxBlobBytes())
	leafSrv, err := NewServer(lcfg, leng, leafID, lreg, lblobs, nil)
	if err != nil {
		t.Fatalf("NewServer(leaf): %v", err)
	}
	if _, err := leafSrv.Start(); err != nil {
		t.Fatalf("Start(leaf): %v", err)
	}
	t.Cleanup(leafSrv.Stop)
	leafToMid := mustClient(t, maddr, midID.PublicHex(), leafID)
	leafSrv.SetUpstream(leafToMid)

	return &chainNode{Server: rootSrv, client: nil},
		&chainNode{Server: midSrv, client: midToRoot},
		&chainNode{Server: leafSrv, client: leafToMid}
}
