package httpapi

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// resourceAPI is a published-door test fixture — the same shape as newAPI in
// httpapi_test.go — that also keeps the blob store reachable, because these
// tests seed a resource's bytes directly (the door under test has no route
// that accepts an upload; only /resources/{id}/file, which reads). blobsDir
// is kept (rather than opening via the package's testBlobs helper) so a test
// can reach into the store's on-disk layout to simulate a genuine I/O fault.
type resourceAPI struct {
	url      string
	eng      *engine.Engine
	reg      *registry.Manager
	blobs    *blobstore.Store
	blobsDir string
	m        *metrics.Metrics
}

func newResourceAPI(t *testing.T) *resourceAPI {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	nodeID, err := identity.Generate(filepath.Join(t.TempDir(), "n.key"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(s, "n-test")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{ULID: "n-test", API: config.API{Token: "tok"}}
	m := metrics.New(s, config.Retention{}, nil)
	e := engine.New(s, cfg, reg, nil, m, nil)
	reg.SetNamespace(e.Elements())
	blobsDir := t.TempDir()
	blobs, err := blobstore.Open(blobsDir, cfg.Limits.EffectiveMaxBlobBytes())
	if err != nil {
		t.Fatal(err)
	}

	tlsCfg, err := TLSConfig(nodeID, "n-test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: Handler(e, cfg, reg, nil, m, blobs, nodeID.PublicHex(), false, nil)}
	go func() { _ = srv.Serve(tls.NewListener(ln, tlsCfg)) }()
	t.Cleanup(func() { _ = srv.Close() })

	return &resourceAPI{url: "https://" + ln.Addr().String(), eng: e, reg: reg, blobs: blobs, blobsDir: blobsDir, m: m}
}

// breakBlobPermissions makes an already-stored blob's file unreadable — a
// stand-in for a disk or permission fault on this node. blobs.Get(sha) then
// returns a generic wrapped I/O error: neither ErrNotFound (the file is
// still there, just unreadable) nor ErrBadDigest (sha is well-formed). This
// is the only one of blobstore's three read-error kinds a test can trigger
// without corrupting the store's validated write path — a malformed digest
// can never reach here in the first place, because uns.ResourceID and
// uns.ResourceBlob both gate on the record's full validity (the same
// isSHA256Hex check blobstore itself applies), so a record naming a bad
// digest is never even found by id.
func (a *resourceAPI) breakBlobPermissions(t *testing.T, sha string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("running as root: a permission-denied file would still be readable")
	}
	path := filepath.Join(a.blobsDir, sha[:2], sha)
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
}

// putBlob stores body in the fixture's blob store directly (bypassing HTTP —
// this door has no upload route) and returns its digest and size.
func (a *resourceAPI) putBlob(t *testing.T, body []byte) (string, int64) {
	t.Helper()
	sha, size, err := a.blobs.Put(bytes.NewReader(body), "")
	if err != nil {
		t.Fatal(err)
	}
	return sha, size
}

// authorResource places an element at elementPath and publishes a _Resource
// record naming it id/sha/size at elementPath+"/"+id — exactly the topic
// shape ConfigExec.resourceTopic produces for a resource attached there.
func (a *resourceAPI) authorResource(t *testing.T, elementPath, id, sha string, size int64) {
	t.Helper()
	elementID := authtest.Place(t, a.eng, elementPath)
	topic := "colca/v1/_Resource/n-test/" + elementPath + "/" + id
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
	if _, err := a.eng.IngestAdmin(topic, payload); err != nil {
		t.Fatalf("author resource at %s: %v", topic, err)
	}
}

func TestResourceFileServesTheBlobToAGrantedCaller(t *testing.T) {
	a := newResourceAPI(t)
	body := []byte("mixing instructions")
	sha, size := a.putBlob(t, body)
	a.authorResource(t, "press3", "r1", sha, size)

	m1 := authtest.NewMachine(t, "m1")
	authtest.EnrollAt(t, a.reg, a.eng, m1, "press3")

	resp, got := raw(t, client(m1), "GET", a.url+"/resources/r1/file", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /resources/r1/file = %d, want 200 (%s)", resp.StatusCode, got)
	}
	if got != string(body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestResourceFileReportsPendingWhenTheBlobHasNotArrived(t *testing.T) {
	a := newResourceAPI(t)
	body := []byte("mixing instructions, not yet replicated")
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	a.authorResource(t, "press3", "r1", sha, int64(len(body)))

	m1 := authtest.NewMachine(t, "m1")
	authtest.EnrollAt(t, a.reg, a.eng, m1, "press3")

	resp, got := raw(t, client(m1), "GET", a.url+"/resources/r1/file", "", "")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("blob absent: got %d, want 409 (%s)", resp.StatusCode, got)
	}
	if !strings.Contains(got, "blob_pending") || !strings.Contains(got, sha) {
		t.Fatalf("409 body must name blob_pending and the digest: %s", got)
	}

	// Denominator: once the blob lands, the identical request returns 200 —
	// otherwise the 409 above would just as well be a broken route.
	if _, _, err := a.blobs.Put(bytes.NewReader(body), sha); err != nil {
		t.Fatal(err)
	}
	resp, got = raw(t, client(m1), "GET", a.url+"/resources/r1/file", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("blob present: got %d, want 200 (%s)", resp.StatusCode, got)
	}
	if got != string(body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestResourceFileRefusesACallerWithoutAGrant(t *testing.T) {
	a := newResourceAPI(t)
	body := []byte("mixing instructions")
	sha, size := a.putBlob(t, body)
	a.authorResource(t, "press3", "r1", sha, size)

	elsewhere := authtest.NewMachine(t, "elsewhere")
	authtest.EnrollAt(t, a.reg, a.eng, elsewhere, "line2")

	resp, got := raw(t, client(elsewhere), "GET", a.url+"/resources/r1/file", "", "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("caller granted elsewhere: got %d, want 403 (%s)", resp.StatusCode, got)
	}

	// Denominator: a caller granted on the resource's own element gets 200 —
	// otherwise the 403 above would just as well mean the route is broken.
	granted := authtest.NewMachine(t, "granted")
	authtest.EnrollAt(t, a.reg, a.eng, granted, "press3")
	resp, got = raw(t, client(granted), "GET", a.url+"/resources/r1/file", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("caller granted on the element: got %d, want 200 (%s)", resp.StatusCode, got)
	}
}

func TestResourceFileReportsAnUnknownId(t *testing.T) {
	a := newResourceAPI(t)
	body := []byte("mixing instructions")
	sha, size := a.putBlob(t, body)
	a.authorResource(t, "press3", "r1", sha, size)

	m1 := authtest.NewMachine(t, "m1")
	authtest.EnrollAt(t, a.reg, a.eng, m1, "press3")

	resp, got := raw(t, client(m1), "GET", a.url+"/resources/no-such-id/file", "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown id: got %d, want 404 (%s)", resp.StatusCode, got)
	}

	// Denominator: a known id returns 200 in the same test — otherwise the
	// 404 above would just as well mean the route was never registered.
	resp, got = raw(t, client(m1), "GET", a.url+"/resources/r1/file", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("known id: got %d, want 200 (%s)", resp.StatusCode, got)
	}
}

// TestResourceFileNeverReportsPendingForANonRetryableBlobError pins the
// 409 blob_pending is a promise the file is still in
// flight, and only blobstore.ErrNotFound (the blob genuinely has not
// replicated here yet) may make that promise. Anything else — a malformed
// stored digest, a disk or permission fault on this node — is an internal
// fault that will never clear on its own, so it must never collapse into
// pending.
func TestResourceFileNeverReportsPendingForANonRetryableBlobError(t *testing.T) {
	a := newResourceAPI(t)
	body := []byte("mixing instructions")
	sha, size := a.putBlob(t, body)
	a.authorResource(t, "press3", "r1", sha, size)
	a.breakBlobPermissions(t, sha)

	m1 := authtest.NewMachine(t, "m1")
	authtest.EnrollAt(t, a.reg, a.eng, m1, "press3")

	resp, got := raw(t, client(m1), "GET", a.url+"/resources/r1/file", "", "")
	if resp.StatusCode == http.StatusConflict {
		t.Fatalf("an unreadable blob must not answer 409 blob_pending — that promises the file will still arrive: %s", got)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("unreadable blob: got %d, want 500 (%s)", resp.StatusCode, got)
	}

	// Denominator: a genuinely absent digest still answers 409 in the same
	// test — otherwise "not 409" above could pass just as well because the
	// route broke for everyone, not because the discrimination works.
	absent := strings.Repeat("c", 64)
	a.authorResource(t, "press3", "r2", absent, 5)
	resp, got = raw(t, client(m1), "GET", a.url+"/resources/r2/file", "", "")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("genuinely absent blob: got %d, want 409 (%s)", resp.StatusCode, got)
	}
	if !strings.Contains(got, "blob_pending") {
		t.Fatalf("409 body must name blob_pending: %s", got)
	}
}
