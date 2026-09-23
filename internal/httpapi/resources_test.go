package httpapi

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
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
	"github.com/alpamayo-solutions/colca/internal/tokenauth"
	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
)

// resourceAPI is a published-door fixture like newAPI that keeps the blob store
// reachable, so tests can seed bytes and break file permissions.
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

// breakBlobPermissions makes a stored blob's file unreadable, standing in for a
// disk fault. Get then returns a generic I/O error, neither ErrNotFound nor
// ErrBadDigest.
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

// putBlob stores body directly in the blob store and returns its digest and size.
func putBlob(t *testing.T, blobs *blobstore.Store, body []byte) (string, int64) {
	t.Helper()
	sha, size, err := blobs.Put(bytes.NewReader(body), "")
	if err != nil {
		t.Fatal(err)
	}
	return sha, size
}

// authorResource places an element at elementPath and publishes a _Resource for
// id, sha and size under it, the topic ConfigExec.resourceTopic produces.
func authorResource(t *testing.T, eng *engine.Engine, elementPath, id, sha string, size int64) {
	t.Helper()
	elementID := authtest.Place(t, eng, elementPath)
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
	if _, err := eng.IngestAdmin(topic, payload); err != nil {
		t.Fatalf("author resource at %s: %v", topic, err)
	}
}

func TestResourceFileServesTheBlobToAGrantedCaller(t *testing.T) {
	a := newResourceAPI(t)
	body := []byte("mixing instructions")
	sha, size := putBlob(t, a.blobs, body)
	authorResource(t, a.eng, "press3", "r1", sha, size)

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
	authorResource(t, a.eng, "press3", "r1", sha, int64(len(body)))

	m1 := authtest.NewMachine(t, "m1")
	authtest.EnrollAt(t, a.reg, a.eng, m1, "press3")

	resp, got := raw(t, client(m1), "GET", a.url+"/resources/r1/file", "", "")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("blob absent: got %d, want 409 (%s)", resp.StatusCode, got)
	}
	if !strings.Contains(got, "blob_pending") || !strings.Contains(got, sha) {
		t.Fatalf("409 body must name blob_pending and the digest: %s", got)
	}

	// Denominator: once the blob lands the same request returns 200.
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
	sha, size := putBlob(t, a.blobs, body)
	authorResource(t, a.eng, "press3", "r1", sha, size)

	elsewhere := authtest.NewMachine(t, "elsewhere")
	authtest.EnrollAt(t, a.reg, a.eng, elsewhere, "line2")

	resp, got := raw(t, client(elsewhere), "GET", a.url+"/resources/r1/file", "", "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("caller granted elsewhere: got %d, want 403 (%s)", resp.StatusCode, got)
	}

	// Denominator: a caller granted on the resource's element gets 200.
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
	sha, size := putBlob(t, a.blobs, body)
	authorResource(t, a.eng, "press3", "r1", sha, size)

	m1 := authtest.NewMachine(t, "m1")
	authtest.EnrollAt(t, a.reg, a.eng, m1, "press3")

	resp, got := raw(t, client(m1), "GET", a.url+"/resources/no-such-id/file", "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown id: got %d, want 404 (%s)", resp.StatusCode, got)
	}

	// Denominator: a known id returns 200.
	resp, got = raw(t, client(m1), "GET", a.url+"/resources/r1/file", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("known id: got %d, want 200 (%s)", resp.StatusCode, got)
	}
}

// 409 blob_pending means the file is still on its way, and only ErrNotFound may
// say that. Other errors will not clear on their own and must not look pending.
func TestResourceFileNeverReportsPendingForANonRetryableBlobError(t *testing.T) {
	a := newResourceAPI(t)
	body := []byte("mixing instructions")
	sha, size := putBlob(t, a.blobs, body)
	authorResource(t, a.eng, "press3", "r1", sha, size)
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

	// Denominator: a genuinely absent digest still answers 409.
	absent := strings.Repeat("c", 64)
	authorResource(t, a.eng, "press3", "r2", absent, 5)
	resp, got = raw(t, client(m1), "GET", a.url+"/resources/r2/file", "", "")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("genuinely absent blob: got %d, want 409 (%s)", resp.StatusCode, got)
	}
	if !strings.Contains(got, "blob_pending") {
		t.Fatalf("409 body must name blob_pending: %s", got)
	}
}

// localResourceAPI is the local-door counterpart of resourceAPI, with a human
// verifier so tests can forward a bearer as the api does.
type localResourceAPI struct {
	http.Handler
	eng   *engine.Engine
	blobs *blobstore.Store
}

func newLocalResourceAPI(t *testing.T) (*localResourceAPI, *tokentest.Issuer) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	reg, err := registry.New(s, "n-test")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{ULID: "n-test"}
	m := metrics.New(s, config.Retention{}, nil)
	e := engine.New(s, cfg, reg, nil, m, nil)
	reg.SetNamespace(e.Elements())
	blobs := testBlobs(t, cfg)

	iss := tokentest.NewIssuer(t)
	ver, err := tokenauth.New(tokenauth.Config{
		Issuers: []tokenauth.Issuer{{ID: iss.Iss(), JWKSURL: iss.JWKSURL()}}, Audience: iss.Aud(),
	}, e.Store(), m)
	if err != nil {
		t.Fatal(err)
	}
	primeVerifier(t, ver)

	h := Handler(e, cfg, reg, ver, m, blobs, "deadbeef", true, nil)
	return &localResourceAPI{Handler: h, eng: e, blobs: blobs}, iss
}

// resourceFileRequest makes one GET on the local door with an optional forwarded
// bearer and service name.
func resourceFileRequest(a *localResourceAPI, id, bearer, serviceName string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/resources/"+id+"/file", nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if serviceName != "" {
		req.Header.Set("X-Colca-Service", serviceName)
	}
	rec := httptest.NewRecorder()
	a.ServeHTTP(rec, req)
	return rec
}

// A forwarded bearer scoped to the resource's element reads the bytes on the local
// door; the person is authorized, not the service's placement.
func TestTheLocalDoorServesAResourceFileToAForwardedBearerScopedToItsElement(t *testing.T) {
	a, iss := newLocalResourceAPI(t)
	body := []byte("mixing instructions")
	sha, size := putBlob(t, a.blobs, body)
	authorResource(t, a.eng, "press3", "r1", sha, size)

	token := iss.MintOpt(tokentest.MintOpts{
		Sub: "kc-sub-anna", Username: "anna",
		Grants: []string{"read:" + authtest.ElementID("press3") + "/#"},
	})
	rec := resourceFileRequest(a, "r1", token, "api")
	if rec.Code != http.StatusOK {
		t.Fatalf("forwarded Bearer scoped to the element = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != string(body) {
		t.Fatalf("body = %q, want %q", rec.Body.String(), body)
	}
}

// The same person scoped to another element gets 403 on the local door.
func TestTheLocalDoorRefusesAForwardedBearerScopedElsewhere(t *testing.T) {
	a, iss := newLocalResourceAPI(t)
	body := []byte("mixing instructions")
	sha, size := putBlob(t, a.blobs, body)
	authorResource(t, a.eng, "press3", "r1", sha, size)

	elsewhere := iss.MintOpt(tokentest.MintOpts{
		Sub: "kc-sub-anna", Username: "anna",
		Grants: []string{"read:" + authtest.ElementID("line2") + "/#"},
	})
	rec := resourceFileRequest(a, "r1", elsewhere, "api")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("forwarded Bearer scoped elsewhere: got %d, want 403 (%s)", rec.Code, rec.Body.String())
	}

	// Denominator: a grant covering the element gets 200.
	granted := iss.MintOpt(tokentest.MintOpts{
		Sub: "kc-sub-anna", Username: "anna",
		Grants: []string{"read:" + authtest.ElementID("press3") + "/#"},
	})
	rec = resourceFileRequest(a, "r1", granted, "api")
	if rec.Code != http.StatusOK {
		t.Fatalf("forwarded Bearer scoped to the element: got %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
}

// No credential is 401 on the local door, as for every local route. A plain local
// service reads the same resource in the same test.
func TestTheLocalDoorRefusesAResourceFileReadWithNoCredential(t *testing.T) {
	a, _ := newLocalResourceAPI(t)
	body := []byte("mixing instructions")
	sha, size := putBlob(t, a.blobs, body)
	authorResource(t, a.eng, "press3", "r1", sha, size)

	rec := resourceFileRequest(a, "r1", "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no credential: got %d, want 401 (%s)", rec.Code, rec.Body.String())
	}

	// Denominator: an unplaced local service is bound to the whole node, so it may
	// read.
	rec = resourceFileRequest(a, "r1", "", "connector-opcua")
	if rec.Code != http.StatusOK {
		t.Fatalf("plain local-service caller: got %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
}
