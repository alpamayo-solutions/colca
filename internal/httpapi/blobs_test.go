package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// newLocalTestHandler builds a local-door Handler (local=true) with the
// default blob-size ceiling — reachability from inside the deployment's own
// network is the credential on this door, so requests need only name
// themselves via X-Colca-Service, exactly as doLocal does below.
func newLocalTestHandler(t *testing.T) http.Handler {
	t.Helper()
	return newLocalTestHandlerWithBlobCap(t, config.Limits{}.EffectiveMaxBlobBytes())
}

// newLocalTestHandlerWithBlobCap builds a local-door Handler whose blob store
// is capped at max bytes, for the oversize-rejection test.
func newLocalTestHandlerWithBlobCap(t *testing.T, max uint64) http.Handler {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-test", Limits: config.Limits{MaxBlobBytes: config.ByteSize(max)}}
	reg, err := registry.New(s, cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(s, config.Retention{}, nil)
	eng := engine.New(s, cfg, reg, nil, m, nil)
	reg.SetNamespace(eng.Elements())
	blobs := testBlobs(t, cfg)
	return Handler(eng, cfg, reg, nil, m, blobs, "deadbeef", true, nil)
}

// doLocal performs a request against a local-door Handler's mux (no
// listener), naming the caller "connector" via X-Colca-Service exactly as a
// local service would — the local door has nothing else to authenticate.
func doLocal(t *testing.T, h http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r, err := http.NewRequest(method, path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Colca-Service", "connector")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	return rr
}

func TestLocalDoorStoresAndServesABlob(t *testing.T) {
	h := newLocalTestHandler(t) // local=true, service "connector" registered
	body := []byte("mixing instructions")

	rr := doLocal(t, h, "POST", "/blobs", body)
	if rr.Code != http.StatusCreated {
		t.Fatalf("POST /blobs = %d, want 201 (%s)", rr.Code, rr.Body)
	}
	var created struct {
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if created.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("sha = %s, want %s", created.SHA256, hex.EncodeToString(sum[:]))
	}

	got := doLocal(t, h, "GET", "/blobs/"+created.SHA256, nil)
	if got.Code != http.StatusOK {
		t.Fatalf("GET = %d, want 200", got.Code)
	}
	if !bytes.Equal(got.Body.Bytes(), body) {
		t.Fatal("GET returned different bytes")
	}

	head := doLocal(t, h, "HEAD", "/blobs/"+created.SHA256, nil)
	if head.Code != http.StatusOK {
		t.Fatalf("HEAD = %d, want 200", head.Code)
	}
	if head.Header().Get("Content-Length") != strconv.Itoa(len(body)) {
		t.Fatalf("HEAD Content-Length = %q, want %d", head.Header().Get("Content-Length"), len(body))
	}
}

func TestLocalDoorReportsAMissingBlob(t *testing.T) {
	h := newLocalTestHandler(t)
	absent := strings.Repeat("a", 64)
	if rr := doLocal(t, h, "GET", "/blobs/"+absent, nil); rr.Code != http.StatusNotFound {
		t.Fatalf("GET missing = %d, want 404", rr.Code)
	}
	if rr := doLocal(t, h, "HEAD", "/blobs/"+absent, nil); rr.Code != http.StatusNotFound {
		t.Fatalf("HEAD missing = %d, want 404", rr.Code)
	}
	// Denominator: the same routes answer 200 for a blob that IS present.
	created := doLocal(t, h, "POST", "/blobs", []byte("present"))
	var body struct {
		SHA256 string `json:"sha256"`
	}
	_ = json.Unmarshal(created.Body.Bytes(), &body)
	if rr := doLocal(t, h, "GET", "/blobs/"+body.SHA256, nil); rr.Code != http.StatusOK {
		t.Fatalf("GET present = %d, want 200 — the 404s above prove nothing otherwise", rr.Code)
	}
}

func TestLocalDoorRefusesAnOversizeBlob(t *testing.T) {
	h := newLocalTestHandlerWithBlobCap(t, 64)
	if rr := doLocal(t, h, "POST", "/blobs", make([]byte, 32)); rr.Code != http.StatusCreated {
		t.Fatalf("under-cap POST = %d, want 201", rr.Code)
	}
	if rr := doLocal(t, h, "POST", "/blobs", make([]byte, 4096)); rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize POST = %d, want 413", rr.Code)
	}
}

func TestLocalDoorRejectsAMalformedDigest(t *testing.T) {
	h := newLocalTestHandler(t)
	for _, bad := range []string{"nothex", "../etc/passwd", strings.Repeat("z", 64)} {
		if rr := doLocal(t, h, "GET", "/blobs/"+url.PathEscape(bad), nil); rr.Code != http.StatusBadRequest {
			t.Fatalf("GET %q = %d, want 400", bad, rr.Code)
		}
	}
}

func TestBlobRoutesAreAbsentFromThePublishedDoor(t *testing.T) {
	h := newTestHandler(t, &config.Config{ULID: "n-test", API: config.API{Token: "tok"}}) // local=false
	if rr := doAdmin(t, h, "POST", "/blobs", []byte("x")); rr.Code != http.StatusNotFound {
		t.Fatalf("POST /blobs on the 443 door = %d, want 404 — raw blob access must not be reachable there", rr.Code)
	}
	// Denominator: the 443 door does serve its own routes in this same setup.
	if rr := doAdmin(t, h, "GET", "/kv", nil); rr.Code != http.StatusOK {
		t.Fatalf("GET /kv = %d, want 200 — the 404 above would otherwise prove only that the handler is broken", rr.Code)
	}
}
