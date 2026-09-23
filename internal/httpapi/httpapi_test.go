package httpapi

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/httplimit"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/repl"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/internal/tokenauth"
	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// testBlobs opens a blob store capped like the node would cap it from cfg.Limits.
func testBlobs(t *testing.T, cfg *config.Config) *blobstore.Store {
	t.Helper()
	blobs, err := blobstore.Open(t.TempDir(), cfg.Limits.EffectiveMaxBlobBytes())
	if err != nil {
		t.Fatal(err)
	}
	return blobs
}

// bearerReq performs a request authenticated with a Bearer token.
func bearerReq(t *testing.T, hc *http.Client, method, url, token string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if raw, ok := body.([]byte); ok {
			buf.Write(raw)
		} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	r, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	resp, err := hc.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	return resp, out
}

// api is the fixture: a TLS server built like node assembly builds it, with machine
// m1 at mount m1 and admin token "tok". st and m are exposed for tests that seed
// records, prune or read metrics.
type api struct {
	url string
	eng *engine.Engine
	reg *registry.Manager
	m1  *authtest.Machine
	st  *store.Store
	m   *metrics.Metrics
	iss *tokentest.Issuer
}

// mint issues a valid human token for the fixture's issuer.
func (a *api) mint(sub string, grants []string) string {
	return a.iss.Mint(sub, grants, time.Now().Add(5*time.Minute))
}

// place authors an element at path in the fixture node's namespace and returns its
// id.
func (a *api) place(t *testing.T, path string) string {
	t.Helper()
	return authtest.Place(t, a.eng, path)
}

func newAPI(t *testing.T) *api {
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
	reg.SetMetrics(m) // the registry owns colca_drains_active
	e := engine.New(s, cfg, reg, nil, m, nil)
	// The registry resolves placements through the engine's element index, so wiring
	// and element come before enrollment.
	reg.SetNamespace(e.Elements())
	m1 := authtest.NewMachine(t, "m1")
	authtest.EnrollAt(t, reg, e, m1, "m1", "write:"+authtest.ElementID("m1")+"/#")

	iss := tokentest.NewIssuer(t)
	ver, err := tokenauth.New(tokenauth.Config{
		Issuer: iss.Iss(), Audience: iss.Aud(), JWKSURL: iss.JWKSURL(),
	}, s, m)
	if err != nil {
		t.Fatal(err)
	}
	primeVerifier(t, ver)

	tlsCfg, err := TLSConfig(nodeID, "n-test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: Handler(e, cfg, reg, ver, m, testBlobs(t, cfg), nodeID.PublicHex(), false, nil)}
	go func() { _ = srv.Serve(tls.NewListener(ln, tlsCfg)) }()
	t.Cleanup(func() { _ = srv.Close() })

	return &api{url: "https://" + ln.Addr().String(), eng: e, reg: reg, m1: m1, st: s, m: m, iss: iss}
}

// primeVerifier runs one immediate JWKS fetch (Run's first tick, no loop).
func primeVerifier(t *testing.T, ver *tokenauth.Verifier) {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { ver.Run(stop); close(done) }()
	time.Sleep(50 * time.Millisecond)
	close(stop)
	<-done
}

// client builds an HTTP client: with a machine identity when mch != nil,
// certless otherwise.
func client(mch *authtest.Machine) *http.Client {
	tlsCfg := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13} // #nosec G402 -- test, pinning model
	if mch != nil {
		tlsCfg.Certificates = []tls.Certificate{mch.Cert}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}
}

func req(t *testing.T, hc *http.Client, method, url, token string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if raw, ok := body.([]byte); ok {
			buf.Write(raw)
		} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	r, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		r.Header.Set("X-Colca-Token", token)
	}
	resp, err := hc.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	return resp, out
}

// raw performs a request and returns the body as a string; req decodes into a map,
// which would lose the exact JSON the gap tests assert.
func raw(t *testing.T, hc *http.Client, method, url, token, body string) (*http.Response, string) {
	t.Helper()
	r, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		r.Header.Set("X-Colca-Token", token)
	}
	resp, err := hc.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(b)
}

func TestAdminPublishFetchAckKV(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)

	// no token, no cert → 401
	resp, _ := req(t, admin, "POST", a.url+"/publish", "", map[string]any{"topic": "colca/v1/_Metric/x/y", "payload": map[string]any{"v": 1.0}})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}

	resp, out := req(t, admin, "POST", a.url+"/publish", "tok", map[string]any{
		"topic": "colca/v1/_Metric/n-test/x", "payload": map[string]any{"v": 1.0},
		"written_by": "api", "actor_id": "user-anna",
		"actor_label": "anna@example.com", "actor_kind": "human",
	})
	if resp.StatusCode != http.StatusOK || out["stream"] != "metrics" {
		t.Fatalf("%d %v", resp.StatusCode, out)
	}

	// invalid payload → 422
	resp, _ = req(t, admin, "POST", a.url+"/publish", "tok", map[string]any{"topic": "colca/v1/_Metric/n-test/x", "payload": map[string]any{"v": "bad"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d", resp.StatusCode)
	}

	_, out = req(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=c1&max=10", "tok", nil)
	if recs := out["records"].([]any); len(recs) != 1 {
		t.Fatalf("%v", out)
	} else if rec := recs[0].(map[string]any); rec["written_by"] != "api" ||
		rec["actor_id"] != "user-anna" || rec["actor_label"] != "anna@example.com" || rec["actor_kind"] != "human" {
		t.Fatalf("attribution did not survive publish/fetch: %v", rec)
	}
	_, out = req(t, admin, "POST", a.url+"/ack", "tok", map[string]any{"cursor": "c1", "stream": "metrics", "offset": 1})
	if out["moved"] != true {
		t.Fatalf("%v", out)
	}
	_, out = req(t, admin, "GET", a.url+"/kv?prefix=x", "tok", nil)
	if entries := out["entries"].([]any); len(entries) != 1 {
		t.Fatalf("%v", out)
	} else if entry := entries[0].(map[string]any); entry["written_by"] != "api" ||
		entry["actor_id"] != "user-anna" || entry["actor_label"] != "anna@example.com" || entry["actor_kind"] != "human" {
		t.Fatalf("/kv dropped the attribution /fetch already carries: %v", entry)
	}
	resp, _ = req(t, admin, "GET", a.url+"/debug/state", "tok", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("debug/state: %d", resp.StatusCode)
	}
	// healthz and metrics are open
	resp, _ = req(t, admin, "GET", a.url+"/healthz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
	r2, err := client(nil).Get(a.url + "/metrics")
	if err != nil || r2.StatusCode != http.StatusOK {
		t.Fatalf("metrics: %v %d", err, r2.StatusCode)
	}
	r2.Body.Close()
}

func TestFetchFiltersMetricsByRepeatedSignalIDAndAdvancesPastSkippedRecords(t *testing.T) {
	a := newAPI(t)
	_, _, err := a.st.Append("metrics", []store.Record{
		{Topic: "colca/v1/_Metric/n-test/line1/s1", Payload: []byte(`{"signal_id":"s1","value":1}`), TS: 1},
		{Topic: "colca/v1/_Metric/n-test/line1/s2", Payload: []byte(`{"signal_id":"s2","value":2}`), TS: 2},
		{Topic: "colca/v1/_AlarmStateChange/n-test/alarms/a1", Payload: []byte(`{"event_id":"e1"}`), TS: 3},
		{Topic: "colca/v1/_Metric/n-test/line2/s3", Payload: []byte(`{"signal_id":"s3","value":3}`), TS: 4},
		{Topic: "colca/v1/_Metric/n-test/line1/bad", Payload: []byte(`{"signal_id":`), TS: 5},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, out := req(t, client(nil), "GET",
		a.url+"/fetch?stream=metrics&cursor=filtered&max=10&prefix=line1&signal_id=s1&signal_id=s3",
		"tok", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fetch = %d: %v", resp.StatusCode, out)
	}
	records := out["records"].([]any)
	if len(records) != 1 || records[0].(map[string]any)["offset"] != float64(1) {
		t.Fatalf("filtered records = %v, want only line1/s1", records)
	}
	if out["next"] != float64(6) {
		t.Fatalf("next = %v, want 6 after scanning every skipped record", out["next"])
	}

	resp, out = req(t, client(nil), "GET",
		a.url+"/fetch?stream=metrics&cursor=empty-filter&max=10&signal_id=absent",
		"tok", nil)
	if resp.StatusCode != http.StatusOK || len(out["records"].([]any)) != 0 || out["next"] != float64(6) {
		t.Fatalf("all-filtered fetch = %d %v", resp.StatusCode, out)
	}
}

func TestFetchRejectsInvalidSignalFilters(t *testing.T) {
	a := newAPI(t)
	tests := []string{
		"/fetch?stream=entities&cursor=c&signal_id=s1",
		"/fetch?stream=metrics&cursor=c&signal_id=",
	}
	for _, path := range tests {
		resp, _ := req(t, client(nil), "GET", a.url+path, "tok", nil)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("GET %s = %d, want 400", path, resp.StatusCode)
		}
	}

	path := "/fetch?stream=metrics&cursor=c"
	for i := 0; i <= 1000; i++ {
		path += fmt.Sprintf("&signal_id=s%d", i)
	}
	resp, _ := req(t, client(nil), "GET", a.url+path, "tok", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("fetch with 1001 signal ids = %d, want 400", resp.StatusCode)
	}
}

func TestSignalFilterDoesNotBypassMachineReadScope(t *testing.T) {
	a := newAPI(t)
	_, _, err := a.st.Append("metrics", []store.Record{
		{Topic: "colca/v1/_Metric/n-test/m1/temperature", Payload: []byte(`{"signal_id":"shared-id","value":1}`), TS: 1},
		{Topic: "colca/v1/_Metric/n-test/other/temperature", Payload: []byte(`{"signal_id":"shared-id","value":2}`), TS: 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, out := req(t, client(a.m1), "GET",
		a.url+"/fetch?stream=metrics&cursor=m1%2Ffiltered&signal_id=shared-id", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("machine fetch = %d: %v", resp.StatusCode, out)
	}
	records := out["records"].([]any)
	if len(records) != 1 || records[0].(map[string]any)["topic"] != "colca/v1/_Metric/n-test/m1/temperature" {
		t.Fatalf("scoped filtered records = %v", records)
	}
}

func TestAdminConfigureResponseNamesProducedStateOffset(t *testing.T) {
	a := newAPI(t)
	a.eng.SetExecutor(engine.Executors(uns.NewConfigExec(a.eng.EntityStore(), a.reg, nil, nil, nil, nil)))

	resp, out := req(t, client(nil), "POST", a.url+"/publish", "tok", map[string]any{
		"topic": "colca/v1/_CmdConfigure/n-test/definition/upsert",
		"payload": map[string]any{
			"correlation_id": "api-rw-1",
			"expires_at":     time.Now().Add(time.Minute).UnixMilli(),
			"definitions": []map[string]any{{
				"contract": "_ExternalSystem",
				"definition": map[string]any{
					"id": "ext-1", "key": "tcdb", "name": "TCDB", "system_type": "database",
				},
			}},
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("publish = %d, body %v", resp.StatusCode, out)
	}
	command := out["command"].(map[string]any)
	writes := command["state_writes"].([]any)
	write := writes[0].(map[string]any)
	if command["result_code"].(float64) != 200 || write["stream"] != "definitions" || write["offset"].(float64) == 0 {
		t.Fatalf("command outcome = %v", command)
	}
}

// Route matrix for a machine: data routes allowed and scoped, admin routes
// forbidden.
func TestMachineRouteMatrix(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	mc := client(a.m1)

	// Seed: one record in m1's zone, one outside (admin publish).
	for _, tp := range []string{"colca/v1/_Metric/m1/m1/temp", "colca/v1/_Metric/other/elsewhere/temp"} {
		resp, out := req(t, admin, "POST", a.url+"/publish", "tok", map[string]any{"topic": tp, "payload": map[string]any{"v": 1.0}})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("seed %s: %d %v", tp, resp.StatusCode, out)
		}
	}

	// machine publish: own zone OK (engine rules apply)
	resp, _ := req(t, mc, "POST", a.url+"/publish", "", map[string]any{"topic": "colca/v1/_Metric/n-test/m1/rpm", "payload": map[string]any{"v": 2.0}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("machine publish: %d", resp.StatusCode)
	}
	// Wrong level 4: 422 from the level-4 rule; the write-scope check is never reached.
	resp, _ = req(t, mc, "POST", a.url+"/publish", "", map[string]any{"topic": "colca/v1/_Metric/other/x", "payload": map[string]any{"v": 2.0}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("machine spoof publish: want 422, got %d", resp.StatusCode)
	}
	// Right level 4 but outside the write zone: 403 write_denied.
	resp, out := req(t, mc, "POST", a.url+"/publish", "", map[string]any{"topic": "colca/v1/_Metric/n-test/outside-m1/x", "payload": map[string]any{"v": 2.0}})
	if resp.StatusCode != http.StatusForbidden || out["reason"] != "write_denied" {
		t.Fatalf("machine publish outside its write zone: want 403 reason write_denied, got %d %v", resp.StatusCode, out)
	}

	// machine fetch: cursor must be namespaced, records scope-filtered
	resp, _ = req(t, mc, "GET", a.url+"/fetch?stream=metrics&cursor=foreign&max=10", "", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("un-namespaced machine cursor: want 403, got %d", resp.StatusCode)
	}
	_, out = req(t, mc, "GET", a.url+"/fetch?stream=metrics&cursor=m1/c&max=10", "", nil)
	recs := out["records"].([]any)
	for _, r := range recs {
		topic := r.(map[string]any)["topic"].(string)
		if topic == "colca/v1/_Metric/other/elsewhere/temp" {
			t.Fatalf("out-of-zone record leaked to machine: %v", recs)
		}
	}
	if len(recs) != 2 { // m1/temp + m1/rpm, not the foreign one
		t.Fatalf("machine fetch records = %d, want 2: %v", len(recs), recs)
	}

	// machine ack on its own cursor OK; foreign cursor 403
	resp, _ = req(t, mc, "POST", a.url+"/ack", "", map[string]any{"cursor": "m1/c", "stream": "metrics", "offset": 1})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("machine ack own cursor: %d", resp.StatusCode)
	}
	resp, _ = req(t, mc, "POST", a.url+"/ack", "", map[string]any{"cursor": "other/c", "stream": "metrics", "offset": 1})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("machine ack foreign cursor: want 403, got %d", resp.StatusCode)
	}

	// machine kv: scope-filtered (own zone + own _EnrolledIdentity entry visible, the
	// foreign record not)
	_, out = req(t, mc, "GET", a.url+"/kv", "", nil)
	for _, e := range out["entries"].([]any) {
		if e.(map[string]any)["path"] == "elsewhere/temp" {
			t.Fatalf("out-of-zone kv entry leaked: %v", out)
		}
	}

	// admin routes forbidden for machines
	for _, probe := range []struct{ method, path string }{
		{"GET", "/enroll"},
		{"GET", "/debug/state"},
	} {
		resp, _ := req(t, mc, probe.method, a.url+probe.path, "", nil)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s %s as machine: want 403, got %d", probe.method, probe.path, resp.StatusCode)
		}
	}
	resp, _ = req(t, mc, "POST", a.url+"/enroll", "", a.m1.EntryJSON(t, "external", "m1"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("machine enroll: want 403, got %d", resp.StatusCode)
	}
}

func TestEnrollmentRoutes(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)

	m2 := authtest.NewMachine(t, "m2")
	resp, out := req(t, admin, "POST", a.url+"/enroll", "tok", m2.EntryJSON(t, "external", a.place(t, "m2")))
	if resp.StatusCode != http.StatusOK || out["ulid"] != "m2" {
		t.Fatalf("enroll: %d %v", resp.StatusCode, out)
	}
	// the new identity works immediately
	resp, _ = req(t, client(m2), "GET", a.url+"/fetch?stream=metrics&cursor=m2/c&max=1", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("enrolled machine fetch: %d", resp.StatusCode)
	}

	// duplicate pubkey → 409
	dup := &authtest.Machine{ULID: "m3", Pubkey: m2.Pubkey}
	resp, _ = req(t, admin, "POST", a.url+"/enroll", "tok", dup.EntryJSON(t, "external", a.place(t, "m3")))
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("dup pubkey: want 409, got %d", resp.StatusCode)
	}
	// invalid entry → 422
	resp, _ = req(t, admin, "POST", a.url+"/enroll", "tok", []byte(`{"ulid":"","pubkey":"x","kind":"external"}`))
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("invalid entry: want 422, got %d", resp.StatusCode)
	}

	// list
	_, out = req(t, admin, "GET", a.url+"/enroll", "tok", nil)
	if entries := out["entries"].([]any); len(entries) != 2 {
		t.Fatalf("list: %v", out)
	}

	// revoke; the key stops working
	resp, _ = req(t, admin, "DELETE", a.url+"/enroll/m2", "tok", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("revoke: %d", resp.StatusCode)
	}
	resp, _ = req(t, client(m2), "GET", a.url+"/fetch?stream=metrics&cursor=m2/c&max=1", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked machine fetch: want 401, got %d", resp.StatusCode)
	}
	// second revoke → 404
	resp, _ = req(t, admin, "DELETE", a.url+"/enroll/m2", "tok", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("second revoke: want 404, got %d", resp.StatusCode)
	}
}

// Drain route: 404 for an unknown ULID, 409 for a machine or a second drain, 200
// with status draining on success, and the drained identity keeps working.
func TestDrainRouteDoorSemantics(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)

	// Unknown ulid → 404.
	resp, out := req(t, admin, "POST", a.url+"/enroll/nosuch/drain", "tok", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("drain unknown ulid: want 404, got %d (%v)", resp.StatusCode, out)
	}

	// A machine (kind=machine, already enrolled as a.m1) → 409.
	resp, out = req(t, admin, "POST", a.url+"/enroll/"+a.m1.ULID+"/drain", "tok", nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("drain a machine: want 409, got %d (%v)", resp.StatusCode, out)
	}

	// Enroll a node child, then drain it.
	child := authtest.NewMachine(t, "n-child1")
	authtest.EnrollNodeAt(t, a.reg, a.eng, child.ULID, child.Pubkey, "child1")
	resp, out = req(t, admin, "POST", a.url+"/enroll/"+child.ULID+"/drain", "tok", nil)
	if resp.StatusCode != http.StatusOK || out["status"] != "draining" {
		t.Fatalf("drain: want 200 status=draining, got %d %v", resp.StatusCode, out)
	}

	// Second drain on the same child → 409.
	resp, out = req(t, admin, "POST", a.url+"/enroll/"+child.ULID+"/drain", "tok", nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second drain: want 409, got %d (%v)", resp.StatusCode, out)
	}

	// The list reflects the draining status.
	_, out = req(t, admin, "GET", a.url+"/enroll", "tok", nil)
	entries := out["entries"].([]any)
	found := false
	for _, raw := range entries {
		e := raw.(map[string]any)
		if e["ulid"] == child.ULID {
			found = true
			if e["status"] != "draining" {
				t.Fatalf("listed entry status = %v, want draining", e["status"])
			}
		}
	}
	if !found {
		t.Fatalf("drained child missing from GET /enroll: %v", entries)
	}
}

// DELETE works immediately during a drain and records the forced outcome,
// decrementing colca_drains_active.
func TestDeleteDuringDrainIsImmediateAndRecordsForced(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)

	child := authtest.NewMachine(t, "n-child1")
	authtest.EnrollNodeAt(t, a.reg, a.eng, child.ULID, child.Pubkey, "child1")
	if resp, out := req(t, admin, "POST", a.url+"/enroll/"+child.ULID+"/drain", "tok", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("drain: %d %v", resp.StatusCode, out)
	}
	if v := metricstest.Value(t, a.m, `colca_drains_active`); v != 1 {
		t.Fatalf("colca_drains_active after drain start = %v, want 1", v)
	}

	resp, out := req(t, admin, "DELETE", a.url+"/enroll/"+child.ULID, "tok", nil)
	if resp.StatusCode != http.StatusOK || out["revoked"] != true {
		t.Fatalf("DELETE during drain must succeed immediately: %d %v", resp.StatusCode, out)
	}
	if _, ok := a.reg.Get(child.ULID); ok {
		t.Fatal("child must be gone from the registry after DELETE")
	}
	if v := metricstest.Value(t, a.m, `colca_drains_active`); v != 0 {
		t.Fatalf("colca_drains_active after forced revoke = %v, want 0", v)
	}
	if v := metricstest.Value(t, a.m, `colca_drains_completed_total{outcome="forced"}`); v != 1 {
		t.Fatalf(`colca_drains_completed_total{outcome="forced"} = %v, want 1`, v)
	}
	if v := metricstest.Value(t, a.m, `colca_drains_completed_total{outcome="delivered"}`); v != 0 {
		t.Fatalf(`colca_drains_completed_total{outcome="delivered"} = %v, want 0 (this was forced, not delivered)`, v)
	}

	// Deleting a machine that never drained leaves the drain counters alone.
	m2 := authtest.NewMachine(t, "m2")
	authtest.EnrollAt(t, a.reg, a.eng, m2, "m2")
	if resp, out := req(t, admin, "DELETE", a.url+"/enroll/m2", "tok", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("plain delete: %d %v", resp.StatusCode, out)
	}
	if v := metricstest.Value(t, a.m, `colca_drains_completed_total{outcome="forced"}`); v != 1 {
		t.Fatalf(`plain (non-draining) DELETE must not add to forced outcomes, colca_drains_completed_total{outcome="forced"} = %v, want still 1`, v)
	}
}

// _EnrolledIdentity may not enter through /publish, not even with the admin token.
func TestEnrolledIdentityRejectedOnPublish(t *testing.T) {
	a := newAPI(t)
	resp, out := req(t, client(nil), "POST", a.url+"/publish", "tok",
		map[string]any{"topic": "colca/v1/_EnrolledIdentity/n1/_colca/identities/x", "payload": map[string]any{"ulid": "x"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d (%v)", resp.StatusCode, out)
	}
}

// An unknown client key must not fall through to token auth, even when a
// valid token is attached.
func TestUnknownClientCertNeverFallsThrough(t *testing.T) {
	a := newAPI(t)
	stranger := authtest.NewMachine(t, "stranger")
	resp, _ := req(t, client(stranger), "GET", a.url+"/fetch?stream=metrics&cursor=x/c&max=1", "tok", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown cert with valid token: want 401, got %d", resp.StatusCode)
	}
}

// /fetch keeps its exact response fields; the gap object composes into this
// response, so the base shape is a contract.
func TestFetchWireShape(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	if resp, out := req(t, admin, "POST", a.url+"/publish", "tok",
		map[string]any{"topic": "colca/v1/_Metric/n-test/x", "payload": map[string]any{"v": 7.5}}); resp.StatusCode != http.StatusOK {
		t.Fatalf("%v", out)
	}
	_, out := req(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=w/c&max=10", "tok", nil)
	recs := out["records"].([]any)
	if len(recs) != 1 {
		t.Fatalf("%v", out)
	}
	rec := recs[0].(map[string]any)
	for _, k := range []string{"offset", "origin_offset", "topic", "payload", "ts", "written_by", "actor_id", "actor_label", "actor_kind"} {
		if _, ok := rec[k]; !ok {
			t.Fatalf("record missing %q: %v", k, rec)
		}
	}
	if fmt.Sprintf("%v", rec["payload"]) != "map[v:7.5]" {
		t.Fatalf("payload not passed through raw: %v", rec["payload"])
	}
	if _, ok := out["next"]; !ok {
		t.Fatalf("response missing next: %v", out)
	}
}

// seedMetrics appends n records with deterministic timestamps 1000, 2000…
// directly through the store, so the gap tests can assert exact wire JSON.
func seedMetrics(t *testing.T, s *store.Store, n int, topic string) {
	t.Helper()
	var recs []store.Record
	for i := 1; i <= n; i++ {
		recs = append(recs, store.Record{
			Topic:   topic,
			Payload: []byte(fmt.Sprintf(`{"v":%d}`, i)),
			TS:      int64(i) * 1000,
		})
	}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
}

// Below the LWM, /fetch adds the gap object (from_offset is the cursor position,
// to_offset LWM-1, first_ts and last_ts from the prune journal) and records start
// at the LWM. Asserted as exact JSON.
func TestFetchGapExactWireShape(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	const topic = "colca/v1/_Metric/n-test/line1/temp"
	seedMetrics(t, a.st, 5, topic) // offsets 1..5, TS 1000..5000
	if n, err := a.st.Prune("metrics", 4, nil, nil); err != nil || n != 3 {
		t.Fatalf("prune: %d %v", n, err)
	}
	// A consumer that had read offset 1 before the prune: position 2 < LWM 4.
	if !a.st.CursorAck("lag", "metrics", 2) {
		t.Fatal("ack must move")
	}

	surviving := `{"actor_id":"","actor_kind":"","actor_label":"","offset":4,"origin_offset":4,"payload":{"v":4},"topic":"` + topic + `","ts":4000,"written_by":""},` +
		`{"actor_id":"","actor_kind":"","actor_label":"","offset":5,"origin_offset":5,"payload":{"v":5},"topic":"` + topic + `","ts":5000,"written_by":""}`

	resp, body := raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=lag", "tok", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fetch: %d %s", resp.StatusCode, body)
	}
	want := `{"gap":{"stream":"metrics","from_offset":2,"to_offset":3,"first_ts":1000,"last_ts":3000,"approx":false},` +
		`"next":6,"records":[` + surviving + `]}` + "\n"
	if body != want {
		t.Fatalf("fetch gap wire shape:\n got %s\nwant %s", body, want)
	}

	// The gap is per-consumer and side-effect free: an identical second fetch
	// sees the identical gap (no cursor movement).
	_, body2 := raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=lag", "tok", "")
	if body2 != want {
		t.Fatalf("second fetch differs — /fetch must not move the cursor:\n%s", body2)
	}

	// A new cursor on a pruned stream gets the gap too.
	_, body = raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=fresh", "tok", "")
	want = `{"gap":{"stream":"metrics","from_offset":1,"to_offset":3,"first_ts":1000,"last_ts":3000,"approx":false},` +
		`"next":6,"records":[` + surviving + `]}` + "\n"
	if body != want {
		t.Fatalf("fresh-cursor gap wire shape:\n got %s\nwant %s", body, want)
	}

	// Acking at/past the LWM is the consumer's explicit acknowledgment of the
	// gap: it disappears, and /ack itself is unaffected by the gap machinery.
	_, out := req(t, admin, "POST", a.url+"/ack", "tok", map[string]any{"cursor": "lag", "stream": "metrics", "offset": 4})
	if out["moved"] != true {
		t.Fatalf("ack past the LWM must move the cursor: %v", out)
	}
	_, body = raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=lag", "tok", "")
	want = `{"next":6,"records":[{"actor_id":"","actor_kind":"","actor_label":"","offset":5,"origin_offset":5,"payload":{"v":5},"topic":"` + topic + `","ts":5000,"written_by":""}]}` + "\n"
	if body != want {
		t.Fatalf("after ack past LWM the gap object must disappear:\n got %s\nwant %s", body, want)
	}

	// A cursor exactly AT the LWM was never pruned past: no gap.
	if !a.st.CursorAck("edge", "metrics", 4) {
		t.Fatal("ack must move")
	}
	_, body = raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=edge", "tok", "")
	if strings.Contains(body, `"gap"`) {
		t.Fatalf("cursor at the LWM must not see a gap: %s", body)
	}

	// An untouched stream never carries a gap key at all.
	_, body = raw(t, admin, "GET", a.url+"/fetch?stream=entities&cursor=any", "tok", "")
	if strings.Contains(body, `"gap"`) {
		t.Fatalf("unpruned stream must not carry a gap: %s", body)
	}
}

// Every response carrying a gap counts
// colca_gap_served_total{stream="metrics",surface="fetch"}; a response without one
// does not.
func TestFetchGapCountsGapServed(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	const topic = "colca/v1/_Metric/n-test/line1/temp"
	seedMetrics(t, a.st, 5, topic) // offsets 1..5
	if n, err := a.st.Prune("metrics", 4, nil, nil); err != nil || n != 3 {
		t.Fatalf("prune: %d %v", n, err)
	}

	const gapServed = `colca_gap_served_total{stream="metrics",surface="fetch"}`
	if v := metricstest.Value(t, a.m, gapServed); v != 0 {
		t.Fatalf("%s = %v before any gap-carrying fetch, want 0", gapServed, v)
	}

	// A cursor at the LWM never sees a gap: the counter must not move.
	if !a.st.CursorAck("edge", "metrics", 4) {
		t.Fatal("ack must move")
	}
	if _, body := raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=edge", "tok", ""); strings.Contains(body, `"gap"`) {
		t.Fatalf("cursor at the LWM must not see a gap: %s", body)
	}
	if v := metricstest.Value(t, a.m, gapServed); v != 0 {
		t.Fatalf("%s = %v after a gap-FREE fetch, want unchanged 0", gapServed, v)
	}

	// A cursor below the LWM sees a gap: exactly one increment.
	if !a.st.CursorAck("lag", "metrics", 2) {
		t.Fatal("ack must move")
	}
	if _, body := raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=lag", "tok", ""); !strings.Contains(body, `"gap"`) {
		t.Fatalf("cursor below the LWM must see a gap: %s", body)
	}
	if v := metricstest.Value(t, a.m, gapServed); v != 1 {
		t.Fatalf("%s = %v after one gap-carrying fetch, want 1", gapServed, v)
	}

	// The gap does not move the cursor: a second fetch sees the same gap and counts
	// again.
	if _, body := raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=lag", "tok", ""); !strings.Contains(body, `"gap"`) {
		t.Fatalf("repeat fetch must still see the gap: %s", body)
	}
	if v := metricstest.Value(t, a.m, gapServed); v != 2 {
		t.Fatalf("%s = %v after two gap-carrying fetches, want 2", gapServed, v)
	}
}

// approx is true exactly when a coalesced journal entry answered first_ts, driven
// through real coalescing with more prune runs than the journal holds.
func TestFetchGapApproxFromCoalescedJournal(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	seedMetrics(t, a.st, 70, "colca/v1/_Metric/n-test/line1/temp")
	// 66 single-record prune runs → 66 journal entries → the cap (64) forces
	// the two oldest pairs to coalesce. LWM ends at 67.
	for upTo := uint64(2); upTo <= 67; upTo++ {
		if n, err := a.st.Prune("metrics", upTo, nil, nil); err != nil || n != 1 {
			t.Fatalf("prune to %d: %d %v", upTo, n, err)
		}
	}

	// A fresh cursor's from_offset (1) falls into the coalesced oldest entry.
	_, body := raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=fresh", "tok", "")
	var out struct {
		Gap struct {
			FromOffset uint64 `json:"from_offset"`
			ToOffset   uint64 `json:"to_offset"`
			FirstTS    int64  `json:"first_ts"`
			LastTS     int64  `json:"last_ts"`
			Approx     bool   `json:"approx"`
		} `json:"gap"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("%v in %s", err, body)
	}
	if out.Gap.FromOffset != 1 || out.Gap.ToOffset != 66 {
		t.Fatalf("gap span [%d..%d], want [1..66]", out.Gap.FromOffset, out.Gap.ToOffset)
	}
	if out.Gap.FirstTS != 1000 || out.Gap.LastTS != 66000 {
		t.Fatalf("gap time span [%d..%d], want [1000..66000] (coalescing must keep the union)", out.Gap.FirstTS, out.Gap.LastTS)
	}
	if !out.Gap.Approx {
		t.Fatal("first_ts answered by a coalesced journal entry must set approx=true")
	}

	// A position answered by an untouched (non-coalesced) entry stays exact.
	if !a.st.CursorAck("mid", "metrics", 50) {
		t.Fatal("ack must move")
	}
	_, body = raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=mid", "tok", "")
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("%v in %s", err, body)
	}
	if out.Gap.FromOffset != 50 || out.Gap.Approx {
		t.Fatalf("gap %+v: a non-coalesced entry answering first_ts must keep approx=false", out.Gap)
	}
}

// An unknown stream is 400 with an error body: no gap arithmetic and no empty 200.
func TestFetchUnknownStreamRejected(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	for _, stream := range []string{"bogus", ""} {
		resp, body := raw(t, admin, "GET", a.url+"/fetch?stream="+stream+"&cursor=c1", "tok", "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("stream %q: want 400, got %d (%s)", stream, resp.StatusCode, body)
		}
		if !strings.Contains(body, `"error"`) {
			t.Fatalf("stream %q: 400 must carry an error body: %s", stream, body)
		}
		if strings.Contains(body, `"gap"`) {
			t.Fatalf("stream %q: fabricated a gap: %s", stream, body)
		}
	}
}

// Payloads stay raw JSON both ways: an integer beyond float64 keeps its digits and
// is never base64 or quoted. Asserted on the raw body.
func TestPayloadStaysRawJSON(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	if resp, out := req(t, admin, "POST", a.url+"/publish", "tok", map[string]any{
		"topic":   "colca/v1/_Metric/n-test/line1/temp",
		"payload": json.RawMessage(`{"v":1,"seq":9007199254740993}`),
	}); resp.StatusCode != http.StatusOK {
		t.Fatalf("publish: %d %v", resp.StatusCode, out)
	}

	_, body := raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=rawc", "tok", "")
	if !strings.Contains(body, `"payload":{`) {
		t.Fatalf("fetch payload must be a JSON object, got %s", body)
	}
	if !strings.Contains(body, `"seq":9007199254740993`) {
		t.Fatalf("fetch payload lost exact number form: %s", body)
	}

	_, body = raw(t, admin, "GET", a.url+"/kv?prefix=line1", "tok", "")
	if !strings.Contains(body, `"payload":{`) {
		t.Fatalf("kv payload must be a JSON object, got %s", body)
	}
	if !strings.Contains(body, `"seq":9007199254740993`) {
		t.Fatalf("kv payload lost exact number form: %s", body)
	}
	if !strings.Contains(body, `"path":"line1/temp"`) || !strings.Contains(body, `"node_id":"n-test"`) {
		t.Fatalf("kv entry must carry path and node_id: %s", body)
	}
}

// prefix filters on the hierarchy path, not the raw topic, and max outside 1..1000
// falls back to 100. Run as admin to keep grants out of it.
func TestFetchPrefixAndMax(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	for i, tp := range []string{"colca/v1/_Metric/n-test/line1/temp", "colca/v1/_Metric/n-test/line2/temp"} {
		if resp, out := req(t, admin, "POST", a.url+"/publish", "tok",
			map[string]any{"topic": tp, "payload": map[string]any{"v": float64(i)}}); resp.StatusCode != http.StatusOK {
			t.Fatalf("seed %s: %d %v", tp, resp.StatusCode, out)
		}
	}

	_, out := req(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=p1&prefix=line1", "tok", nil)
	recs := out["records"].([]any)
	if len(recs) != 1 {
		t.Fatalf("prefix must filter on the hierarchy path: %v", out)
	}
	if topic := recs[0].(map[string]any)["topic"]; topic != "colca/v1/_Metric/n-test/line1/temp" {
		t.Fatalf("wrong record survived the prefix filter: %v", topic)
	}
	// "n-test" is in the topic but not at the head of the path → no match.
	_, out = req(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=p2&prefix=n-test", "tok", nil)
	if len(out["records"].([]any)) != 0 {
		t.Fatalf("prefix must not match the raw topic: %v", out)
	}
	for _, q := range []string{"&max=0", "&max=-5", "&max=99999", "&max=nonsense", ""} {
		_, out = req(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=m1"+q, "tok", nil)
		if len(out["records"].([]any)) != 2 {
			t.Fatalf("max=%q must fall back to the default: %v", q, out)
		}
	}
}

// An empty-payload tombstone comes out of /fetch as payload: null. The sequence
// mirrors a local service registering with _ServiceDetails and then tombstoning
// it.
func TestFetchServesATombstoneAsJSONNull(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "svc1", "")
	entry, ok := testRegistry(t, h).ByName("svc1")
	if !ok {
		t.Fatal("svc1 did not register")
	}

	publish := func(body string) {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(body))
		r.Header.Set("X-Colca-Service", "svc1")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Code != 200 {
			t.Fatalf("publish %s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
	publish(fmt.Sprintf(`{"topic":"colca/v1/_ServiceDetails/n-test/_service","payload":{"id":%q}}`, entry.ULID))
	// The tombstone goes straight through IngestClient with a non-nil empty []byte, as
	// an empty MQTT PUBLISH does. An omitted payload through POST /publish would be a
	// nil RawMessage and would not reproduce the failure.
	if _, err := h.eng.IngestClient(entry.ULID, "colca/v1/_ServiceDetails/n-test/_service", []byte{}); err != nil {
		t.Fatalf("tombstone ingest: %v", err)
	}

	fetchReq := httptest.NewRequest(http.MethodGet, "/fetch?stream=entities&cursor="+uns.LocalCursorPrefix+"svc1/tomb1&max=10", nil)
	fetchReq.Header.Set("X-Colca-Service", "svc1")
	fetchRec := httptest.NewRecorder()
	h.ServeHTTP(fetchRec, fetchReq)
	if fetchRec.Code != 200 {
		t.Fatalf("GET /fetch = %d: %s", fetchRec.Code, fetchRec.Body.String())
	}
	var out struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.Unmarshal(fetchRec.Body.Bytes(), &out); err != nil {
		// The regression: a 200 whose body decodes to nothing.
		t.Fatalf("response body did not decode as JSON (the tombstone bug): %v; body=%q", err, fetchRec.Body.String())
	}
	if len(out.Records) != 3 {
		t.Fatalf("records = %v, want the fixture's own _EnrolledIdentity plus the _ServiceDetails record and its tombstone", out.Records)
	}
	tomb := out.Records[2]
	if tomb["topic"] != "colca/v1/_ServiceDetails/n-test/_service" {
		t.Fatalf("wrong record at index 1: %v", tomb)
	}
	if payload, has := tomb["payload"]; !has || payload != nil {
		t.Fatalf("tombstone record must carry payload:null, got %#v", tomb)
	}
}

// /debug/state reports the node's ulid and per-stream next offsets. Offsets are
// checked as the delta a publish causes on top of the fixture's baseline.
func TestDebugStateFieldCorrectness(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)

	before := map[string]float64{}
	_, out := req(t, admin, "GET", a.url+"/debug/state", "tok", nil)
	if out["ulid"] != "n-test" {
		t.Fatalf("debug state must report the node ulid: %v", out)
	}
	for stream, v := range out["streams"].(map[string]any) {
		before[stream] = v.(map[string]any)["next_offset"].(float64)
	}
	// The baseline is deterministic: one enrolled machine is two entity records, its
	// element and its _EnrolledIdentity.
	if before["metrics"] != 1 || before["entities"] != 3 || before["commands"] != 1 ||
		before["definitions"] != 1 || before["audit"] != 1 {
		t.Fatalf("fixture baseline offsets = %v, want all event streams enumerated", before)
	}

	if resp, pub := req(t, admin, "POST", a.url+"/publish", "tok", map[string]any{
		"topic": "colca/v1/_Metric/n-test/line1/temp", "payload": map[string]any{"v": 1.0},
	}); resp.StatusCode != http.StatusOK {
		t.Fatalf("publish: %d %v", resp.StatusCode, pub)
	}

	_, out = req(t, admin, "GET", a.url+"/debug/state", "tok", nil)
	streams := out["streams"].(map[string]any)
	for stream, delta := range map[string]float64{
		"metrics": 1, "entities": 0, "commands": 0, "definitions": 0, "audit": 0,
	} {
		got := streams[stream].(map[string]any)["next_offset"].(float64)
		if want := before[stream] + delta; got != want {
			t.Fatalf("debug state %s.next_offset = %v, want %v", stream, got, want)
		}
	}
}

// A correct token authorizes, a wrong token of the same length is rejected, and an
// empty configured token authorizes neither an empty nor a non-empty header.
func TestAdminTokenComparisonRejectsSameLengthMismatch(t *testing.T) {
	cfg := &config.Config{ULID: "n-test", API: config.API{Token: "tok"}}
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	srv := plainHandler(t, cfg, metrics.New(st, config.Retention{}, nil))

	resp, _ := req(t, srv.Client(), "GET", srv.URL+"/kv", "tok", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("correct token: want 200, got %d", resp.StatusCode)
	}

	resp, _ = req(t, srv.Client(), "GET", srv.URL+"/kv", "tik", nil) // same length as "tok", one byte off
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong same-length token: want 401, got %d", resp.StatusCode)
	}

	emptyCfg := &config.Config{ULID: "n-notoken"}
	emptySt, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { emptySt.Close() })
	emptySrv := plainHandler(t, emptyCfg, metrics.New(emptySt, config.Retention{}, nil))
	for _, header := range []string{"", "tok"} {
		resp, _ = req(t, emptySrv.Client(), "GET", emptySrv.URL+"/kv", header, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("empty configured token with header %q: want 401, got %d", header, resp.StatusCode)
		}
	}
}

// plainHandler serves a Handler over plain httptest, for tests that do not depend
// on TLS.
func plainHandler(t *testing.T, cfg *config.Config, m *metrics.Metrics) *httptest.Server {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	reg, err := registry.New(s, cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(s, cfg, reg, nil, m, nil)
	reg.SetNamespace(eng.Elements())
	srv := httptest.NewServer(Handler(eng, cfg, reg, nil, m, testBlobs(t, cfg), "deadbeef", false, nil))
	t.Cleanup(srv.Close)
	return srv
}

// newTestHandler builds a Handler without a listener. Set cfg.Limits first, since
// Handler fixes its request-size cap at build time.
func newTestHandler(t *testing.T, cfg *config.Config) http.Handler {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	reg, err := registry.New(s, cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(s, config.Retention{}, nil)
	eng := engine.New(s, cfg, reg, nil, m, nil)
	reg.SetNamespace(eng.Elements())
	return Handler(eng, cfg, reg, nil, m, testBlobs(t, cfg), "deadbeef", false, nil)
}

// doAdmin performs a request against a Handler's mux as admin via X-Colca-Token.
func doAdmin(t *testing.T, h http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r, err := http.NewRequest(method, path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Colca-Token", "tok")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	return rr
}

// publishBody builds a POST /publish body for _Metric padded by roughly padBytes;
// json.Marshal base64-encodes the []byte, like a real oversize payload.
func publishBody(t *testing.T, topic string, padBytes int) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"topic":   topic,
		"payload": map[string]any{"v": 1.0, "pad": make([]byte, padBytes)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The door caps the body before reading it into memory; Store.Append's record cap
// comes later.
func TestPublishRefusesAnOversizeBody(t *testing.T) {
	cfg := &config.Config{ULID: "n-test", API: config.API{Token: "tok"},
		Limits: config.Limits{MaxRecordBytes: config.ByteSize(1024)}}
	h := newTestHandler(t, cfg)

	// Presence first: a small publish must succeed through this same path.
	small := publishBody(t, "colca/v1/_Metric/n-test/a", 64)
	if rr := doAdmin(t, h, "POST", "/publish", small); rr.Code != http.StatusOK {
		t.Fatalf("small publish = %d, want 200: %s", rr.Code, rr.Body.String())
	}

	big := publishBody(t, "colca/v1/_Metric/n-test/b", 64*1024)
	if rr := doAdmin(t, h, "POST", "/publish", big); rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize publish = %d, want 413: %s", rr.Code, rr.Body.String())
	}
}

func TestAckAndEnrollRefuseOversizeBodies(t *testing.T) {
	local := newLocalHandler(t)
	ackBody := []byte(`{"cursor":"` + strings.Repeat("x", maxAckBodyBytes) + `","stream":"metrics","offset":1}`)
	ack := httptest.NewRequest(http.MethodPost, "/ack", bytes.NewReader(ackBody))
	ack.Header.Set("X-Colca-Service", "projector")
	ackResult := httptest.NewRecorder()
	local.ServeHTTP(ackResult, ack)
	if ackResult.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize ack = %d, want 413: %s", ackResult.Code, ackResult.Body.String())
	}

	admin := newTestHandler(t, &config.Config{ULID: "n-test", API: config.API{Token: "tok"}})
	enrollBody := []byte(`{"ulid":"` + strings.Repeat("x", maxEnrollBodyBytes) + `","kind":"external"}`)
	enrollResult := doAdmin(t, admin, http.MethodPost, "/enroll", enrollBody)
	if enrollResult.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize enroll = %d, want 413: %s", enrollResult.Code, enrollResult.Body.String())
	}
}

func TestRequestLimitReturns429RetryAfterAndMetric(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	m := metrics.New(st, config.Retention{}, nil)
	limiter := httplimit.New()
	policy := httplimit.Policy{RatePerSecond: 1, Burst: 1, PerCallerConcurrent: 1, GlobalConcurrent: 1}

	release, ok := acquireRequest(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/kv", nil),
		limiter, m, metrics.DoorHTTP, limitClassScan, "caller-1", policy)
	if !ok {
		t.Fatal("initial request was rejected")
	}
	release()

	result := httptest.NewRecorder()
	if _, ok := acquireRequest(result, httptest.NewRequest(http.MethodGet, "/kv", nil),
		limiter, m, metrics.DoorHTTP, limitClassScan, "caller-1", policy); ok {
		t.Fatal("request beyond burst was admitted")
	}
	if result.Code != http.StatusTooManyRequests || result.Header().Get("Retry-After") != "1" {
		t.Fatalf("limited response = %d Retry-After=%q, want 429/1", result.Code, result.Header().Get("Retry-After"))
	}
	line := `colca_http_request_limited_total{class="scan",door="http"}`
	if got := metricstest.Value(t, m, line); got != 1 {
		t.Fatalf("%s = %v, want 1", line, got)
	}
}

// The raw-body limit and Store.Append's record cap never both fire for one
// request, so a record rejection is counted once. This body fits the wire cap but
// decodes above MaxRecordBytes.
func TestPublishOversizeRecordCountsRecordRejectedOnce(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	cfg := &config.Config{ULID: "n-test", API: config.API{Token: "tok"},
		Limits: config.Limits{MaxRecordBytes: config.ByteSize(1024)}}
	// This test needs Store.Append's own cap, so it sets it on the store as node
	// startup does.
	st.SetMaxRecordBytes(cfg.Limits.EffectiveMaxRecordBytes())
	reg, err := registry.New(st, cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(st, config.Retention{}, nil)
	eng := engine.New(st, cfg, reg, nil, m, nil)
	reg.SetNamespace(eng.Elements())
	srv := httptest.NewServer(Handler(eng, cfg, reg, nil, m, testBlobs(t, cfg), "deadbeef", false, nil))
	t.Cleanup(srv.Close)
	line := `colca_record_rejects_total{reason="too_large"}`
	before := metricstest.Value(t, m, line)

	// maxPublishBody is 2*1024+4096 = 6144. This pad exceeds MaxRecordBytes once
	// decoded but keeps the body well below 6144, so only Store.Append can return the
	// 413.
	body := publishBody(t, "colca/v1/_Metric/n-test/oversize", 1200)
	if len(body) >= 6144 {
		t.Fatalf("test body is %d bytes, already at/over maxPublishBody (6144) — "+
			"shrink padBytes so only Store.Append's cap, not MaxBytesReader, can fire", len(body))
	}
	resp, body2 := req(t, srv.Client(), "POST", srv.URL+"/publish", "tok", body)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize record = %d, want 413: %v", resp.StatusCode, body2)
	}

	if got := metricstest.Value(t, m, line) - before; got != 1 {
		t.Fatalf("%s moved by %v, want exactly 1 (no double count between the "+
			"MaxBytesReader arm and Store.Append's ErrRecordTooLarge arm)", line, got)
	}
}

// A node built without a metrics registry has no /metrics route at all.
func TestNoMetricsRegistryMeansNoRoute(t *testing.T) {
	srv := plainHandler(t, &config.Config{ULID: "n-test", API: config.API{Token: "tok"}}, nil)
	resp, _ := raw(t, srv.Client(), "GET", srv.URL+"/metrics", "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("nil metrics: want 404 on /metrics, got %d", resp.StatusCode)
	}
}

// An empty configured token is a missing secret, not an invitation: nothing
// authenticates, and /healthz stays tokenless.
func TestEmptyConfiguredTokenDeniesEveryone(t *testing.T) {
	cfg := &config.Config{ULID: "n-notoken"}
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	srv := plainHandler(t, cfg, metrics.New(st, config.Retention{}, nil))

	for _, token := range []string{"", "tok"} {
		resp, _ := req(t, srv.Client(), "GET", srv.URL+"/kv", token, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("empty configured token with %q: want 401, got %d", token, resp.StatusCode)
		}
	}
	resp, _ := req(t, srv.Client(), "GET", srv.URL+"/healthz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatal("healthz must stay tokenless")
	}
}

// A machine below the LWM gets the gap even when every surviving record is outside
// its grants, while the records stay filtered. The gap carries only offsets.
func TestFetchGapServedToScopedMachine(t *testing.T) {
	a := newAPI(t)
	mc := client(a.m1) // enrolled with mount "m1": reads only its own zone

	// Every record lives outside m1's zone.
	seedMetrics(t, a.st, 5, "colca/v1/_Metric/other/elsewhere/temp")
	if n, err := a.st.Prune("metrics", 4, nil, nil); err != nil || n != 3 {
		t.Fatalf("prune: %d %v", n, err)
	}

	// m1's namespaced cursor starts at 1 < LWM 4 → gap; records filtered empty.
	resp, body := raw(t, mc, "GET", a.url+"/fetch?stream=metrics&cursor=m1/c", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("machine fetch: %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"gap"`) {
		t.Fatalf("machine below the LWM must see the gap regardless of grants: %s", body)
	}
	if !strings.Contains(body, `"records":[]`) {
		t.Fatalf("out-of-scope records must stay filtered next to the gap: %s", body)
	}

	// Acking past the LWM clears the gap for a machine as for admin.
	if resp, _ := req(t, mc, "POST", a.url+"/ack", "", map[string]any{"cursor": "m1/c", "stream": "metrics", "offset": 4}); resp.StatusCode != http.StatusOK {
		t.Fatalf("machine ack: %d", resp.StatusCode)
	}
	_, body = raw(t, mc, "GET", a.url+"/fetch?stream=metrics&cursor=m1/c", "", "")
	if strings.Contains(body, `"gap"`) {
		t.Fatalf("gap must clear after acking past the LWM: %s", body)
	}
}

// ---- human callers ----

// Route matrix for a human: scoped reads, {sub}/ cursors, commands-only publish,
// admin routes gated on admin:#.
func TestHumanRouteMatrix(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	hc := client(nil) // humans are certless; the token is the credential

	// Seed: one record in the granted zone, one outside.
	for _, tp := range []string{"colca/v1/_Metric/m1/m1/temp", "colca/v1/_Metric/x/elsewhere/temp"} {
		if resp, out := req(t, admin, "POST", a.url+"/publish", "tok",
			map[string]any{"topic": tp, "payload": map[string]any{"v": 1.0}}); resp.StatusCode != http.StatusOK {
			t.Fatalf("seed %s: %d %v", tp, resp.StatusCode, out)
		}
	}
	tok := a.mint("anna", []string{"read:" + authtest.ElementID("m1") + "/#", "cmd:" + authtest.ElementID("m1") + "/#:param"})

	// fetch: scoped to read grants, cursor must be {sub}/-namespaced.
	resp, _ := bearerReq(t, hc, "GET", a.url+"/fetch?stream=metrics&cursor=foreign&max=10", tok, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("un-namespaced human cursor: want 403, got %d", resp.StatusCode)
	}
	_, out := bearerReq(t, hc, "GET", a.url+"/fetch?stream=metrics&cursor=anna/c&max=10", tok, nil)
	recs := out["records"].([]any)
	if len(recs) != 1 || recs[0].(map[string]any)["topic"] != "colca/v1/_Metric/m1/m1/temp" {
		t.Fatalf("human fetch must be scoped to grants: %v", recs)
	}

	// ack: own cursor moves, foreign cursor 403.
	if resp, _ := bearerReq(t, hc, "POST", a.url+"/ack", tok,
		map[string]any{"cursor": "anna/c", "stream": "metrics", "offset": 1}); resp.StatusCode != http.StatusOK {
		t.Fatalf("human ack own cursor: %d", resp.StatusCode)
	}
	if resp, _ := bearerReq(t, hc, "POST", a.url+"/ack", tok,
		map[string]any{"cursor": "m1/c", "stream": "metrics", "offset": 1}); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("human ack foreign cursor: want 403, got %d", resp.StatusCode)
	}

	// kv: scoped.
	_, out = bearerReq(t, hc, "GET", a.url+"/kv", tok, nil)
	for _, e := range out["entries"].([]any) {
		if e.(map[string]any)["path"] == "elsewhere/temp" {
			t.Fatalf("out-of-scope kv entry leaked to human: %v", out)
		}
	}

	// Publishing a command with a grant works; data is 403 human_write, an
	// authorization denial.
	if resp, out := bearerReq(t, hc, "POST", a.url+"/publish", tok, map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/m1/set-speed",
		"payload": map[string]any{"correlation_id": "h1", "expires_at": float64(99999999999999)},
	}); resp.StatusCode != http.StatusOK || out["stream"] != "commands" {
		t.Fatalf("human command publish: %d %v", resp.StatusCode, out)
	}
	if resp, out := bearerReq(t, hc, "POST", a.url+"/publish", tok, map[string]any{
		"topic": "colca/v1/_Metric/m1/m1/temp", "payload": map[string]any{"v": 666.0},
	}); resp.StatusCode != http.StatusForbidden || out["reason"] != "human_write" {
		t.Fatalf("human data publish: want 403 reason human_write, got %d %v", resp.StatusCode, out)
	}

	// command outside the grant → 403 cmd_denied (a different denial reason
	// than human_write above, both mapped to the same status).
	if resp, out := bearerReq(t, hc, "POST", a.url+"/publish", tok, map[string]any{
		"topic":   "colca/v1/_CmdParam/other/elsewhere/set-speed",
		"payload": map[string]any{"correlation_id": "h2", "expires_at": float64(99999999999999)},
	}); resp.StatusCode != http.StatusForbidden || out["reason"] != "cmd_denied" {
		t.Fatalf("human command outside grant: want 403 reason cmd_denied, got %d %v", resp.StatusCode, out)
	}

	// admin routes: 403 without admin:#.
	for _, probe := range []struct{ method, path string }{
		{"GET", "/enroll"}, {"GET", "/debug/state"},
	} {
		if resp, _ := bearerReq(t, hc, probe.method, a.url+probe.path, tok, nil); resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s %s as non-admin human: want 403, got %d", probe.method, probe.path, resp.StatusCode)
		}
	}
}

// admin:# opens the admin surface to a human, with attributable actions.
func TestHumanAdminGrant(t *testing.T) {
	a := newAPI(t)
	hc := client(nil)
	adminTok := a.mint("boss", []string{"admin:#"})

	m2 := authtest.NewMachine(t, "m2")
	resp, out := bearerReq(t, hc, "POST", a.url+"/enroll", adminTok, m2.EntryJSON(t, "external", a.place(t, "m2")))
	if resp.StatusCode != http.StatusOK || out["ulid"] != "m2" {
		t.Fatalf("human admin enroll: %d %v", resp.StatusCode, out)
	}
	if resp, _ := bearerReq(t, hc, "GET", a.url+"/enroll", adminTok, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("human admin list: %d", resp.StatusCode)
	}
	if resp, _ := bearerReq(t, hc, "DELETE", a.url+"/enroll/m2", adminTok, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("human admin revoke: %d", resp.StatusCode)
	}
	// admin:# does not widen reads: boss has no read grant and fetches nothing.
	if resp, out := req(t, client(nil), "POST", a.url+"/publish", "tok",
		map[string]any{"topic": "colca/v1/_Metric/m1/m1/t", "payload": map[string]any{"v": 1.0}}); resp.StatusCode != http.StatusOK {
		t.Fatalf("seed: %v", out)
	}
	_, out = bearerReq(t, hc, "GET", a.url+"/fetch?stream=metrics&cursor=boss/c&max=10", adminTok, nil)
	if recs := out["records"].([]any); len(recs) != 0 {
		t.Fatalf("admin:# must not widen reads, got %v", recs)
	}
}

// A failed bearer credential never falls through, neither to the admin token nor to
// anonymous.
func TestBearerFailuresAreTerminal(t *testing.T) {
	a := newAPI(t)
	hc := client(nil)

	expired := a.iss.MintOpt(tokentest.MintOpts{Sub: "anna", Exp: time.Now().Add(-3 * time.Minute)})
	resp, _ := bearerReq(t, hc, "GET", a.url+"/fetch?stream=metrics&cursor=anna/c&max=1", expired, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired bearer: want 401, got %d", resp.StatusCode)
	}

	// A bad bearer with a valid admin token in the same request is still 401.
	r, err := http.NewRequest(http.MethodGet, a.url+"/debug/state", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer garbage")
	r.Header.Set("X-Colca-Token", "tok")
	res, err := hc.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad bearer with valid admin token: want 401 (no fallthrough), got %d", res.StatusCode)
	}

	// Metrics: the http door counted the rejections.
	if v := metricstest.Value(t, a.m, `colca_auth_rejections_total{door="http",reason="`+tokenauth.ReasonExpired+`"}`); v < 1 {
		t.Fatalf("expired bearer not counted: %v", v)
	}
}

func TestHealthzCarriesThePubkeySoAParentCanEnrollIt(t *testing.T) {
	// /healthz must show the ULID and pubkey, because enrollment needs them before the
	// node is trusted.
	srv := plainHandler(t, &config.Config{ULID: "n-edge-a", API: config.API{Token: "tok"}}, nil)

	resp, err := srv.Client().Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body struct {
		ULID   string `json:"ulid"`
		Pubkey string `json:"pubkey"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.ULID != "n-edge-a" {
		t.Fatalf("ulid = %q", body.ULID)
	}
	if body.Pubkey != "deadbeef" {
		t.Fatalf("pubkey = %q — /healthz must carry it, or enrollment needs a shell on the device",
			body.Pubkey)
	}
}

// healthzUplink is the shape chaski.Node.status() (and any other caller)
// reads off /healthz's "uplink" field.
type healthzUplink struct {
	State string `json:"state"`
}

type healthzBody struct {
	Uplink *healthzUplink `json:"uplink"`
}

// A node without a parent reports uplink "none" explicitly rather than omitting the
// field.
func TestHealthzUplinkStateIsNoneWithoutAParent(t *testing.T) {
	// newTestHandler's cfg carries no Parent block.
	h := newTestHandler(t, &config.Config{ULID: "n-root"})

	rr := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.ServeHTTP(rr, req)

	var body healthzBody
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Uplink == nil || body.Uplink.State != "none" {
		t.Fatalf("uplink = %+v, want state \"none\" — a parentless node has nothing to report", body.Uplink)
	}
}

// A node with a parent reports its replication client's status: "connecting"
// before the parent was ever reached.
func TestHealthzReportsTheUplinkClientsStatus(t *testing.T) {
	nodeID, err := identity.Generate(filepath.Join(t.TempDir(), "n.key"))
	if err != nil {
		t.Fatal(err)
	}
	cl, err := repl.NewClient("https://parent.invalid:443", "deadparentpub", nodeID)
	if err != nil {
		t.Fatal(err)
	}

	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-child", Parent: &config.Parent{URL: "https://parent.invalid:443", Pubkey: "deadparentpub"}}
	reg, err := registry.New(s, cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(s, config.Retention{}, nil)
	eng := engine.New(s, cfg, reg, nil, m, nil)
	reg.SetNamespace(eng.Elements())
	h := Handler(eng, cfg, reg, nil, m, testBlobs(t, cfg), "deadbeef", false, cl)

	rr := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	h.ServeHTTP(rr, req)

	var body healthzBody
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Uplink == nil || body.Uplink.State != "connecting" {
		t.Fatalf("uplink = %+v, want state \"connecting\" — a freshly built client that has never "+
			"reached its parent must report that, not be silently omitted like a parentless node",
			body.Uplink)
	}
}

// localAPI is the local HTTP door fixture: no TLS, no admin routes, and mount
// authoring wired as node startup wires it, like mqttsrv's
// startServerWithLocalDoor.
type localAPI struct {
	http.Handler
	reg *registry.Manager
	eng *engine.Engine
	m   *metrics.Metrics
}

func newLocalHandler(t *testing.T) *localAPI {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-test"}
	reg, err := registry.New(s, cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(s, config.Retention{}, nil)
	reg.SetMetrics(m)
	eng := engine.New(s, cfg, reg, nil, m, nil)
	domain := uns.NewConfigExec(eng.EntityStore(), reg, eng.Elements(), nil, registry.NewULID, cfg.Plugin)
	eng.SetExecutor(engine.Executors(engine.NewAdminExecutor(reg), domain))
	eng.SetObserver(domain)
	reg.SetNamespace(eng.Elements())
	reg.SetAuthoring(func(path string) (string, error) {
		payload, err := json.Marshal(map[string]string{"path": path})
		if err != nil {
			return "", err
		}
		code, msg, _ := domain.Execute(uns.CommandContext{}, "_CmdConfigure", "element/author", payload)
		if code != 200 {
			return "", fmt.Errorf("author element at %s: %s", path, msg)
		}
		return msg, nil
	})
	h := Handler(eng, cfg, reg, nil, m, testBlobs(t, cfg), "deadbeef", true, nil)
	return &localAPI{Handler: h, reg: reg, eng: eng, m: m}
}

// testRegistry gives a test direct access to the registry a local handler was
// built over, so it can assert what self-registration actually wrote.
func testRegistry(t *testing.T, h *localAPI) *registry.Manager {
	t.Helper()
	return h.reg
}

func TestTheLocalHandlerIdentifiesByHeaderAndRegisters(t *testing.T) {
	h := newLocalHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/kv", nil)
	req.Header.Set("X-Colca-Service", "connector-opcua")
	req.Header.Set("X-Colca-Mount", "line1/press3")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("GET /kv on the local door = %d; want 200 with no credential", rec.Code)
	}
	entry, ok := testRegistry(t, h).ByName("connector-opcua")
	if !ok {
		t.Fatal("the request did not register the service")
	}
	if entry.Kind != uns.KindLocal {
		t.Fatalf("registered kind = %q, want %q", entry.Kind, uns.KindLocal)
	}
	path, ok := h.eng.Elements().PathOf(entry.Element)
	if !ok {
		t.Fatalf("entry's element %q does not resolve to any path", entry.Element)
	}
	if path != "line1/press3" {
		t.Fatalf("registered at %q; want the declared mount line1/press3", path)
	}
}

func TestTheLocalKVRoutePaginatesAndRejectsBadTokens(t *testing.T) {
	h := newLocalHandler(t)
	if _, _, err := h.eng.Store().Append("entities", []store.Record{
		{Topic: "colca/v1/_SystemElement/n-test/line/a", Payload: []byte(`{"id":"a","name":"a"}`), TS: 1, KVPath: "line/a", KVNode: "n-test"},
		{Topic: "colca/v1/_SystemElement/n-test/line/b", Payload: []byte(`{"id":"b","name":"b"}`), TS: 2, KVPath: "line/b", KVNode: "n-test"},
	}); err != nil {
		t.Fatal(err)
	}

	request := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("X-Colca-Service", "projector")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		return rr
	}
	firstResult := request("/kv?prefix=line%2F&max=1")
	if firstResult.Code != http.StatusOK {
		t.Fatalf("first page = %d: %s", firstResult.Code, firstResult.Body.String())
	}
	var first struct {
		Entries []struct {
			Path string `json:"path"`
		} `json:"entries"`
		Next string `json:"next"`
	}
	if err := json.Unmarshal(firstResult.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Entries) != 1 || first.Next == "" {
		t.Fatalf("first page = %+v", first)
	}

	secondResult := request("/kv?prefix=line%2F&max=1&after=" + first.Next)
	var second struct {
		Entries []struct {
			Path string `json:"path"`
		} `json:"entries"`
		Next string `json:"next"`
	}
	if err := json.Unmarshal(secondResult.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if secondResult.Code != http.StatusOK || len(second.Entries) != 1 || second.Next != "" {
		t.Fatalf("second page = %d %+v", secondResult.Code, second)
	}
	if bad := request("/kv?prefix=line%2F&max=1&after=not-a-token!"); bad.Code != http.StatusBadRequest {
		t.Fatalf("bad page token = %d, want 400: %s", bad.Code, bad.Body.String())
	}
}

// ?contract= selects only the requested one of two contracts at the same path. The
// unfiltered fetch sees both and is checked before the absence assertions.
func TestTheLocalKVRouteFiltersByContractAndRejectsUnknownOnes(t *testing.T) {
	h := newLocalHandler(t)
	if _, _, err := h.eng.Store().Append("definitions", []store.Record{
		{Topic: "colca/v1/_Group/n-test/grp/a", Payload: []byte(`{"id":"a"}`), TS: 1, KVPath: "grp/a", KVNode: "n-test"},
		{Topic: "colca/v1/_MetadataType/n-test/grp/a", Payload: []byte(`{"id":"a"}`), TS: 2, KVPath: "grp/a", KVNode: "n-test"},
	}); err != nil {
		t.Fatal(err)
	}

	request := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("X-Colca-Service", "projector")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		return rr
	}
	type page struct {
		Entries []struct {
			Topic string `json:"topic"`
		} `json:"entries"`
		Next string `json:"next"`
	}
	decode := func(rr *httptest.ResponseRecorder) page {
		t.Helper()
		var p page
		if err := json.Unmarshal(rr.Body.Bytes(), &p); err != nil {
			t.Fatalf("decode %s: %v", rr.Body.String(), err)
		}
		return p
	}

	unfiltered := decode(request("/kv?prefix=grp%2Fa"))
	if len(unfiltered.Entries) != 2 {
		t.Fatalf("unfiltered = %+v, want both contracts at this path", unfiltered)
	}

	groupOnly := request("/kv?prefix=grp%2Fa&contract=_Group")
	if groupOnly.Code != http.StatusOK {
		t.Fatalf("contract=_Group = %d: %s", groupOnly.Code, groupOnly.Body.String())
	}
	groups := decode(groupOnly)
	if len(groups.Entries) != 1 || !strings.HasPrefix(groups.Entries[0].Topic, "colca/v1/_Group/") {
		t.Fatalf("contract=_Group entries = %+v, want exactly the _Group record", groups)
	}

	metaOnly := decode(request("/kv?prefix=grp%2Fa&contract=_MetadataType"))
	if len(metaOnly.Entries) != 1 || !strings.HasPrefix(metaOnly.Entries[0].Topic, "colca/v1/_MetadataType/") {
		t.Fatalf("contract=_MetadataType entries = %+v, want exactly the _MetadataType record", metaOnly)
	}

	if bad := request("/kv?prefix=grp%2Fa&contract=_NotARealContract"); bad.Code != http.StatusBadRequest {
		t.Fatalf("unknown contract = %d, want 400: %s", bad.Code, bad.Body.String())
	} else if !strings.Contains(bad.Body.String(), "_NotARealContract") {
		t.Fatalf("400 body must name the unknown contract, got %s", bad.Body.String())
	}

	// _DataTags is stored because the bundle declares it, not uns. The filter must ask
	// the engine's authority, or connectors cannot read their catalogue back.
	h.eng.SetContracts(bundleWith(t, map[string]any{
		"_DataTags": map[string]any{
			"class": "entity", "tombstone": true,
			"schema": map[string]any{"type": "object", "properties": map[string]any{}},
		},
	}))
	if _, _, err := h.eng.Store().Append("entities", []store.Record{
		{Topic: "colca/v1/_DataTags/n-test/grp/a", Payload: []byte(`{"connector":"c"}`), TS: 3, KVPath: "grp/a", KVNode: "n-test"},
	}); err != nil {
		t.Fatal(err)
	}
	catalogue := request("/kv?prefix=grp%2Fa&contract=_DataTags")
	if catalogue.Code != http.StatusOK {
		t.Fatalf("contract=_DataTags = %d, want 200: %s", catalogue.Code, catalogue.Body.String())
	}
	tags := decode(catalogue)
	if len(tags.Entries) != 1 || !strings.HasPrefix(tags.Entries[0].Topic, "colca/v1/_DataTags/") {
		t.Fatalf("contract=_DataTags entries = %+v, want exactly the catalogue record", tags)
	}
	// A name no authority knows is still 400, bundle or not.
	if bad := request("/kv?prefix=grp%2Fa&contract=_StillNotReal"); bad.Code != http.StatusBadRequest {
		t.Fatalf("unknown contract with a bundle loaded = %d, want 400: %s", bad.Code, bad.Body.String())
	}
}

// bundleWith writes a minimal loadable bundle declaring entries and returns its
// table.
func bundleWith(t *testing.T, entries map[string]any) *contracts.Table {
	t.Helper()
	body := map[string]any{
		"bundle_version": "test",
		"source":         map[string]any{"package": "colca-data-contracts", "git_sha": "fixture"},
		"contracts":      entries,
	}
	canon, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canon)
	full := map[string]any{"digest": hex.EncodeToString(sum[:]), "generated_at": "2026-09-07T00:00:00Z"}
	for k, v := range body {
		full[k] = v
	}
	raw, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bundle.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	table, err := contracts.Load(path, "")
	if err != nil {
		t.Fatalf("fixture bundle failed to load: %v", err)
	}
	return table
}

func TestLocalSelfReturnsTheMintedIdentityAndAuthoritativeMount(t *testing.T) {
	h := newLocalHandler(t)
	// Seed an unrelated element first, so a client guessing its mount from the first
	// visible _SystemElement would get it wrong; /self must use the registry binding.
	authtest.Place(t, h.eng, "unrelated")

	req := httptest.NewRequest(http.MethodGet, "/self", nil)
	req.Header.Set("X-Colca-Service", "connector-opcua")
	req.Header.Set("X-Colca-Mount", "line1/press3")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /self = %d: %s", rec.Code, rec.Body.String())
	}
	var got struct {
		ULID    string `json:"ulid"`
		Name    string `json:"name"`
		Node    string `json:"node"`
		Element string `json:"element"`
		Mount   string `json:"mount"`
		Limits  struct {
			MaxRecordBytes uint64 `json:"max_record_bytes"`
			MaxBlobBytes   uint64 `json:"max_blob_bytes"`
		} `json:"limits"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ULID == "" || got.ULID == got.Name {
		t.Fatalf("self-registration did not return a separately minted identity: %+v", got)
	}
	if got.Name != "connector-opcua" || got.Node != h.eng.NodeID() || got.Mount != "line1/press3" || got.Element == "" {
		t.Fatalf("GET /self = %+v; want the named service at its declared mount", got)
	}
	entry, ok := h.reg.Get(got.ULID)
	if !ok || entry.Name != got.Name || entry.Element != got.Element {
		t.Fatalf("GET /self does not describe the registry entry: response=%+v entry=%+v", got, entry)
	}
	// /self is where a caller reads the node's upload caps.
	wantMaxRecord := config.Limits{}.EffectiveMaxRecordBytes()
	wantMaxBlob := config.Limits{}.EffectiveMaxBlobBytes()
	if got.Limits.MaxRecordBytes != wantMaxRecord || got.Limits.MaxBlobBytes != wantMaxBlob {
		t.Fatalf("GET /self limits = %+v; want max_record_bytes=%d max_blob_bytes=%d",
			got.Limits, wantMaxRecord, wantMaxBlob)
	}
}

func TestLocalSelfRegistersAnUndeclaredServiceAtTheNode(t *testing.T) {
	h := newLocalHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/self", nil)
	req.Header.Set("X-Colca-Service", "dataops")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /self = %d: %s", rec.Code, rec.Body.String())
	}
	var got struct {
		ULID    string `json:"ulid"`
		Element string `json:"element"`
		Mount   string `json:"mount"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ULID == "" || got.Element != "" || got.Mount != "" {
		t.Fatalf("unplaced GET /self = %+v; want a minted identity bound to the node", got)
	}
}

func TestLocalConfigureResponseNamesProducedStateOffset(t *testing.T) {
	h := newLocalHandler(t)
	body := []byte(`{
		"topic":"colca/v1/_CmdConfigure/n-test/definition/upsert",
		"payload":{
			"correlation_id":"api-local-rw-1",
			"expires_at":9999999999999,
			"definitions":[{
				"contract":"_ExternalSystem",
				"definition":{"id":"ext-local-1","key":"tcdb","name":"TCDB","system_type":"database"}
			}]
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(body))
	req.Header.Set("X-Colca-Service", "api")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("local publish = %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	command, ok := out["command"].(map[string]any)
	if !ok || command["result_code"].(float64) != 200 {
		t.Fatalf("local response omitted command outcome: %v", out)
	}
	writes := command["state_writes"].([]any)
	write := writes[0].(map[string]any)
	if write["stream"] != "definitions" || write["offset"].(float64) == 0 {
		t.Fatalf("local command outcome omitted produced state: %v", command)
	}
}

func TestLocalSelfReflectsOperatorRepositionInsteadOfTheDeclaration(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "connector-opcua", "line1/press3")
	entry, ok := h.reg.ByName("connector-opcua")
	if !ok {
		t.Fatal("local service was not registered")
	}

	repositioned := *entry
	repositioned.Element = authtest.Place(t, h.eng, "line2/press9")
	raw, err := json.Marshal(&repositioned)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.reg.Enroll(raw); err != nil {
		t.Fatalf("reposition local service: %v", err)
	}

	// The container still declares its original mount; /self must return the current
	// registry position.
	req := httptest.NewRequest(http.MethodGet, "/self", nil)
	req.Header.Set("X-Colca-Service", "connector-opcua")
	req.Header.Set("X-Colca-Mount", "line1/press3")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /self after reposition = %d: %s", rec.Code, rec.Body.String())
	}
	var got struct {
		Element string `json:"element"`
		Mount   string `json:"mount"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Element != repositioned.Element || got.Mount != "line2/press9" {
		t.Fatalf("GET /self after reposition = %+v; want current entry at line2/press9", got)
	}
}

func TestSelfIsNotMountedOnTheExternalAPIDoor(t *testing.T) {
	a := newAPI(t)
	resp, _ := req(t, client(nil), "GET", a.url+"/self", "tok", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /self on external API = %d; want 404", resp.StatusCode)
	}
}

func TestTheLocalHandlerRefusesAdminRoutes(t *testing.T) {
	h := newLocalHandler(t)
	for _, route := range []struct{ method, path string }{
		{"POST", "/enroll"}, {"DELETE", "/enroll/01J"}, {"GET", "/debug/state"},
	} {
		req := httptest.NewRequest(route.method, route.path, strings.NewReader("{}"))
		req.Header.Set("X-Colca-Service", "connector-opcua")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 404 {
			t.Fatalf("%s %s on the local door = %d; provisioning is not a local service's job",
				route.method, route.path, rec.Code)
		}
	}
}

func TestTheLocalHandlerServesHealthAndMetrics(t *testing.T) {
	h := newLocalHandler(t)
	for _, path := range []string{"/healthz", "/metrics"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != 200 {
			t.Fatalf("GET %s on the local door = %d; Prometheus is itself a local service", path, rec.Code)
		}
	}
}

func TestTheLocalHandlerRequiresAName(t *testing.T) {
	h := newLocalHandler(t)
	const line = `colca_auth_rejections_total{door="local",reason="no_name"}`
	if v := metricstest.Value(t, h.m, line); v != 0 {
		t.Fatalf("%s = %v before any request, want 0", line, v)
	}

	req := httptest.NewRequest(http.MethodGet, "/kv", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("nameless request on the local door = %d; want 401 — the name is how its scope is found", rec.Code)
	}
	// Check the reason too, so a 401 for the wrong reason fails, as in the MQTT twin.
	if v := metricstest.Value(t, h.m, line); v != 1 {
		t.Fatalf("%s = %v after the nameless request, want exactly 1", line, v)
	}
}

// A machine given a friendly name must not be handed out by name on the local door:
// MayUseDoor(DoorLocal) after Register refuses it. Mirrors
// TestALocalNameThatResolvesToAKeyedIdentityByNameIsRefused.
func TestTheLocalHandlerRefusesAKeyedIdentityFoundByName(t *testing.T) {
	h := newLocalHandler(t)
	reg := testRegistry(t, h)
	element := authtest.Place(t, h.eng, "press3")
	m := authtest.NewMachine(t, "01JNAMEDMACHINE")
	entry := uns.Entry{ULID: m.ULID, Pubkey: m.Pubkey, Kind: uns.KindExternal, Name: "friendly-name", Element: element}
	raw, err := json.Marshal(&entry)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reg.Enroll(raw); err != nil {
		t.Fatalf("enroll a named machine: %v", err)
	}
	// friendly-name is nobody's ULID, so only the MayUseDoor check can refuse it.
	if _, ok := reg.Get("friendly-name"); ok {
		t.Fatal("precondition broken: \"friendly-name\" must not itself be a ulid")
	}

	const line = `colca_auth_rejections_total{door="local",reason="kind"}`
	if v := metricstest.Value(t, h.m, line); v != 0 {
		t.Fatalf("%s = %v before any request, want 0", line, v)
	}

	req := httptest.NewRequest(http.MethodGet, "/kv", nil)
	req.Header.Set("X-Colca-Service", "friendly-name")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("a machine's friendly name was claimed by a certless request on the local door: got %d, want 401", rec.Code)
	}
	// Check the reason too, so a 401 for the wrong reason fails, as in the MQTT twin.
	if v := metricstest.Value(t, h.m, line); v != 1 {
		t.Fatalf("%s = %v after the named-machine request, want exactly 1", line, v)
	}
	// The machine's own entry must stay untouched, not just the request refused.
	got, ok := reg.Get(m.ULID)
	if !ok || got.Kind != uns.KindExternal {
		t.Fatal("the machine entry itself must be untouched by the refused request")
	}
}

// A machine ULID must not be assumable as a name on the local door. Register's
// uniqueness check only covers names, so the reg.Get(name) check in resolve catches
// this.
func TestTheLocalHandlerRefusesANameThatCollidesWithAnotherEntrysULID(t *testing.T) {
	h := newLocalHandler(t)
	reg := testRegistry(t, h)
	element := authtest.Place(t, h.eng, "press3")
	m := authtest.NewMachine(t, "01JMACHINE")
	authtest.Enroll(t, reg, m, element)

	const line = `colca_auth_rejections_total{door="local",reason="kind"}`
	if v := metricstest.Value(t, h.m, line); v != 0 {
		t.Fatalf("%s = %v before any request, want 0", line, v)
	}

	req := httptest.NewRequest(http.MethodGet, "/kv", nil)
	req.Header.Set("X-Colca-Service", "01JMACHINE")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("a machine's identity was claimed by ulid-as-name on the local door: got %d, want 401", rec.Code)
	}
	if v := metricstest.Value(t, h.m, line); v != 1 {
		t.Fatalf("%s = %v after the collision request, want exactly 1", line, v)
	}
	// The local door must not have quietly minted an unrelated kind=local
	// entry under this name instead of refusing outright.
	if _, ok := reg.ByName("01JMACHINE"); ok {
		t.Fatal("the collision silently registered a new local entry instead of being refused")
	}
}

// registerLocal makes the first GET a local service makes, so later requests have
// a registered entry behind them, placed when mount is set.
func registerLocal(t *testing.T, h *localAPI, name, mount string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/kv", nil)
	req.Header.Set("X-Colca-Service", name)
	if mount != "" {
		req.Header.Set("X-Colca-Mount", mount)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("registering %q at mount %q: %d %s", name, mount, rec.Code, rec.Body.String())
	}
}

// A local caller never learns its ULID, so its cursors are namespaced by name under
// c/{name}/. /fetch and /ack work end to end: seed a record in its zone, read it
// back, move the cursor.
func TestTheLocalHandlerFetchAndAckWorkWithANameNamespacedCursor(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "connector-opcua", "press3")

	if _, err := h.eng.IngestAdmin("colca/v1/_Metric/n-test/press3/temp", []byte(`{"v":1.0}`)); err != nil {
		t.Fatalf("seed metric: %v", err)
	}

	cursor := uns.LocalCursorPrefix + "connector-opcua/c1"
	fetchReq := httptest.NewRequest(http.MethodGet, "/fetch?stream=metrics&cursor="+cursor+"&max=10", nil)
	fetchReq.Header.Set("X-Colca-Service", "connector-opcua")
	fetchRec := httptest.NewRecorder()
	h.ServeHTTP(fetchRec, fetchReq)
	if fetchRec.Code != 200 {
		t.Fatalf("GET /fetch with a name-namespaced cursor = %d: %s", fetchRec.Code, fetchRec.Body.String())
	}
	var out struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.NewDecoder(fetchRec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Records) != 1 {
		t.Fatalf("records = %v, want the one seeded metric — own-zone read is implicit for a placed entry (readZones)", out.Records)
	}
	offset, ok := out.Records[0]["offset"].(float64)
	if !ok {
		t.Fatalf("record has no numeric offset: %v", out.Records[0])
	}

	ackBody := fmt.Sprintf(`{"cursor":%q,"stream":"metrics","offset":%d}`, cursor, int64(offset))
	ackReq := httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(ackBody))
	ackReq.Header.Set("X-Colca-Service", "connector-opcua")
	ackRec := httptest.NewRecorder()
	h.ServeHTTP(ackRec, ackReq)
	if ackRec.Code != 200 {
		t.Fatalf("POST /ack with a name-namespaced cursor = %d: %s", ackRec.Code, ackRec.Body.String())
	}
	var ackOut struct {
		Moved bool `json:"moved"`
	}
	if err := json.NewDecoder(ackRec.Body).Decode(&ackOut); err != nil {
		t.Fatal(err)
	}
	if !ackOut.Moved {
		t.Fatal("POST /ack reported moved=false; the cursor did not actually advance")
	}

	// And the cursor genuinely moved: fetching again from the same cursor now
	// returns nothing left to read.
	fetchReq2 := httptest.NewRequest(http.MethodGet, "/fetch?stream=metrics&cursor="+cursor+"&max=10", nil)
	fetchReq2.Header.Set("X-Colca-Service", "connector-opcua")
	fetchRec2 := httptest.NewRecorder()
	h.ServeHTTP(fetchRec2, fetchReq2)
	var out2 struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.NewDecoder(fetchRec2.Body).Decode(&out2); err != nil {
		t.Fatal(err)
	}
	if len(out2.Records) != 0 {
		t.Fatalf("records after ack = %v, want none — /ack must have actually moved the cursor", out2.Records)
	}
}

// One local service can never move another's cursor.
func TestTheLocalHandlerRefusesToAckAnotherServicesCursor(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "connector-a", "")
	registerLocal(t, h, "connector-b", "")

	body := fmt.Sprintf(`{"cursor":%q,"stream":"metrics","offset":0}`, uns.LocalCursorPrefix+"connector-a/c1")
	req := httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(body))
	req.Header.Set("X-Colca-Service", "connector-b")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("connector-b acking connector-a's cursor = %d, want 403", rec.Code)
	}
}

// Deleting a cursor erases its position: CursorGet reports the default again. A
// consumer that mints a new cursor per rebuild relies on this so old cursors do not
// hold back retention.
func TestTheLocalHandlerDeletesItsOwnCursor(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "connector-opcua", "")
	cursor := uns.LocalCursorPrefix + "connector-opcua/gen1"

	// Ack forward first, so the later default reading proves the delete.
	ackBody := fmt.Sprintf(`{"cursor":%q,"stream":"metrics","offset":5}`, cursor)
	ackReq := httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(ackBody))
	ackReq.Header.Set("X-Colca-Service", "connector-opcua")
	ackRec := httptest.NewRecorder()
	h.ServeHTTP(ackRec, ackReq)
	if ackRec.Code != http.StatusOK {
		t.Fatalf("seeding the cursor via /ack = %d: %s", ackRec.Code, ackRec.Body.String())
	}
	if got := h.eng.Store().CursorGet(cursor, "metrics"); got != 6 {
		t.Fatalf("cursor after seed ack = %d, want 6 (offset+1) — the presence this test's delete assertion depends on", got)
	}

	delBody := fmt.Sprintf(`{"cursor":%q,"stream":"metrics","delete":true}`, cursor)
	delReq := httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(delBody))
	delReq.Header.Set("X-Colca-Service", "connector-opcua")
	delRec := httptest.NewRecorder()
	h.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("POST /ack delete=true = %d: %s", delRec.Code, delRec.Body.String())
	}
	var out struct {
		Deleted bool `json:"deleted"`
	}
	if err := json.NewDecoder(delRec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.Deleted {
		t.Fatal(`POST /ack delete=true reported deleted=false`)
	}

	if got := h.eng.Store().CursorGet(cursor, "metrics"); got != 1 {
		t.Fatalf("cursor after delete = %d, want 1 (the never-acked default) — delete did not erase the position", got)
	}
}

// One local service cannot delete another's cursor, and the refusal is audited.
func TestTheLocalHandlerRefusesToDeleteAnotherServicesCursor(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "connector-a", "")
	registerLocal(t, h, "connector-b", "")
	cursor := uns.LocalCursorPrefix + "connector-a/gen1"

	// Give connector-a's cursor a position first, so a delete that wrongly succeeded
	// would show.
	ackBody := fmt.Sprintf(`{"cursor":%q,"stream":"metrics","offset":2}`, cursor)
	ackReq := httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(ackBody))
	ackReq.Header.Set("X-Colca-Service", "connector-a")
	ackRec := httptest.NewRecorder()
	h.ServeHTTP(ackRec, ackReq)
	if ackRec.Code != http.StatusOK {
		t.Fatalf("seeding connector-a's cursor = %d: %s", ackRec.Code, ackRec.Body.String())
	}

	before, _, err := h.eng.Store().Read("audit", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}

	body := fmt.Sprintf(`{"cursor":%q,"stream":"metrics","delete":true}`, cursor)
	req := httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(body))
	req.Header.Set("X-Colca-Service", "connector-b")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("connector-b deleting connector-a's cursor = %d, want 403", rec.Code)
	}
	if got := h.eng.Store().CursorGet(cursor, "metrics"); got != 3 {
		t.Fatalf("connector-a's cursor after the refused delete = %d, want 3 (untouched) — the refusal must be a no-op", got)
	}

	after, _, err := h.eng.Store().Read("audit", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before)+1 {
		t.Fatalf("audit records = %d, want %d (one denial appended)", len(after), len(before)+1)
	}
	var payload map[string]any
	last := after[len(after)-1]
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["outcome"] != "denied" || payload["operation"] != "cursor_delete" || payload["reason_code"] != "cursor_denied" {
		t.Fatalf("audit payload = %#v, want outcome=denied operation=cursor_delete reason_code=cursor_denied", payload)
	}
}

// After a delete, acking the same cursor name starts a new cursor instead of
// resuming.
func TestTheLocalHandlerAcksFreshAfterDeletingACursor(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "connector-opcua", "")
	cursor := uns.LocalCursorPrefix + "connector-opcua/gen1"

	ackBody := fmt.Sprintf(`{"cursor":%q,"stream":"metrics","offset":9}`, cursor)
	ackReq := httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(ackBody))
	ackReq.Header.Set("X-Colca-Service", "connector-opcua")
	ackRec := httptest.NewRecorder()
	h.ServeHTTP(ackRec, ackReq)
	if ackRec.Code != http.StatusOK {
		t.Fatalf("seeding the cursor via /ack = %d: %s", ackRec.Code, ackRec.Body.String())
	}
	if got := h.eng.Store().CursorGet(cursor, "metrics"); got != 10 {
		t.Fatalf("cursor after seed ack = %d, want 10", got)
	}

	delBody := fmt.Sprintf(`{"cursor":%q,"stream":"metrics","delete":true}`, cursor)
	delReq := httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(delBody))
	delReq.Header.Set("X-Colca-Service", "connector-opcua")
	delRec := httptest.NewRecorder()
	h.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("POST /ack delete=true = %d: %s", delRec.Code, delRec.Body.String())
	}

	// A fresh ack under the same name moves from the default of 1, not from the erased
	// position of 10.
	reAckBody := fmt.Sprintf(`{"cursor":%q,"stream":"metrics","offset":1}`, cursor)
	reAckReq := httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(reAckBody))
	reAckReq.Header.Set("X-Colca-Service", "connector-opcua")
	reAckRec := httptest.NewRecorder()
	h.ServeHTTP(reAckRec, reAckReq)
	if reAckRec.Code != http.StatusOK {
		t.Fatalf("re-ack after delete = %d: %s", reAckRec.Code, reAckRec.Body.String())
	}
	var out struct {
		Moved bool `json:"moved"`
	}
	if err := json.NewDecoder(reAckRec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.Moved {
		t.Fatal("re-ack after delete reported moved=false — the deleted cursor's old position is still being compared against")
	}
	if got := h.eng.Store().CursorGet(cursor, "metrics"); got != 2 {
		t.Fatalf("cursor after re-ack = %d, want 2 (offset 1 + 1) — a stale position survived the delete", got)
	}
}

// ownsCursor itself refuses a nil entry. CursorPrefix on nil would match every
// cursor, so a caller that forgot the nil check would otherwise own everything.
func TestOwnsCursorIsFailClosedOnANilEntry(t *testing.T) {
	if ownsCursor(nil, "") {
		t.Fatal("a nil entry must own no cursor, including the empty one")
	}
	if ownsCursor(nil, "anything-at-all") {
		t.Fatal("a nil entry must own no cursor")
	}
}

// tail answers what happened most recently, which a forward read from an unacked
// cursor cannot.
func TestFetchTailReadsTheEndOfTheStream(t *testing.T) {
	a := newAPI(t)
	records := make([]store.Record, 0, 12)
	for i := 1; i <= 12; i++ {
		records = append(records, store.Record{
			Topic:   "colca/v1/_Metric/n-test/line1/s1",
			Payload: []byte(fmt.Sprintf(`{"signal_id":"s1","value":%d}`, i)),
			TS:      int64(i),
		})
	}
	if _, _, err := a.st.Append("metrics", records); err != nil {
		t.Fatal(err)
	}

	// Denominator: without tail the same request returns the oldest three.
	_, out := req(t, client(nil), "GET", a.url+"/fetch?stream=metrics&cursor=viewer&max=3", "tok", nil)
	if got := offsetsOf(t, out); !reflect.DeepEqual(got, []float64{1, 2, 3}) {
		t.Fatalf("without tail, want the oldest three, got %v", got)
	}

	_, out = req(t, client(nil), "GET", a.url+"/fetch?stream=metrics&cursor=viewer&max=3&tail=1", "tok", nil)
	if got := offsetsOf(t, out); !reflect.DeepEqual(got, []float64{10, 11, 12}) {
		t.Fatalf("with tail, want the newest three, got %v", got)
	}

	// A stream shorter than the window starts at the beginning rather than
	// underflowing into an enormous offset.
	_, out = req(t, client(nil), "GET", a.url+"/fetch?stream=metrics&cursor=viewer&max=100&tail=1", "tok", nil)
	if got := offsetsOf(t, out); len(got) != 12 || got[0] != 1 {
		t.Fatalf("a short stream should tail from its first record, got %v", got)
	}

	// tail leaves the cursor where it was, compared with its value before rather than
	// a guess.
	before := a.st.CursorGet("consumer", "metrics")
	req(t, client(nil), "GET", a.url+"/fetch?stream=metrics&cursor=consumer&max=3&tail=1", "tok", nil)
	if after := a.st.CursorGet("consumer", "metrics"); after != before {
		t.Fatalf("tail moved the cursor from %d to %d", before, after)
	}
}

func offsetsOf(t *testing.T, out map[string]any) []float64 {
	t.Helper()
	records, ok := out["records"].([]any)
	if !ok {
		t.Fatalf("no records in %v", out)
	}
	offsets := make([]float64, 0, len(records))
	for _, record := range records {
		offsets = append(offsets, record.(map[string]any)["offset"].(float64))
	}
	return offsets
}

// newLocalHandlerWithVerifier is newLocalHandler with the human verifier, so the
// local door can resolve a forwarded bearer.
func newLocalHandlerWithVerifier(t *testing.T) (*localAPI, *tokentest.Issuer) {
	t.Helper()
	h := newLocalHandler(t)
	iss := tokentest.NewIssuer(t)
	ver, err := tokenauth.New(tokenauth.Config{
		Issuer: iss.Iss(), Audience: iss.Aud(), JWKSURL: iss.JWKSURL(),
	}, h.eng.Store(), h.m)
	if err != nil {
		t.Fatal(err)
	}
	primeVerifier(t, ver)
	h.Handler = Handler(h.eng, &config.Config{ULID: "n-test"}, h.reg, ver, h.m, testBlobs(t, &config.Config{ULID: "n-test"}), "deadbeef", true, nil)
	return h, iss
}

func localPublish(t *testing.T, h *localAPI, headers map[string]string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(raw))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// A local service acting as a person forwards their token: the door resolves it to
// the human entry, groups included, and the record is attributed to the person.
func TestTheLocalDoorResolvesAForwardedBearerToThePerson(t *testing.T) {
	h, iss := newLocalHandlerWithVerifier(t)
	token := iss.MintOpt(tokentest.MintOpts{
		Sub: "kc-sub-anna", Grants: []string{"cmd:#:configure"}, Groups: []string{"operators"}, Username: "anna",
	})
	rec := localPublish(t, h,
		map[string]string{"Authorization": "Bearer " + token, "X-Colca-Service": "api"},
		map[string]any{"topic": "colca/v1/_CmdEdit/n-test/apply",
			"payload": map[string]any{"correlation_id": "c-fwd", "expires_at": 9999999999999}})
	if rec.Code != http.StatusOK {
		t.Fatalf("forwarded Bearer on the local door = %d: %s", rec.Code, rec.Body.String())
	}
	recs, _, err := h.eng.Store().Read("commands", 1, 10, nil)
	if err != nil || len(recs) != 1 {
		t.Fatalf("commands stream: %v %+v", err, recs)
	}
	got := recs[0]
	if got.WrittenBy != "kc-sub-anna" || got.ActorKind != "human" || got.ActorLabel != "anna" ||
		len(got.ActorGroups) != 1 || got.ActorGroups[0] != "operators" {
		t.Fatalf("record attributed to %+v, want anna (human) with her groups", got)
	}
	if _, registered := h.reg.ByName("api"); registered {
		t.Fatal("a forwarded Bearer must not also register the carrying service")
	}
}

// A presented token that fails is 401 and never falls back to the service name;
// without a verifier the door knows no humans at all.
func TestTheLocalDoorNeverFallsBackFromABadBearerToTheServiceName(t *testing.T) {
	withVerifier, _ := newLocalHandlerWithVerifier(t)
	without := newLocalHandler(t)
	for name, h := range map[string]*localAPI{"bad token": withVerifier, "no verifier": without} {
		rec := localPublish(t, h,
			map[string]string{"Authorization": "Bearer garbage", "X-Colca-Service": "api"},
			map[string]any{"topic": "colca/v1/_CmdConfigure/n-test/definition/upsert",
				"payload": map[string]any{"correlation_id": "c-bad", "expires_at": 9999999999999}})
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: = %d, want 401: %s", name, rec.Code, rec.Body.String())
		}
		if _, registered := h.reg.ByName("api"); registered {
			t.Fatalf("%s: a rejected Bearer fell through to the service identity", name)
		}
	}
}

// A job without the token may attest the person's group ids but must state a
// reason. Without one it is refused; with one the record carries the groups.
func TestTheLocalDoorRequiresAReasonToAttestAPersonsGroups(t *testing.T) {
	h := newLocalHandler(t)
	if _, err := h.eng.EntityStore().PublishBatch(uns.CommandContext{}, []uns.StateRecord{{
		Topic: "colca/v1/_Group/n-test/operators", Payload: []byte(`{"id":"operators","grants":["cmd:#:configure"]}`),
	}}); err != nil {
		t.Fatal(err)
	}
	body := func(corr string, reason string) map[string]any {
		b := map[string]any{"topic": "colca/v1/_CmdEdit/n-test/apply",
			"payload":  map[string]any{"correlation_id": corr, "expires_at": 9999999999999},
			"actor_id": "kc-sub-anna", "actor_label": "anna", "actor_kind": "human",
			"actor_groups": []string{"operators"}}
		if reason != "" {
			b["fallback_reason"] = reason
		}
		return b
	}
	svc := map[string]string{"X-Colca-Service": "api"}
	if rec := localPublish(t, h, svc, body("c-noreason", "")); rec.Code != http.StatusBadRequest {
		t.Fatalf("attested groups without a reason = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if rec := localPublish(t, h, svc, body("c-reason", "background job after request")); rec.Code != http.StatusOK {
		t.Fatalf("attested groups with a reason = %d: %s", rec.Code, rec.Body.String())
	}
	recs, _, err := h.eng.Store().Read("commands", 1, 10, nil)
	if err != nil || len(recs) != 1 {
		t.Fatalf("commands stream: %v %+v", err, recs)
	}
	if got := recs[0]; got.ActorKind != "human" || got.ActorID != "kc-sub-anna" || len(got.ActorGroups) != 1 {
		t.Fatalf("attested record = %+v, want anna with her groups", got)
	}
}

// installPersonalAccessToken publishes a hash-only _PersonalAccessToken
// definition, wires the verifier's index to it and returns the plaintext.
func installPersonalAccessToken(t *testing.T, h *localAPI, ver *tokenauth.Verifier, id string, scopes []string) string {
	t.Helper()
	token := "pk_pat_" + id + "_secret"
	digest := sha256.Sum256([]byte(token))
	payload, err := json.Marshal(uns.PersonalAccessToken{
		ID: id, HashedSecret: hex.EncodeToString(digest[:]), OwnerSub: "kc-sub-franz",
		OwnerEmail: "franz@example.com", Scopes: scopes, Roles: []string{"admin"},
		Grants: []string{"read:#", "cmd:#:configure"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.eng.EntityStore().PublishBatch(uns.CommandContext{}, []uns.StateRecord{{
		Topic: "colca/v1/" + uns.PersonalAccessTokenContract + "/n-test/" + id, Payload: payload,
	}}); err != nil {
		t.Fatalf("publish PAT definition: %v", err)
	}
	ver.SetPersonalAccessTokenIndex(uns.NewPersonalAccessTokenIndex(h.eng.EntityStore()))
	return token
}

func newLocalHandlerWithPersonalAccessToken(t *testing.T, id string, scopes []string) (*localAPI, string) {
	t.Helper()
	h := newLocalHandler(t)
	iss := tokentest.NewIssuer(t)
	ver, err := tokenauth.New(tokenauth.Config{
		Issuer: iss.Iss(), Audience: iss.Aud(), JWKSURL: iss.JWKSURL(),
	}, h.eng.Store(), h.m)
	if err != nil {
		t.Fatal(err)
	}
	primeVerifier(t, ver)
	token := installPersonalAccessToken(t, h, ver, id, scopes)
	h.Handler = Handler(h.eng, &config.Config{ULID: "n-test"}, h.reg, ver, h.m, testBlobs(t, &config.Config{ULID: "n-test"}), "deadbeef", true, nil)
	return h, token
}

// A personal access token forwarded to the local door needs the api scope, which
// every issued key carries, not broker-http.
func TestTheLocalDoorAcceptsAForwardedPersonalAccessTokenScopedToTheApi(t *testing.T) {
	h, token := newLocalHandlerWithPersonalAccessToken(t, "01PATAPIONLY000000000000AA", []string{uns.ScopeAPI, uns.ScopeI3X})
	rec := localPublish(t, h,
		map[string]string{"Authorization": "Bearer " + token, "X-Colca-Service": "api"},
		map[string]any{"topic": "colca/v1/_CmdEdit/n-test/apply",
			"payload": map[string]any{"correlation_id": "c-pat", "expires_at": 9999999999999}})
	if rec.Code != http.StatusOK {
		t.Fatalf("forwarded api-scoped PAT on the local door = %d: %s", rec.Code, rec.Body.String())
	}
	recs, _, err := h.eng.Store().Read("commands", 1, 10, nil)
	if err != nil || len(recs) != 1 {
		t.Fatalf("commands stream: %v %+v", err, recs)
	}
	if recs[0].WrittenBy != "kc-sub-franz" || recs[0].ActorKind != "human" {
		t.Fatalf("record attributed to %+v, want the token's owner as a human", recs[0])
	}
}

// A token scoped to the human doors alone is not one the person used at the
// api; the local door refuses it, the published door is where it belongs.
func TestTheLocalDoorRefusesAPersonalAccessTokenWithoutTheApiScope(t *testing.T) {
	h, token := newLocalHandlerWithPersonalAccessToken(t, "01PATBROKERONLY00000000000", []string{uns.ScopeBrokerHTTP})
	rec := localPublish(t, h,
		map[string]string{"Authorization": "Bearer " + token, "X-Colca-Service": "api"},
		map[string]any{"topic": "colca/v1/_CmdEdit/n-test/apply",
			"payload": map[string]any{"correlation_id": "c-pat2", "expires_at": 9999999999999}})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("broker-only PAT on the local door = %d, want 401: %s", rec.Code, rec.Body.String())
	}
}

// adminHandlerFor mounts the admin door over the engine and registry a local
// handler already holds, so one test can register a service the way a service
// registers and then revoke it the way an operator revokes it.
func adminHandlerFor(t *testing.T, h *localAPI) http.Handler {
	t.Helper()
	cfg := &config.Config{ULID: "n-test", API: config.API{Token: "tok"}}
	return Handler(h.eng, cfg, h.reg, nil, h.m, testBlobs(t, cfg), "deadbeef", false, nil)
}

// serviceRecords returns the _ServiceDetails records this node holds, so a test
// can name what survived a revoke instead of asserting on a count alone.
func serviceRecords(t *testing.T, h *localAPI) []string {
	t.Helper()
	entries, err := h.eng.Store().KVScan("")
	if err != nil {
		t.Fatal(err)
	}
	var topics []string
	for _, kv := range entries {
		if strings.Contains(kv.Topic, "/_ServiceDetails/") {
			topics = append(topics, kv.Topic)
		}
	}
	return topics
}

// Revoking an identity takes the records that identity authored with it. A
// _ServiceDetails record is observed state only its own service may write — the
// admin door refuses that contract outright — so one left behind by a revoke can
// never be retired by anyone again. A live node had three for one service after
// its mount was moved twice, two of them under elements that had since been
// deleted, and the hub above it folded them by name and showed the service as
// inactive while it was running and publishing.
func TestRevokingAnIdentityRetiresTheServiceRecordItAuthored(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "tcdb-api", "events")
	entry, ok := h.reg.ByName("tcdb-api")
	if !ok {
		t.Fatal("the local door did not register tcdb-api")
	}
	topic := "colca/v1/_ServiceDetails/n-test/events/tcdb-api/_service"
	if rec := localPublish(t, h, map[string]string{"X-Colca-Service": "tcdb-api"},
		map[string]any{"topic": topic, "payload": map[string]any{"id": entry.ULID, "name": "tcdb-api"}},
	); rec.Code != http.StatusOK {
		t.Fatalf("the service publishing its own record = %d: %s", rec.Code, rec.Body.String())
	}
	// The denominator for the absence assertion below: the record is there to
	// retire, and this is the topic it sits at.
	if got := serviceRecords(t, h); len(got) != 1 || got[0] != topic {
		t.Fatalf("after registration the node holds %v, want exactly %s", got, topic)
	}

	admin := adminHandlerFor(t, h)
	if rec := doAdmin(t, admin, http.MethodDelete, "/enroll/"+entry.ULID, nil); rec.Code != http.StatusOK {
		t.Fatalf("DELETE /enroll/%s = %d: %s", entry.ULID, rec.Code, rec.Body.String())
	}

	if got := serviceRecords(t, h); len(got) != 0 {
		t.Fatalf("the revoke left %v standing — nothing can retire a _ServiceDetails "+
			"record once the identity that authored it is gone", got)
	}
}
