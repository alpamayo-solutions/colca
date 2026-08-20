package httpapi

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/internal/tokenauth"
	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

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

// api is the fixture: a TLS server exactly as node assembly builds it, with
// one enrolled machine (m1, mount "m1") and the admin token "tok". st and m
// are handed out for tests that must seed records with exact timestamps,
// prune directly, or assert metric values (the gap-contract tests).
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

// place authors an element at path in the fixture node's namespace and returns
// its id — what an enrollment binds to (id-grants design §4).
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
	reg.SetMetrics(m) // move-drain design §3.4: colca_drains_active is registry-owned
	e := engine.New(s, cfg, reg, nil, m, nil)
	// The registry resolves placements through the engine's element index, so
	// the wiring — and the element — come before any enrollment (id-grants §4).
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
	srv := &http.Server{Handler: Handler(e, cfg, reg, ver, m, nodeID.PublicHex(), false)}
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

// raw performs a request and returns the response together with its body as a
// string — req() decodes into map[string]any, which destroys exactly the JSON
// shapes the gap wire-contract tests assert byte-for-byte.
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
	if resp.StatusCode != 200 || out["stream"] != "metrics" {
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
	}
	resp, _ = req(t, admin, "GET", a.url+"/debug/state", "tok", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("debug/state: %d", resp.StatusCode)
	}
	// healthz and metrics are open
	resp, _ = req(t, admin, "GET", a.url+"/healthz", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
	r2, err := client(nil).Get(a.url + "/metrics")
	if err != nil || r2.StatusCode != 200 {
		t.Fatalf("metrics: %v %d", err, r2.StatusCode)
	}
	r2.Body.Close()
}

func TestAdminConfigureResponseNamesProducedStateOffset(t *testing.T) {
	a := newAPI(t)
	a.eng.SetExecutor(engine.Executors(uns.NewConfigExec(a.eng.EntityStore(), a.reg, nil, nil, nil)))

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

// The §6.3 route matrix for a MACHINE caller: data routes allowed and scoped,
// admin routes forbidden.
func TestMachineRouteMatrix(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	mc := client(a.m1)

	// Seed: one record in m1's zone, one outside (admin publish).
	for _, tp := range []string{"colca/v1/_Metric/m1/m1/temp", "colca/v1/_Metric/other/elsewhere/temp"} {
		resp, out := req(t, admin, "POST", a.url+"/publish", "tok", map[string]any{"topic": tp, "payload": map[string]any{"v": 1.0}})
		if resp.StatusCode != 200 {
			t.Fatalf("seed %s: %d %v", tp, resp.StatusCode, out)
		}
	}

	// machine publish: own zone OK (engine rules apply)
	resp, _ := req(t, mc, "POST", a.url+"/publish", "", map[string]any{"topic": "colca/v1/_Metric/n-test/m1/rpm", "payload": map[string]any{"v": 2.0}})
	if resp.StatusCode != 200 {
		t.Fatalf("machine publish: %d", resp.StatusCode)
	}
	// machine publish with the wrong level-4 → 422 (engine level-4 rule)
	resp, _ = req(t, mc, "POST", a.url+"/publish", "", map[string]any{"topic": "colca/v1/_Metric/other/x", "payload": map[string]any{"v": 2.0}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("machine spoof publish: want 422, got %d", resp.StatusCode)
	}

	// machine fetch: cursor must be namespaced, records scope-filtered
	resp, _ = req(t, mc, "GET", a.url+"/fetch?stream=metrics&cursor=foreign&max=10", "", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("un-namespaced machine cursor: want 403, got %d", resp.StatusCode)
	}
	_, out := req(t, mc, "GET", a.url+"/fetch?stream=metrics&cursor=m1/c&max=10", "", nil)
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
	if resp.StatusCode != 200 {
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
	resp, _ = req(t, mc, "POST", a.url+"/enroll", "", a.m1.EntryJSON(t, "machine", "m1"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("machine enroll: want 403, got %d", resp.StatusCode)
	}
}

func TestEnrollmentRoutes(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)

	m2 := authtest.NewMachine(t, "m2")
	resp, out := req(t, admin, "POST", a.url+"/enroll", "tok", m2.EntryJSON(t, "machine", a.place(t, "m2")))
	if resp.StatusCode != 200 || out["ulid"] != "m2" {
		t.Fatalf("enroll: %d %v", resp.StatusCode, out)
	}
	// the new identity works immediately
	resp, _ = req(t, client(m2), "GET", a.url+"/fetch?stream=metrics&cursor=m2/c&max=1", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("enrolled machine fetch: %d", resp.StatusCode)
	}

	// duplicate pubkey → 409
	dup := &authtest.Machine{ULID: "m3", Pubkey: m2.Pubkey}
	resp, _ = req(t, admin, "POST", a.url+"/enroll", "tok", dup.EntryJSON(t, "machine", a.place(t, "m3")))
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("dup pubkey: want 409, got %d", resp.StatusCode)
	}
	// invalid entry → 422
	resp, _ = req(t, admin, "POST", a.url+"/enroll", "tok", []byte(`{"ulid":"","pubkey":"x","kind":"machine"}`))
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
	if resp.StatusCode != 200 {
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

// Move-drain door semantics (design §3.1): 404 for an unknown ulid, 409 for
// a non-node entry (machines are out of scope, design §3.2 [delta]), 409 for
// a second drain on the same child, 200 + status=draining on success — and
// the drained identity keeps working (still authenticates, still fetches).
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

// Move-drain design §3.1/§3.4: DELETE stays immediate and works during an
// active drain — no drain precondition on the kill-switch — and records the
// "forced" outcome (colca_drains_completed_total{outcome="forced"}),
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

	// A plain DELETE on a never-draining machine must NOT touch the drain
	// counters at all — the "forced" bookkeeping is drain-specific.
	m2 := authtest.NewMachine(t, "m2")
	authtest.EnrollAt(t, a.reg, a.eng, m2, "m2")
	if resp, out := req(t, admin, "DELETE", a.url+"/enroll/m2", "tok", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("plain delete: %d %v", resp.StatusCode, out)
	}
	if v := metricstest.Value(t, a.m, `colca_drains_completed_total{outcome="forced"}`); v != 1 {
		t.Fatalf(`plain (non-draining) DELETE must not add to forced outcomes, colca_drains_completed_total{outcome="forced"} = %v, want still 1`, v)
	}
}

// _EnrolledIdentity may not enter through /publish — not even with the admin token.
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

// Wire-shape guard: /fetch keeps its exact response fields (records with
// local/owner offsets, topic/payload/ts and attribution, plus next) — the retention gap object composes
// into this same response, so the base shape is a contract.
func TestFetchWireShape(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	if resp, out := req(t, admin, "POST", a.url+"/publish", "tok",
		map[string]any{"topic": "colca/v1/_Metric/n-test/x", "payload": map[string]any{"v": 7.5}}); resp.StatusCode != 200 {
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

// Spec §6.1: when the cursor's position is below the stream's LWM, /fetch
// gains the gap object — from_offset = cursor position, to_offset = LWM−1,
// first_ts/last_ts from the prune journal — and records begin at the LWM.
// The response is a wire contract, so the assertions are exact-JSON.
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

	// Spec §6.1 [delta]: a brand-new cursor (position 1) on a pruned stream
	// gets the gap too — a new consumer genuinely cannot see history.
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

// design §8: every /fetch response carrying a gap object counts
// colca_gap_served_total{stream="metrics",surface="fetch"} — a response
// WITHOUT a gap must never move it.
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

	// The gap is side-effect free (does not move the cursor): a second fetch
	// sees the SAME gap and counts a SECOND time — one increment per response
	// actually served, not per distinct gap.
	if _, body := raw(t, admin, "GET", a.url+"/fetch?stream=metrics&cursor=lag", "tok", ""); !strings.Contains(body, `"gap"`) {
		t.Fatalf("repeat fetch must still see the gap: %s", body)
	}
	if v := metricstest.Value(t, a.m, gapServed); v != 2 {
		t.Fatalf("%s = %v after two gap-carrying fetches, want 2", gapServed, v)
	}
}

// Spec §6.1/§6.2: approx is true exactly when a COALESCED journal entry
// answered first_ts. Driven end-to-end through real journal coalescing: more
// prune runs than the journal cap, so the oldest entries merge.
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

// An unknown stream is a malformed request: 400 with an error body, never a
// fabricated gap (an unknown stream has no LWM, and the gap arithmetic must
// not run on it) and never a silently empty 200.
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

// The payload must survive as raw JSON in both directions: never decoded into
// map[string]any and re-encoded, so an integer too large for a float64 keeps
// its exact digits, and never emitted as a base64 or quoted string. Asserted
// on the RAW body bytes — req()'s map decode would round the digits away and
// could never see this regression.
func TestPayloadStaysRawJSON(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	if resp, out := req(t, admin, "POST", a.url+"/publish", "tok", map[string]any{
		"topic":   "colca/v1/_Metric/n-test/line1/temp",
		"payload": json.RawMessage(`{"v":1,"seq":9007199254740993}`),
	}); resp.StatusCode != 200 {
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

// prefix filters on the uns hierarchy path, not on the raw topic, and max is
// bounded (<=0 or >1000 falls back to 100). Run as the admin (unscoped) so
// the prefix logic is isolated from the grant filter it composes with.
func TestFetchPrefixAndMax(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	for i, tp := range []string{"colca/v1/_Metric/n-test/line1/temp", "colca/v1/_Metric/n-test/line2/temp"} {
		if resp, out := req(t, admin, "POST", a.url+"/publish", "tok",
			map[string]any{"topic": tp, "payload": map[string]any{"v": float64(i)}}); resp.StatusCode != 200 {
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

// /debug/state reports the node's ulid and correct per-stream next offsets —
// field correctness, not just a 200. Offsets are relative to the fixture's
// own baseline (enrollment appends an _EnrolledIdentity record to entities), so the
// assertion pins the DELTA a publish causes plus the exact ulid.
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
	// The fixture baseline itself is deterministic: one enrolled machine, which
	// is two entity records — the element it binds to, then its _EnrolledIdentity.
	if before["metrics"] != 1 || before["entities"] != 3 || before["commands"] != 1 ||
		before["definitions"] != 1 || before["audit"] != 1 {
		t.Fatalf("fixture baseline offsets = %v, want all event streams enumerated", before)
	}

	if resp, pub := req(t, admin, "POST", a.url+"/publish", "tok", map[string]any{
		"topic": "colca/v1/_Metric/n-test/line1/temp", "payload": map[string]any{"v": 1.0},
	}); resp.StatusCode != 200 {
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

// plainHandler builds a Handler over a plain httptest server (no TLS): the
// two defensive-construction tests below pin Handler-level contracts that do
// not depend on the listener's TLS wrapping.
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
	srv := httptest.NewServer(Handler(eng, cfg, reg, nil, m, "deadbeef", false))
	t.Cleanup(srv.Close)
	return srv
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
	if resp.StatusCode != 200 {
		t.Fatal("healthz must stay tokenless")
	}
}

// Gap × grants composition: pruning is offset-based and stream-wide, so a
// machine whose cursor sits below the LWM must receive the gap object even
// when every surviving record is OUTSIDE its read grants — it has to learn
// its position is inside a hole — while the records stay scope-filtered
// (no content leak). The gap rides outside the grant filter by design: it
// carries only stream offsets, the same metadata "next" already exposes to
// every caller admitted through this door.
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

	// Acking past the LWM clears the gap for the machine exactly as for the
	// admin — the gap lifecycle is cursor-driven, not grant-driven.
	if resp, _ := req(t, mc, "POST", a.url+"/ack", "", map[string]any{"cursor": "m1/c", "stream": "metrics", "offset": 4}); resp.StatusCode != 200 {
		t.Fatalf("machine ack: %d", resp.StatusCode)
	}
	_, body = raw(t, mc, "GET", a.url+"/fetch?stream=metrics&cursor=m1/c", "", "")
	if strings.Contains(body, `"gap"`) {
		t.Fatalf("gap must clear after acking past the LWM: %s", body)
	}
}

// ---- human callers (human-authz design §5.3) ----

// The §5.3 route matrix for a HUMAN caller: scoped reads, {sub}/ cursors,
// commands-only publish, admin routes gated on admin:#.
func TestHumanRouteMatrix(t *testing.T) {
	a := newAPI(t)
	admin := client(nil)
	hc := client(nil) // humans are certless; the token is the credential

	// Seed: one record in the granted zone, one outside.
	for _, tp := range []string{"colca/v1/_Metric/m1/m1/temp", "colca/v1/_Metric/x/elsewhere/temp"} {
		if resp, out := req(t, admin, "POST", a.url+"/publish", "tok",
			map[string]any{"topic": tp, "payload": map[string]any{"v": 1.0}}); resp.StatusCode != 200 {
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
		map[string]any{"cursor": "anna/c", "stream": "metrics", "offset": 1}); resp.StatusCode != 200 {
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

	// publish: command with grant OK; data → 422 human_write.
	if resp, out := bearerReq(t, hc, "POST", a.url+"/publish", tok, map[string]any{
		"topic":   "colca/v1/_CmdParam/m1/m1/set-speed",
		"payload": map[string]any{"correlation_id": "h1", "expires_at": float64(99999999999999)},
	}); resp.StatusCode != 200 || out["stream"] != "commands" {
		t.Fatalf("human command publish: %d %v", resp.StatusCode, out)
	}
	if resp, _ := bearerReq(t, hc, "POST", a.url+"/publish", tok, map[string]any{
		"topic": "colca/v1/_Metric/m1/m1/temp", "payload": map[string]any{"v": 666.0},
	}); resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("human data publish: want 422, got %d", resp.StatusCode)
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

// admin:# unlocks the admin surface for a human — attributable admin actions.
func TestHumanAdminGrant(t *testing.T) {
	a := newAPI(t)
	hc := client(nil)
	adminTok := a.mint("boss", []string{"admin:#"})

	m2 := authtest.NewMachine(t, "m2")
	resp, out := bearerReq(t, hc, "POST", a.url+"/enroll", adminTok, m2.EntryJSON(t, "machine", a.place(t, "m2")))
	if resp.StatusCode != 200 || out["ulid"] != "m2" {
		t.Fatalf("human admin enroll: %d %v", resp.StatusCode, out)
	}
	if resp, _ := bearerReq(t, hc, "GET", a.url+"/enroll", adminTok, nil); resp.StatusCode != 200 {
		t.Fatalf("human admin list: %d", resp.StatusCode)
	}
	if resp, _ := bearerReq(t, hc, "DELETE", a.url+"/enroll/m2", adminTok, nil); resp.StatusCode != 200 {
		t.Fatalf("human admin revoke: %d", resp.StatusCode)
	}
	// admin does NOT widen reads: fetch returns only what read grants cover
	// (boss has none → zero records despite seeded data).
	if resp, out := req(t, client(nil), "POST", a.url+"/publish", "tok",
		map[string]any{"topic": "colca/v1/_Metric/m1/m1/t", "payload": map[string]any{"v": 1.0}}); resp.StatusCode != 200 {
		t.Fatalf("seed: %v", out)
	}
	_, out = bearerReq(t, hc, "GET", a.url+"/fetch?stream=metrics&cursor=boss/c&max=10", adminTok, nil)
	if recs := out["records"].([]any); len(recs) != 0 {
		t.Fatalf("admin:# must not widen reads, got %v", recs)
	}
}

// Failed bearer credentials never fall through — not to the admin token, not
// to anonymous.
func TestBearerFailuresAreTerminal(t *testing.T) {
	a := newAPI(t)
	hc := client(nil)

	expired := a.iss.MintOpt(tokentest.MintOpts{Sub: "anna", Exp: time.Now().Add(-3 * time.Minute)})
	resp, _ := bearerReq(t, hc, "GET", a.url+"/fetch?stream=metrics&cursor=anna/c&max=1", expired, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired bearer: want 401, got %d", resp.StatusCode)
	}

	// Bad bearer + VALID admin token in the same request: still 401 (mutation
	// guard for the no-fallthrough rule).
	r, err := http.NewRequest("GET", a.url+"/debug/state", nil)
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
	// Enrollment needs the child's ULID and pubkey BEFORE the child is trusted
	// by anything, so /healthz — the one unauthenticated door — is where they
	// have to be readable. Without it, enrolling a node means a shell on it.
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

// localAPI is the fixture for the local HTTP door (local-service-trust design
// §4): no TLS, no admin routes, and self-registration's mount-authoring wired
// EXACTLY as node.Start wires it — domain.Execute("_CmdConfigure",
// "element/upsert", ...) is the one authoring path in this system. Mirrors
// mqttsrv_test.go's startServerWithLocalDoor so both local doors are proven
// against the same wiring, not a test-only shortcut that could pass while
// node.go's own wiring stayed broken.
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
	domain := uns.NewConfigExec(eng.EntityStore(), reg, eng.Elements(), registry.NewULID, cfg.Plugin)
	eng.SetExecutor(engine.Executors(engine.NewAdminExecutor(reg), domain))
	eng.SetObserver(domain)
	reg.SetNamespace(eng.Elements())
	reg.SetAuthoring(eng.Elements(), func(path, elementID string) error {
		name := path
		if i := strings.LastIndexByte(path, '/'); i >= 0 {
			name = path[i+1:]
		}
		payload, err := json.Marshal(map[string]any{
			"elements": []map[string]any{
				{"path": path, "element": map[string]any{"id": elementID, "name": name}},
			},
		})
		if err != nil {
			return err
		}
		code, msg, _ := domain.Execute("_CmdConfigure", "element/upsert", payload)
		if code != 200 {
			return fmt.Errorf("author element at %s: %s", path, msg)
		}
		return nil
	})
	h := Handler(eng, cfg, reg, nil, m, "deadbeef", true)
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
	req := httptest.NewRequest("GET", "/kv", nil)
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

func TestLocalSelfReturnsTheMintedIdentityAndAuthoritativeMount(t *testing.T) {
	h := newLocalHandler(t)
	// Seed an unrelated element first. A client that guesses its own mount by
	// taking the first visible _SystemElement can now return the wrong answer;
	// /self must resolve the caller's registry binding directly.
	authtest.Place(t, h.eng, "unrelated")

	req := httptest.NewRequest("GET", "/self", nil)
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
}

func TestLocalSelfRegistersAnUndeclaredServiceAtTheNode(t *testing.T) {
	h := newLocalHandler(t)
	req := httptest.NewRequest("GET", "/self", nil)
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
	req := httptest.NewRequest("POST", "/publish", bytes.NewReader(body))
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

	// The container still declares its original startup mount. Register must
	// treat that declaration as a seed only, and /self must return the current
	// registry position chosen by the operator.
	req := httptest.NewRequest("GET", "/self", nil)
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
		h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
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

	req := httptest.NewRequest("GET", "/kv", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("nameless request on the local door = %d; want 401 — the name is how its scope is found", rec.Code)
	}
	// A 401 for the WRONG reason would still pass a status-only assertion —
	// pin the reason the code actually produces, mirroring the MQTT twin
	// (mqttsrv_test.go's TestTheLocalDoorRequiresAName).
	if v := metricstest.Value(t, h.m, line); v != 1 {
		t.Fatalf("%s = %v after the nameless request, want exactly 1", line, v)
	}
}

// A machine (or child node) may be given a friendly `name` — uns.Entry.Validate
// permits it on any kind, and Manager.Enroll indexes any non-empty Name into
// byName regardless of kind (registry.go). That makes it resolvable through
// ByName, and Register's own idempotent-reconnect branch ("entry exists?
// return it") does no kind check — so without the post-Register
// MayUseDoor(DoorLocal) check, a certless local request could present that
// name and be handed the machine's own ULID: its topic identity, its grants,
// its mount. Mirrors mqttsrv_test.go's
// TestALocalNameThatResolvesToAKeyedIdentityByNameIsRefused so both local
// doors are pinned against the identical hole.
func TestTheLocalHandlerRefusesAKeyedIdentityFoundByName(t *testing.T) {
	h := newLocalHandler(t)
	reg := testRegistry(t, h)
	element := authtest.Place(t, h.eng, "press3")
	m := authtest.NewMachine(t, "01JNAMEDMACHINE")
	entry := uns.Entry{ULID: m.ULID, Pubkey: m.Pubkey, Kind: uns.KindMachine, Name: "friendly-name", Element: element}
	raw, err := json.Marshal(&entry)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reg.Enroll(raw); err != nil {
		t.Fatalf("enroll a named machine: %v", err)
	}
	// Get(name) must NOT be the thing catching this: "friendly-name" is not
	// anyone's ULID, so that pre-check passes clean through, and only the
	// post-Register MayUseDoor check can still refuse it.
	if _, ok := reg.Get("friendly-name"); ok {
		t.Fatal("precondition broken: \"friendly-name\" must not itself be a ulid")
	}

	const line = `colca_auth_rejections_total{door="local",reason="kind"}`
	if v := metricstest.Value(t, h.m, line); v != 0 {
		t.Fatalf("%s = %v before any request, want 0", line, v)
	}

	req := httptest.NewRequest("GET", "/kv", nil)
	req.Header.Set("X-Colca-Service", "friendly-name")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("a machine's friendly name was claimed by a certless request on the local door: got %d, want 401", rec.Code)
	}
	// A 401 for the WRONG reason would still pass a status-only assertion —
	// pin the reason the code actually produces, mirroring the MQTT twin
	// (mqttsrv_test.go's TestALocalNameThatResolvesToAKeyedIdentityByNameIsRefused).
	if v := metricstest.Value(t, h.m, line); v != 1 {
		t.Fatalf("%s = %v after the named-machine request, want exactly 1", line, v)
	}
	// And the request must not have been treated as SOME other newly-minted
	// local identity either — the machine's own entry is what must stay
	// untouched, not just "some name got refused".
	got, ok := reg.Get(m.ULID)
	if !ok || got.Kind != uns.KindMachine {
		t.Fatal("the machine entry itself must be untouched by the refused request")
	}
}

// The MQTT door has a twin (TestAMachineKeyIsNotAcceptedOnTheLocalDoor): a
// machine ULID enrolled on the main door must not be assumable by name on the
// local door either. Register alone would not catch this — its own
// uniqueness check is scoped to the byName index, a different key space than
// byID — so this is what the reg.Get(name) pre-check in resolve exists for.
// Without a dedicated test the collision guard had zero coverage: deleting it
// left the whole suite green.
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

	req := httptest.NewRequest("GET", "/kv", nil)
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

// registerLocal sends the CONNECT-equivalent GET a local service makes on
// first contact, so subsequent /fetch and /ack calls in a test have a real
// registered (and, when mount != "", placed) entry behind them — not a name
// the test merely intends to use.
func registerLocal(t *testing.T, h *localAPI, name, mount string) {
	t.Helper()
	req := httptest.NewRequest("GET", "/kv", nil)
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

// A local caller never learns the ULID Register mints for it — only the name
// it presented — so ownsCursor's ULID-prefix rule (correct for a machine or a
// human, who both already know their own identifier) made /fetch and /ack
// permanently unreachable from this door: no cursor name it could construct
// would ever pass. Design §4 specifies the fix: cursors namespaced by NAME
// under "c/{name}/…". This proves both routes actually work end to end —
// not merely that the cursor-ownership check stops 403ing — by seeding a
// real record in the entry's own (implicitly readable) zone and reading it
// back through /fetch, then moving the cursor through /ack.
func TestTheLocalHandlerFetchAndAckWorkWithANameNamespacedCursor(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "connector-opcua", "press3")

	if _, err := h.eng.IngestAdmin("colca/v1/_Metric/n-test/press3/temp", []byte(`{"v":1.0}`)); err != nil {
		t.Fatalf("seed metric: %v", err)
	}

	cursor := uns.LocalCursorPrefix + "connector-opcua/c1"
	fetchReq := httptest.NewRequest("GET", "/fetch?stream=metrics&cursor="+cursor+"&max=10", nil)
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
	ackReq := httptest.NewRequest("POST", "/ack", strings.NewReader(ackBody))
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
	fetchReq2 := httptest.NewRequest("GET", "/fetch?stream=metrics&cursor="+cursor+"&max=10", nil)
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

// One local service must never be able to move another's cursor: that is the
// one thing name-namespacing has to guarantee (design §4: "so two local
// services cannot collide on /ack").
func TestTheLocalHandlerRefusesToAckAnotherServicesCursor(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "connector-a", "")
	registerLocal(t, h, "connector-b", "")

	body := fmt.Sprintf(`{"cursor":%q,"stream":"metrics","offset":0}`, uns.LocalCursorPrefix+"connector-a/c1")
	req := httptest.NewRequest("POST", "/ack", strings.NewReader(body))
	req.Header.Set("X-Colca-Service", "connector-b")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("connector-b acking connector-a's cursor = %d, want 403", rec.Code)
	}
}
