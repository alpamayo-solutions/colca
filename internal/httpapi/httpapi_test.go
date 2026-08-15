package httpapi

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
)

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
	m1 := authtest.NewMachine(t, "m1")
	authtest.Enroll(t, reg, m1, "m1")

	cfg := &config.Config{ULID: "n-test", API: config.API{Token: "tok"}}
	m := metrics.New(s, config.Retention{})
	e := engine.New(s, cfg, reg, nil, m)

	tlsCfg, err := TLSConfig(nodeID, "n-test")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: Handler(e, cfg, reg, m)}
	go func() { _ = srv.Serve(tls.NewListener(ln, tlsCfg)) }()
	t.Cleanup(func() { _ = srv.Close() })

	return &api{url: "https://" + ln.Addr().String(), eng: e, reg: reg, m1: m1, st: s, m: m}
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

	resp, out := req(t, admin, "POST", a.url+"/publish", "tok", map[string]any{"topic": "colca/v1/_Metric/n-test/x", "payload": map[string]any{"v": 1.0}})
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
	resp, _ := req(t, mc, "POST", a.url+"/publish", "", map[string]any{"topic": "colca/v1/_Metric/m1/rpm", "payload": map[string]any{"v": 2.0}})
	if resp.StatusCode != 200 {
		t.Fatalf("machine publish: %d", resp.StatusCode)
	}
	// machine publish outside its identity → 422 (engine identity rule)
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

	// machine kv: scope-filtered (own zone + own _EdgeNode entry visible, the
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
	resp, out := req(t, admin, "POST", a.url+"/enroll", "tok", m2.EntryJSON(t, "machine", "m2"))
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
	resp, _ = req(t, admin, "POST", a.url+"/enroll", "tok", dup.EntryJSON(t, "machine", "m3"))
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

// _EdgeNode may not enter through /publish — not even with the admin token.
func TestEdgeNodeRejectedOnPublish(t *testing.T) {
	a := newAPI(t)
	resp, out := req(t, client(nil), "POST", a.url+"/publish", "tok",
		map[string]any{"topic": "colca/v1/_EdgeNode/x/somewhere", "payload": map[string]any{"ulid": "x"}})
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
// offset/topic/payload/ts, plus next) — the retention gap object composes
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
	for _, k := range []string{"offset", "topic", "payload", "ts"} {
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

	surviving := `{"offset":4,"payload":{"v":4},"topic":"` + topic + `","ts":4000},` +
		`{"offset":5,"payload":{"v":5},"topic":"` + topic + `","ts":5000}`

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
	want = `{"next":6,"records":[{"offset":5,"payload":{"v":5},"topic":"` + topic + `","ts":5000}]}` + "\n"
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
