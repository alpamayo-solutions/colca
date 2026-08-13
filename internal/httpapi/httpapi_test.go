package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func newAPI(t *testing.T) *httptest.Server {
	t.Helper()
	s, _ := store.Open(t.TempDir())
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-test", API: config.API{Token: "tok"}}
	e := engine.New(s, cfg, nil)
	srv := httptest.NewServer(Handler(e, cfg))
	t.Cleanup(srv.Close)
	return srv
}

func req(t *testing.T, srv *httptest.Server, method, path, token string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	r, _ := http.NewRequest(method, srv.URL+path, &buf)
	if token != "" {
		r.Header.Set("X-Colca-Token", token)
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	return resp, out
}

func TestPublishFetchAckKV(t *testing.T) {
	srv := newAPI(t)
	// auth required
	resp, _ := req(t, srv, "POST", "/publish", "", map[string]any{"topic": "colca/v1/_Metric/n-test/x", "payload": map[string]any{"v": 1.0}})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}

	resp, out := req(t, srv, "POST", "/publish", "tok", map[string]any{"topic": "colca/v1/_Metric/n-test/x", "payload": map[string]any{"v": 1.0}})
	if resp.StatusCode != 200 || out["stream"] != "metrics" || out["offset"].(float64) != 1 {
		t.Fatalf("%d %v", resp.StatusCode, out)
	}

	// invalid payload → 422
	resp, _ = req(t, srv, "POST", "/publish", "tok", map[string]any{"topic": "colca/v1/_Metric/n-test/x", "payload": map[string]any{"v": "bad"}})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d", resp.StatusCode)
	}

	_, out = req(t, srv, "GET", "/fetch?stream=metrics&cursor=c1&max=10", "tok", nil)
	recs := out["records"].([]any)
	if len(recs) != 1 || out["next"].(float64) != 2 {
		t.Fatalf("%v", out)
	}

	_, out = req(t, srv, "POST", "/ack", "tok", map[string]any{"cursor": "c1", "stream": "metrics", "offset": 1})
	if out["moved"] != true {
		t.Fatalf("%v", out)
	}

	_, out = req(t, srv, "GET", "/kv?prefix=x", "tok", nil)
	if len(out["entries"].([]any)) != 1 {
		t.Fatalf("%v", out)
	}

	resp, _ = req(t, srv, "GET", "/healthz", "", nil)
	if resp.StatusCode != 200 {
		t.Fatal("healthz must be tokenless")
	}
}

// raw performs a request and returns the response together with its body as a
// string — the plan's req() helper decodes into map[string]any, which destroys
// exactly the JSON shapes the integration suite and the smoke script parse.
func raw(t *testing.T, srv *httptest.Server, method, path, token, body string) (*http.Response, string) {
	t.Helper()
	r, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		r.Header.Set("X-Colca-Token", token)
	}
	resp, err := http.DefaultClient.Do(r)
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

func publish(t *testing.T, srv *httptest.Server, topic, payload string) {
	t.Helper()
	resp, out := req(t, srv, "POST", "/publish", "tok",
		map[string]any{"topic": topic, "payload": json.RawMessage(payload)})
	if resp.StatusCode != 200 {
		t.Fatalf("publish %s: %d %v", topic, resp.StatusCode, out)
	}
}

// The payload must survive as raw JSON in both directions: never decoded into
// map[string]any and re-encoded, so an integer too large for a float64 keeps
// its exact digits, and never emitted as a base64 or quoted string.
func TestPayloadStaysRawJSON(t *testing.T) {
	srv := newAPI(t)
	publish(t, srv, "colca/v1/_Metric/n-test/line1/temp", `{"v":1,"seq":9007199254740993}`)

	_, body := raw(t, srv, "GET", "/fetch?stream=metrics&cursor=rawc", "tok", "")
	if !strings.Contains(body, `"payload":{`) {
		t.Fatalf("fetch payload must be a JSON object, got %s", body)
	}
	if !strings.Contains(body, `"seq":9007199254740993`) {
		t.Fatalf("fetch payload lost exact number form: %s", body)
	}

	_, body = raw(t, srv, "GET", "/kv?prefix=line1", "tok", "")
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

// Reading is side-effect free: /fetch starts at the cursor's current position
// and never moves it, so independent cursor names each see the full history.
// Only /ack moves a cursor, and it stores offset+1 ("next to read").
func TestFetchNeverAdvancesCursor(t *testing.T) {
	srv := newAPI(t)
	publish(t, srv, "colca/v1/_Metric/n-test/line1/temp", `{"v":1}`)
	publish(t, srv, "colca/v1/_Metric/n-test/line2/temp", `{"v":2}`)

	fetch := func(cursor string) (int, float64) {
		t.Helper()
		_, out := req(t, srv, "GET", "/fetch?stream=metrics&cursor="+cursor, "tok", nil)
		return len(out["records"].([]any)), out["next"].(float64)
	}

	for _, cursor := range []string{"catchup", "catchup", "catchup2"} {
		if n, next := fetch(cursor); n != 2 || next != 3 {
			t.Fatalf("cursor %q: want 2 records and next=3, got %d/%v", cursor, n, next)
		}
	}

	_, out := req(t, srv, "POST", "/ack", "tok", map[string]any{"cursor": "catchup", "stream": "metrics", "offset": 1})
	if out["moved"] != true {
		t.Fatalf("first ack must move the cursor: %v", out)
	}
	// Acked offset 1 → cursor holds 2 → only the second record is left.
	if n, next := fetch("catchup"); n != 1 || next != 3 {
		t.Fatalf("after ack: want 1 record and next=3, got %d/%v", n, next)
	}
	// Monotonic: re-acking an older offset must not move it back.
	_, out = req(t, srv, "POST", "/ack", "tok", map[string]any{"cursor": "catchup", "stream": "metrics", "offset": 0})
	if out["moved"] != false {
		t.Fatalf("backwards ack must not move the cursor: %v", out)
	}
	if n, _ := fetch("catchup"); n != 1 {
		t.Fatalf("backwards ack changed the cursor position: %d records", n)
	}
	// An untouched cursor still sees everything.
	if n, _ := fetch("catchup2"); n != 2 {
		t.Fatalf("ack on one cursor leaked into another: %d records", n)
	}
}

// prefix filters on the uns hierarchy path, not on the raw topic, and max is
// bounded (<=0 or >1000 falls back to 100).
func TestFetchPrefixAndMax(t *testing.T) {
	srv := newAPI(t)
	publish(t, srv, "colca/v1/_Metric/n-test/line1/temp", `{"v":1}`)
	publish(t, srv, "colca/v1/_Metric/n-test/line2/temp", `{"v":2}`)

	_, out := req(t, srv, "GET", "/fetch?stream=metrics&cursor=p1&prefix=line1", "tok", nil)
	recs := out["records"].([]any)
	if len(recs) != 1 {
		t.Fatalf("prefix must filter on the hierarchy path: %v", out)
	}
	if topic := recs[0].(map[string]any)["topic"]; topic != "colca/v1/_Metric/n-test/line1/temp" {
		t.Fatalf("wrong record survived the prefix filter: %v", topic)
	}
	// "n-test" is in the topic but not at the head of the path → no match.
	_, out = req(t, srv, "GET", "/fetch?stream=metrics&cursor=p2&prefix=n-test", "tok", nil)
	if len(out["records"].([]any)) != 0 {
		t.Fatalf("prefix must not match the raw topic: %v", out)
	}
	for _, q := range []string{"&max=0", "&max=-5", "&max=99999", "&max=nonsense", ""} {
		_, out = req(t, srv, "GET", "/fetch?stream=metrics&cursor=m1"+q, "tok", nil)
		if len(out["records"].([]any)) != 2 {
			t.Fatalf("max=%q must fall back to the default: %v", q, out)
		}
	}
}

// records and entries are always JSON arrays, never null — the integration
// suite indexes into them without a nil check.
func TestEmptyCollectionsAreArrays(t *testing.T) {
	srv := newAPI(t)
	_, body := raw(t, srv, "GET", "/fetch?stream=entities&cursor=empty", "tok", "")
	if !strings.Contains(body, `"records":[]`) {
		t.Fatalf("empty fetch must return [], got %s", body)
	}
	_, body = raw(t, srv, "GET", "/kv?prefix=nothing", "tok", "")
	if !strings.Contains(body, `"entries":[]`) {
		t.Fatalf("empty kv must return [], got %s", body)
	}
}

func TestPublishErrorCodes(t *testing.T) {
	srv := newAPI(t)
	// malformed request body → 400
	resp, body := raw(t, srv, "POST", "/publish", "tok", "{not json")
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, `"error"`) {
		t.Fatalf("want 400 with an error body, got %d %s", resp.StatusCode, body)
	}
	// well-formed request, unacceptable content → 422
	for _, tc := range []struct{ name, topic, payload string }{
		{"non-UNS topic", "other/v1/_Metric/n-test/x", `{"v":1}`},
		{"bad grammar", "colca/v1/_Metric/n-test", `{"v":1}`},
		{"unknown contract", "colca/v1/_Nope/n-test/x", `{"v":1}`},
		{"invalid payload", "colca/v1/_Metric/n-test/x", `{"v":"bad"}`},
	} {
		resp, out := req(t, srv, "POST", "/publish", "tok",
			map[string]any{"topic": tc.topic, "payload": json.RawMessage(tc.payload)})
		if resp.StatusCode != http.StatusUnprocessableEntity {
			t.Fatalf("%s: want 422, got %d", tc.name, resp.StatusCode)
		}
		if _, ok := out["error"].(string); !ok {
			t.Fatalf("%s: 422 must carry an error string, got %v", tc.name, out)
		}
	}
}

func TestAuthAndDebugState(t *testing.T) {
	srv := newAPI(t)
	protected := []struct{ method, path string }{
		{"POST", "/publish"},
		{"GET", "/fetch?stream=metrics&cursor=c"},
		{"POST", "/ack"},
		{"GET", "/kv"},
		{"GET", "/debug/state"},
	}
	for _, p := range protected {
		for _, token := range []string{"", "wrong"} {
			resp, out := req(t, srv, p.method, p.path, token, map[string]any{})
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("%s %s with token %q: want 401, got %d", p.method, p.path, token, resp.StatusCode)
			}
			if _, ok := out["error"].(string); !ok {
				t.Fatalf("%s %s: 401 must carry a JSON error body, got %v", p.method, p.path, out)
			}
		}
	}

	publish(t, srv, "colca/v1/_Metric/n-test/line1/temp", `{"v":1}`)
	_, out := req(t, srv, "GET", "/debug/state", "tok", nil)
	if out["ulid"] != "n-test" {
		t.Fatalf("debug state must report the node ulid: %v", out)
	}
	streams := out["streams"].(map[string]any)
	for stream, want := range map[string]float64{"metrics": 2, "entities": 1, "commands": 1} {
		got := streams[stream].(map[string]any)["next_offset"].(float64)
		if got != want {
			t.Fatalf("debug state %s.next_offset: want %v, got %v", stream, want, got)
		}
	}

	_, out = req(t, srv, "GET", "/healthz", "", nil)
	if out["ok"] != true || out["ulid"] != "n-test" {
		t.Fatalf("healthz body: %v", out)
	}
}

// An empty configured token is a missing secret, not an invitation: nothing
// authenticates, and /healthz stays tokenless.
func TestEmptyConfiguredTokenDeniesEveryone(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-notoken"}
	srv := httptest.NewServer(Handler(engine.New(s, cfg, nil), cfg))
	t.Cleanup(srv.Close)

	for _, token := range []string{"", "tok"} {
		resp, _ := req(t, srv, "GET", "/kv", token, nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("empty configured token with %q: want 401, got %d", token, resp.StatusCode)
		}
	}
	resp, _ := req(t, srv, "GET", "/healthz", "", nil)
	if resp.StatusCode != 200 {
		t.Fatal("healthz must stay tokenless")
	}
}
