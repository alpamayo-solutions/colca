package door

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The client is shared by every service, so these tests pin its contract: route,
// parameters, headers, and that a refusal is an error rather than an empty
// answer.

func TestFetchAsksForTheStreamAndCursorItWasGiven(t *testing.T) {
	var gotPath, gotQuery, gotToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotToken = r.URL.Path, r.URL.RawQuery, r.Header.Get("X-Colca-Token")
		_, _ = w.Write([]byte(`{"records":[{"offset":7,"topic":"colca/v1/_Metric/m1/t","payload":{"v":1},"ts":9}],"next":8}`))
	}))
	defer srv.Close()

	page, err := (&Client{BaseURL: srv.URL, Token: "tok"}).
		Fetch(context.Background(), "metrics", "c/historian/metrics", 250)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if gotPath != "/fetch" {
		t.Fatalf("path = %q, want /fetch", gotPath)
	}
	if gotQuery != "cursor=c%2Fhistorian%2Fmetrics&max=250&stream=metrics" {
		t.Fatalf("query = %q", gotQuery)
	}
	if gotToken != "tok" {
		t.Fatalf("token header = %q", gotToken)
	}
	if page.Next != 8 || len(page.Records) != 1 || page.Records[0].Offset != 7 {
		t.Fatalf("page = %+v", page)
	}
}

func TestFilteredFetchEncodesRepeatedSignalIDsAndDecodesGapAttribution(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"records":[{"offset":7,"origin_offset":4,"topic":"colca/v1/_Metric/n/s","payload":{"signal_id":"s-1","value":1},"ts":9,"written_by":"connector","actor_id":"svc-1","actor_label":"OPC UA","actor_kind":"service"}],"next":12,"gap":{"stream":"metrics","from_offset":2,"to_offset":6,"first_ts":1,"last_ts":8,"approx":false}}`))
	}))
	defer srv.Close()

	page, err := (&Client{BaseURL: srv.URL}).FetchWithOptions(context.Background(), FetchOptions{
		Stream: "metrics", Cursor: "c/notifications/alarm-metrics-v1", Max: 100,
		Prefix: "line1", SignalIDs: []string{"s-1", "s-2"},
	})
	if err != nil {
		t.Fatalf("FetchWithOptions: %v", err)
	}

	wantQuery := "cursor=c%2Fnotifications%2Falarm-metrics-v1&max=100&prefix=line1&signal_id=s-1&signal_id=s-2&stream=metrics"
	if gotQuery != wantQuery {
		t.Fatalf("query = %q, want %q", gotQuery, wantQuery)
	}
	if page.Gap == nil || page.Gap.ToOffset != 6 || page.Gap.Stream != "metrics" {
		t.Fatalf("gap = %+v", page.Gap)
	}
	if len(page.Records) != 1 || page.Records[0].WrittenBy != "connector" || page.Records[0].ActorID != "svc-1" {
		t.Fatalf("records = %+v", page.Records)
	}
}

func TestKVReturnsRetainedPayloadsVerbatim(t *testing.T) {
	var gotQueries []string
	var gotService string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQueries = append(gotQueries, r.URL.RawQuery)
		gotService = r.Header.Get("X-Colca-Service")
		if r.URL.Query().Get("after") == "page-1" {
			_, _ = w.Write([]byte(`{"entries":[{"path":"_colca/config/c2","node_id":"n1","topic":"colca/v1/_AlarmNotificationConfig/n1/_colca/config/c2","payload":{"integer":2},"ts":8,"offset":4}],"next":""}`))
			return
		}
		_, _ = w.Write([]byte(`{"entries":[{"path":"_colca/config/c1","node_id":"n1","topic":"colca/v1/_AlarmNotificationConfig/n1/_colca/config/c1","payload":{"integer":9007199254740993},"ts":7,"offset":3,"written_by":"operator-ui","actor_id":"kc-sub-anna","actor_label":"anna","actor_kind":"human"}],"next":"page-1"}`))
	}))
	defer srv.Close()

	entries, err := (&Client{BaseURL: srv.URL, Service: "notifications"}).KV(context.Background(), "_colca/config")
	if err != nil {
		t.Fatalf("KV: %v", err)
	}
	if len(gotQueries) != 2 || gotQueries[0] != "max=10000&prefix=_colca%2Fconfig" ||
		gotQueries[1] != "after=page-1&max=10000&prefix=_colca%2Fconfig" || gotService != "notifications" {
		t.Fatalf("queries/service = %q/%q", gotQueries, gotService)
	}
	if len(entries) != 2 || string(entries[0].Payload) != `{"integer":9007199254740993}` {
		t.Fatalf("entries = %+v", entries)
	}
	// The first entry's write was attributed; the /fetch-style actor fields
	// survive onto the KV entry. The second carries none, and stays empty
	// rather than inventing a value.
	if entries[0].WrittenBy != "operator-ui" || entries[0].ActorID != "kc-sub-anna" ||
		entries[0].ActorLabel != "anna" || entries[0].ActorKind != "human" {
		t.Fatalf("entries[0] attribution = %+v", entries[0])
	}
	if entries[1].WrittenBy != "" || entries[1].ActorID != "" || entries[1].ActorLabel != "" || entries[1].ActorKind != "" {
		t.Fatalf("entries[1] should carry no attribution: %+v", entries[1])
	}
}

func TestSelfReturnsTheResolvedLocalIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/self" || r.Header.Get("X-Colca-Service") != "notifications" {
			t.Fatalf("request = %s service=%q", r.URL.Path, r.Header.Get("X-Colca-Service"))
		}
		_, _ = w.Write([]byte(`{"ulid":"svc-1","name":"notifications","node":"node-1","element":"element-1","mount":"line1"}`))
	}))
	defer srv.Close()

	self, err := (&Client{BaseURL: srv.URL, Service: "notifications"}).Self(context.Background())
	if err != nil {
		t.Fatalf("Self: %v", err)
	}
	if self.ULID != "svc-1" || self.Node != "node-1" || self.Mount != "line1" {
		t.Fatalf("self = %+v", self)
	}
}

func TestPayloadsArriveVerbatim(t *testing.T) {
	// The door hands payloads through without re-encoding so a large integer
	// survives bit-for-bit; a client that decoded into a map would undo that.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"records":[{"offset":1,"topic":"t","payload":{"v":9007199254740993},"ts":1}],"next":2}`))
	}))
	defer srv.Close()

	page, err := (&Client{BaseURL: srv.URL}).Fetch(context.Background(), "metrics", "c", 1)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(page.Records[0].Payload) != `{"v":9007199254740993}` {
		t.Fatalf("payload = %s, want the digits unchanged", page.Records[0].Payload)
	}
}

func TestALocalServiceSendsItsNameAndNoToken(t *testing.T) {
	var token, service string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, service = r.Header.Get("X-Colca-Token"), r.Header.Get("X-Colca-Service")
		_, _ = w.Write([]byte(`{"records":[],"next":0}`))
	}))
	defer srv.Close()

	_, err := (&Client{BaseURL: srv.URL, Service: "historian"}).
		Fetch(context.Background(), "metrics", "c", 1)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if service != "historian" || token != "" {
		t.Fatalf("service = %q, token = %q", service, token)
	}
}

func TestAckReportsWhetherTheCursorMoved(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte(`{"moved":false}`))
	}))
	defer srv.Close()

	moved, err := (&Client{BaseURL: srv.URL}).Ack(context.Background(), "metrics", "c", 12)
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if moved {
		t.Fatal("moved = true, want false — acking backwards changes nothing")
	}
	if body["cursor"] != "c" || body["stream"] != "metrics" || body["offset"].(float64) != 12 {
		t.Fatalf("body = %v", body)
	}
}

func TestARefusalIsAnErrorNotAnEmptyPage(t *testing.T) {
	// The failure this prevents: a consumer reading 401 as "no records", acking
	// nothing, and reporting itself healthy forever.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer srv.Close()

	if _, err := (&Client{BaseURL: srv.URL}).Fetch(context.Background(), "metrics", "c", 1); err == nil {
		t.Fatal("a 401 was read as an empty page")
	}
}

func TestULIDRefusesANodeThatNamesNoone(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	if _, err := (&Client{BaseURL: srv.URL}).ULID(context.Background()); err == nil {
		t.Fatal("a /healthz without a ulid was accepted")
	}
}
