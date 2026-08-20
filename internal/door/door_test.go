package door

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The client is shared by every service that talks to a node, so what it must
// get right is the CONTRACT: which route, which params, which header, and that
// a refusal is surfaced rather than read as an empty answer.

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
