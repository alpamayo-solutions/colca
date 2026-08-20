package grantsync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func kv(topic, node string, payload string) KVEntry {
	e := KVEntry{Topic: topic, NodeID: node}
	if payload != "" {
		e.Payload = json.RawMessage(payload)
	}
	return e
}

func TestElementsMapIDToThePathTheRootHoldsThemAt(t *testing.T) {
	view := ParseKV([]KVEntry{
		kv("colca/v1/_SystemElement/01HROOT/site1/edge1/m6", "01HROOT", `{"id":"01HM6","name":"m6"}`),
	})
	if got := view.Elements["01HM6"]; got != "site1/edge1/m6" {
		t.Fatalf("element 01HM6 resolved to %q, want site1/edge1/m6", got)
	}
}

func TestATombstonedElementIsNotInTheView(t *testing.T) {
	// A retired element must not keep a resource somebody can grant on.
	view := ParseKV([]KVEntry{kv("colca/v1/_SystemElement/01HROOT/site1/m6", "01HROOT", "")})
	if len(view.Elements) != 0 {
		t.Fatalf("retired element still present: %v", view.Elements)
	}
}

func TestHeldGroupsCarryTheirAuthor(t *testing.T) {
	// The service retracts only what IT wrote; a definition authored elsewhere
	// is not the root's to withdraw.
	view := ParseKV([]KVEntry{
		kv("colca/v1/_Group/01HEDGE1/ops", "01HEDGE1", `{"id":"ops","grants":["read:01HM6/#"]}`),
	})
	g, ok := view.Groups["ops"]
	if !ok {
		t.Fatal("group ops missing from the view")
	}
	if g.Author != "01HEDGE1" {
		t.Fatalf("author is %q, want 01HEDGE1", g.Author)
	}
	if len(g.Grants) != 1 || g.Grants[0] != "read:01HM6/#" {
		t.Fatalf("grants parsed as %v", g.Grants)
	}
}

func TestUnrelatedContractsAndJunkTopicsAreIgnored(t *testing.T) {
	// /kv returns the node's whole projection; only two contracts are ours.
	view := ParseKV([]KVEntry{
		kv("colca/v1/_Signal/01HROOT/site1/temp", "01HROOT", `{"id":"01HSIG"}`),
		kv("not-an-uns-topic", "01HROOT", `{"id":"x"}`),
		kv("colca/v1/_SystemElement/01HROOT/site1", "01HROOT", `{"not":"an element"}`),
	})
	if len(view.Elements) != 0 || len(view.Groups) != 0 {
		t.Fatalf("view should be empty, got %+v", view)
	}
}

func TestTreeReadsBothProjectionsFromOneCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Colca-Service"); got != "grantsync" {
			t.Errorf("service header was %q", got)
		}
		if got := r.Header.Get("X-Colca-Token"); got != "" {
			t.Errorf("local tree read carried token %q", got)
		}
		_, _ = w.Write([]byte(`{"entries":[
			{"topic":"colca/v1/_SystemElement/01HROOT/site1","node_id":"01HROOT","payload":{"id":"01HSITE1"}},
			{"topic":"colca/v1/_Group/01HROOT/ops","node_id":"01HROOT","payload":{"id":"ops","grants":["read:01HSITE1/#"]}}
		]}`))
	}))
	defer srv.Close()

	view, err := (&NodeClient{BaseURL: srv.URL, Service: "grantsync"}).Tree(context.Background())
	if err != nil {
		t.Fatalf("Tree: %v", err)
	}
	if view.Elements["01HSITE1"] != "site1" {
		t.Fatalf("elements: %v", view.Elements)
	}
	if view.Groups["ops"].Grants[0] != "read:01HSITE1/#" {
		t.Fatalf("groups: %+v", view.Groups)
	}
}

func TestTreeFailsLoudOnAnErrorStatusThatParsesAsAnEmptyTree(t *testing.T) {
	// The dangerous shape, and the reason the status is checked at all: an
	// error response whose body happens to be readable JSON with no entries.
	// Ignore the status and this is indistinguishable from a tree that holds
	// nothing — which is the one input that would make convergence revoke
	// every group in it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer srv.Close()

	if _, err := (&NodeClient{BaseURL: srv.URL, Service: "grantsync"}).Tree(context.Background()); err == nil {
		t.Fatal("a 503 with a parseable body returned no error — it would look like an empty tree")
	}
}

func TestTreeFailsLoudOnAnUnreadableBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{not json"))
	}))
	defer srv.Close()

	if _, err := (&NodeClient{BaseURL: srv.URL, Service: "grantsync"}).Tree(context.Background()); err == nil {
		t.Fatal("unparseable body returned no error")
	}
}

func TestPublishPostsTheEnvelopeAndSurfacesRefusals(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":"definition: unknown contract"}`))
	}))
	defer srv.Close()

	err := (&NodeClient{BaseURL: srv.URL, Service: "grantsync"}).Publish(
		context.Background(), "colca/v1/_CmdConfigure/01HROOT/definition/upsert",
		map[string]any{"correlation_id": "x"})
	if err == nil {
		t.Fatal("a node refusal was reported as success")
	}
	if got["topic"] != "colca/v1/_CmdConfigure/01HROOT/definition/upsert" {
		t.Fatalf("posted body was %v", got)
	}
}

func TestULIDIsReadFromHealthz(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ulid":"01HROOT","ok":true}`))
	}))
	defer srv.Close()

	got, err := (&NodeClient{BaseURL: srv.URL, Service: "grantsync"}).ULID(context.Background())
	if err != nil || got != "01HROOT" {
		t.Fatalf("ULID = %q, %v", got, err)
	}
}
