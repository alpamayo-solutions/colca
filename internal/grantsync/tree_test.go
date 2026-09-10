package grantsync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/alpamayo-solutions/colca/door"
)

func kv(topic, node string, payload string) door.KVEntry {
	e := door.KVEntry{Topic: topic, NodeID: node}
	if payload != "" {
		e.Payload = json.RawMessage(payload)
	}
	return e
}

// pagedNode serves /kv the way colcad does — one page per request, `next`
// naming the page after it, empty on the last — and records every query it
// was asked. It records /publish too, so a Syncer can run against it.
type pagedNode struct {
	client  *NodeClient
	seen    *published
	mu      sync.Mutex
	queries []url.Values
}

// servePaged starts a node answering /kv from entries in pages of pageSize.
// A page number at or beyond failFrom (1-based; 0 never fails) answers 503 —
// the failure that arrives after a first page already looked healthy.
func servePaged(t *testing.T, entries []door.KVEntry, pageSize, failFrom int) *pagedNode {
	t.Helper()
	n := &pagedNode{seen: &published{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/kv"):
			n.mu.Lock()
			n.queries = append(n.queries, r.URL.Query())
			n.mu.Unlock()
			page := 1
			if after := r.URL.Query().Get("after"); after != "" {
				served, err := strconv.Atoi(strings.TrimPrefix(after, "page-"))
				if err != nil {
					t.Errorf("client sent a page token the server never issued: %q", after)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				page = served + 1
			}
			if failFrom > 0 && page >= failFrom {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			start := min((page-1)*pageSize, len(entries))
			end := min(start+pageSize, len(entries))
			next := ""
			if end < len(entries) {
				next = fmt.Sprintf("page-%d", page)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries[start:end], "next": next})
		case strings.HasSuffix(r.URL.Path, "/publish"):
			var row map[string]any
			_ = json.NewDecoder(r.Body).Decode(&row)
			n.seen.add(row)
		}
	}))
	t.Cleanup(srv.Close)
	n.client = &NodeClient{BaseURL: srv.URL, Service: "grantsync"}
	return n
}

func (n *pagedNode) asked() []url.Values {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]url.Values(nil), n.queries...)
}

func elementsAt(paths ...string) []door.KVEntry {
	entries := make([]door.KVEntry, 0, len(paths))
	for _, path := range paths {
		id := "01H" + strings.ToUpper(strings.ReplaceAll(path, "/", "-"))
		entries = append(entries, kv("colca/v1/_SystemElement/01HROOT/"+path, "01HROOT",
			fmt.Sprintf(`{"id":%q,"name":%q}`, id, path)))
	}
	return entries
}

func TestElementsMapIDToThePathTheRootHoldsThemAt(t *testing.T) {
	view := ParseKV([]door.KVEntry{
		kv("colca/v1/_SystemElement/01HROOT/site1/edge1/m6", "01HROOT", `{"id":"01HM6","name":"m6"}`),
	})
	if got := view.Elements["01HM6"]; got != "site1/edge1/m6" {
		t.Fatalf("element 01HM6 resolved to %q, want site1/edge1/m6", got)
	}
}

func TestATombstonedElementIsNotInTheView(t *testing.T) {
	// A retired element must not keep a resource somebody can grant on.
	view := ParseKV([]door.KVEntry{kv("colca/v1/_SystemElement/01HROOT/site1/m6", "01HROOT", "")})
	if len(view.Elements) != 0 {
		t.Fatalf("retired element still present: %v", view.Elements)
	}
}

func TestHeldGroupsCarryTheirAuthor(t *testing.T) {
	// The service retracts only what IT wrote; a definition authored elsewhere
	// is not the root's to withdraw.
	view := ParseKV([]door.KVEntry{
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
	// Tree asks the node for two contracts, but the parse does not lean on
	// that: a node answering more than it was asked must never widen the view.
	view := ParseKV([]door.KVEntry{
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

func TestTreeAsksForOnlyTheContractsItConsumes(t *testing.T) {
	// Tree asks only for the two contracts it reads, which keeps the read bounded by
	// the size of the tree.
	node := servePaged(t, elementsAt("site1"), 10, 0)
	if _, err := node.client.Tree(context.Background()); err != nil {
		t.Fatalf("Tree: %v", err)
	}
	asked := node.asked()
	if len(asked) != 1 {
		t.Fatalf("one page should take one request, took %d", len(asked))
	}
	want := []string{"_SystemElement", "_Group"}
	if got := asked[0]["contract"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("contract filter was %v, want %v (query %v)", got, want, asked[0])
	}
	if got := asked[0].Get("prefix"); got != "" {
		t.Fatalf("a prefix would hide part of the tree: %q", got)
	}
}

func TestTreeFollowsEveryPageOfTheListing(t *testing.T) {
	// Five elements in pages of two take three requests, and all five must be in the
	// view.
	node := servePaged(t, elementsAt("a/e1", "a/e2", "a/e3", "a/e4", "a/e5"), 2, 0)
	view, err := node.client.Tree(context.Background())
	if err != nil {
		t.Fatalf("Tree: %v", err)
	}
	for _, want := range []string{"01HA-E1", "01HA-E2", "01HA-E3", "01HA-E4", "01HA-E5"} {
		if _, ok := view.Elements[want]; !ok {
			t.Fatalf("element %s missing from the view %v — a page was dropped", want, view.Elements)
		}
	}
	asked := node.asked()
	if len(asked) != 3 {
		t.Fatalf("five entries in pages of two should take three requests, took %d: %v", len(asked), asked)
	}
	if got := asked[1].Get("after"); got != "page-1" {
		t.Fatalf("second request did not continue from the first page's token: %v", asked[1])
	}
}

func TestTreeFailsWhenALaterPageCannotBeRead(t *testing.T) {
	// A healthy first page followed by a 503 is an error, not a smaller tree.
	node := servePaged(t, elementsAt("a/e1", "a/e2", "a/e3", "a/e4"), 2, 2)
	view, err := node.client.Tree(context.Background())
	if err == nil {
		t.Fatalf("a listing that failed on page 2 returned a view of %d elements", len(view.Elements))
	}
	if len(view.Elements) != 0 {
		t.Fatalf("an error came with a partial view: %v", view.Elements)
	}
	if asked := node.asked(); len(asked) != 2 {
		t.Fatalf("expected the client to have tried page 2, it made %d requests", len(asked))
	}
}

func TestTreeFailsLoudOnAnErrorStatusThatParsesAsAnEmptyTree(t *testing.T) {
	// An error response with a readable, empty JSON body must not look like an empty
	// tree, which would revoke every group.
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
