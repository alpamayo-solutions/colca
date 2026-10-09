package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// childWithReplicatedState enrolls a child node at site/edge1 and applies what it
// replicated: an element and a signal below its mount. It returns the admin door
// and the replicated topics.
func childWithReplicatedState(t *testing.T) (*localAPI, http.Handler, []string) {
	t.Helper()
	h := newLocalHandler(t)
	if _, err := h.eng.IngestAdmin("colca/v1/_SystemElement/n-test/site/edge1", []byte(`{"id":"edge1","name":"edge1"}`)); err != nil {
		t.Fatalf("author the mount: %v", err)
	}
	admin := adminHandlerFor(t, h)
	entry, _ := json.Marshal(map[string]any{
		"ulid": "01NCHILD", "pubkey": strings.Repeat("ab", 32), "kind": "node", "element": "edge1",
	})
	// A node is not enrolled through POST /enroll; its approval writes the
	// registry entry, as here.
	if rec := doAdmin(t, admin, http.MethodPost, "/enroll", entry); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("POST /enroll of a node = %d: %s, want 422", rec.Code, rec.Body.String())
	}
	if _, _, err := h.reg.Enroll(entry); err != nil {
		t.Fatal(err)
	}
	element := "colca/v1/_SystemElement/01NCHILD/site/edge1/press3"
	signal := "colca/v1/_Signal/01NCHILD/site/edge1/press3/machine_state"
	if _, _, err := h.eng.IngestReplicated("01NCHILD", "entities", []store.ReplRecord{
		{ChildOffset: 1, Topic: element, Payload: []byte(`{"id":"press3","name":"press3"}`), TS: 1,
			KVPath: "site/edge1/press3", KVNode: "01NCHILD"},
		{ChildOffset: 2, Topic: signal, Payload: []byte(`{"id":"sig-state"}`), TS: 1,
			KVPath: "site/edge1/press3/machine_state", KVNode: "01NCHILD"},
	}); err != nil {
		t.Fatalf("replicate: %v", err)
	}
	if _, ok := h.eng.Elements().PathOf("press3"); !ok {
		t.Fatal("the replicated element does not resolve before the retire")
	}
	return h, admin, []string{element, signal}
}

func holds(t *testing.T, h *localAPI, topic string) bool {
	t.Helper()
	entries, err := h.eng.Store().KVScan("")
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range entries {
		if kv.Topic == topic {
			return true
		}
	}
	return false
}

// DELETE /enroll/{ulid}?retire=true decommissions a child node: the identity
// goes, and with it everything the child replicated, so a node replaced at the
// same mount leaves no ghost machines or signals behind.
func TestRetireDeletesTheChildsReplicatedState(t *testing.T) {
	h, admin, topics := childWithReplicatedState(t)

	rec := doAdmin(t, admin, http.MethodDelete, "/enroll/01NCHILD?retire=true", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE ?retire=true = %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["revoked"] != true || out["retired"] != true || out["records_retired"] != 2.0 || out["offset"] == nil {
		t.Fatalf("response = %v, want revoked, retired, records_retired 2 and an offset", out)
	}
	for _, topic := range topics {
		if holds(t, h, topic) {
			t.Errorf("%s survived the retire", topic)
		}
	}
	if _, ok := h.eng.Elements().PathOf("press3"); ok {
		t.Error("the retired child's element still resolves")
	}
	if !holds(t, h, "colca/v1/_SystemElement/n-test/site/edge1") {
		t.Error("the retire took this node's own mount element")
	}
	if got := h.eng.Store().HWMGet("01NCHILD", "entities"); got != 0 {
		t.Errorf("HWM = %d after the retire, want 0", got)
	}
}

// Without retire=true the DELETE is the kill switch it always was: the child
// may be enrolled again and resume from its own cursor, so its state stays.
func TestRevokeWithoutRetireKeepsTheChildsReplicatedState(t *testing.T) {
	h, admin, topics := childWithReplicatedState(t)

	rec := doAdmin(t, admin, http.MethodDelete, "/enroll/01NCHILD", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE = %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if _, present := out["retired"]; present {
		t.Fatalf("response = %v, want no retired field on a plain revoke", out)
	}
	for _, topic := range topics {
		if !holds(t, h, topic) {
			t.Errorf("%s went with a plain revoke", topic)
		}
	}
	if got := h.eng.Store().HWMGet("01NCHILD", "entities"); got != 2 {
		t.Errorf("HWM = %d after a plain revoke, want 2 kept", got)
	}
}

// retire applies to child nodes and takes a boolean; anything else is refused
// before the identity is touched.
func TestRetireRefusesWhatItCannotDo(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "tcdb-api", "events")
	svc, ok := h.reg.ByName("tcdb-api")
	if !ok {
		t.Fatal("the local door did not register tcdb-api")
	}
	admin := adminHandlerFor(t, h)

	if rec := doAdmin(t, admin, http.MethodDelete, "/enroll/"+svc.ULID+"?retire=maybe", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("retire=maybe = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if rec := doAdmin(t, admin, http.MethodDelete, "/enroll/"+svc.ULID+"?retire=true", nil); rec.Code != http.StatusConflict {
		t.Fatalf("retire of a local service = %d, want 409: %s", rec.Code, rec.Body.String())
	}
	if _, ok := h.reg.ByName("tcdb-api"); !ok {
		t.Fatal("a refused retire revoked the service")
	}
	if rec := doAdmin(t, admin, http.MethodDelete, "/enroll/01NOPE?retire=true", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("retire of an unknown identity = %d, want 404", rec.Code)
	}
}
