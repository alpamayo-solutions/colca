package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// stateTopic is a key of a service's own state at its position: mount, then
// name, as its _ServiceDetails sits.
func stateTopic(position, key string) string {
	return "colca/v1/_ServiceState/n-test/" + position + "/" + key
}

func localGet(t *testing.T, h http.Handler, service, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Colca-Service", service)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// kvTopics lists what /kv shows the caller under prefix.
func kvTopics(t *testing.T, h http.Handler, service, query string) []string {
	t.Helper()
	rec := localGet(t, h, service, "/kv?"+query)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /kv?%s as %s = %d: %s", query, service, rec.Code, rec.Body.String())
	}
	var out struct {
		Entries []struct {
			Topic string `json:"topic"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	var topics []string
	for _, entry := range out.Entries {
		topics = append(topics, entry.Topic)
	}
	return topics
}

func publishState(t *testing.T, h *localAPI, service, topic string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	return localPublish(t, h, map[string]string{"X-Colca-Service": service}, map[string]any{"topic": topic, "payload": payload})
}

// A service writes its state at its own position and reads it back; it lands
// on its own stream, so no entities consumer is woken by a checkpoint.
func TestAServiceWritesAndReadsItsOwnState(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "notifications", "")
	topic := stateTopic("notifications", "evaluation/01ALARM")
	entitiesHead := h.eng.Store().NextOffset("entities")

	rec := publishState(t, h, "notifications", topic, map[string]any{"version": 1, "status": "firing"})
	if rec.Code != http.StatusOK {
		t.Fatalf("publish own state = %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["stream"] != "service_state" {
		t.Fatalf("stream = %v, want service_state", out["stream"])
	}
	if got := h.eng.Store().NextOffset("entities"); got != entitiesHead {
		t.Fatalf("the entities stream moved %d -> %d: a checkpoint must not wake its consumers", entitiesHead, got)
	}
	if got := kvTopics(t, h, "notifications", "contract=_ServiceState"); len(got) != 1 || got[0] != topic {
		t.Fatalf("the owner's /kv = %v, want %s", got, topic)
	}
	fetch := localGet(t, h, "notifications", "/fetch?stream=service_state&cursor=c/notifications/own")
	if fetch.Code != http.StatusOK || !strings.Contains(fetch.Body.String(), topic) {
		t.Fatalf("the owner's /fetch = %d: %s", fetch.Code, fetch.Body.String())
	}

	// A tombstone retires the key.
	if rec := localPublish(t, h, map[string]string{"X-Colca-Service": "notifications"}, map[string]any{"topic": topic}); rec.Code != http.StatusOK {
		t.Fatalf("tombstone = %d: %s", rec.Code, rec.Body.String())
	}
	if got := kvTopics(t, h, "notifications", "contract=_ServiceState"); len(got) != 0 {
		t.Fatalf("after the tombstone /kv = %v, want nothing", got)
	}
}

// A placed service's position is its mount, then its name.
func TestAPlacedServiceOwnsStateBelowItsMountAndName(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "connector", "line1/press3")
	own := stateTopic("line1/press3/connector", "checkpoint")
	if rec := publishState(t, h, "connector", own, map[string]any{"version": 2}); rec.Code != http.StatusOK {
		t.Fatalf("placed service, own position = %d: %s", rec.Code, rec.Body.String())
	}
	// Its write zone covers line1/press3/x, its state position does not.
	if rec := publishState(t, h, "connector", stateTopic("line1/press3/other", "checkpoint"), map[string]any{"version": 1}); rec.Code == http.StatusOK {
		t.Fatal("a service wrote state at another position inside its own write zone")
	}
}

// Nobody else writes it: not another service, whose write zone (an unplaced
// service writes the whole node) covers the path, and not at the bare position
// with no key.
func TestNoOtherServiceWritesAServicesState(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "notifications", "")
	registerLocal(t, h, "projector", "")
	for name, topic := range map[string]string{
		"another service":   stateTopic("notifications", "evaluation/01ALARM"),
		"the bare position": "colca/v1/_ServiceState/n-test/projector",
	} {
		if rec := publishState(t, h, "projector", topic, map[string]any{"version": 1}); rec.Code != http.StatusForbidden {
			t.Errorf("%s: publish = %d, want 403: %s", name, rec.Code, rec.Body.String())
		}
	}
}

// Nobody else reads it: another local service, whose read zone is the whole
// node and who reads every service's _ServiceDetails, sees no entry, and its
// fetch of the stream skips the record.
func TestNoOtherServiceReadsAServicesState(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "notifications", "")
	registerLocal(t, h, "projector", "")
	topic := stateTopic("notifications", "deliveries/job1")
	if rec := publishState(t, h, "notifications", topic, map[string]any{"version": 1, "address": "a@example.com"}); rec.Code != http.StatusOK {
		t.Fatalf("publish = %d: %s", rec.Code, rec.Body.String())
	}
	if got := kvTopics(t, h, "projector", ""); containsString(got, topic) {
		t.Fatalf("another service's /kv shows %s", topic)
	}
	if got := kvTopics(t, h, "projector", "contract=_ServiceState"); len(got) != 0 {
		t.Fatalf("another service's /kv?contract=_ServiceState = %v, want nothing", got)
	}
	fetch := localGet(t, h, "projector", "/fetch?stream=service_state&cursor=c/projector/peek")
	if fetch.Code != http.StatusOK || strings.Contains(fetch.Body.String(), "a@example.com") {
		t.Fatalf("another service's /fetch = %d: %s", fetch.Code, fetch.Body.String())
	}
	body, _ := json.Marshal(map[string]any{"topics": []string{topic}})
	lookup := doLocalAs(t, h, "projector", http.MethodPost, "/kv/lookup", body)
	if lookup.Code != http.StatusOK || strings.Contains(lookup.Body.String(), topic) {
		t.Fatalf("another service's /kv/lookup = %d: %s", lookup.Code, lookup.Body.String())
	}
}

func doLocalAs(t *testing.T, h http.Handler, service, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(string(body)))
	req.Header.Set("X-Colca-Service", service)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// colca reads version and nothing else, and the record cap bounds the rest.
func TestServiceStatePayloadNeedsAVersionAndFitsTheRecordCap(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "notifications", "")
	topic := stateTopic("notifications", "k")
	for name, payload := range map[string]any{
		"no version":       map[string]any{"status": "x"},
		"zero version":     map[string]any{"version": 0},
		"string version":   map[string]any{"version": "1"},
		"not an object":    []any{1},
		"fraction version": map[string]any{"version": 1.5},
	} {
		if rec := publishState(t, h, "notifications", topic, payload); rec.Code == http.StatusOK {
			t.Errorf("%s: accepted, want refused", name)
		}
	}
	h.eng.Store().SetMaxRecordBytes(1024)
	huge := map[string]any{"version": 1, "blob": strings.Repeat("x", 2048)}
	if rec := publishState(t, h, "notifications", topic, huge); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("a payload over the record cap = %d, want 413: %s", rec.Code, rec.Body.String())
	}
}

// Revoking a service takes its state with it, on the state's own stream.
func TestRevokingAServiceRetiresItsState(t *testing.T) {
	h := newLocalHandler(t)
	registerLocal(t, h, "notifications", "")
	registerLocal(t, h, "projector", "")
	entry, _ := h.reg.ByName("notifications")
	mine := stateTopic("notifications", "evaluation/01ALARM")
	theirs := stateTopic("projector", "checkpoint")
	if rec := publishState(t, h, "notifications", mine, map[string]any{"version": 1}); rec.Code != http.StatusOK {
		t.Fatalf("publish = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := publishState(t, h, "projector", theirs, map[string]any{"version": 1}); rec.Code != http.StatusOK {
		t.Fatalf("publish = %d: %s", rec.Code, rec.Body.String())
	}
	head := h.eng.Store().NextOffset("service_state")

	admin := adminHandlerFor(t, h)
	if rec := doAdmin(t, admin, http.MethodDelete, "/enroll/"+entry.ULID, nil); rec.Code != http.StatusOK {
		t.Fatalf("DELETE /enroll = %d: %s", rec.Code, rec.Body.String())
	}
	if holds(t, h, mine) {
		t.Fatal("the revoked service's state survived")
	}
	if !holds(t, h, theirs) {
		t.Fatal("another service's state went with the revoke")
	}
	if got := h.eng.Store().NextOffset("service_state"); got != head+1 {
		t.Fatalf("service_state head %d -> %d, want its tombstone there", head, got)
	}
}

// The admin door reads and writes any service's state, for repair.
func TestTheAdminDoorReadsAndWritesServiceState(t *testing.T) {
	h := newLocalHandler(t)
	topic := stateTopic("notifications", "k")
	if _, err := h.eng.IngestAdmin(topic, []byte(`{"version":1}`)); err != nil {
		t.Fatalf("admin write: %v", err)
	}
	if !holds(t, h, topic) {
		t.Fatal("the admin write is not in KV")
	}
}

// It never rises: no uplink stream carries it.
func TestServiceStateDoesNotReplicateUp(t *testing.T) {
	if uns.FlowsUp(uns.ClassServiceState) || uns.IsUplinkStream("service_state") {
		t.Fatal("service state would replicate up")
	}
	if uns.OnBusTopic(stateTopic("notifications", "k")) {
		t.Fatal("service state would be on the bus")
	}
}
