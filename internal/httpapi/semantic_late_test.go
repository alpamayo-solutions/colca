package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// A semantic tag that comes down from the parent after a service's tags were
// bound reaches the bound signals through the wired engine: the engine hands
// the definition to the configure executor, which writes through the engine
// again without deadlocking.
func TestADownlinkedSemanticTagFillsSignalsBoundBeforeIt(t *testing.T) {
	h := newLocalHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/kv", nil)
	req.Header.Set("X-Colca-Service", "dataops")
	req.Header.Set("X-Colca-Mount", "line1")
	h.ServeHTTP(httptest.NewRecorder(), req)
	entry, ok := h.reg.ByName("dataops")
	if !ok {
		t.Fatal("the request did not register the service")
	}

	catalogue, err := json.Marshal(map[string]any{"data_tags": []map[string]any{{
		"id": "01TAGHEALTH", "name": "bearing_health", "data_type": "float",
		"meta": map[string]any{"unit": "%", "semantic_type": "health_indicator"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	// The bundle declares _DataTags, not uns; the catalogue is stored as the
	// service's publish would store it.
	if _, _, err := h.eng.Store().Append("entities", []store.Record{{
		Topic: "colca/v1/_DataTags/n-test/line1/dataops", Payload: catalogue, TS: 1,
		KVPath: "line1/dataops", KVNode: "n-test",
	}}); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"correlation_id": "autobind-1", "expires_at": time.Now().Add(time.Minute).UnixMilli(),
		"connector": entry.ULID,
	})
	res, err := h.eng.IngestAdmin("colca/v1/_CmdConfigure/n-test/signal/autobind", payload)
	if err != nil {
		t.Fatalf("autobind: %v", err)
	}
	if res.Command == nil || res.Command.ResultCode != 200 {
		t.Fatalf("autobind = %+v", res.Command)
	}
	signal := func() map[string]any {
		t.Helper()
		raw, ok := h.eng.EntityStore().KVGet("colca/v1/_Signal/n-test/line1/bearing_health")
		if !ok {
			t.Fatal("autobind minted no signal at line1/bearing_health")
		}
		var record map[string]any
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		return record
	}
	if got, present := signal()["semantic_type_id"]; present {
		t.Fatalf("bound before the semantic tag existed, yet semantic_type_id = %v", got)
	}

	done := make(chan error, 1)
	go func() {
		_, err := h.eng.IngestDownlinkDefinition("colca/v1/_SemanticTag/n-parent/01STAGHEALTH",
			[]byte(`{"id":"01STAGHEALTH","name":"health_indicator"}`), time.Now().UnixMilli())
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("downlink definition: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the downlinked definition did not return: the observer deadlocked")
	}

	if got := signal()["semantic_type_id"]; got != "01STAGHEALTH" {
		t.Fatalf("semantic_type_id = %v, want the semantic tag that arrived after the binding", got)
	}
}
