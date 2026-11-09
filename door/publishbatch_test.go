package door

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPublishBatchSendsTombstonesWithoutAPayload(t *testing.T) {
	var got struct {
		Records []map[string]json.RawMessage `json:"records"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/publish/batch" || r.Header.Get("X-Colca-Service") != "svc" {
			t.Errorf("request %s service %q", r.URL.Path, r.Header.Get("X-Colca-Service"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte(`{"accepted":1,"results":[{"stream":"service_state","offset":7},{"error":"no","reason":"write_denied"}]}`))
	}))
	defer server.Close()
	client := &Client{BaseURL: server.URL, Service: "svc"}
	results, err := client.PublishBatch(context.Background(), []BatchRecord{
		{Topic: "colca/v1/_ServiceState/n/svc/a", Payload: json.RawMessage(`{"version":1}`)},
		{Topic: "colca/v1/_ServiceState/n/svc/b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !results[0].Stored() || results[0].Offset != 7 || results[1].Stored() || results[1].Reason != "write_denied" {
		t.Fatalf("results = %+v", results)
	}
	if _, has := got.Records[1]["payload"]; has {
		t.Fatalf("the tombstone carried a payload: %s", got.Records[1]["payload"])
	}
	if string(got.Records[0]["payload"]) != `{"version":1}` {
		t.Fatalf("payload = %s", got.Records[0]["payload"])
	}
}

func TestPublishBatchFailsOnAShortAnswer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"accepted":0,"results":[]}`))
	}))
	defer server.Close()
	client := &Client{BaseURL: server.URL, Service: "svc"}
	if _, err := client.PublishBatch(context.Background(), []BatchRecord{{Topic: "t"}}); err == nil {
		t.Fatal("a missing result was not an error")
	}
}
