package httpapi

import (
	"encoding/json"
	"github.com/alpamayo-solutions/colca/internal/store"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBacklogReadsDurableCursorWithoutAcknowledging(t *testing.T) {
	h := newLocalHandler(t)
	s := h.eng.Store()
	if _, _, err := s.Append("entities", []store.Record{{Topic: "a", Payload: []byte(`1`)}, {Topic: "b", Payload: []byte(`2`)}, {Topic: "c", Payload: []byte(`3`)}}); err != nil {
		t.Fatal(err)
	}
	s.CursorAck("c/projector/cache", "entities", 2)
	s.CursorAck("c/other/cache", "entities", 2)
	req := httptest.NewRequest(http.MethodGet, "/backlog?prefix=c/projector/", nil)
	req.Header.Set("X-Colca-Service", "test-backlog")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("%d: %s", w.Code, w.Body.String())
	}
	var response struct {
		Queues []struct {
			Cursor   string `json:"cursor"`
			Position uint64 `json:"position"`
			Head     uint64 `json:"head"`
			Lag      uint64 `json:"lag_records"`
		} `json:"queues"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Queues) != 1 {
		t.Fatalf("%+v", response)
	}
	q := response.Queues[0]
	if q.Cursor != "c/projector/cache" || q.Position != 2 || q.Head != s.NextOffset("entities") || q.Lag != q.Head-q.Position {
		t.Fatalf("%+v", q)
	}
	if s.CursorGet("c/projector/cache", "entities") != 2 {
		t.Fatal("telemetry acknowledged work")
	}
}

func TestBacklogRequiresNonemptyPrefix(t *testing.T) {
	for _, url := range []string{"/backlog", "/backlog?prefix="} {
		h := newLocalHandler(t)
		r := httptest.NewRequest(http.MethodGet, url, nil)
		r.Header.Set("X-Colca-Service", "test-backlog")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("%s: %d", url, w.Code)
		}
	}
}

func TestBacklogOverlappingPrefixesDoNotDuplicateCursors(t *testing.T) {
	h := newLocalHandler(t)
	h.eng.Store().CursorAck("c/projector/cache", "entities", 2)
	r := httptest.NewRequest(http.MethodGet, "/backlog?prefix=c/projector/&prefix=c/projector/cache", nil)
	r.Header.Set("X-Colca-Service", "test-backlog")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var result struct {
		Queues []map[string]any `json:"queues"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.Queues) != 1 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}

func TestBacklogReportsLastAckAndStaleness(t *testing.T) {
	h := newLocalHandler(t)
	s := h.eng.Store()
	if _, _, err := s.Append("commands", []store.Record{{Topic: "a", Payload: []byte(`1`)}, {Topic: "b", Payload: []byte(`2`)}}); err != nil {
		t.Fatal(err)
	}
	before := time.Now().UnixMilli()
	s.CursorAck("c/projector/commands", "commands", 2)
	req := httptest.NewRequest(http.MethodGet, "/backlog?prefix=c/projector/", nil)
	req.Header.Set("X-Colca-Service", "test-backlog")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	var response struct {
		Queues []struct {
			LastAckMS      int64 `json:"last_ack_ms"`
			Stale          *bool `json:"stale"`
			ReadSinceStart *bool `json:"read_since_start"`
		} `json:"queues"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &response) != nil || len(response.Queues) != 1 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	q := response.Queues[0]
	if q.LastAckMS < before || q.Stale == nil || *q.Stale || q.ReadSinceStart == nil || *q.ReadSinceStart {
		t.Fatalf("a just-acked, never-fetched cursor reads %s", w.Body.String())
	}
}

// The admin lists every cursor without a prefix, finds the one a removed
// consumer left, and retires it with POST /ack {"delete":true}.
func TestTheAdminListsEveryCursorAndRetiresOne(t *testing.T) {
	a := newAPI(t)
	if _, _, err := a.st.Append("commands", []store.Record{{Topic: "a", Payload: []byte(`1`)}, {Topic: "b", Payload: []byte(`2`)}}); err != nil {
		t.Fatal(err)
	}
	a.st.CursorAck("c/hygentile-app/reconcile", "commands", 2)
	a.st.CursorAck("01OTHER/ingest", "commands", 3)
	hc := client(nil)

	resp, body := req(t, hc, http.MethodGet, a.url+"/backlog", "tok", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /backlog as admin = %d %v", resp.StatusCode, body)
	}
	queues, _ := body["queues"].([]any)
	names := map[string]bool{}
	for _, q := range queues {
		names[q.(map[string]any)["cursor"].(string)] = true
	}
	if !names["c/hygentile-app/reconcile"] || !names["01OTHER/ingest"] {
		t.Fatalf("admin inventory misses cursors: %v", body)
	}

	resp, _ = req(t, hc, http.MethodGet, a.url+"/backlog", "", nil)
	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET /backlog without the admin token = %d, want refused", resp.StatusCode)
	}

	resp, body = req(t, hc, http.MethodPost, a.url+"/ack", "tok",
		map[string]any{"cursor": "c/hygentile-app/reconcile", "stream": "commands", "delete": true})
	if resp.StatusCode != http.StatusOK || body["deleted"] != true {
		t.Fatalf("admin retire = %d %v", resp.StatusCode, body)
	}
	if _, ok := a.st.CursorLookup("c/hygentile-app/reconcile", "commands"); ok {
		t.Fatal("the retired cursor still exists")
	}
	if _, ok := a.st.CursorLookup("01OTHER/ingest", "commands"); !ok {
		t.Fatal("retiring one cursor removed another")
	}
}
