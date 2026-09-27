package httpapi

import (
	"encoding/json"
	"github.com/alpamayo-solutions/colca/internal/store"
	"net/http/httptest"
	"testing"
)

func TestBacklogReadsDurableCursorWithoutAcknowledging(t *testing.T) {
	h := newLocalHandler(t)
	s := h.eng.Store()
	if _, _, err := s.Append("entities", []store.Record{{Topic: "a", Payload: []byte(`1`)}, {Topic: "b", Payload: []byte(`2`)}, {Topic: "c", Payload: []byte(`3`)}}); err != nil {
		t.Fatal(err)
	}
	s.CursorAck("c/projector/cache", "entities", 2)
	s.CursorAck("c/other/cache", "entities", 2)
	req := httptest.NewRequest("GET", "/backlog?prefix=c/projector/", nil)
	req.Header.Set("X-Colca-Service", "test-backlog")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
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
		r := httptest.NewRequest("GET", url, nil)
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
	r := httptest.NewRequest("GET", "/backlog?prefix=c/projector/&prefix=c/projector/cache", nil)
	r.Header.Set("X-Colca-Service", "test-backlog")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var result struct {
		Queues []map[string]any `json:"queues"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.Queues) != 1 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}
