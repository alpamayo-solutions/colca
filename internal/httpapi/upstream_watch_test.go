package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
)

type hintReader struct {
	t     *testing.T
	lines chan watchHint
}

// openWatch opens GET /watch on the local door and hands each line to the test.
func openWatch(t *testing.T, h *localAPI, query string) *hintReader {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/watch?"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Colca-Service", "projector")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("watch = %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	r := &hintReader{t: t, lines: make(chan watchHint, 64)}
	go func() {
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			var hint watchHint
			if json.Unmarshal(scanner.Bytes(), &hint) == nil {
				r.lines <- hint
			}
		}
		close(r.lines)
	}()
	return r
}

func (r *hintReader) next() watchHint {
	r.t.Helper()
	select {
	case hint, ok := <-r.lines:
		if !ok {
			r.t.Fatal("watch closed")
		}
		return hint
	case <-time.After(3 * time.Second):
		r.t.Fatal("no hint within 3s")
	}
	return watchHint{}
}

func (r *hintReader) none(within time.Duration) {
	r.t.Helper()
	select {
	case hint := <-r.lines:
		r.t.Fatalf("unexpected hint %+v", hint)
	case <-time.After(within):
	}
}

func appendEntity(t *testing.T, h *localAPI, stream, topic string) {
	t.Helper()
	if _, _, err := h.eng.Store().Append(stream, []store.Record{{Topic: topic, Payload: []byte(`{}`), TS: 1}}); err != nil {
		t.Fatal(err)
	}
}

// The first line names every selected stream; after that a line names only the
// streams that grew, with their next offsets, and another stream's appends say
// nothing.
func TestWatchNamesTheStreamsThatGrew(t *testing.T) {
	h := newLocalHandler(t)
	w := openWatch(t, h, "stream=annotations&stream=definitions&interval_ms=0")

	first := w.next()
	if len(first.Streams) != 2 {
		t.Fatalf("first line = %+v, want both streams", first)
	}

	appendEntity(t, h, "annotations", "colca/v1/_Annotation/n-test/a/1")
	hint := w.next()
	if len(hint.Streams) != 1 || hint.Streams[0] != "annotations" || hint.Next["annotations"] != first.Next["annotations"]+1 {
		t.Fatalf("hint = %+v after one annotation, first was %+v", hint, first)
	}

	appendEntity(t, h, "metrics", "colca/v1/_Metric/n-test/a/b")
	w.none(300 * time.Millisecond)
}

// Appends inside the interval are merged into one line that names each stream
// once.
func TestWatchMergesAppendsWithinTheInterval(t *testing.T) {
	h := newLocalHandler(t)
	w := openWatch(t, h, "stream=annotations&stream=definitions&interval_ms=400")
	w.next()

	appendEntity(t, h, "annotations", "colca/v1/_Annotation/n-test/a/1")
	if hint := w.next(); len(hint.Streams) != 1 {
		t.Fatalf("hint = %+v", hint)
	}
	for i := 0; i < 5; i++ {
		appendEntity(t, h, "annotations", "colca/v1/_Annotation/n-test/a/1")
		appendEntity(t, h, "definitions", "colca/v1/_AnnotationType/n-test/t")
	}
	merged := w.next()
	if len(merged.Streams) != 2 {
		t.Fatalf("merged = %+v, want both streams once", merged)
	}
	w.none(600 * time.Millisecond)
}

func TestWatchRefusesBadSelections(t *testing.T) {
	h := newLocalHandler(t)
	for _, query := range []string{"", "stream=nope", "stream=metrics&interval_ms=-1", "stream=metrics&interval_ms=60000"} {
		r := httptest.NewRequest(http.MethodGet, "/watch?"+query, nil)
		r.Header.Set("X-Colca-Service", "projector")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("/watch?%s = %d, want 400", query, rr.Code)
		}
	}
}

// ?contract= on /fetch returns only those contracts and moves next past the rest.
func TestFetchFiltersByContract(t *testing.T) {
	h := newLocalHandler(t)
	if _, _, err := h.eng.Store().Append("commands", []store.Record{
		{Topic: "colca/v1/_CmdOperate/n-test/line/a", Payload: []byte(`{}`), TS: 1},
		{Topic: "colca/v1/_CmdAcknowledge/n-test/alarms/a", Payload: []byte(`{}`), TS: 2},
		{Topic: "colca/v1/_CmdOperate/n-test/line/b", Payload: []byte(`{}`), TS: 3},
	}); err != nil {
		t.Fatal(err)
	}
	request := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("X-Colca-Service", "alarms")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		return rr
	}
	rr := request("/fetch?stream=commands&cursor=c/alarms/c&contract=_CmdAcknowledge")
	var page struct {
		Records []struct {
			Topic string `json:"topic"`
		} `json:"records"`
		Next int `json:"next"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &page); err != nil || rr.Code != http.StatusOK {
		t.Fatalf("fetch = %d %s", rr.Code, rr.Body.String())
	}
	if len(page.Records) != 1 || page.Records[0].Topic != "colca/v1/_CmdAcknowledge/n-test/alarms/a" || page.Next != 4 {
		t.Fatalf("page = %+v", page)
	}
	if bad := request("/fetch?stream=commands&cursor=c/alarms/c&contract=_NotAContract"); bad.Code != http.StatusBadRequest {
		t.Fatalf("unknown contract = %d, want 400", bad.Code)
	}
}

// ?depth= on /kv keeps the level asked for.
func TestKVDepth(t *testing.T) {
	h := newLocalHandler(t)
	if _, _, err := h.eng.Store().Append("entities", []store.Record{
		{Topic: "colca/v1/_SystemElement/n-test/plant/l1", Payload: []byte(`{"id":"l1","name":"l1"}`), TS: 1, KVPath: "plant/l1", KVNode: "n-test"},
		{Topic: "colca/v1/_SystemElement/n-test/plant/l1/m1", Payload: []byte(`{"id":"m1","name":"m1"}`), TS: 2, KVPath: "plant/l1/m1", KVNode: "n-test"},
	}); err != nil {
		t.Fatal(err)
	}
	request := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("X-Colca-Service", "explorer")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		return rr
	}
	rr := request("/kv?prefix=plant%2F&depth=1")
	var page struct {
		Entries []struct {
			Path string `json:"path"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &page); err != nil || rr.Code != http.StatusOK {
		t.Fatalf("kv = %d %s", rr.Code, rr.Body.String())
	}
	if len(page.Entries) != 1 || page.Entries[0].Path != "plant/l1" {
		t.Fatalf("depth 1 = %+v", page.Entries)
	}
	if bad := request("/kv?prefix=plant%2F&depth=0"); bad.Code != http.StatusBadRequest {
		t.Fatalf("depth 0 = %d, want 400", bad.Code)
	}
	if _, ok := decodeRaw(t, rr)["folders"]; ok {
		t.Fatal("folders sent without folders=true")
	}
	var level struct {
		Entries []struct {
			Path string `json:"path"`
		} `json:"entries"`
		Folders []string `json:"folders"`
	}
	withFolders := request("/kv?prefix=plant%2F&depth=1&folders=true")
	if err := json.Unmarshal(withFolders.Body.Bytes(), &level); err != nil || withFolders.Code != http.StatusOK {
		t.Fatalf("kv folders = %d %s", withFolders.Code, withFolders.Body.String())
	}
	if len(level.Entries) != 1 || level.Entries[0].Path != "plant/l1" || fmt.Sprint(level.Folders) != "[plant/l1]" {
		t.Fatalf("folders page = %+v", level)
	}
	if bad := request("/kv?prefix=plant%2F&folders=true"); bad.Code != http.StatusBadRequest {
		t.Fatalf("folders without depth = %d, want 400", bad.Code)
	}
	if bad := request("/kv?prefix=plant%2F&depth=1&folders=maybe"); bad.Code != http.StatusBadRequest {
		t.Fatalf("folders=maybe = %d, want 400", bad.Code)
	}
}

func decodeRaw(t *testing.T, rr *httptest.ResponseRecorder) map[string]json.RawMessage {
	t.Helper()
	var out map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}
