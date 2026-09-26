package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// /kv and /fetch are counted per caller, with the contract, prefix depth and
// stream as bounded labels, and a /kv page's size lands in the histogram.
func TestReadsAreCountedPerCaller(t *testing.T) {
	h := newLocalHandler(t)
	if _, _, err := h.eng.Store().Append("entities", []store.Record{
		{Topic: "colca/v1/_SystemElement/n-test/plant/l1", Payload: []byte(`{"id":"l1","name":"l1"}`), TS: 1, KVPath: "plant/l1", KVNode: "n-test"},
		{Topic: "colca/v1/_SystemElement/n-test/plant/l2", Payload: []byte(`{"id":"l2","name":"l2"}`), TS: 1, KVPath: "plant/l2", KVNode: "n-test"},
	}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/kv?prefix=plant%2F&contract=_SystemElement",
		"/kv?prefix=plant%2Fl1%2Fm1%2Fh1%2Fx%2Fy",
		"/kv?contract=_SystemElement&contract=_Signal",
		"/fetch?stream=entities&cursor=c/projector/x",
	} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("X-Colca-Service", "projector")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s = %d %s", path, rr.Code, rr.Body.String())
		}
	}
	for line, want := range map[string]float64{
		`colca_http_kv_requests_total{caller="local:projector",contract="_SystemElement",prefix_depth="1"}`: 1,
		`colca_http_kv_requests_total{caller="local:projector",contract="all",prefix_depth="5+"}`:           1,
		`colca_http_kv_requests_total{caller="local:projector",contract="multiple",prefix_depth="0"}`:       1,
		`colca_http_fetch_requests_total{caller="local:projector",stream="entities"}`:                       1,
		`colca_http_kv_entries_bucket{caller="local:projector",le="1"}`:                                     1,
		`colca_http_kv_entries_count{caller="local:projector"}`:                                             3,
	} {
		if got := metricstest.Value(t, h.m, line); got != want {
			t.Errorf("%s = %v, want %v", line, got, want)
		}
	}
}

func TestLabelsStayBounded(t *testing.T) {
	for prefix, want := range map[string]string{"": "0", "/": "0", "plant": "1", "plant/": "1", "a/b/c/d": "4", "a/b/c/d/e": "5+"} {
		if got := prefixDepthLabel(prefix); got != want {
			t.Errorf("prefixDepthLabel(%q) = %q, want %q", prefix, got, want)
		}
	}
	if contractLabel(nil) != "all" || contractLabel([]string{"_Signal"}) != "_Signal" || contractLabel([]string{"a", "b"}) != "multiple" {
		t.Error("contract labels")
	}
	if callerLabel(caller{}) != "admin" {
		t.Error("admin caller label")
	}
}

// A 429 from the per-caller limits is counted by route pattern and caller.
func TestLimitedRequestsAreCountedPerCaller(t *testing.T) {
	h := newLocalHandler(t)
	limited := 0
	for i := 0; i < 20; i++ {
		r := httptest.NewRequest(http.MethodGet, "/kv?prefix=x", nil)
		r.Header.Set("X-Colca-Service", "busy")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		if rr.Code == http.StatusTooManyRequests {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("the scan limit never answered 429")
	}
	line := `colca_http_request_limited_by_caller_total{caller="local:busy",route="GET /kv"}`
	if got := metricstest.Value(t, h.m, line); got != float64(limited) {
		t.Fatalf("%s = %v, want %d", line, got, limited)
	}
}
