package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// POST /store/incarnation is the operator's way to say "this store's history
// regressed" when no peer can prove it: the store takes a new incarnation id,
// which is what makes children re-sync, and /healthz says when and why.
func TestAnOperatorGivesTheStoreANewIncarnation(t *testing.T) {
	h := newLocalHandler(t)
	admin := adminHandlerFor(t, h)
	before := h.eng.Store().StoreID()

	health := func() map[string]any {
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
		var out struct {
			Store map[string]any `json:"store"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("/healthz: %v: %s", err, rec.Body.String())
		}
		return out.Store
	}
	if store := health(); store["id"] != before || store["changed"] != nil {
		t.Fatalf("/healthz store = %v, want id %q and no change on a store that never changed", store, before)
	}

	// Without the admin token nothing changes.
	anon := httptest.NewRecorder()
	admin.ServeHTTP(anon, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/store/incarnation", nil))
	if anon.Code != http.StatusUnauthorized || h.eng.Store().StoreID() != before {
		t.Fatalf("without a token: status %d, store changed=%v — want 401 and the same store",
			anon.Code, h.eng.Store().StoreID() != before)
	}

	rec := doAdmin(t, admin, http.MethodPost, "/store/incarnation", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /store/incarnation = %d: %s", rec.Code, rec.Body.String())
	}
	var out struct{ Store, Previous string }
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	after := h.eng.Store().StoreID()
	if after == before || out.Store != after || out.Previous != before {
		t.Fatalf("answer %+v, store now %q, was %q — want the new id and the previous one", out, after, before)
	}
	store := health()
	changed, _ := store["changed"].(map[string]any)
	if store["id"] != after || changed["previous"] != before || changed["reason"] == "" || changed["at"] == "" {
		t.Fatalf("/healthz store after the change = %v, want the new id, the previous one, a reason and a time", store)
	}
	// The local door has no administration.
	if local := doAdmin(t, h, http.MethodPost, "/store/incarnation", nil); local.Code == http.StatusOK {
		t.Fatal("the local door changed the store's incarnation")
	}
}
