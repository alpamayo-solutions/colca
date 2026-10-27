package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/repl"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The admin door lists the records the uplink set aside and clears them one by
// one; nobody else may.
func TestAdminListsAndClearsSetAsideUplinkRecords(t *testing.T) {
	nodeID, err := identity.Generate(filepath.Join(t.TempDir(), "n.key"))
	if err != nil {
		t.Fatal(err)
	}
	cl, err := repl.NewClient("https://parent.invalid:443", "deadparentpub", nodeID)
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-child", API: config.API{Token: "tok"},
		Parent: &config.Parent{URL: "https://parent.invalid:443", Pubkey: "deadparentpub"}}
	reg, err := registry.New(s, cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(s, config.Retention{}, nil)
	eng := engine.New(s, cfg, reg, nil, m, nil)
	reg.SetNamespace(eng.Elements())
	h := Handler(eng, cfg, reg, nil, m, testBlobs(t, cfg), "deadbeef", false, cl)

	if _, _, err := s.Append("metrics", []store.Record{{Topic: "colca/v1/_Metric/n-child/x", Payload: []byte(`{"v":1}`), TS: 1}}); err != nil {
		t.Fatal(err)
	}
	if moved, err := s.SetAsideUplink(uns.UplinkCursor("deadparentpub"), store.SetAsideRecord{
		Parent: "deadparentpub", Stream: "metrics", Offset: 1,
		Record: store.StoredRecord{Offset: 1, Topic: "colca/v1/_Metric/n-child/x", Payload: []byte(`{"v":1}`), TS: 1},
		Status: 413, Reason: "too large",
	}); err != nil || !moved {
		t.Fatalf("SetAsideUplink = %v, %v", moved, err)
	}

	do := func(method, path, token string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		r := httptest.NewRequest(method, path, nil)
		if token != "" {
			r.Header.Set("X-Colca-Token", token)
		}
		h.ServeHTTP(rr, r)
		return rr
	}
	if rr := do(http.MethodGet, "/uplink/set-aside", ""); rr.Code == http.StatusOK {
		t.Fatal("the list answered without the admin token")
	}
	rr := do(http.MethodGet, "/uplink/set-aside", "tok")
	var list struct {
		Records []store.SetAsideRecord `json:"records"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&list); err != nil || rr.Code != http.StatusOK {
		t.Fatalf("GET = %d (%v)", rr.Code, err)
	}
	if len(list.Records) != 1 || list.Records[0].Offset != 1 || list.Records[0].Status != 413 {
		t.Fatalf("records = %+v, want the one set aside", list.Records)
	}
	if rr := do(http.MethodDelete, "/uplink/set-aside/metrics/2", "tok"); rr.Code != http.StatusNotFound {
		t.Fatalf("DELETE of nothing = %d, want 404", rr.Code)
	}
	if rr := do(http.MethodDelete, "/uplink/set-aside/metrics/1", "tok"); rr.Code != http.StatusOK {
		t.Fatalf("DELETE = %d: %s", rr.Code, rr.Body.String())
	}
	if rest, _ := cl.SetAsideRecords(s); len(rest) != 0 {
		t.Fatalf("still set aside after DELETE: %+v", rest)
	}
}
