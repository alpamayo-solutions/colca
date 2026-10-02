package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
)

func postLogout(t *testing.T, hc *http.Client, base string, form url.Values) (*http.Response, map[string]any) {
	t.Helper()
	resp, err := hc.Post(base+"/auth/backchannel-logout", "application/x-www-form-urlencoded",
		strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

// Keycloak's back-channel logout on the main door: a valid logout token is
// answered 200 and the session's tokens stop working at the door at once.
func TestBackchannelLogoutEndsTheSessionAtTheDoor(t *testing.T) {
	a := newAPI(t)
	hc := client(nil)
	token := a.iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-a", Grants: []string{"read:#"}})
	if resp, _ := bearerReq(t, hc, http.MethodGet, a.url+"/kv", token, nil); resp.StatusCode == http.StatusUnauthorized {
		t.Fatal("token refused before the logout")
	}

	resp, _ := postLogout(t, hc, a.url, url.Values{"logout_token": {a.iss.Logout("sid-a", "anna")}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid logout token = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if resp, _ := bearerReq(t, hc, http.MethodGet, a.url+"/kv", token, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("token of the logged-out session = %d, want 401", resp.StatusCode)
	}
}

func TestBackchannelLogoutRefusesABadRequest(t *testing.T) {
	a := newAPI(t)
	hc := client(nil)
	for name, form := range map[string]url.Values{
		"no token":        {},
		"an access token": {"logout_token": {a.mint("anna", nil)}},
		"garbage":         {"logout_token": {"x.y.z"}},
	} {
		t.Run(name, func(t *testing.T) {
			resp, out := postLogout(t, hc, a.url, form)
			if resp.StatusCode != http.StatusBadRequest || out["error"] != "invalid_request" {
				t.Fatalf("= %d %v, want 400 invalid_request", resp.StatusCode, out)
			}
		})
	}
}

// Keycloak calls the node over the deployment network, so the local door serves
// the endpoint as well.
func TestBackchannelLogoutOnTheLocalDoor(t *testing.T) {
	h, iss := newLocalHandlerWithVerifier(t)
	form := url.Values{"logout_token": {iss.Logout("sid-a", "")}}
	req := httptest.NewRequest(http.MethodPost, "/auth/backchannel-logout", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("local door = %d: %s", rec.Code, rec.Body.String())
	}
}
