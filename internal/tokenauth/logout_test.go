package tokenauth

import (
	"errors"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
)

func logoutEvents() map[string]any {
	return map[string]any{BackchannelLogoutEvent: map[string]any{}}
}

func TestBackchannelLogoutOfASessionRefusesItsTokensOnly(t *testing.T) {
	iss, v := world(t)
	var heard []Logout
	v.OnLogout(func(l Logout) { heard = append(heard, l) })
	ended := iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-a"})
	other := iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-b"})

	got, err := v.BackchannelLogout(iss.Logout("sid-a", "anna"))
	if err != nil {
		t.Fatalf("valid logout token refused: %v", err)
	}
	if got.SID != "sid-a" || got.Sub != "anna" {
		t.Fatalf("logout = %+v", got)
	}
	if len(heard) != 1 || heard[0].SID != "sid-a" {
		t.Fatalf("listeners heard %+v", heard)
	}
	if _, reason, err := v.Verify(ended); err == nil || reason != ReasonLoggedOut {
		t.Fatalf("token of the ended session: reason %q err %v, want %q", reason, err, ReasonLoggedOut)
	}
	if verified, _, err := v.Verify(other); err != nil || verified.SessionID != "sid-b" {
		t.Fatalf("token of another session of the same person: %+v %v", verified, err)
	}
}

func TestBackchannelLogoutOfAPersonEndsWhatWasIssuedBeforeIt(t *testing.T) {
	iss, v := world(t)
	before := iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-a", Iat: time.Now().Add(-time.Minute)})
	if _, err := v.BackchannelLogout(iss.Logout("", "anna")); err != nil {
		t.Fatalf("sub-only logout token refused: %v", err)
	}
	if _, reason, _ := v.Verify(before); reason != ReasonLoggedOut {
		t.Fatalf("token issued before the logout: reason %q, want %q", reason, ReasonLoggedOut)
	}
	after := iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-new", Iat: time.Now().Add(time.Second)})
	if _, _, err := v.Verify(after); err != nil {
		t.Fatalf("a login after the logout must work: %v", err)
	}
	if _, _, err := v.Verify(iss.MintOpt(tokentest.MintOpts{Sub: "bert"})); err != nil {
		t.Fatalf("another person is not logged out: %v", err)
	}
}

func TestBackchannelLogoutRefusesInvalidTokens(t *testing.T) {
	iss, v := world(t)
	now := time.Now()
	valid := func() map[string]any {
		return map[string]any{
			"sub": "anna", "sid": "sid-a", "iat": now.Unix(), "exp": now.Add(time.Minute).Unix(),
			"jti": "j-" + time.Now().Format(time.RFC3339Nano), "events": logoutEvents(),
		}
	}
	with := func(change func(map[string]any)) string {
		c := valid()
		change(c)
		return iss.Claims(c)
	}
	cases := map[string]string{
		"an access token":       iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-a"}),
		"a nonce":               with(func(c map[string]any) { c["nonce"] = "n" }),
		"no events":             with(func(c map[string]any) { delete(c, "events") }),
		"another event":         with(func(c map[string]any) { c["events"] = map[string]any{"urn:other": map[string]any{}} }),
		"neither sid nor sub":   with(func(c map[string]any) { delete(c, "sid"); delete(c, "sub") }),
		"no jti":                with(func(c map[string]any) { delete(c, "jti") }),
		"no iat":                with(func(c map[string]any) { delete(c, "iat") }),
		"expired":               with(func(c map[string]any) { c["exp"] = now.Add(-5 * time.Minute).Unix() }),
		"old without exp":       with(func(c map[string]any) { delete(c, "exp"); c["iat"] = now.Add(-time.Hour).Unix() }),
		"another audience":      with(func(c map[string]any) { c["aud"] = "someone-else" }),
		"an unaccepted issuer":  with(func(c map[string]any) { c["iss"] = "https://evil.test/realms/x" }),
		"a signature by nobody": tokentest.NewIssuer(t).Claims(valid()),
		"garbage":               "not.a.jwt",
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := v.BackchannelLogout(token); !errors.Is(err, ErrLogoutToken) {
				t.Fatalf("err = %v, want ErrLogoutToken", err)
			}
		})
	}
	// Nothing above ended anything.
	if _, _, err := v.Verify(iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-a"})); err != nil {
		t.Fatalf("a refused logout token ended the session: %v", err)
	}
}

func TestBackchannelLogoutRefusesAReplayedToken(t *testing.T) {
	iss, v := world(t)
	token := iss.Logout("sid-a", "anna")
	if _, err := v.BackchannelLogout(token); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if _, err := v.BackchannelLogout(token); !errors.Is(err, ErrLogoutToken) {
		t.Fatalf("replay: err = %v, want ErrLogoutToken", err)
	}
}
