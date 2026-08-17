package tokenauth

import (
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// world builds a verifier against a fresh fake issuer with the JWKS already
// fetched (one explicit refresh — Run's first-tick behavior without the loop).
func world(t *testing.T) (*tokentest.Issuer, *Verifier) {
	t.Helper()
	iss := tokentest.NewIssuer(t)
	v, err := New(Config{Issuer: iss.Iss(), Audience: iss.Aud(), JWKSURL: iss.JWKSURL()}, openStore(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	v.refresh()
	return iss, v
}

func TestVerifyTruthTable(t *testing.T) {
	iss, v := world(t)
	future := time.Now().Add(5 * time.Minute)

	t.Run("valid token", func(t *testing.T) {
		tok := iss.MintOpt(tokentest.MintOpts{Sub: "anna", Grants: []string{"read:werk1/#"},
			Exp: future, Username: "anna@plant"})
		got, reason, err := v.Verify(tok)
		if err != nil || reason != "" {
			t.Fatalf("valid token rejected: %s %v", reason, err)
		}
		if got.Sub != "anna" || got.Username != "anna@plant" || got.Entry.Kind != uns.KindHuman {
			t.Fatalf("verified shape: %+v", got)
		}
		if !uns.Authorize(got.Entry, uns.ActReadRecord, "colca/v1/_Metric/x/werk1/temp") {
			t.Fatal("grant from the claim must authorize")
		}
		if got.Exp.Unix() != future.Unix() {
			t.Fatalf("exp: %v want %v", got.Exp, future)
		}
	})

	cases := []struct {
		name   string
		opts   tokentest.MintOpts
		reason string
	}{
		{"expired", tokentest.MintOpts{Sub: "s", Exp: time.Now().Add(-2 * time.Minute)}, ReasonExpired},
		{"nbf in future", tokentest.MintOpts{Sub: "s", Exp: future, Nbf: time.Now().Add(5 * time.Minute)}, ReasonExpired},
		{"wrong issuer", tokentest.MintOpts{Sub: "s", Exp: future, Iss: "https://evil.test"}, ReasonIssuer},
		{"wrong audience", tokentest.MintOpts{Sub: "s", Exp: future, Aud: "other-api"}, ReasonIssuer},
		{"alg none", tokentest.MintOpts{Sub: "s", Exp: future, Alg: "none"}, ReasonBadToken},
		{"alg HS256", tokentest.MintOpts{Sub: "s", Exp: future, Alg: "HS256"}, ReasonBadToken},
		{"wrong key signature", tokentest.MintOpts{Sub: "s", Exp: future, WrongKey: true}, ReasonBadToken},
		{"empty sub", tokentest.MintOpts{Sub: "", Exp: future}, ReasonBadToken},
		{"bad grant in claim", tokentest.MintOpts{Sub: "s", Exp: future, Grants: []string{"write:z/#"}}, ReasonBadToken},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, reason, err := v.Verify(iss.MintOpt(c.opts))
			if err == nil || got != nil {
				t.Fatalf("%s: token must be rejected, got %+v", c.name, got)
			}
			if reason != c.reason {
				t.Fatalf("%s: reason = %q, want %q (%v)", c.name, reason, c.reason, err)
			}
		})
	}

	t.Run("garbage", func(t *testing.T) {
		if got, reason, err := v.Verify("not.a.jwt"); err == nil || got != nil || reason != ReasonBadToken {
			t.Fatalf("garbage: %v %s %v", got, reason, err)
		}
	})

	t.Run("grants absent means zero grants", func(t *testing.T) {
		got, _, err := v.Verify(iss.MintOpt(tokentest.MintOpts{Sub: "bare", Exp: future}))
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Entry.Grants) != 0 || uns.Authorize(got.Entry, uns.ActSub, "colca/#") {
			t.Fatalf("bare token must carry no authority: %+v", got.Entry)
		}
	})
}

// Rotation heals via refresh-on-unknown-kid: exactly ONE re-fetch, then the
// new-key token validates (§2.2).
func TestUnknownKidTriggersOneRefetchAndValidates(t *testing.T) {
	iss, v := world(t)
	before := iss.Requests()

	iss.Rotate(t) // new key + kid; JWKS now serves ONLY the new key
	tok := iss.Mint("anna", nil, time.Now().Add(5*time.Minute))
	got, reason, err := v.Verify(tok)
	if err != nil || reason != "" || got.Sub != "anna" {
		t.Fatalf("rotation must heal via re-fetch: %s %v", reason, err)
	}
	if iss.Requests() != before+1 {
		t.Fatalf("expected exactly one re-fetch, server saw %d", iss.Requests()-before)
	}
}

// Unknown kid within the rate limit: rejected WITHOUT hitting the server.
func TestUnknownKidRespectsRateLimit(t *testing.T) {
	iss, v := world(t)
	// Arm the limiter with a genuine miss.
	if _, reason, _ := v.Verify(iss.MintOpt(tokentest.MintOpts{
		Sub: "s", Exp: time.Now().Add(time.Minute), Kid: "ghost"})); reason != ReasonBadToken {
		t.Fatalf("ghost kid: %s", reason)
	}
	before := iss.Requests()
	if _, reason, _ := v.Verify(iss.MintOpt(tokentest.MintOpts{
		Sub: "s", Exp: time.Now().Add(time.Minute), Kid: "ghost2"})); reason != ReasonBadToken {
		t.Fatalf("ghost2 kid: %s", reason)
	}
	if iss.Requests() != before {
		t.Fatal("second unknown kid within the rate limit must not hit the server")
	}
}

// Fail closed: unknown kid with the issuer unreachable rejects.
func TestUnknownKidOfflineFailsClosed(t *testing.T) {
	iss, v := world(t)
	iss.Rotate(t)
	iss.CloseServer()
	if got, reason, _ := v.Verify(iss.Mint("s", nil, time.Now().Add(time.Minute))); got != nil || reason != ReasonBadToken {
		t.Fatalf("offline rotation must fail closed: %v %s", got, reason)
	}
}

// The persisted JWKS survives a restart: a fresh Verifier on the same store
// validates with the ISSUER DOWN (§2.2 offline restart).
func TestJWKSPersistsAcrossRestartOffline(t *testing.T) {
	iss := tokentest.NewIssuer(t)
	st := openStore(t)
	v1, err := New(Config{Issuer: iss.Iss(), Audience: iss.Aud(), JWKSURL: iss.JWKSURL()}, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	v1.refresh() // fetch + persist
	tok := iss.Mint("anna", []string{"read:z/#"}, time.Now().Add(5*time.Minute))
	iss.CloseServer()

	v2, err := New(Config{Issuer: iss.Iss(), Audience: iss.Aud(), JWKSURL: iss.JWKSURL()}, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	// NO refresh — v2 must run on the persisted document alone.
	got, reason, err := v2.Verify(tok)
	if err != nil || got == nil || got.Sub != "anna" {
		t.Fatalf("persisted JWKS must validate offline: %s %v", reason, err)
	}
}

// A failed refresh keeps the cached keys serving.
func TestFailedRefreshKeepsCachedKeys(t *testing.T) {
	iss, v := world(t)
	tok := iss.Mint("anna", nil, time.Now().Add(5*time.Minute))
	iss.CloseServer()
	v.refresh() // fails; must not drop keys
	if _, reason, err := v.Verify(tok); err != nil {
		t.Fatalf("cached keys must keep serving after a failed refresh: %s %v", reason, err)
	}
}

// Root-frame grants are translated at verification using the node's prefix
// (cmdadmin design §3). Without a source the verifier behaves as root.
func TestVerifyTranslatesGrantsWithPrefixSource(t *testing.T) {
	iss, v := world(t)
	tok := iss.Mint("anna", []string{"read:site1/#", "cmd:site1/edge1/m1/#:param", "admin:#"}, time.Now().Add(5*time.Minute))

	// Default (no source): root behavior — grants verbatim.
	got, _, err := v.Verify(tok)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entry.Grants) != 3 || got.Entry.Grants[0] != "read:site1/#" {
		t.Fatalf("root grants = %v", got.Entry.Grants)
	}

	// Mid-tree node: translated to local frame.
	v.SetPrefixSource(func() (string, bool) { return "site1/edge1", true })
	got, _, err = v.Verify(tok)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"read:#", "cmd:m1/#:param", "admin:#"}
	if len(got.Entry.Grants) != len(want) {
		t.Fatalf("translated grants = %v, want %v", got.Entry.Grants, want)
	}
	for i := range want {
		if got.Entry.Grants[i] != want[i] {
			t.Fatalf("translated grants = %v, want %v", got.Entry.Grants, want)
		}
	}

	// Prefix never learned: scoped grants fail closed, admin survives.
	v.SetPrefixSource(func() (string, bool) { return "", false })
	got, _, err = v.Verify(tok)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entry.Grants) != 1 || got.Entry.Grants[0] != "admin:#" {
		t.Fatalf("fail-closed grants = %v, want [admin:#]", got.Entry.Grants)
	}
}
