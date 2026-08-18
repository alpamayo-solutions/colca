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

// atWerk1 is a node holding one element, 01HWERK1, at path "werk1" — enough
// scope for a verified grant to resolve to something.
type atWerk1 struct{}

func (atWerk1) PathOf(id string) (string, bool) { return "werk1", id == "01HWERK1" }
func (atWerk1) Reaches(string) bool             { return false }

func TestVerifyTruthTable(t *testing.T) {
	iss, v := world(t)
	future := time.Now().Add(5 * time.Minute)

	t.Run("valid token", func(t *testing.T) {
		tok := iss.MintOpt(tokentest.MintOpts{Sub: "anna", Grants: []string{"read:01HWERK1/#"},
			Exp: future, Username: "anna@plant"})
		got, reason, err := v.Verify(tok)
		if err != nil || reason != "" {
			t.Fatalf("valid token rejected: %s %v", reason, err)
		}
		if got.Sub != "anna" || got.Username != "anna@plant" || got.Entry.Kind != uns.KindHuman {
			t.Fatalf("verified shape: %+v", got)
		}
		if !uns.Authorize(atWerk1{}, got.Entry, uns.ActReadRecord, "colca/v1/_Metric/x/werk1/temp") {
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
		if len(got.Entry.Grants) != 0 || uns.Authorize(nil, got.Entry, uns.ActSub, "colca/#") {
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
	tok := iss.Mint("anna", []string{"read:01HZ/#"}, time.Now().Add(5*time.Minute))
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

// A verified token's grants arrive exactly as authored and are stored exactly
// as authored: a grant names a system element, and an element id means the same
// thing at every node (id-grants design §4). What used to be prefix arithmetic
// here is a lookup at decision time now, so there is nothing left to translate
// and nothing about the verifier that depends on where the node sits.
func TestVerifyKeepsGrantsVerbatim(t *testing.T) {
	iss, v := world(t)
	authored := []string{"read:01HSITE1/#", "cmd:01HM1/#:param", "admin:#"}
	tok := iss.Mint("anna", authored, time.Now().Add(5*time.Minute))

	got, _, err := v.Verify(tok)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entry.Grants) != len(authored) {
		t.Fatalf("grants = %v, want %v", got.Entry.Grants, authored)
	}
	for i := range authored {
		if got.Entry.Grants[i] != authored[i] {
			t.Fatalf("grants = %v, want %v", got.Entry.Grants, authored)
		}
	}
}

// A path-shaped grant is refused at verification, not silently dropped: it is
// an authoring mistake, and the token carrying it should fail loudly.
func TestVerifyRejectsAPathShapedGrant(t *testing.T) {
	iss, v := world(t)
	tok := iss.Mint("anna", []string{"read:site1/edge1/#"}, time.Now().Add(5*time.Minute))

	if _, reason, err := v.Verify(tok); err == nil || reason != ReasonBadToken {
		t.Fatalf("Verify = (%q, %v), want a bad-token rejection", reason, err)
	}
}
