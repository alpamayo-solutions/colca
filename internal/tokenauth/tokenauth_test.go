package tokenauth

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
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

// world builds a verifier against a fresh fake issuer, with the JWKS already
// fetched.
func world(t *testing.T) (*tokentest.Issuer, *Verifier) {
	t.Helper()
	iss := tokentest.NewIssuer(t)
	v, err := New(Config{Issuers: []Issuer{{ID: iss.Iss(), JWKSURL: iss.JWKSURL()}}, Audience: iss.Aud()}, openStore(t), nil)
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
		{"bad grant in claim", tokentest.MintOpts{Sub: "s", Exp: future, Grants: []string{"cmd:z/#"}}, ReasonBadToken},
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

// Key rotation heals through one refetch on the unknown kid.
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

// Within the rate limit, an unknown kid is rejected without calling the issuer.
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

// An identity provider that restarts with new keys: the refetch the first login
// triggers finds it still down, and a login after the rate limit heals.
func TestRotationWhileTheIssuerRestartsHealsOnALaterLogin(t *testing.T) {
	iss, v := world(t)
	v.refetchAfter = 50 * time.Millisecond

	iss.SetDown(true)
	iss.Rotate(t)
	tok := iss.Mint("anna", nil, time.Now().Add(5*time.Minute))
	if got, reason, _ := v.Verify(tok); got != nil || reason != ReasonBadToken {
		t.Fatalf("while the issuer is down: %v %s", got, reason)
	}

	iss.SetDown(false)
	time.Sleep(60 * time.Millisecond)
	if got, reason, err := v.Verify(tok); err != nil || got.Sub != "anna" {
		t.Fatalf("after the restart the new key must be fetched: %s %v", reason, err)
	}
}

// The identity provider starts after the node: every fetch fails, including the
// one a login triggers. The first login once it answers succeeds, because failed
// fetches do not count against the unknown-kid rate limit.
func TestFirstLoginAfterTheIssuerComesUpSucceeds(t *testing.T) {
	iss := tokentest.NewIssuer(t)
	v, err := New(Config{Issuers: []Issuer{{ID: iss.Iss(), JWKSURL: iss.JWKSURL()}}, Audience: iss.Aud()}, openStore(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	iss.SetDown(true)
	v.refresh() // startup refresh: the issuer is unreachable
	tok := iss.Mint("anna", nil, time.Now().Add(5*time.Minute))
	if got, reason, _ := v.Verify(tok); got != nil || reason != ReasonBadToken {
		t.Fatalf("while the issuer is down: %v %s", got, reason)
	}

	iss.SetDown(false)
	if got, reason, err := v.Verify(tok); err != nil || got.Sub != "anna" {
		t.Fatalf("first login after the issuer came up: %s %v", reason, err)
	}
}

// Concurrent logins with an unknown kid share one fetch.
func TestConcurrentUnknownKidsShareOneFetch(t *testing.T) {
	iss := tokentest.NewIssuer(t)
	v, err := New(Config{Issuers: []Issuer{{ID: iss.Iss(), JWKSURL: iss.JWKSURL()}}, Audience: iss.Aud()}, openStore(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	tok := iss.Mint("anna", nil, time.Now().Add(5*time.Minute))
	var wg sync.WaitGroup
	failed := make(chan string, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, reason, err := v.Verify(tok); err != nil {
				failed <- reason
			}
		}()
	}
	wg.Wait()
	close(failed)
	for reason := range failed {
		t.Fatalf("a concurrent login was refused: %s", reason)
	}
	if n := iss.Requests(); n > 2 {
		t.Fatalf("concurrent unknown kids must share fetches, server saw %d", n)
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

// The persisted JWKS survives a restart: a new verifier validates while the
// issuer is down.
func TestJWKSPersistsAcrossRestartOffline(t *testing.T) {
	iss := tokentest.NewIssuer(t)
	st := openStore(t)
	v1, err := New(Config{Issuers: []Issuer{{ID: iss.Iss(), JWKSURL: iss.JWKSURL()}}, Audience: iss.Aud()}, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	v1.refresh() // fetch + persist
	tok := iss.Mint("anna", []string{"read:01HZ/#"}, time.Now().Add(5*time.Minute))
	iss.CloseServer()

	v2, err := New(Config{Issuers: []Issuer{{ID: iss.Iss(), JWKSURL: iss.JWKSURL()}}, Audience: iss.Aud()}, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	// No refresh: v2 runs on the persisted document alone.
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

// Grants are stored exactly as authored; an element id means the same at every
// node.
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

func TestStandaloneRejectsAllTokensUntilReadyAndThenOldSessions(t *testing.T) {
	iss, v := world(t)
	v.cfg.NotBefore = time.Now().Unix()
	state := &store.StandaloneState{Since: v.cfg.NotBefore, PATs: map[string]bool{}}
	if err := v.st.StandalonePut(state); err != nil {
		t.Fatal(err)
	}
	fresh := iss.MintOpt(tokentest.MintOpts{Sub: "operator", Iat: time.Now().Add(time.Second)})
	if _, _, err := v.Verify(fresh); err == nil {
		t.Fatal("human authentication opened before identity retirement")
	}
	completed, err := v.st.CompleteStandalone()
	if err != nil {
		t.Fatal(err)
	}
	old := iss.MintOpt(tokentest.MintOpts{Sub: "oem", Iat: time.Unix(completed.Since-1, 0)})
	if _, _, err := v.Verify(old); err == nil {
		t.Fatal("old fleet session survived cutoff")
	}
	fresh = iss.MintOpt(tokentest.MintOpts{Sub: "operator", Iat: time.Unix(completed.Since, 0)})
	if _, _, err := v.Verify(fresh); err != nil {
		t.Fatal(err)
	}
}

// One identity provider reached under two host names signs with the same keys
// but writes the host the browser used into `iss`. Both are accepted when both
// are listed, and only the listed ones.
func TestSharedJWKSAcceptsEveryListedIssuer(t *testing.T) {
	idp := tokentest.NewIssuer(t)
	red, green := "https://red.plant/realms/unity", "https://green.plant/realms/unity"
	future := time.Now().Add(5 * time.Minute)
	viaGreen := idp.MintOpt(tokentest.MintOpts{Sub: "anna", Exp: future, Iss: green})

	redOnly, err := New(Config{Issuers: []Issuer{{ID: red, JWKSURL: idp.JWKSURL()}}, Audience: idp.Aud()}, openStore(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	redOnly.refresh()
	if got, reason, err := redOnly.Verify(viaGreen); got != nil || reason != ReasonIssuer {
		t.Fatalf("an unlisted issuer must be rejected with reason issuer: %v %q %v", got, reason, err)
	}

	both, err := New(Config{Issuers: []Issuer{
		{ID: red, JWKSURL: idp.JWKSURL()}, {ID: green, JWKSURL: idp.JWKSURL()},
	}, Audience: idp.Aud()}, openStore(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	before := idp.Requests()
	both.refresh()
	if idp.Requests() != before+1 {
		t.Fatalf("issuers sharing a JWKS URL must share one fetch, server saw %d", idp.Requests()-before)
	}
	for _, iss := range []string{red, green} {
		tok := idp.MintOpt(tokentest.MintOpts{Sub: "anna", Exp: future, Iss: iss})
		if got, reason, err := both.Verify(tok); err != nil || got.Sub != "anna" {
			t.Fatalf("listed issuer %s rejected: %q %v", iss, reason, err)
		}
	}
	if got, reason, _ := both.Verify(idp.MintOpt(tokentest.MintOpts{Sub: "s", Exp: future, Iss: "https://evil.test"})); got != nil || reason != ReasonIssuer {
		t.Fatalf("unlisted issuer: %v %q", got, reason)
	}
	if got, reason, _ := both.Verify(idp.MintOpt(tokentest.MintOpts{Sub: "s", Exp: future, Iss: green, Aud: "other"})); got != nil || reason != ReasonIssuer {
		t.Fatalf("audience is still checked for every issuer: %v %q", got, reason)
	}
}

// Issuers with their own JWKS: a token is checked against its issuer's keys
// only, so a token signed by A cannot pass by claiming to be from B.
func TestPerIssuerJWKSChecksTheClaimedIssuersKeys(t *testing.T) {
	a, b := tokentest.NewIssuer(t), tokentest.NewIssuer(t)
	issA, issB := "https://a.test/realms/x", "https://b.test/realms/x"
	st := openStore(t)
	cfg := Config{Issuers: []Issuer{{ID: issA, JWKSURL: a.JWKSURL()}, {ID: issB, JWKSURL: b.JWKSURL()}}, Audience: a.Aud()}
	v, err := New(cfg, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	v.refresh()
	future := time.Now().Add(5 * time.Minute)
	fromA := a.MintOpt(tokentest.MintOpts{Sub: "anna", Exp: future, Iss: issA})
	fromB := b.MintOpt(tokentest.MintOpts{Sub: "ben", Exp: future, Iss: issB})
	for tok, sub := range map[string]string{fromA: "anna", fromB: "ben"} {
		if got, reason, err := v.Verify(tok); err != nil || got.Sub != sub {
			t.Fatalf("%s: %q %v", sub, reason, err)
		}
	}
	forged := a.MintOpt(tokentest.MintOpts{Sub: "mallory", Exp: future, Iss: issB})
	if got, reason, _ := v.Verify(forged); got != nil || reason != ReasonBadToken {
		t.Fatalf("A-signed token claiming issuer B must fail the signature: %v %q", got, reason)
	}

	// Each JWKS is persisted on its own: a restarted verifier with both
	// issuers offline still validates tokens from both.
	a.CloseServer()
	b.CloseServer()
	v2, err := New(cfg, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tok := range []string{fromA, fromB} {
		if _, reason, err := v2.Verify(tok); err != nil {
			t.Fatalf("persisted per-issuer JWKS must validate offline: %q %v", reason, err)
		}
	}
}

// noGroups is a node that defines no _Group at all.
type noGroups struct{ uns.EntityStore }

func (noGroups) KVScanAll(string) []uns.KVRecord { return nil }

// Identity providers put their own roles into the groups claim. Each one the
// node does not define grants nothing and is logged once, not on every request.
func TestAnUnknownTokenGroupIsLoggedOncePerGroup(t *testing.T) {
	iss, v := world(t)
	var buf bytes.Buffer
	v.log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	v.SetGroupIndex(uns.NewGroupIndex(noGroups{}))

	tok := iss.MintOpt(tokentest.MintOpts{Sub: "anna", Exp: time.Now().Add(5 * time.Minute),
		Groups: []string{"offline_access", "uma_authorization"}})
	for range 3 {
		if _, reason, err := v.Verify(tok); err != nil {
			t.Fatalf("verify: %s %v", reason, err)
		}
	}
	out := buf.String()
	if strings.Contains(out, "level=WARN") {
		t.Fatalf("an unknown group must not warn:\n%s", out)
	}
	for _, group := range []string{"offline_access", "uma_authorization"} {
		if got := strings.Count(out, "group="+group); got != 1 {
			t.Fatalf("%s logged %d times, want once:\n%s", group, got, out)
		}
	}
}
