package uns

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func putPersonalAccessToken(f *fakeStore, token string, scopes []string, expiresAt string) string {
	id, ok := personalAccessTokenID(token)
	if !ok {
		panic("bad test token")
	}
	digest := sha256.Sum256([]byte(token))
	topic := "colca/v1/" + PersonalAccessTokenContract + "/n-root/" + id
	f.records[topic] = mustJSON(PersonalAccessToken{
		ID: id, HashedSecret: hex.EncodeToString(digest[:]), OwnerSub: "person-1",
		OwnerEmail: "person@example.com", Scopes: scopes,
		Grants: []string{"read:01HLINE/#"}, ExpiresAt: expiresAt,
	})
	return topic
}

func TestPersonalAccessTokenAuthenticatesFromLocalDefinition(t *testing.T) {
	f := newStore("n-edge")
	token := "pk_pat_01M0ZPAT000000000000000001_secret"
	putPersonalAccessToken(f, token, []string{"broker-http"}, "")
	idx := NewPersonalAccessTokenIndex(f)

	record, entry, err := idx.Authenticate(token, "broker-http", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if record.OwnerSub != "person-1" || entry.ULID != "person-1" || entry.Kind != KindHuman {
		t.Fatalf("record/entry = %#v / %#v", record, entry)
	}
	if len(entry.Grants) != 1 || entry.Grants[0] != "read:01HLINE/#" {
		t.Fatalf("grants = %v", entry.Grants)
	}
}

func TestPersonalAccessTokenScopeExpirySecretAndTombstoneFailClosed(t *testing.T) {
	f := newStore("n-edge")
	token := "pk_pat_01M0ZPAT000000000000000002_secret"
	topic := putPersonalAccessToken(f, token, []string{"broker-mqtt"}, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	idx := NewPersonalAccessTokenIndex(f)
	digest := sha256.Sum256([]byte(token))
	authenticatedDigest := hex.EncodeToString(digest[:])
	if err := idx.AuthorizeSession("01M0ZPAT000000000000000002", authenticatedDigest, "broker-mqtt", time.Now()); err != nil {
		t.Fatalf("live session recheck: %v", err)
	}
	if err := idx.AuthorizeSession("01M0ZPAT000000000000000002", authenticatedDigest, "broker-http", time.Now()); err == nil || !strings.Contains(err.Error(), "scope") {
		t.Fatalf("live session wrong-scope error = %v", err)
	}
	replacementDigest := sha256.Sum256([]byte("replacement credential"))
	f.records[topic] = mustJSON(PersonalAccessToken{
		ID: "01M0ZPAT000000000000000002", HashedSecret: hex.EncodeToString(replacementDigest[:]),
		OwnerSub: "person-1", Scopes: []string{"broker-mqtt"},
	})
	if err := idx.AuthorizeSession("01M0ZPAT000000000000000002", authenticatedDigest, "broker-mqtt", time.Now()); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("replaced credential session error = %v", err)
	}
	putPersonalAccessToken(f, token, []string{"broker-mqtt"}, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))

	if _, _, err := idx.Authenticate(token, "broker-http", time.Now()); err == nil || !strings.Contains(err.Error(), "scope") {
		t.Fatalf("wrong scope error = %v", err)
	}
	if _, _, err := idx.Authenticate(token+"wrong", "broker-mqtt", time.Now()); err == nil {
		t.Fatal("wrong secret authenticated")
	}
	if _, _, err := idx.Authenticate(token, "broker-mqtt", time.Now().Add(2*time.Hour)); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expiry error = %v", err)
	}
	f.records[topic] = nil
	if _, _, err := idx.Authenticate(token, "broker-mqtt", time.Now()); err == nil {
		t.Fatal("tombstoned token authenticated")
	}
	if err := idx.AuthorizeSession("01M0ZPAT000000000000000002", authenticatedDigest, "broker-mqtt", time.Now()); err == nil {
		t.Fatal("tombstoned token retained an established session")
	}
}

func TestPersonalAccessTokenDefinitionValidation(t *testing.T) {
	digest := strings.Repeat("a", 64)
	good := mustJSON(PersonalAccessToken{
		ID: "01M0ZPAT000000000000000003", HashedSecret: digest,
		OwnerSub: "person-1", Scopes: []string{"api", "mcp"},
	})
	if ClassOf(PersonalAccessTokenContract) != ClassDefinition {
		t.Fatal("personal access token must ride the definitions stream")
	}
	if err := checkDefinitionContents(PersonalAccessTokenContract, good); err != nil {
		t.Fatal(err)
	}
	bad := mustJSON(PersonalAccessToken{
		ID: "01M0ZPAT000000000000000003", HashedSecret: digest,
		OwnerSub: "person-1", Scopes: []string{"all"},
	})
	if err := checkDefinitionContents(PersonalAccessTokenContract, bad); err == nil {
		t.Fatal("unknown integration scope accepted")
	}
}

func TestStandalonePATsRequireLocalAuthorAndFreshIdentity(t *testing.T) {
	f := newStore("n-machine")
	token := "pk_pat_01M0ZPAT000000000000000099_secret"
	topic := putPersonalAccessToken(f, token, []string{"broker-http"}, "")
	idx := NewPersonalAccessTokenIndex(f).WithAuthority("n-machine", true, map[string]bool{})
	if _, _, err := idx.Authenticate(token, "broker-http", time.Now()); err == nil {
		t.Fatal("foreign PAT survived handover")
	}
	local := strings.Replace(topic, "/n-root/", "/n-machine/", 1)
	f.records[local] = f.records[topic]
	delete(f.records, topic)
	if _, _, err := idx.Authenticate(token, "broker-http", time.Now()); err != nil {
		t.Fatal(err)
	}
	idx.WithAuthority("n-machine", true, map[string]bool{"01M0ZPAT000000000000000099": true})
	if _, _, err := idx.Authenticate(token, "broker-http", time.Now()); err == nil {
		t.Fatal("retired PAT survived under local author")
	}
}

func TestAnMCPTokenIsAnAPITokenAndNotTheReverse(t *testing.T) {
	f := newStore("n-edge")
	mcp := "pk_pat_01M0ZPAT000000000000000010_secret"
	api := "pk_pat_01M0ZPAT000000000000000011_secret"
	putPersonalAccessToken(f, mcp, []string{"mcp"}, "")
	putPersonalAccessToken(f, api, []string{"api"}, "")
	idx := NewPersonalAccessTokenIndex(f)

	if _, _, err := idx.Authenticate(mcp, ScopeAPI, time.Now()); err != nil {
		t.Fatalf("mcp token at an api door: %v", err)
	}
	if _, _, err := idx.Authenticate(api, ScopeMCP, time.Now()); err == nil || !strings.Contains(err.Error(), "scope") {
		t.Fatalf("api token at an mcp door: err = %v", err)
	}
	if _, _, err := idx.Authenticate(mcp, ScopeI3X, time.Now()); err == nil {
		t.Fatal("mcp token at an i3x door")
	}
}

func TestAnAttestedTokenResolvesFromThisNodesDefinitionOnly(t *testing.T) {
	f := newStore("n-edge")
	token := "pk_pat_01M0ZPAT000000000000000012_secret"
	topic := putPersonalAccessToken(f, token, []string{"api"}, "")
	idx := NewPersonalAccessTokenIndex(f)
	_, signedIn, err := idx.Authenticate(token, ScopeAPI, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(signedIn.Credential, "01M0ZPAT000000000000000012:") {
		t.Fatalf("credential = %q", signedIn.Credential)
	}

	entry, err := idx.Attested(signedIn.Credential, "person-1", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if entry.ULID != "person-1" || len(entry.Grants) != 1 || entry.Grants[0] != "read:01HLINE/#" {
		t.Fatalf("entry = %#v", entry)
	}
	// Another person's command naming this token gets nobody.
	if _, err := idx.Attested(signedIn.Credential, "person-2", time.Now()); err == nil {
		t.Fatal("token attested for another person")
	}
	// A credential that names the token with another digest gets nobody.
	if _, err := idx.Attested("01M0ZPAT000000000000000012:"+strings.Repeat("0", 64), "person-1", time.Now()); err == nil {
		t.Fatal("wrong digest attested")
	}
	for _, malformed := range []string{"", "01M0ZPAT000000000000000012", ":abc", "id:"} {
		if _, err := idx.Attested(malformed, "person-1", time.Now()); err == nil {
			t.Fatalf("malformed credential %q attested", malformed)
		}
	}
	// Revoked here: the definition is gone, so is the person.
	f.records[topic] = nil
	if _, err := idx.Attested(signedIn.Credential, "person-1", time.Now()); err == nil {
		t.Fatal("revoked token attested")
	}
}
