package tokenauth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

type patStore struct {
	records []uns.KVRecord
}

func (p *patStore) KVGet(string) ([]byte, bool)          { return nil, false }
func (p *patStore) KVScan(string, string) []uns.KVRecord { return nil }
func (p *patStore) KVScanAll(contract string) []uns.KVRecord {
	if contract == uns.PersonalAccessTokenContract {
		return p.records
	}
	return nil
}
func (p *patStore) PublishBatch(uns.CommandContext, []uns.StateRecord) ([]uns.StateWrite, error) {
	return nil, nil
}
func (p *patStore) PublishEvent(uns.CommandContext, uns.StateRecord) (uns.StateWrite, error) {
	return uns.StateWrite{}, nil
}
func (p *patStore) NodeID() string { return "n-edge" }

func TestVerifyForScopeAcceptsOnlyThePATsReplicatedScope(t *testing.T) {
	token := "pk_pat_01M0ZPAT000000000000000004_secret"
	digest := sha256.Sum256([]byte(token))
	payload, err := json.Marshal(uns.PersonalAccessToken{
		ID: "01M0ZPAT000000000000000004", HashedSecret: hex.EncodeToString(digest[:]),
		OwnerSub: "person-1", OwnerEmail: "person@example.com",
		Scopes: []string{"broker-http"}, Grants: []string{"read:01HLINE/#"},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &patStore{records: []uns.KVRecord{{
		Path: "01M0ZPAT000000000000000004", NodeID: "n-root", Payload: payload,
	}}}
	verifier := &Verifier{}
	verifier.SetPersonalAccessTokenIndex(uns.NewPersonalAccessTokenIndex(store))

	verified, reason, err := verifier.VerifyForScope(token, "broker-http")
	if err != nil || reason != "" {
		t.Fatalf("verify = %#v, %q, %v", verified, reason, err)
	}
	if verified.Credential != "pat" || verified.CredentialID != "01M0ZPAT000000000000000004" || verified.Sub != "person-1" {
		t.Fatalf("verified = %#v", verified)
	}
	if reason, err := verifier.VerifyPersonalAccessTokenSession(
		verified.CredentialID, verified.CredentialDigest, "broker-http", time.Now(),
	); err != nil || reason != "" {
		t.Fatalf("session recheck = %q, %v", reason, err)
	}
	if _, reason, err := verifier.VerifyForScope(token, "broker-mqtt"); err == nil || reason != ReasonScope {
		t.Fatalf("wrong-scope result = %q, %v", reason, err)
	}
	store.records = nil
	if reason, err := verifier.VerifyPersonalAccessTokenSession(
		verified.CredentialID, verified.CredentialDigest, "broker-http", time.Now(),
	); err == nil || reason != ReasonBadToken {
		t.Fatalf("revoked session recheck = %q, %v", reason, err)
	}
}
