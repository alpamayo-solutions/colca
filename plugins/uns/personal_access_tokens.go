package uns

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// PersonalAccessTokenContract is the contract of personal access token definitions.
const PersonalAccessTokenContract = "_PersonalAccessToken"

// PersonalAccessToken is the hash-only credential definition that descends
// the node tree. It deliberately contains no plaintext secret.
type PersonalAccessToken struct {
	ID           string   `json:"id"`
	HashedSecret string   `json:"hashed_secret"`
	OwnerSub     string   `json:"owner_sub"`
	OwnerEmail   string   `json:"owner_email"`
	Scopes       []string `json:"scopes"`
	Roles        []string `json:"roles"`
	Grants       []string `json:"grants"`
	ExpiresAt    string   `json:"expires_at,omitempty"`
}

// PersonalAccessTokenIndex resolves a presented opaque token against the
// replicated definitions held locally by this node.
type PersonalAccessTokenIndex struct {
	store   EntityStore
	author  string
	retired map[string]bool
}

// NewPersonalAccessTokenIndex returns an index over the tokens in store.
func NewPersonalAccessTokenIndex(store EntityStore) *PersonalAccessTokenIndex {
	return &PersonalAccessTokenIndex{store: store}
}

// WithAuthority excludes foreign and pre-handover credentials permanently.
func (p *PersonalAccessTokenIndex) WithAuthority(node string, standalone bool, retired map[string]bool) *PersonalAccessTokenIndex {
	if standalone {
		p.author, p.retired = node, retired
	}
	return p
}

func personalAccessTokenID(token string) (string, bool) {
	parts := strings.SplitN(token, "_", 4)
	if len(parts) != 4 || parts[0] != "pk" || parts[1] != "pat" || parts[2] == "" || parts[3] == "" {
		return "", false
	}
	return parts[2], true
}

func (p *PersonalAccessTokenIndex) lookup(id string) (*PersonalAccessToken, error) {
	if p.retired[id] {
		return nil, fmt.Errorf("personal access token retired at handover")
	}
	var matches []PersonalAccessToken
	var authors []string
	for _, rec := range p.store.KVScanAll(PersonalAccessTokenContract) {
		if rec.Path != id || (p.author != "" && rec.NodeID != p.author) {
			continue
		}
		var candidate PersonalAccessToken
		if json.Unmarshal(rec.Payload, &candidate) != nil || candidate.ID != id {
			continue
		}
		matches = append(matches, candidate)
		authors = append(authors, rec.NodeID)
	}
	if len(matches) != 1 {
		sort.Strings(authors)
		if len(matches) == 0 {
			return nil, fmt.Errorf("personal access token %s: this node holds no such token", id)
		}
		return nil, fmt.Errorf("personal access token %s is claimed by %s", id, strings.Join(authors, " and "))
	}
	return &matches[0], nil
}

func personalAccessTokenDigest(record *PersonalAccessToken) ([]byte, error) {
	want, err := hex.DecodeString(record.HashedSecret)
	if err != nil || len(want) != sha256.Size {
		return nil, fmt.Errorf("personal access token %s: invalid stored digest", record.ID)
	}
	return want, nil
}

func authorizePersonalAccessToken(record *PersonalAccessToken, requiredScope string, now time.Time) error {
	if record.ExpiresAt != "" {
		expires, err := time.Parse(time.RFC3339, record.ExpiresAt)
		if err != nil {
			return fmt.Errorf("personal access token %s: invalid expiry", record.ID)
		}
		if !now.Before(expires) {
			return fmt.Errorf("personal access token %s: expired", record.ID)
		}
	}
	if requiredScope != "" && !patContainsString(record.Scopes, requiredScope) {
		return fmt.Errorf("personal access token %s: scope %s denied", record.ID, requiredScope)
	}
	return nil
}

// AuthorizeSession rechecks an authenticated PAT against this node's current
// definition, using only the lookup id. CONNECT already proved the secret; this
// ends the session on the next sweep after a tombstone, expiry, duplicate,
// corrupt digest or removed scope.
func (p *PersonalAccessTokenIndex) AuthorizeSession(
	id, authenticatedDigest, requiredScope string, now time.Time,
) error {
	record, err := p.lookup(id)
	if err != nil {
		return err
	}
	current, err := personalAccessTokenDigest(record)
	if err != nil {
		return err
	}
	authenticated, err := hex.DecodeString(authenticatedDigest)
	if err != nil || len(authenticated) != sha256.Size || subtle.ConstantTimeCompare(current, authenticated) != 1 {
		return fmt.Errorf("personal access token %s: credential definition changed", id)
	}
	return authorizePersonalAccessToken(record, requiredScope, now)
}

// Authenticate verifies the token digest, expiry and requested integration
// scope, then reconstructs its human entry. Duplicate definitions for one id
// fail closed instead of allowing one authoring node to shadow another.
func (p *PersonalAccessTokenIndex) Authenticate(token, requiredScope string, now time.Time) (*PersonalAccessToken, *Entry, error) {
	id, ok := personalAccessTokenID(token)
	if !ok {
		return nil, nil, fmt.Errorf("personal access token: malformed token")
	}
	record, err := p.lookup(id)
	if err != nil {
		return nil, nil, err
	}
	want, err := personalAccessTokenDigest(record)
	if err != nil {
		return nil, nil, err
	}
	got := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(got[:], want) != 1 {
		return nil, nil, fmt.Errorf("personal access token %s: secret mismatch", id)
	}
	if err := authorizePersonalAccessToken(record, requiredScope, now); err != nil {
		return nil, nil, err
	}
	entry, err := TokenEntry(record.OwnerSub, record.Grants)
	if err != nil {
		return nil, nil, err
	}
	return record, entry, nil
}

func patContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
