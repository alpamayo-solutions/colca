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

const PersonalAccessTokenContract = "_PersonalAccessToken"

// PersonalAccessToken is the hash-only credential definition that descends
// the node tree. It deliberately contains no plaintext secret.
type PersonalAccessToken struct {
	ID                        string   `json:"id"`
	HashedSecret              string   `json:"hashed_secret"`
	OwnerSub                  string   `json:"owner_sub"`
	OwnerEmail                string   `json:"owner_email"`
	Scopes                    []string `json:"scopes"`
	Roles                     []string `json:"roles"`
	Grants                    []string `json:"grants"`
	NamespaceReadPermissions  []string `json:"namespace_read_permissions"`
	NamespaceWritePermissions []string `json:"namespace_write_permissions"`
	ExpiresAt                 string   `json:"expires_at,omitempty"`
}

// PersonalAccessTokenIndex resolves a presented opaque token against the
// replicated definitions held locally by this node.
type PersonalAccessTokenIndex struct {
	store EntityStore
}

func NewPersonalAccessTokenIndex(store EntityStore) *PersonalAccessTokenIndex {
	return &PersonalAccessTokenIndex{store: store}
}

func personalAccessTokenID(token string) (string, bool) {
	parts := strings.SplitN(token, "_", 4)
	if len(parts) != 4 || parts[0] != "pk" || parts[1] != "pat" || parts[2] == "" || parts[3] == "" {
		return "", false
	}
	return parts[2], true
}

// Authenticate verifies the token digest, expiry and requested integration
// scope, then reconstructs its human entry. Duplicate definitions for one id
// fail closed instead of allowing one authoring node to shadow another.
func (p *PersonalAccessTokenIndex) Authenticate(token, requiredScope string, now time.Time) (*PersonalAccessToken, *Entry, error) {
	id, ok := personalAccessTokenID(token)
	if !ok {
		return nil, nil, fmt.Errorf("personal access token: malformed token")
	}
	var matches []PersonalAccessToken
	var authors []string
	for _, rec := range p.store.KVScanAll(PersonalAccessTokenContract) {
		if rec.Path != id {
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
			return nil, nil, fmt.Errorf("personal access token %s: this node holds no such token", id)
		}
		return nil, nil, fmt.Errorf("personal access token %s is claimed by %s", id, strings.Join(authors, " and "))
	}
	record := matches[0]
	want, err := hex.DecodeString(record.HashedSecret)
	if err != nil || len(want) != sha256.Size {
		return nil, nil, fmt.Errorf("personal access token %s: invalid stored digest", id)
	}
	got := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(got[:], want) != 1 {
		return nil, nil, fmt.Errorf("personal access token %s: secret mismatch", id)
	}
	if record.ExpiresAt != "" {
		expires, err := time.Parse(time.RFC3339, record.ExpiresAt)
		if err != nil {
			return nil, nil, fmt.Errorf("personal access token %s: invalid expiry", id)
		}
		if !now.Before(expires) {
			return nil, nil, fmt.Errorf("personal access token %s: expired", id)
		}
	}
	if requiredScope != "" && !patContainsString(record.Scopes, requiredScope) {
		return nil, nil, fmt.Errorf("personal access token %s: scope %s denied", id, requiredScope)
	}
	entry, err := TokenEntry(record.OwnerSub, record.Grants)
	if err != nil {
		return nil, nil, err
	}
	return &record, entry, nil
}

func patContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
