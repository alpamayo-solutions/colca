package grantsync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// authzVectorPath is the golden dataset for the Keycloak authorization objects
// a grant is made of (schema-bundle design §2, tier 2 -- the same mechanism
// plugins/uns uses for the topic grammar and the slot vocabulary). One
// checked-in file that all three native copies of this vocabulary answer to:
// this package (the reader), api/src/authentication/authz_client.py (the
// editor's runtime writer) and node-manager's auth_apply.py (`colca auth
// apply`).
const authzVectorPath = "../../contracts/src/colca_data_contracts/vectors/authz_objects.json"

type authzVectors struct {
	Scopes              []string `json:"scopes"`
	ElementResourceType string   `json:"element_resource_type"`
	ManagedByAttr       string   `json:"managed_by_attr"`
}

func loadAuthzVectors(t *testing.T) authzVectors {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(authzVectorPath))
	if err != nil {
		t.Fatalf("read authz vectors: %v", err)
	}
	var vectors authzVectors
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("decode authz vectors: %v", err)
	}
	if len(vectors.Scopes) == 0 {
		t.Fatal("authz vectors carry no scopes")
	}
	return vectors
}

// Vocabulary pin (architecture principle 2: one owner per fact). Neither Python
// side can import this package and it cannot import them, so a scope renamed on
// one side would otherwise show up only as a grant that silently stops
// compiling. With this pin, renaming it here fails until the vectors move, and
// moving the vectors fails the two Python suites until they move too.
func TestTheAuthzVocabularyMatchesTheGoldenVectors(t *testing.T) {
	vectors := loadAuthzVectors(t)

	if len(AuthzScopes) != len(vectors.Scopes) {
		t.Fatalf("AuthzScopes has %d entries, vectors have %d: %v vs %v",
			len(AuthzScopes), len(vectors.Scopes), AuthzScopes, vectors.Scopes)
	}
	for i, want := range vectors.Scopes {
		if AuthzScopes[i] != want {
			t.Errorf("scope %d: AuthzScopes has %q, vectors have %q", i, AuthzScopes[i], want)
		}
	}
	if ElementResourceType != vectors.ElementResourceType {
		t.Errorf("ElementResourceType is %q, vectors say %q",
			ElementResourceType, vectors.ElementResourceType)
	}
	if ManagedByAttr != vectors.ManagedByAttr {
		t.Errorf("ManagedByAttr is %q, vectors say %q", ManagedByAttr, vectors.ManagedByAttr)
	}
}

// The constants above are only worth pinning if the objects BUILT from them
// carry the same values -- a right constant behind a wrong payload is the drift
// this vector exists to catch.
func TestTheResourceThisPackageAuthorsCarriesTheGoldenVocabulary(t *testing.T) {
	vectors := loadAuthzVectors(t)

	plan := PlanResources(map[string]string{"01HM6": "site1/edge1/m6"}, nil, "n-root")
	if len(plan.Create) != 1 {
		t.Fatalf("planning one unknown element created %d resources, want 1", len(plan.Create))
	}
	desired := plan.Create[0]

	if desired.Type != vectors.ElementResourceType {
		t.Errorf("authored resource type is %q, vectors say %q",
			desired.Type, vectors.ElementResourceType)
	}
	if got := desired.Attributes[vectors.ManagedByAttr]; len(got) != 1 || got[0] != "n-root" {
		t.Errorf("authored resource carries %v under %q, want [n-root]",
			got, vectors.ManagedByAttr)
	}
}
