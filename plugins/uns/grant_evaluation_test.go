package uns

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// grantEvaluationVectorPath is the shared human-authorization dataset (see
// the vector file's own "description" field for the full rationale): ONE
// checked-in file that every implementation of these rules is judged by. Same
// shared-file mechanism annotation_id.json and authz_objects.json already
// use — never a colca-local copy, which would just be the drift the vector
// exists to prevent.
const grantEvaluationVectorPath = "../../contracts/src/colca_data_contracts/vectors/grant_evaluation.json"

type grantEvaluationQuestion struct {
	Type    string `json:"type"`
	Element string `json:"element,omitempty"`
	Class   string `json:"class,omitempty"`
}

type grantEvaluationCase struct {
	Name            string                  `json:"name"`
	PrincipalGroups []string                `json:"principal_groups"`
	Question        grantEvaluationQuestion `json:"question"`
	Allowed         bool                    `json:"allowed"`
	Disputed        string                  `json:"disputed,omitempty"`
}

type grantEvaluationVectorFile struct {
	Elements map[string]*string    `json:"elements"`
	Groups   map[string][][]string `json:"groups"`
	Cases    []grantEvaluationCase `json:"cases"`
}

func loadGrantEvaluationVectors(t *testing.T) grantEvaluationVectorFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(grantEvaluationVectorPath))
	if err != nil {
		t.Fatalf("the shared grant_evaluation vectors are unreadable (%v). It is what keeps "+
			"colca's Authorize/AuthorizeCmdAt equal to the api's grants.py/namespace_filter.py; "+
			"without it this test proves nothing.", err)
	}
	var v grantEvaluationVectorFile
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("golden grant_evaluation vectors unparseable: %v", err)
	}
	if len(v.Cases) == 0 {
		t.Fatal("golden grant_evaluation vectors carry no cases")
	}
	return v
}

// grantEvaluationScope resolves the vectors' plain id->parent tree into the
// Scope Authorize/AuthorizeCmdAt need: every element's local path is the
// chain of ids from its topmost ancestor down to itself, joined by "/". The
// exact string is this test's own affair — Authorize only ever compares two
// paths built the same way — so it need not match a path convention built
// from element NAMES; implementations are pinned on subtree membership, not
// on string equality of an internal path.
type grantEvaluationScope struct {
	paths map[string]string
}

func (s grantEvaluationScope) PathOf(id string) (string, bool) { p, ok := s.paths[id]; return p, ok }
func (grantEvaluationScope) Reaches(string) bool               { return false }

func buildGrantEvaluationScope(t *testing.T, elements map[string]*string) grantEvaluationScope {
	t.Helper()
	paths := map[string]string{}
	var resolve func(id string, seen map[string]bool) string
	resolve = func(id string, seen map[string]bool) string {
		if p, ok := paths[id]; ok {
			return p
		}
		if seen[id] {
			t.Fatalf("grant_evaluation vectors: cycle in elements at %q", id)
		}
		seen[id] = true
		parent, ok := elements[id]
		if !ok {
			t.Fatalf("grant_evaluation vectors: element %q has no entry", id)
		}
		var p string
		if parent == nil {
			p = id
		} else {
			p = resolve(*parent, seen) + "/" + id
		}
		paths[id] = p
		return p
	}
	for id := range elements {
		resolve(id, map[string]bool{})
	}
	return grantEvaluationScope{paths: paths}
}

// seedGrantEvaluationGroups writes the vectors' group definitions into a fake
// store, one synthetic authoring node per definition — so a group with more
// than one definition is a genuine collision (two nodes claiming the same
// id), exactly as GroupIndex.GrantsOf requires to resolve it to nothing.
func seedGrantEvaluationGroups(f *fakeStore, groups map[string][][]string) {
	for groupID, definitions := range groups {
		for i, grants := range definitions {
			author := fmt.Sprintf("n-author-%d", i)
			f.records["colca/v1/_Group/"+author+"/"+groupID] = mustJSON(map[string]any{
				"id": groupID, "name": groupID, "grants": grants,
			})
		}
	}
}

// TestGrantEvaluationMatchesTheGoldenVectors pins colca's Authorize /
// AuthorizeCmdAt against the same human-authorization cases the api's
// grants.py + namespace_filter.py answer to. A vector marked "disputed" names
// a case where this test's author found the two real implementations
// disagree; it is skipped here (never bent to pass) and must be skipped on
// the Python side too, so the disagreement stays visible instead of being
// silently resolved by whichever suite runs last.
func TestGrantEvaluationMatchesTheGoldenVectors(t *testing.T) {
	vectors := loadGrantEvaluationVectors(t)
	scope := buildGrantEvaluationScope(t, vectors.Elements)
	store := newStore("n-node")
	seedGrantEvaluationGroups(store, vectors.Groups)
	idx := NewGroupIndex(store)

	for _, c := range vectors.Cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			if c.Disputed != "" {
				t.Skipf("disputed vector, not pinned here: %s", c.Disputed)
			}
			entry, _, err := TokenEntryWithGroups("u-test", nil, c.PrincipalGroups, idx)
			if err != nil {
				t.Fatalf("TokenEntryWithGroups: %v", err)
			}

			var got bool
			switch c.Question.Type {
			case "read":
				path, ok := scope.PathOf(c.Question.Element)
				if !ok {
					t.Fatalf("vector element %q has no path", c.Question.Element)
				}
				topic := "colca/v1/_Metric/n-node/" + path
				got = Authorize(scope, entry, ActReadRecord, topic)
			case "read_all":
				got = Authorize(scope, entry, ActSub, "colca/#")
			case "cmd":
				path, ok := scope.PathOf(c.Question.Element)
				if !ok {
					t.Fatalf("vector element %q has no path", c.Question.Element)
				}
				got = AuthorizeCmdAt(scope, entry, c.Question.Class, path)
			default:
				t.Fatalf("vector question type %q is not one of read|read_all|cmd", c.Question.Type)
			}

			if got != c.Allowed {
				t.Errorf("case %q: got allowed=%v, vectors want %v (groups=%v, question=%+v)",
					c.Name, got, c.Allowed, c.PrincipalGroups, c.Question)
			}
		})
	}
}
