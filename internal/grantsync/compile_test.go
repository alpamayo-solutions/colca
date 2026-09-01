package grantsync

import (
	"reflect"
	"testing"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

func TestCompile(t *testing.T) {
	for _, tc := range []struct {
		name  string
		perms []Permission
		attrs map[string][]string
		want  map[string][]string
	}{
		{
			name:  "read scope becomes a read grant over the element subtree",
			perms: []Permission{{Groups: []string{"ops"}, Elements: []string{"01HM6"}, Scopes: []string{"read"}}},
			want:  map[string][]string{"ops": {"read:01HM6/#"}},
		},
		{
			name: "command scopes collapse into one cmd grant, classes sorted",
			perms: []Permission{{Groups: []string{"ops"}, Elements: []string{"01HM6"},
				Scopes: []string{"operate", "param"}}},
			want: map[string][]string{"ops": {"cmd:01HM6/#:operate,param"}},
		},
		{
			name: "read and commands together produce both grants",
			perms: []Permission{{Groups: []string{"ops"}, Elements: []string{"01HM6"},
				Scopes: []string{"read", "operate"}}},
			want: map[string][]string{"ops": {"cmd:01HM6/#:operate", "read:01HM6/#"}},
		},
		{
			name: "one permission over several groups and elements fans out",
			perms: []Permission{{Groups: []string{"ops", "leads"}, Elements: []string{"01HA", "01HB"},
				Scopes: []string{"read"}}},
			want: map[string][]string{
				"leads": {"read:01HA/#", "read:01HB/#"},
				"ops":   {"read:01HA/#", "read:01HB/#"},
			},
		},
		{
			name: "two permissions on one group merge",
			perms: []Permission{
				{Groups: []string{"ops"}, Elements: []string{"01HA"}, Scopes: []string{"read"}},
				{Groups: []string{"ops"}, Elements: []string{"01HB"}, Scopes: []string{"maintain"}},
			},
			want: map[string][]string{"ops": {"cmd:01HB/#:maintain", "read:01HA/#"}},
		},
		{
			name:  "the attribute hatch is merged verbatim and deduplicated",
			perms: []Permission{{Groups: []string{"admins"}, Elements: []string{"01HA"}, Scopes: []string{"read"}}},
			attrs: map[string][]string{"admins": {"admin:#", "read:01HA/#"}},
			want:  map[string][]string{"admins": {"admin:#", "read:01HA/#"}},
		},
		{
			name:  "a group with an empty permission set does not appear at all",
			perms: []Permission{{Groups: []string{"ops"}, Elements: []string{"01HA"}, Scopes: nil}},
			want:  map[string][]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, problems := CompileGrants(tc.perms, tc.attrs)
			if len(problems) != 0 {
				t.Fatalf("unexpected problems: %v", problems)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("compiled %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTheSameInputAlwaysCompilesToTheSameOrder(t *testing.T) {
	// The result is diffed against the tree every cycle. If ordering wobbled,
	// every definition would be rewritten forever.
	perm := Permission{Groups: []string{"ops"}, Elements: []string{"01HB", "01HA"},
		Scopes: []string{"operate", "read", "param"}}
	first, _ := CompileGrants([]Permission{perm}, nil)
	for i := 0; i < 20; i++ {
		again, _ := CompileGrants([]Permission{perm}, nil)
		if !reflect.DeepEqual(first, again) {
			t.Fatalf("run %d differed: %v vs %v", i, again, first)
		}
	}
}

func TestAnUnparseableAttributeGrantIsDroppedAndReported(t *testing.T) {
	// A hand-typed grant is where a typo comes from, and one bad string must
	// not cost the group its good ones.
	got, problems := CompileGrants(nil, map[string][]string{"ops": {"read:01HA/#", "nonsense"}})
	if len(got["ops"]) != 1 || got["ops"][0] != "read:01HA/#" {
		t.Fatalf("good grants lost: %v", got)
	}
	if len(problems) != 1 {
		t.Fatalf("problems: %v", problems)
	}
}

func TestAnUnknownScopeIsReportedNotGuessed(t *testing.T) {
	got, problems := CompileGrants([]Permission{{
		Name: "ops@01HA", Groups: []string{"ops"}, Elements: []string{"01HA"},
		Scopes: []string{"read", "sudo"},
	}}, nil)
	if len(problems) != 1 {
		t.Fatalf("problems: %v", problems)
	}
	if got["ops"][0] != "read:01HA/#" {
		t.Fatalf("the understood scope should still grant: %v", got)
	}
}

func TestEveryCompiledGrantSurvivesTheNodesOwnParser(t *testing.T) {
	// The claim this whole package rests on: what it emits, uns accepts. If the
	// two ever diverge the service publishes definitions every node rejects —
	// and being in colca's module is what lets this be a test rather than a hope.
	perms := []Permission{{
		Groups:   []string{"ops"},
		Elements: []string{"01HM6", "01HSITE1"},
		Scopes:   AuthzScopes[:],
	}}
	got, problems := CompileGrants(perms, map[string][]string{"ops": {"admin:#", "read:#"}})
	if len(problems) != 0 {
		t.Fatalf("problems: %v", problems)
	}
	if len(got["ops"]) == 0 {
		t.Fatal("nothing compiled")
	}
	for _, grant := range got["ops"] {
		if _, err := uns.ParseGrant(grant); err != nil {
			t.Fatalf("compiled %q which uns rejects: %v", grant, err)
		}
	}
}

func TestAGrantThatWouldNotParseIsNeverReturned(t *testing.T) {
	// An element id carrying a separator would split the grant into a different
	// one than was authored. It must not reach the tree.
	got, problems := CompileGrants([]Permission{{
		Name: "ops@bad", Groups: []string{"ops"}, Elements: []string{"site1/edge1"},
		Scopes: []string{"read"},
	}}, nil)
	if len(got) != 0 {
		t.Fatalf("a path-shaped element compiled to %v", got)
	}
	if len(problems) != 1 {
		t.Fatalf("problems: %v", problems)
	}
}

// A resource name that is a wildcard rather than an identity WIDENS instead of
// failing: FormatGrant reads "#" (and "") as the whole namespace, so a
// permission on such a resource renders as "read:#" — the entire tree — and
// the downstream ParseGrant re-check accepts it, because by then the two are
// the same string. The name is therefore judged as an element id while it is
// still one.
//
// The last row is the denominator: an ordinary id through the identical call
// still compiles, so a refusal above is this rule and not compilation failing
// outright.
func TestAResourceNameThatWidensTheGrantIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		element string
		want    []string
	}{
		{"the whole-namespace wildcard", "#", nil},
		{"a single-level wildcard", "+", nil},
		{"no name at all", "", nil},
		{"an ordinary element id", "01HM6", []string{"cmd:01HM6/#:configure", "read:01HM6/#"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, problems := CompileGrants([]Permission{{
				Name: "ops@" + tc.element, Groups: []string{"ops"},
				Elements: []string{tc.element}, Scopes: []string{"read", "configure"},
			}}, nil)
			if tc.want == nil {
				if len(got) != 0 {
					t.Fatalf("resource %q compiled to %v", tc.element, got)
				}
				if len(problems) != 1 {
					t.Fatalf("resource %q reported %v, want exactly one problem", tc.element, problems)
				}
				return
			}
			if len(problems) != 0 {
				t.Fatalf("unexpected problems: %v", problems)
			}
			if !reflect.DeepEqual(got["ops"], tc.want) {
				t.Fatalf("compiled %v, want %v", got["ops"], tc.want)
			}
		})
	}
}
