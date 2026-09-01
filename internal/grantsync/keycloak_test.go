package grantsync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// realmFixture describes a fake realm precisely enough to drive View, in the
// shapes a REAL Keycloak returns (measured against 26.6.3):
//   - a permission's list row carries none of its parts;
//   - its associatedPolicies view carries policy ids and an EMPTY config;
//   - membership lives in the group-policy list, as a real array.
type realmFixture struct {
	Groups []fakeGroup
	// Children maps a parent group's ID to what GET /groups/{id}/children
	// answers — Keycloak 23+'s ONLY source of a group's members; the top-level
	// listing carries a subGroupCount instead of inlining them.
	Children    map[string][]fakeGroup
	Policies    []fakePolicy
	Permissions []fakePerm
	Resources   string // raw JSON for the resource list
	TokenStatus int    // non-200 to fail the token request
	FailPath    string // any request path containing this substring 500s
}

type fakeGroup struct {
	ID, Name      string
	Hatch         []string // colca_grants attribute
	SubGroupCount int      // triggers a GET .../children fetch when > 0
}

type fakePolicy struct {
	ID, Name   string
	GroupUUIDs []string
}

type fakePerm struct {
	ID, Name   string
	PolicyIDs  []string
	Elements   []string
	ScopeNames []string
}

func fakeRealm(t *testing.T, f realmFixture) *Keycloak {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/realms/colca/protocol/openid-connect/token",
		func(w http.ResponseWriter, _ *http.Request) {
			if f.TokenStatus != 0 {
				w.WriteHeader(f.TokenStatus)
				return
			}
			_, _ = w.Write([]byte(`{"access_token":"t","expires_in":60}`))
		})

	write := func(w http.ResponseWriter, r *http.Request, body string) {
		if f.FailPath != "" && strings.Contains(r.URL.Path, f.FailPath) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom"))
			return
		}
		_, _ = w.Write([]byte(body))
	}

	mux.HandleFunc("/admin/realms/colca/clients", func(w http.ResponseWriter, r *http.Request) {
		write(w, r, `[{"id":"u1","clientId":"colca-authz"}]`)
	})
	groupRow := func(g fakeGroup) map[string]any {
		row := map[string]any{"id": g.ID, "name": g.Name, "subGroupCount": g.SubGroupCount}
		if len(g.Hatch) > 0 {
			row["attributes"] = map[string][]string{ColcaGrantsAttr: g.Hatch}
		}
		return row
	}
	mux.HandleFunc("/admin/realms/colca/groups", func(w http.ResponseWriter, r *http.Request) {
		rows := make([]map[string]any, 0, len(f.Groups))
		for _, g := range f.Groups {
			rows = append(rows, groupRow(g))
		}
		body, _ := json.Marshal(rows)
		write(w, r, string(body))
	})
	mux.HandleFunc("/admin/realms/colca/groups/", func(w http.ResponseWriter, r *http.Request) {
		// Only .../{id}/children is real Keycloak 23+ shape; nothing else in
		// this package fetches under /groups/{id}.
		rest := strings.TrimPrefix(r.URL.Path, "/admin/realms/colca/groups/")
		id, part, _ := strings.Cut(rest, "/")
		if part != "children" {
			write(w, r, "[]")
			return
		}
		children := f.Children[id]
		rows := make([]map[string]any, 0, len(children))
		for _, g := range children {
			rows = append(rows, groupRow(g))
		}
		body, _ := json.Marshal(rows)
		write(w, r, string(body))
	})

	base := "/admin/realms/colca/clients/u1/authz/resource-server"
	mux.HandleFunc(base+"/scope", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
			return
		}
		write(w, r, "[]")
	})
	mux.HandleFunc(base+"/resource", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
			return
		}
		body := f.Resources
		if body == "" {
			body = "[]"
		}
		write(w, r, body)
	})
	mux.HandleFunc(base+"/resource/", func(w http.ResponseWriter, r *http.Request) {
		// PUT (relabel) and DELETE (retire) of one resource.
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc(base+"/policy/group", func(w http.ResponseWriter, r *http.Request) {
		rows := make([]map[string]any, 0, len(f.Policies))
		for _, p := range f.Policies {
			groups := make([]map[string]string, 0, len(p.GroupUUIDs))
			for _, id := range p.GroupUUIDs {
				groups = append(groups, map[string]string{"id": id})
			}
			rows = append(rows, map[string]any{"id": p.ID, "name": p.Name, "groups": groups})
		}
		body, _ := json.Marshal(rows)
		write(w, r, string(body))
	})
	mux.HandleFunc(base+"/permission/scope", func(w http.ResponseWriter, r *http.Request) {
		rows := make([]map[string]string, 0, len(f.Permissions))
		for _, p := range f.Permissions {
			rows = append(rows, map[string]string{"id": p.ID, "name": p.Name})
		}
		body, _ := json.Marshal(rows)
		write(w, r, string(body))
	})
	mux.HandleFunc(base+"/policy/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, base+"/policy/")
		id, part, _ := strings.Cut(rest, "/")
		var perm *fakePerm
		for i := range f.Permissions {
			if f.Permissions[i].ID == id {
				perm = &f.Permissions[i]
			}
		}
		if perm == nil {
			write(w, r, "[]")
			return
		}
		switch part {
		case "associatedPolicies":
			rows := make([]map[string]any, 0, len(perm.PolicyIDs))
			for _, pid := range perm.PolicyIDs {
				// Empty config, exactly as Keycloak answers.
				rows = append(rows, map[string]any{"id": pid, "type": "group", "config": map[string]string{}})
			}
			body, _ := json.Marshal(rows)
			write(w, r, string(body))
		case "resources":
			rows := make([]map[string]string, 0, len(perm.Elements))
			for _, e := range perm.Elements {
				rows = append(rows, map[string]string{"name": e})
			}
			body, _ := json.Marshal(rows)
			write(w, r, string(body))
		case "scopes":
			rows := make([]map[string]string, 0, len(perm.ScopeNames))
			for _, s := range perm.ScopeNames {
				rows = append(rows, map[string]string{"name": s})
			}
			body, _ := json.Marshal(rows)
			write(w, r, string(body))
		default:
			write(w, r, "[]")
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &Keycloak{
		BaseURL: srv.URL, Realm: "colca",
		ClientID: "colca-authz", ClientSecret: "s", AuthzClient: "colca-authz",
	}
}

func TestViewResolvesGroupUUIDsToTheNamesTheTokenCarries(t *testing.T) {
	// Two hops, both measured: a permission names its POLICIES, the group-policy
	// list holds the group UUIDs, and the realm's groups map those UUIDs to the
	// NAMES the token carries (the mapper runs with full.path=false). Reading
	// membership off associatedPolicies instead yields nothing at all.
	k := fakeRealm(t, realmFixture{
		Groups:   []fakeGroup{{ID: "g1", Name: "01HGRP-OPS"}},
		Policies: []fakePolicy{{ID: "p-ops", Name: "group:01HGRP-OPS", GroupUUIDs: []string{"g1"}}},
		Permissions: []fakePerm{{
			ID: "perm1", Name: "01HGRP-OPS@01HM6",
			PolicyIDs: []string{"p-ops"}, Elements: []string{"01HM6"}, ScopeNames: []string{"read"},
		}},
	})

	view, err := k.View(context.Background())
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if len(view.Permissions) != 1 {
		t.Fatalf("permissions: %+v", view.Permissions)
	}
	got := view.Permissions[0]
	if len(got.Groups) != 1 || got.Groups[0] != "01HGRP-OPS" {
		t.Fatalf("groups resolved to %v, want [01HGRP-OPS]", got.Groups)
	}
	if got.Elements[0] != "01HM6" || got.Scopes[0] != "read" {
		t.Fatalf("permission parsed as %+v", got)
	}
}

func TestNestedGroupsAreResolvedThroughChildrenAndCompileIntoGrants(t *testing.T) {
	// Keycloak 23+ never inlines a group's children in the /groups listing — it
	// carries subGroupCount instead, and the members come only from
	// GET /groups/{id}/children (measured against the realm image, 26.7.3). A
	// permission bound to a subgroup must still resolve, and its grant must
	// still make it all the way into a compiled _Group definition.
	k := fakeRealm(t, realmFixture{
		Groups: []fakeGroup{{ID: "g-site1", Name: "Site1", SubGroupCount: 1}},
		Children: map[string][]fakeGroup{
			"g-site1": {{ID: "g-werk1", Name: "Werk1"}},
		},
		Policies: []fakePolicy{{ID: "p-werk1", Name: "group:Werk1", GroupUUIDs: []string{"g-werk1"}}},
		Permissions: []fakePerm{{
			ID: "perm1", Name: "Werk1@01HM6",
			PolicyIDs: []string{"p-werk1"}, Elements: []string{"01HM6"}, ScopeNames: []string{"read"},
		}},
	})

	view, err := k.View(context.Background())
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if len(view.Problems) != 0 {
		t.Fatalf("unexpected problems: %v", view.Problems)
	}
	if len(view.Permissions) != 1 || len(view.Permissions[0].Groups) != 1 ||
		view.Permissions[0].Groups[0] != "Werk1" {
		t.Fatalf("permission did not resolve the nested group, got %+v", view.Permissions)
	}

	grants, problems := CompileGrants(view.Permissions, view.Attributes)
	if len(problems) != 0 {
		t.Fatalf("unexpected compile problems: %v", problems)
	}
	want := []string{"read:01HM6/#"}
	got := grants["Werk1"]
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("compiled grants for the nested group = %v, want %v", got, want)
	}
}

func TestAPolicyNamingAnUnknownGroupIsReportedAsAProblem(t *testing.T) {
	// A group deleted in Keycloak without cleaning up the permission still
	// bound to it must not silently vanish from the grant — it must be
	// reported, the way an orphan grant or an unusable hand-typed grant is.
	k := fakeRealm(t, realmFixture{
		Groups:   []fakeGroup{{ID: "g1", Name: "ops"}},
		Policies: []fakePolicy{{ID: "p1", Name: "group:mixed", GroupUUIDs: []string{"g1", "ghost-group-id"}}},
		Permissions: []fakePerm{{
			ID: "perm1", Name: "mixed@01HM6",
			PolicyIDs: []string{"p1"}, Elements: []string{"01HM6"}, ScopeNames: []string{"read"},
		}},
	})

	view, err := k.View(context.Background())
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if len(view.Problems) != 1 || !strings.Contains(view.Problems[0].Error(), "ghost-group-id") {
		t.Fatalf("problems = %v, want exactly one naming ghost-group-id", view.Problems)
	}
	// The known group in the same policy must still resolve — one unresolvable
	// group id must not cost the rest of the policy its grant.
	if len(view.Permissions) != 1 || len(view.Permissions[0].Groups) != 1 ||
		view.Permissions[0].Groups[0] != "ops" {
		t.Fatalf("the resolvable group did not survive alongside the problem, got %+v", view.Permissions)
	}
}

func TestAPermissionBoundToNoGroupIsSkipped(t *testing.T) {
	// It grants nobody anything; carrying it forward would only produce an
	// empty group in the compile.
	k := fakeRealm(t, realmFixture{
		Groups:      []fakeGroup{{ID: "g1", Name: "ops"}},
		Policies:    []fakePolicy{{ID: "p-none", Name: "group:gone"}},
		Permissions: []fakePerm{{ID: "perm1", PolicyIDs: []string{"p-none"}, Elements: []string{"01HM6"}}},
	})
	view, err := k.View(context.Background())
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if len(view.Permissions) != 0 {
		t.Fatalf("permissions: %+v", view.Permissions)
	}
}

func TestTheGroupAttributeHatchIsRead(t *testing.T) {
	k := fakeRealm(t, realmFixture{
		Groups: []fakeGroup{{ID: "g1", Name: "admins", Hatch: []string{"admin:#"}}},
	})
	view, err := k.View(context.Background())
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if got := view.Attributes["admins"]; len(got) != 1 || got[0] != "admin:#" {
		t.Fatalf("hatch read as %v", got)
	}
}

func TestResourcesCarryTheirManagedMarker(t *testing.T) {
	k := fakeRealm(t, realmFixture{
		Resources: `[{"_id":"r1","name":"01HM6","displayName":"site1/m6","type":"colca:element",
		             "attributes":{"colca.managed-by":["dev-hub"]}},
		            {"_id":"r2","name":"hand-made","type":"colca:element"}]`,
	})
	view, err := k.View(context.Background())
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if !view.Resources[0].ManagedBy("dev-hub") {
		t.Fatal("our own resource did not read as managed")
	}
	if view.Resources[1].ManagedBy("dev-hub") {
		t.Fatal("an unmarked resource read as ours — it would be deleted")
	}
}

func TestViewFailsWhenTheTokenRequestFails(t *testing.T) {
	k := fakeRealm(t, realmFixture{TokenStatus: http.StatusUnauthorized})
	if _, err := k.View(context.Background()); err == nil {
		t.Fatal("a rejected token request reported success")
	}
}

func TestTheTokenErrorNeverEchoesTheResponseBody(t *testing.T) {
	// The token request carries the client secret; a reflected body could
	// carry it into a log.
	k := fakeRealm(t, realmFixture{TokenStatus: http.StatusBadRequest})
	_, err := k.View(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "s") && strings.Contains(err.Error(), "client_secret") {
		t.Fatalf("token error leaked request detail: %v", err)
	}
}

func TestViewFailsWhenAnyPartOfTheReadFails(t *testing.T) {
	// A PARTIAL read is the dangerous one: it does not look like a failure, it
	// looks like a smaller set of grants — and the caller would converge to it.
	for _, failing := range []string{"/resource", "/policy/group", "/permission/scope", "/scopes", "/groups"} {
		t.Run(failing, func(t *testing.T) {
			k := fakeRealm(t, realmFixture{
				Groups:   []fakeGroup{{ID: "g1", Name: "ops"}},
				Policies: []fakePolicy{{ID: "p1", Name: "group:ops", GroupUUIDs: []string{"g1"}}},
				Permissions: []fakePerm{{
					ID: "perm1", PolicyIDs: []string{"p1"},
					Elements: []string{"01HM6"}, ScopeNames: []string{"read"},
				}},
				FailPath: failing,
			})
			if _, err := k.View(context.Background()); err == nil {
				t.Fatalf("a failure on %s returned a view anyway", failing)
			}
		})
	}
}

func TestTheResourceListAsksForAttributes(t *testing.T) {
	// deep=false omits them, and every resource would then read as unmanaged.
	var seen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/token") {
			_, _ = w.Write([]byte(`{"access_token":"t","expires_in":60}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/resource") {
			seen = r.URL.RawQuery
		}
		if strings.HasSuffix(r.URL.Path, "/clients") {
			_, _ = w.Write([]byte(`[{"id":"u1","clientId":"colca-authz"}]`))
			return
		}
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	k := &Keycloak{BaseURL: srv.URL, Realm: "colca", ClientID: "c", ClientSecret: "s",
		AuthzClient: "colca-authz"}
	if _, err := k.View(context.Background()); err != nil {
		t.Fatalf("View: %v", err)
	}
	if !strings.Contains(seen, "deep=true") {
		t.Fatalf("resource list query was %q, want deep=true", seen)
	}
}

func TestAMissingResourceServerClientSaysSo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/token") {
			_, _ = w.Write([]byte(`{"access_token":"t","expires_in":60}`))
			return
		}
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	k := &Keycloak{BaseURL: srv.URL, Realm: "colca", ClientID: "c", ClientSecret: "s",
		AuthzClient: "colca-authz"}
	_, err := k.View(context.Background())
	if err == nil || !strings.Contains(err.Error(), "colca-authz") {
		t.Fatalf("error should name the missing client, got %v", err)
	}
}
