package grantsync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// realmFixture describes a fake realm precisely enough to drive View, in the
// shapes a REAL Keycloak returns (measured against 26.6.3):
//   - a permission's list row carries none of its parts;
//   - its associatedPolicies view carries policy ids and an EMPTY config;
//   - membership lives in the group-policy list, as a real array.
type realmFixture struct {
	Groups []fakeGroup
	// Children maps a parent group's ID to what GET /groups/{id}/children returns,
	// the only source of members in Keycloak 23+.
	Children    map[string][]fakeGroup
	Policies    []fakePolicy
	Permissions []fakePerm
	Resources   string // raw JSON for the resource list
	TokenStatus int    // non-200 to fail the token request
	FailPath    string // any request path containing this substring 500s
	// RejectResourceNamed, when set, 500s the POST that registers exactly this
	// resource — Keycloak's own answer when an element's path exceeds the
	// varchar(255) its display_name column holds. Narrower than FailPath,
	// which would break the resource LISTING too and so exercise the
	// short-read path instead of this one.
	RejectResourceNamed string
	// Deleted, when set, records the id of every resource the service retires.
	Deleted *deletions
	// Roles are the realm roles that exist; GET /roles/{name} is 404 for any other.
	Roles []string
	// Users is the user listing, each with the realm roles Keycloak reports as
	// effective for them.
	Users []fakeUser
	// Members maps a group id to the ids of its direct members.
	Members map[string][]string
	// Memberships, when set, records every membership write as "PUT uid gid" or
	// "DELETE uid gid".
	Memberships *deletions
}

type fakeUser struct {
	ID, Username string
	Roles        []string
}

// deletions records the writes a fake realm was asked to make: retired
// resource ids, or membership changes.
type deletions struct {
	mu  sync.Mutex
	ids []string
}

func (d *deletions) add(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ids = append(d.ids, id)
}

func (d *deletions) all() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.ids...)
}

type fakeGroup struct {
	ID, Name      string
	Hatch         []string // colca_grants attribute
	Follows       []string // colca.follows-realm-role attribute
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
		attributes := map[string][]string{}
		if len(g.Hatch) > 0 {
			attributes[ColcaGrantsAttr] = g.Hatch
		}
		if len(g.Follows) > 0 {
			attributes[FollowsRoleAttr] = g.Follows
		}
		if len(attributes) > 0 {
			row["attributes"] = attributes
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
		// This package fetches only .../{id}/children, the Keycloak 23+ shape, and
		// .../{id}/members under /groups/{id}.
		rest := strings.TrimPrefix(r.URL.Path, "/admin/realms/colca/groups/")
		id, part, _ := strings.Cut(rest, "/")
		switch part {
		case "children":
			children := f.Children[id]
			rows := make([]map[string]any, 0, len(children))
			for _, g := range children {
				rows = append(rows, groupRow(g))
			}
			body, _ := json.Marshal(rows)
			write(w, r, string(body))
		case "members":
			rows := make([]map[string]string, 0, len(f.Members[id]))
			for _, uid := range f.Members[id] {
				rows = append(rows, map[string]string{"id": uid})
			}
			body, _ := json.Marshal(rows)
			write(w, r, string(body))
		default:
			write(w, r, "[]")
		}
	})
	mux.HandleFunc("/admin/realms/colca/roles/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/admin/realms/colca/roles/")
		for _, have := range f.Roles {
			if have == name {
				write(w, r, `{"name":"`+name+`"}`)
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/admin/realms/colca/users", func(w http.ResponseWriter, r *http.Request) {
		rows := make([]map[string]string, 0, len(f.Users))
		for _, u := range f.Users {
			rows = append(rows, map[string]string{"id": u.ID, "username": u.Username})
		}
		body, _ := json.Marshal(rows)
		write(w, r, string(body))
	})
	mux.HandleFunc("/admin/realms/colca/users/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/admin/realms/colca/users/")
		id, part, _ := strings.Cut(rest, "/")
		switch {
		case part == "role-mappings/realm/composite":
			rows := []map[string]string{}
			for _, u := range f.Users {
				if u.ID != id {
					continue
				}
				for _, role := range u.Roles {
					rows = append(rows, map[string]string{"name": role})
				}
			}
			body, _ := json.Marshal(rows)
			write(w, r, string(body))
		case strings.HasPrefix(part, "groups/"):
			if f.Memberships != nil {
				f.Memberships.add(r.Method + " " + id + " " + strings.TrimPrefix(part, "groups/"))
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			write(w, r, "[]")
		}
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
			if f.RejectResourceNamed != "" {
				body, _ := io.ReadAll(r.Body)
				var posted struct {
					Name string `json:"name"`
				}
				_ = json.Unmarshal(body, &posted)
				if posted.Name == f.RejectResourceNamed {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":"unknown_error"}`))
					return
				}
			}
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
		if r.Method == http.MethodDelete && f.Deleted != nil {
			f.Deleted.add(strings.TrimPrefix(r.URL.Path, base+"/resource/"))
		}
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
	// A permission bound to a subgroup, which Keycloak 23+ only returns from
	// GET /groups/{id}/children, must still end up in a compiled _Group.
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
	// A group deleted without cleaning up its permission is reported, not dropped
	// silently.
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
	// The known group in the same policy still resolves.
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
	// A partial read would look like fewer grants, so it must be an error.
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

func TestViewReadsAGroupThatFollowsARealmRoleAndWhoHoldsIt(t *testing.T) {
	// The attribute names the role, each user's composite role mappings say who
	// holds it, and the group's member list is what that is diffed against.
	k := fakeRealm(t, realmFixture{
		Groups: []fakeGroup{{ID: "g-admins", Name: "Administrators", Follows: []string{"colca_admin"}}},
		Roles:  []string{"colca_admin", "colca_viewer"},
		Users: []fakeUser{
			{ID: "u-boss", Username: "boss", Roles: []string{"colca_admin", "colca_viewer"}},
			{ID: "u-anna", Username: "anna", Roles: []string{"colca_viewer"}},
		},
		Members: map[string][]string{"g-admins": {"u-anna"}},
	})
	view, err := k.View(context.Background())
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if len(view.Problems) != 0 {
		t.Fatalf("unexpected problems: %v", view.Problems)
	}
	if len(view.RoleGroups) != 1 || view.RoleGroups[0].ID != "g-admins" || view.RoleGroups[0].Roles[0] != "colca_admin" {
		t.Fatalf("role groups read as %+v", view.RoleGroups)
	}
	if got := view.Members["g-admins"]; len(got) != 1 || got[0] != "u-anna" {
		t.Fatalf("members read as %v", got)
	}
	holders := map[string]bool{}
	for _, u := range view.Users {
		holders[u.Username] = u.Roles["colca_admin"]
	}
	if !holders["boss"] || holders["anna"] {
		t.Fatalf("role holders read as %v", holders)
	}
}

func TestAGroupFollowingARoleTheRealmDoesNotHaveIsReportedAndLeftAlone(t *testing.T) {
	// A misspelled role has no holders, so following it would empty the group.
	k := fakeRealm(t, realmFixture{
		Groups:  []fakeGroup{{ID: "g-admins", Name: "Administrators", Follows: []string{"colca_admn"}}},
		Roles:   []string{"colca_admin"},
		Users:   []fakeUser{{ID: "u-boss", Username: "boss", Roles: []string{"colca_admin"}}},
		Members: map[string][]string{"g-admins": {"u-boss"}},
	})
	view, err := k.View(context.Background())
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if len(view.RoleGroups) != 0 {
		t.Fatalf("a group following an unknown role was followed anyway: %+v", view.RoleGroups)
	}
	if len(view.Problems) != 1 || !strings.Contains(view.Problems[0].Error(), "colca_admn") {
		t.Fatalf("problems = %v, want exactly one naming the missing role", view.Problems)
	}
	if len(view.Users) != 0 {
		t.Fatal("users were read although no group is followed")
	}
}

func TestNothingAboutUsersIsReadWhenNoGroupFollowsARole(t *testing.T) {
	var touched []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/token") {
			_, _ = w.Write([]byte(`{"access_token":"t","expires_in":60}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/clients") {
			_, _ = w.Write([]byte(`[{"id":"u1","clientId":"colca-authz"}]`))
			return
		}
		if strings.Contains(r.URL.Path, "/users") || strings.Contains(r.URL.Path, "/roles/") {
			touched = append(touched, r.URL.Path)
		}
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	k := &Keycloak{BaseURL: srv.URL, Realm: "colca", ClientID: "c", ClientSecret: "s",
		AuthzClient: "colca-authz"}
	if _, err := k.View(context.Background()); err != nil {
		t.Fatalf("View: %v", err)
	}
	if len(touched) != 0 {
		t.Fatalf("read %v with no group following a role", touched)
	}
}

func TestViewFailsWhenAMembershipReadFails(t *testing.T) {
	// A short membership read would look like fewer holders and remove the rest.
	for _, failing := range []string{"/members", "/composite", "/roles/"} {
		t.Run(failing, func(t *testing.T) {
			k := fakeRealm(t, realmFixture{
				Groups:   []fakeGroup{{ID: "g-admins", Name: "Administrators", Follows: []string{"colca_admin"}}},
				Roles:    []string{"colca_admin"},
				Users:    []fakeUser{{ID: "u-boss", Username: "boss", Roles: []string{"colca_admin"}}},
				Members:  map[string][]string{"g-admins": {"u-boss"}},
				FailPath: failing,
			})
			if _, err := k.View(context.Background()); err == nil {
				t.Fatalf("a failure on %s returned a view anyway", failing)
			}
		})
	}
}
