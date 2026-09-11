package grantsync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// published records what the fake node was asked to write.
type published struct {
	mu   sync.Mutex
	rows []map[string]any
}

func (p *published) add(row map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows = append(p.rows, row)
}

func (p *published) all() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]map[string]any(nil), p.rows...)
}

// fakeNode serves /kv and records /publish. kvStatus non-200 fails the read.
func fakeNode(t *testing.T, kvBody string, kvStatus int) (*NodeClient, *published) {
	t.Helper()
	seen := &published{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/kv"):
			if kvStatus != 0 && kvStatus != http.StatusOK {
				w.WriteHeader(kvStatus)
			}
			_, _ = w.Write([]byte(kvBody))
		case strings.HasSuffix(r.URL.Path, "/publish"):
			var row map[string]any
			_ = json.NewDecoder(r.Body).Decode(&row)
			seen.add(row)
		}
	}))
	t.Cleanup(srv.Close)
	return &NodeClient{BaseURL: srv.URL, Service: "grantsync"}, seen
}

const treeWithOneElement = `{"entries":[
	{"topic":"colca/v1/_SystemElement/01HROOT/site1","node_id":"01HROOT","payload":{"id":"01HSITE1"}}
]}`

func grantingRealm(t *testing.T) *Keycloak {
	t.Helper()
	return fakeRealm(t, realmFixture{
		Groups:   []fakeGroup{{ID: "g1", Name: "ops"}},
		Policies: []fakePolicy{{ID: "p1", Name: "group:ops", GroupUUIDs: []string{"g1"}}},
		Permissions: []fakePerm{{
			ID: "perm1", Name: "ops@01HSITE1", PolicyIDs: []string{"p1"},
			Elements: []string{"01HSITE1"}, ScopeNames: []string{"read"},
		}},
	})
}

func syncer(node *NodeClient, kc *Keycloak) *Syncer {
	return &Syncer{Node: node, KC: kc, Owner: "dev-hub", RootULID: "01HROOT"}
}

func TestAKeycloakFailureWritesNothingAtAll(t *testing.T) {
	// Absence in an unreachable source is not absence. If this ever converges
	// on a failed read, every human loses every grant while Keycloak restarts.
	node, seen := fakeNode(t, treeWithOneElement, http.StatusOK)
	kc := fakeRealm(t, realmFixture{TokenStatus: http.StatusServiceUnavailable})

	if _, err := syncer(node, kc).Once(context.Background()); err == nil {
		t.Fatal("a failed Keycloak read reported success")
	}
	if rows := seen.all(); len(rows) != 0 {
		t.Fatalf("published %v after a failed read", rows)
	}
}

func TestAPartialKeycloakReadWritesNothingAtAll(t *testing.T) {
	// The shape that does not look like a failure: the token works, some reads
	// succeed, and the result would simply be a smaller set of grants.
	node, seen := fakeNode(t, treeWithOneElement, http.StatusOK)
	kc := fakeRealm(t, realmFixture{
		Groups:   []fakeGroup{{ID: "g1", Name: "ops"}},
		Policies: []fakePolicy{{ID: "p1", Name: "group:ops", GroupUUIDs: []string{"g1"}}},
		Permissions: []fakePerm{{ID: "perm1", PolicyIDs: []string{"p1"},
			Elements: []string{"01HSITE1"}, ScopeNames: []string{"read"}}},
		FailPath: "/scopes",
	})

	if _, err := syncer(node, kc).Once(context.Background()); err == nil {
		t.Fatal("a partial read reported success")
	}
	if rows := seen.all(); len(rows) != 0 {
		t.Fatalf("published %v after a partial read", rows)
	}
}

func TestATreeFailureWritesNothingAtAll(t *testing.T) {
	node, seen := fakeNode(t, `{"entries":[]}`, http.StatusServiceUnavailable)
	if _, err := syncer(node, grantingRealm(t)).Once(context.Background()); err == nil {
		t.Fatal("a failed tree read reported success")
	}
	if rows := seen.all(); len(rows) != 0 {
		t.Fatalf("published %v after a failed tree read", rows)
	}
}

func TestAShortTreeReadRetiresNoResourceAndWritesNothing(t *testing.T) {
	// The door pages /kv. If a later page fails, nothing may be retired and no
	// definition written: the cycle is skipped.
	elements := elementsAt("a", "b", "c")
	deleted := &deletions{}
	kc := fakeRealm(t, realmFixture{
		Groups:   []fakeGroup{{ID: "g1", Name: "ops"}},
		Policies: []fakePolicy{{ID: "p1", Name: "group:ops", GroupUUIDs: []string{"g1"}}},
		Permissions: []fakePerm{{
			ID: "perm1", Name: "ops@01HA", PolicyIDs: []string{"p1"},
			Elements: []string{"01HA"}, ScopeNames: []string{"read"},
		}},
		Resources: `[
			{"_id":"r-a","name":"01HA","displayName":"a","type":"colca:element","attributes":{"colca.managed-by":["dev-hub"]}},
			{"_id":"r-b","name":"01HB","displayName":"b","type":"colca:element","attributes":{"colca.managed-by":["dev-hub"]}},
			{"_id":"r-c","name":"01HC","displayName":"c","type":"colca:element","attributes":{"colca.managed-by":["dev-hub"]}},
			{"_id":"r-gone","name":"01HGONE","displayName":"gone","type":"colca:element","attributes":{"colca.managed-by":["dev-hub"]}}
		]`,
		Deleted: deleted,
	})

	// First the whole read: the element that is really gone is retired and the group
	// defined, so the assertions below are not vacuous.
	whole := servePaged(t, elements, 2, 0)
	if _, err := syncer(whole.client, kc).Once(context.Background()); err != nil {
		t.Fatalf("complete read: %v", err)
	}
	if got := deleted.all(); len(got) != 1 || got[0] != "r-gone" {
		t.Fatalf("a complete listing should retire exactly the element it lacks, retired %v", got)
	}
	if rows := whole.seen.all(); len(rows) != 1 {
		t.Fatalf("a complete read should define the group once, published %v", rows)
	}

	// Now the same tree with its second page unreadable. 01HC sorts past the
	// break and is absent from what arrived.
	short := servePaged(t, elements, 2, 2)
	_, err := syncer(short.client, kc).Once(context.Background())
	if err == nil {
		t.Fatal("a listing that failed on page 2 reported success")
	}
	if !strings.Contains(err.Error(), "tree read failed") {
		t.Fatalf("the cycle should say the tree read is what failed, said: %v", err)
	}
	if got := deleted.all(); len(got) != 1 {
		t.Fatalf("a short read retired %v — permanent grant loss for every element past the break", got[1:])
	}
	if rows := short.seen.all(); len(rows) != 0 {
		t.Fatalf("published %v after a short read", rows)
	}
}

func TestAFullCyclePublishesTheDefinition(t *testing.T) {
	node, seen := fakeNode(t, treeWithOneElement, http.StatusOK)
	report, err := syncer(node, grantingRealm(t)).Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	rows := seen.all()
	if len(rows) != 1 {
		t.Fatalf("published %d records: %v", len(rows), rows)
	}
	if rows[0]["topic"] != "colca/v1/_CmdConfigure/01HROOT/definition/upsert" {
		t.Fatalf("topic was %v", rows[0]["topic"])
	}
	payload := rows[0]["payload"].(map[string]any)
	if payload["correlation_id"] == nil || payload["expires_at"] == nil {
		t.Fatalf("envelope incomplete: %v", payload)
	}
	def := payload["definitions"].([]any)[0].(map[string]any)
	if def["contract"] != "_Group" {
		t.Fatalf("wrong contract: %v", def)
	}
	grants := def["definition"].(map[string]any)["grants"].([]any)
	if len(grants) != 1 || grants[0] != "read:01HSITE1/#" {
		t.Fatalf("grants published as %v", grants)
	}
	if len(report.Changes) == 0 {
		t.Fatal("a cycle that wrote something reported no changes")
	}
}

func TestASecondCycleWithTheSameStateWritesNothing(t *testing.T) {
	// Converged in both directions: the element is registered in Keycloak, and
	// the tree already holds exactly the grants Keycloak expresses.
	held := `{"entries":[
		{"topic":"colca/v1/_SystemElement/01HROOT/site1","node_id":"01HROOT","payload":{"id":"01HSITE1"}},
		{"topic":"colca/v1/_Group/01HROOT/ops","node_id":"01HROOT","payload":{"id":"ops","grants":["read:01HSITE1/#"]}}
	]}`
	node, seen := fakeNode(t, held, http.StatusOK)
	kc := fakeRealm(t, realmFixture{
		Groups:   []fakeGroup{{ID: "g1", Name: "ops"}},
		Policies: []fakePolicy{{ID: "p1", Name: "group:ops", GroupUUIDs: []string{"g1"}}},
		Permissions: []fakePerm{{
			ID: "perm1", Name: "ops@01HSITE1", PolicyIDs: []string{"p1"},
			Elements: []string{"01HSITE1"}, ScopeNames: []string{"read"},
		}},
		Resources: `[{"_id":"r1","name":"01HSITE1","displayName":"site1","type":"colca:element",
		              "attributes":{"colca.managed-by":["dev-hub"]}}]`,
	})
	report, err := syncer(node, kc).Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if rows := seen.all(); len(rows) != 0 {
		t.Fatalf("rewrote an unchanged definition: %v", rows)
	}
	if len(report.Changes) != 0 {
		t.Fatalf("changes reported with nothing to do: %v", report.Changes)
	}
}

func TestDryRunReportsEverythingAndWritesNothing(t *testing.T) {
	node, seen := fakeNode(t, treeWithOneElement, http.StatusOK)
	s := syncer(node, grantingRealm(t))
	s.DryRun = true
	report, err := s.Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if rows := seen.all(); len(rows) != 0 {
		t.Fatalf("dry run published %v", rows)
	}
	if len(report.Changes) == 0 {
		t.Fatal("dry run reported nothing")
	}
}

func TestAnOrphanGrantIsReportedAsAProblem(t *testing.T) {
	node, _ := fakeNode(t, `{"entries":[]}`, http.StatusOK)
	report, err := syncer(node, grantingRealm(t)).Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if len(report.Problems) == 0 || !strings.Contains(report.Problems[0], "01HSITE1") {
		t.Fatalf("problems: %v", report.Problems)
	}
}

func TestRunSurvivesAFailingCycle(t *testing.T) {
	// A Keycloak restart is not a reason to exit, and statelessness means the
	// next cycle needs nothing the failed one produced.
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n <= 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/token") {
			_, _ = w.Write([]byte(`{"access_token":"t","expires_in":60}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/clients") {
			_, _ = w.Write([]byte(`[{"id":"u1","clientId":"colca-authz"}]`))
			return
		}
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	node, _ := fakeNode(t, `{"entries":[]}`, http.StatusOK)
	s := syncer(node, &Keycloak{BaseURL: srv.URL, Realm: "colca", ClientID: "c",
		ClientSecret: "s", AuthzClient: "colca-authz"})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx, time.Millisecond)
		close(done)
	}()

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		n := calls
		mu.Unlock()
		if n >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Run stopped after %d calls — a failed cycle killed the loop", n)
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop on context cancel — SIGTERM would need SIGKILL behind it")
	}
}

func TestRunStopsPromptlyOnContextCancel(t *testing.T) {
	node, _ := fakeNode(t, `{"entries":[]}`, http.StatusOK)
	s := syncer(node, grantingRealm(t))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		// A long interval: cancelling must not wait for the next tick.
		s.Run(ctx, time.Hour)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run waited for the tick instead of the cancel")
	}
}

// ---- direction three: membership follows a realm role ----------------------

func realmWithAdminGroup(t *testing.T, members []string, writes *deletions) *Keycloak {
	t.Helper()
	return fakeRealm(t, realmFixture{
		Groups: []fakeGroup{{ID: "g-admins", Name: "Administrators",
			Hatch: []string{"admin:#", "read:#"}, Follows: []string{"colca_admin"}}},
		Roles: []string{"colca_admin", "colca_viewer"},
		Users: []fakeUser{
			{ID: "u-boss", Username: "boss", Roles: []string{"colca_admin"}},
			{ID: "u-anna", Username: "anna", Roles: []string{"colca_viewer"}},
		},
		Members:     map[string][]string{"g-admins": members},
		Memberships: writes,
	})
}

func TestAFullCycleConvergesMembershipOfARoleFollowingGroup(t *testing.T) {
	// boss holds the role and is not a member; anna is a member without it.
	writes := &deletions{}
	node, _ := fakeNode(t, `{"entries":[]}`, http.StatusOK)
	report, err := syncer(node, realmWithAdminGroup(t, []string{"u-anna"}, writes)).Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	got := writes.all()
	if len(got) != 2 || got[0] != "PUT u-boss g-admins" || got[1] != "DELETE u-anna g-admins" {
		t.Fatalf("membership writes were %v", got)
	}
	if len(report.Changes) < 2 {
		t.Fatalf("a cycle that moved two memberships reported %v", report.Changes)
	}
}

func TestAConvergedMembershipIsNotRewritten(t *testing.T) {
	writes := &deletions{}
	node, _ := fakeNode(t, `{"entries":[
		{"topic":"colca/v1/_Group/01HROOT/Administrators","node_id":"01HROOT","payload":{"id":"Administrators","grants":["admin:#","read:#"]}}
	]}`, http.StatusOK)
	report, err := syncer(node, realmWithAdminGroup(t, []string{"u-boss"}, writes)).Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if got := writes.all(); len(got) != 0 {
		t.Fatalf("rewrote a converged membership: %v", got)
	}
	if len(report.Changes) != 0 {
		t.Fatalf("changes reported with nothing to do: %v", report.Changes)
	}
}

func TestDryRunMovesNoMembership(t *testing.T) {
	writes := &deletions{}
	node, _ := fakeNode(t, `{"entries":[]}`, http.StatusOK)
	s := syncer(node, realmWithAdminGroup(t, []string{"u-anna"}, writes))
	s.DryRun = true
	report, err := s.Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if got := writes.all(); len(got) != 0 {
		t.Fatalf("dry run wrote %v", got)
	}
	if len(report.Changes) == 0 {
		t.Fatal("dry run reported nothing")
	}
}

// stubScope is a node that holds no elements, so only realm-wide grants
// resolve. Those are all the realm admin bundle has.
type stubScope struct{}

func (stubScope) PathOf(string) (string, bool) { return "", false }
func (stubScope) Reaches(string) bool          { return false }

func TestTheRealmAdminBundleReachesTheTreeAndOpensEveryDoor(t *testing.T) {
	// The role holder joins the group, the group's grants become the _Group
	// definition written at the root, and an entry holding that definition's
	// grants passes the admin routes, every command class and a read.
	vectors := loadAuthzVectors(t)
	writes := &deletions{}
	kc := fakeRealm(t, realmFixture{
		Groups: []fakeGroup{{ID: "g-admins", Name: "Administrators",
			Hatch: vectors.RealmAdminBundle, Follows: []string{vectors.RealmAdminRole}}},
		Roles:       []string{vectors.RealmAdminRole},
		Users:       []fakeUser{{ID: "u-boss", Username: "boss", Roles: []string{vectors.RealmAdminRole}}},
		Members:     map[string][]string{},
		Memberships: writes,
	})
	node, seen := fakeNode(t, `{"entries":[]}`, http.StatusOK)
	report, err := syncer(node, kc).Once(context.Background())
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if len(report.Problems) != 0 {
		t.Fatalf("the bundle raised problems: %v", report.Problems)
	}
	if got := writes.all(); len(got) != 1 || got[0] != "PUT u-boss g-admins" {
		t.Fatalf("the role holder was not put in the group: %v", got)
	}
	rows := seen.all()
	if len(rows) != 1 {
		t.Fatalf("published %d records: %v", len(rows), rows)
	}
	def := rows[0]["payload"].(map[string]any)["definitions"].([]any)[0].(map[string]any)
	published := def["definition"].(map[string]any)["grants"].([]any)
	var grants []string
	for _, g := range published {
		grants = append(grants, g.(string))
	}
	if !reflect.DeepEqual(grants, vectors.RealmAdminBundle) {
		t.Fatalf("the definition carries %v, the bundle is %v", grants, vectors.RealmAdminBundle)
	}

	entry, err := uns.TokenEntry("u-boss", grants)
	if err != nil {
		t.Fatalf("a node refused the bundle: %v", err)
	}
	if !entry.IsAdmin() {
		t.Fatal("the bundle does not open the admin routes")
	}
	if !entry.MayPublishContract("_CmdEdit") || !entry.HoldsCmdClass("configure") {
		t.Fatal("the bundle does not pass the door's check for an Edit command")
	}
	for _, class := range uns.CmdClasses() {
		if !uns.AuthorizeCmdAt(stubScope{}, entry, class, "site1/edge1/m6") {
			t.Fatalf("the bundle does not authorize class %q at a position", class)
		}
	}
	if !uns.Authorize(stubScope{}, entry, uns.ActReadRecord, uns.Prefix()+"_Metric/01HROOT/site1/m6/temp") {
		t.Fatal("the bundle does not authorize a read")
	}
}
