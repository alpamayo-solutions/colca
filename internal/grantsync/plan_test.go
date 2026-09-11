package grantsync

import (
	"testing"
)

const owner = "dev-hub"

func managed(name, display string) Resource {
	return Resource{
		ID: "r-" + name, Name: name, DisplayName: display, Type: ElementResourceType,
		Attributes: map[string][]string{ManagedByAttr: {owner}},
	}
}

// ---- direction one: elements become assignable -----------------------------

func TestANewElementBecomesAResourceNamedByItsID(t *testing.T) {
	plan := PlanResources(map[string]string{"01HM6": "site1/edge1/m6"}, nil, owner)
	if len(plan.Create) != 1 {
		t.Fatalf("plan: %+v", plan)
	}
	got := plan.Create[0]
	if got.Name != "01HM6" || got.DisplayName != "site1/edge1/m6" {
		t.Fatalf("resource planned as %+v", got)
	}
	if got.Attributes[ManagedByAttr][0] != owner {
		t.Fatalf("marker missing: %+v", got.Attributes)
	}
}

func TestARenamedElementUpdatesTheDisplayNameAndKeepsTheResource(t *testing.T) {
	// The identity a grant points at must survive; only the label moves.
	plan := PlanResources(
		map[string]string{"01HM6": "site1/edge1/m6-new"},
		[]Resource{managed("01HM6", "site1/edge1/m6")}, owner)
	if len(plan.Create) != 0 || len(plan.Delete) != 0 {
		t.Fatalf("a rename must not create or delete: %+v", plan)
	}
	if len(plan.Update) != 1 || plan.Update[0].DisplayName != "site1/edge1/m6-new" {
		t.Fatalf("update planned as %+v", plan.Update)
	}
	if plan.Update[0].ID != "r-01HM6" {
		t.Fatalf("update lost the resource id: %+v", plan.Update[0])
	}
}

func TestAnUnchangedElementProducesNoCallAtAll(t *testing.T) {
	plan := PlanResources(
		map[string]string{"01HM6": "site1/edge1/m6"},
		[]Resource{managed("01HM6", "site1/edge1/m6")}, owner)
	if len(plan.Create)+len(plan.Update)+len(plan.Delete) != 0 {
		t.Fatalf("plan should be empty: %+v", plan)
	}
}

func TestARetiredElementLosesItsResource(t *testing.T) {
	plan := PlanResources(nil, []Resource{managed("01HGONE", "site1/gone")}, owner)
	if len(plan.Delete) != 1 || plan.Delete[0].Name != "01HGONE" {
		t.Fatalf("delete planned as %+v", plan.Delete)
	}
}

func TestAResourceWeDoNotOwnIsReportedNeverDeleted(t *testing.T) {
	// Somebody made it by hand, in a store this service does not own.
	hand := Resource{ID: "r-x", Name: "hand-made", Type: ElementResourceType}
	plan := PlanResources(nil, []Resource{hand}, owner)
	if len(plan.Delete) != 0 {
		t.Fatalf("deleted an unmanaged resource: %+v", plan.Delete)
	}
	if len(plan.Unmanaged) != 1 || plan.Unmanaged[0] != "hand-made" {
		t.Fatalf("unmanaged: %v", plan.Unmanaged)
	}
}

func TestAResourceOfAnotherTypeIsLeftEntirelyAlone(t *testing.T) {
	other := Resource{ID: "r-y", Name: "Default Resource", Type: "urn:something"}
	plan := PlanResources(nil, []Resource{other}, owner)
	if len(plan.Delete)+len(plan.Unmanaged) != 0 {
		t.Fatalf("a foreign resource type should not appear at all: %+v", plan)
	}
}

func TestAnUnmarkedResourceForALiveElementIsAdopted(t *testing.T) {
	// It matches an element that exists, so it is ours to maintain; taking the
	// marker is how the next cycle stops treating it as a stranger.
	stray := Resource{ID: "r-z", Name: "01HM6", DisplayName: "site1/m6", Type: ElementResourceType}
	plan := PlanResources(map[string]string{"01HM6": "site1/m6"}, []Resource{stray}, owner)
	if len(plan.Update) != 1 {
		t.Fatalf("plan: %+v", plan)
	}
	if plan.Update[0].Attributes[ManagedByAttr][0] != owner {
		t.Fatalf("adoption did not set the marker: %+v", plan.Update[0])
	}
}

// ---- direction two: grants become definitions ------------------------------

func tree(groups map[string]HeldGroup, elements map[string]string) TreeView {
	if elements == nil {
		elements = map[string]string{"01HM6": "site1/m6"}
	}
	if groups == nil {
		groups = map[string]HeldGroup{}
	}
	return TreeView{Elements: elements, Groups: groups}
}

const root = "01HROOT"

func TestANewGroupIsUpsertedWithItsGrants(t *testing.T) {
	plan := PlanDefinitions(map[string][]string{"ops": {"read:01HM6/#"}}, tree(nil, nil), root)
	if len(plan.Upsert) != 1 {
		t.Fatalf("plan: %+v", plan)
	}
	got := plan.Upsert[0]
	if got.ID != "ops" || got.Name != "ops" || got.Grants[0] != "read:01HM6/#" {
		t.Fatalf("definition planned as %+v", got)
	}
}

func TestAnUnchangedGroupProducesNoWrite(t *testing.T) {
	held := map[string]HeldGroup{"ops": {ID: "ops", Grants: []string{"read:01HM6/#"}, Author: root}}
	plan := PlanDefinitions(map[string][]string{"ops": {"read:01HM6/#"}}, tree(held, nil), root)
	if len(plan.Upsert) != 0 || len(plan.Retract) != 0 {
		t.Fatalf("plan should be empty: %+v", plan)
	}
}

func TestAChangedGrantSetIsRewritten(t *testing.T) {
	held := map[string]HeldGroup{"ops": {ID: "ops", Grants: []string{"read:01HM6/#"}, Author: root}}
	plan := PlanDefinitions(
		map[string][]string{"ops": {"cmd:01HM6/#:operate", "read:01HM6/#"}}, tree(held, nil), root)
	if len(plan.Upsert) != 1 || len(plan.Upsert[0].Grants) != 2 {
		t.Fatalf("plan: %+v", plan)
	}
}

func TestAWithdrawnPermissionRetractsTheDefinition(t *testing.T) {
	// Convergence includes removal, or access outlives its own revocation.
	held := map[string]HeldGroup{"ops": {ID: "ops", Grants: []string{"read:01HM6/#"}, Author: root}}
	plan := PlanDefinitions(map[string][]string{}, tree(held, nil), root)
	if len(plan.Retract) != 1 || plan.Retract[0] != "ops" {
		t.Fatalf("retract: %v", plan.Retract)
	}
}

func TestADefinitionAuthoredElsewhereIsReportedNeverRetracted(t *testing.T) {
	// Not ours to withdraw: records are keyed by author, so a tombstone written
	// at the root would not remove an edge's record anyway.
	held := map[string]HeldGroup{"ops": {ID: "ops", Grants: []string{"read:01HM6/#"}, Author: "01HEDGE1"}}
	plan := PlanDefinitions(map[string][]string{}, tree(held, nil), root)
	if len(plan.Retract) != 0 {
		t.Fatalf("retracted another node's definition: %v", plan.Retract)
	}
	if len(plan.Foreign) != 1 || plan.Foreign[0] != "ops" {
		t.Fatalf("foreign: %v", plan.Foreign)
	}
}

func TestAGrantOnAnUnknownElementIsReportedAndStillWritten(t *testing.T) {
	// An orphan grant resolves to nothing at every node: wrong, visible and
	// harmless. Dropping it silently would hide the provisioning error instead.
	plan := PlanDefinitions(map[string][]string{"ops": {"read:01HGHOST/#"}}, tree(nil, nil), root)
	if len(plan.Unresolvable) != 1 || plan.Unresolvable[0].Element != "01HGHOST" {
		t.Fatalf("unresolvable: %+v", plan.Unresolvable)
	}
	if len(plan.Upsert) != 1 || plan.Upsert[0].Grants[0] != "read:01HGHOST/#" {
		t.Fatalf("the grant should still be written: %+v", plan.Upsert)
	}
}

func TestARealmWideGrantIsNeverCalledUnresolvable(t *testing.T) {
	// admin:# and read:# name no element; they resolve everywhere by definition.
	plan := PlanDefinitions(map[string][]string{"admins": {"admin:#", "read:#"}}, tree(nil, nil), root)
	if len(plan.Unresolvable) != 0 {
		t.Fatalf("unresolvable: %+v", plan.Unresolvable)
	}
}

func TestPlanningIsStableAcrossRuns(t *testing.T) {
	// Map iteration order must not leak into what gets written.
	desired := map[string][]string{"b": {"read:01HM6/#"}, "a": {"read:01HM6/#"}, "c": {"read:01HM6/#"}}
	first := PlanDefinitions(desired, tree(nil, nil), root)
	for i := 0; i < 20; i++ {
		again := PlanDefinitions(desired, tree(nil, nil), root)
		for j := range first.Upsert {
			if again.Upsert[j].ID != first.Upsert[j].ID {
				t.Fatalf("run %d ordered differently: %v vs %v", i, again.Upsert, first.Upsert)
			}
		}
	}
}

// ---- direction three: membership follows a realm role ----------------------

func user(id, name string, roles ...string) User {
	u := User{ID: id, Username: name, Roles: map[string]bool{}}
	for _, r := range roles {
		u.Roles[r] = true
	}
	return u
}

var admins = RoleGroup{ID: "g-admins", Name: "Administrators", Roles: []string{"colca_admin"}}

func TestARoleHolderJoinsAndSomebodyWithoutTheRoleLeaves(t *testing.T) {
	// A user granted the role joins, however it reached them; a user added by
	// hand without it leaves.
	plan := PlanMemberships(
		[]RoleGroup{admins},
		[]User{
			user("u-boss", "boss", "colca_admin"),
			user("u-anna", "anna", "colca_viewer"),
			user("u-lena", "lena", "colca_admin"),
		},
		map[string][]string{"g-admins": {"u-anna", "u-lena"}},
	)
	if len(plan.Add) != 1 || plan.Add[0].UserID != "u-boss" || plan.Add[0].GroupID != "g-admins" {
		t.Fatalf("add planned as %+v", plan.Add)
	}
	if len(plan.Remove) != 1 || plan.Remove[0].UserID != "u-anna" || plan.Remove[0].Username != "anna" {
		t.Fatalf("remove planned as %+v", plan.Remove)
	}
	if len(plan.Unaccounted) != 0 {
		t.Fatalf("unaccounted: %+v", plan.Unaccounted)
	}
}

func TestNobodyWithoutTheFollowedRoleIsEverAdded(t *testing.T) {
	// Other roles add nobody. What editors and viewers reach comes from grants
	// on elements.
	plan := PlanMemberships(
		[]RoleGroup{admins},
		[]User{
			user("u-ed", "editor", "colca_editor"),
			user("u-vi", "viewer", "colca_viewer"),
			user("u-none", "nobody"),
		},
		map[string][]string{},
	)
	if len(plan.Add)+len(plan.Remove) != 0 {
		t.Fatalf("plan should be empty: %+v", plan)
	}
}

func TestAConvergedMembershipPlansNothing(t *testing.T) {
	plan := PlanMemberships(
		[]RoleGroup{admins},
		[]User{user("u-boss", "boss", "colca_admin"), user("u-anna", "anna")},
		map[string][]string{"g-admins": {"u-boss"}},
	)
	if len(plan.Add)+len(plan.Remove)+len(plan.Unaccounted) != 0 {
		t.Fatalf("plan should be empty: %+v", plan)
	}
}

func TestAMemberTheUserListingDoesNotShowIsReportedNeverRemoved(t *testing.T) {
	// Keycloak leaves service accounts out of the user listing. Missing from it
	// means unknown, and unknown never removes.
	plan := PlanMemberships(
		[]RoleGroup{admins},
		[]User{user("u-boss", "boss", "colca_admin")},
		map[string][]string{"g-admins": {"u-boss", "u-hidden"}},
	)
	if len(plan.Remove) != 0 {
		t.Fatalf("removed a member whose roles are unknown: %+v", plan.Remove)
	}
	if len(plan.Unaccounted) != 1 || plan.Unaccounted[0].UserID != "u-hidden" {
		t.Fatalf("unaccounted: %+v", plan.Unaccounted)
	}
}

func TestAGroupMayFollowMoreThanOneRole(t *testing.T) {
	both := RoleGroup{ID: "g", Name: "Ops", Roles: []string{"role_a", "role_b"}}
	plan := PlanMemberships(
		[]RoleGroup{both},
		[]User{user("u-a", "a", "role_a"), user("u-b", "b", "role_b"), user("u-c", "c", "role_c")},
		map[string][]string{},
	)
	if len(plan.Add) != 2 || plan.Add[0].Username != "a" || plan.Add[1].Username != "b" {
		t.Fatalf("add planned as %+v", plan.Add)
	}
}
