package uns

import (
	"errors"
	"strings"
	"testing"
)

// withGroup stores a _Group definition as if author wrote it and it had
// descended to this node.
func withGroup(f *fakeStore, author, id string, grants ...string) {
	f.records["colca/v1/_Group/"+author+"/"+id] = mustJSON(map[string]any{
		"id": id, "name": id, "grants": grants,
	})
}

func TestATokenNamingGroupsGetsTheirUnionOfGrants(t *testing.T) {
	f := newStore("n-edge1")
	withGroup(f, "n-global", "01HGRP-OPS", "read:01HLINE1/#", "cmd:01HLINE1/#:param")
	withGroup(f, "n-global", "01HGRP-VIEW", "read:01HLINE2/#", "read:01HLINE1/#") // overlaps

	e, problems, err := TokenEntryWithGroups("anna", nil,
		[]string{"01HGRP-OPS", "01HGRP-VIEW"}, NewGroupIndex(f))
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 0 {
		t.Fatalf("unexpected problems: %v", problems)
	}
	want := []string{"read:01HLINE1/#", "cmd:01HLINE1/#:param", "read:01HLINE2/#"}
	if len(e.Grants) != len(want) {
		t.Fatalf("grants = %v, want the union with the duplicate dropped (%v)", e.Grants, want)
	}
	for i := range want {
		if e.Grants[i] != want[i] {
			t.Fatalf("grants = %v, want %v", e.Grants, want)
		}
	}
}

// Grants the token carries directly still work alongside groups — a human may
// hold something no group gives them.
func TestGrantsOnTheTokenSurviveAlongsideGroups(t *testing.T) {
	f := newStore("n-edge1")
	withGroup(f, "n-global", "01HGRP-OPS", "read:01HLINE1/#")

	e, _, err := TokenEntryWithGroups("anna", []string{"admin:#"},
		[]string{"01HGRP-OPS"}, NewGroupIndex(f))
	if err != nil {
		t.Fatal(err)
	}
	if !e.IsAdmin() {
		t.Fatal("a grant carried by the token itself must survive")
	}
	if len(e.Grants) != 2 {
		t.Fatalf("grants = %v, want the token's own plus the group's", e.Grants)
	}
}

// An unknown group costs the person that group, not everything they hold.
func TestAnUnknownGroupContributesNothingAndIsReported(t *testing.T) {
	f := newStore("n-edge1")
	withGroup(f, "n-global", "01HGRP-OPS", "read:01HLINE1/#")

	e, problems, err := TokenEntryWithGroups("anna", nil,
		[]string{"01HGRP-OPS", "01HGRP-GONE"}, NewGroupIndex(f))
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Grants) != 1 || e.Grants[0] != "read:01HLINE1/#" {
		t.Fatalf("grants = %v, want only the group that resolved", e.Grants)
	}
	if len(problems) != 1 || !strings.Contains(problems[0].Error(), "01HGRP-GONE") {
		t.Fatalf("problems = %v, want one naming the missing group", problems)
	}
}

// Two definitions claiming one id resolve to nothing, and the reason names
// both authors. Picking either could let a node widen its own grants with a
// copied id.
func TestTwoDefinitionsClaimingOneIdResolveToNothing(t *testing.T) {
	f := newStore("n-site1")
	withGroup(f, "n-global", "01HGRP-OPS", "read:01HLINE1/#")
	withGroup(f, "n-site1", "01HGRP-OPS", "read:#", "cmd:#:admin") // a wider shadow

	e, problems, err := TokenEntryWithGroups("anna", nil, []string{"01HGRP-OPS"}, NewGroupIndex(f))
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Grants) != 0 {
		t.Fatalf("grants = %v — an ambiguous id must grant nothing at all", e.Grants)
	}
	if len(problems) != 1 {
		t.Fatalf("problems = %v, want exactly one", problems)
	}
	msg := problems[0].Error()
	for _, author := range []string{"n-global", "n-site1"} {
		if !strings.Contains(msg, author) {
			t.Errorf("the denial must name every author claiming the id; %q omits %s", msg, author)
		}
	}
}

// A node that has received no definitions denies every group — the same
// fail-closed shape as a node that has never learned its position.
func TestANodeWithNoDefinitionsResolvesNoGroup(t *testing.T) {
	f := newStore("n-edge1")

	e, problems, err := TokenEntryWithGroups("anna", []string{"read:#"},
		[]string{"01HGRP-OPS"}, NewGroupIndex(f))
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Grants) != 1 || e.Grants[0] != "read:#" {
		t.Fatalf("grants = %v — only the frame-invariant grant the token carried may survive", e.Grants)
	}
	if len(problems) != 1 {
		t.Fatalf("problems = %v, want the unresolved group reported", problems)
	}
}

// A retracted group stops granting: the tombstone that descended removed it,
// and the index reads the KV view, not history.
func TestARetractedGroupGrantsNothing(t *testing.T) {
	f := newStore("n-edge1")
	withGroup(f, "n-global", "01HGRP-OPS", "read:01HLINE1/#")
	f.records["colca/v1/_Group/n-global/01HGRP-OPS"] = nil // the tombstone arrived

	e, problems, err := TokenEntryWithGroups("anna", nil, []string{"01HGRP-OPS"}, NewGroupIndex(f))
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Grants) != 0 {
		t.Fatalf("grants = %v, want none — the group was retracted", e.Grants)
	}
	if len(problems) != 1 {
		t.Fatalf("problems = %v, want the retraction reported as an unresolved group", problems)
	}
}

// A malformed grant inside a definition is dropped and reported; the rest of
// the group still applies.
func TestAMalformedGrantInsideAGroupIsDroppedNotFatal(t *testing.T) {
	f := newStore("n-edge1")
	withGroup(f, "n-global", "01HGRP-OPS", "read:01HLINE1/#", "cmd:01HLINE1/#")

	e, problems, err := TokenEntryWithGroups("anna", nil, []string{"01HGRP-OPS"}, NewGroupIndex(f))
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Grants) != 1 || e.Grants[0] != "read:01HLINE1/#" {
		t.Fatalf("grants = %v, want the valid one kept", e.Grants)
	}
	if len(problems) != 1 || !strings.Contains(problems[0].Error(), "cmd:") {
		t.Fatalf("problems = %v, want the malformed grant named", problems)
	}
}

func TestStandaloneGroupsIgnoreCachedFleetAuthority(t *testing.T) {
	f := newStore("n-machine")
	withGroup(f, "n-hub", "fleet-admin", "admin:#")
	withGroup(f, "n-machine", "local-operator", "read:#")
	idx := NewGroupIndex(f).WithAuthority("n-machine")
	if _, ok, _ := idx.GrantsOf("fleet-admin"); ok {
		t.Fatal("cached fleet group still authorizes")
	}
	if grants, ok, _ := idx.GrantsOf("local-operator"); !ok || len(grants) != 1 {
		t.Fatal("local operator lost access")
	}
}

// A group the node does not define is reported as such, so callers can treat it
// as the normal case it is rather than a fault.
func TestAnUndefinedGroupIsReportedAsUnknown(t *testing.T) {
	f := newStore("n-edge1")
	withGroup(f, "n-global", "01HGRP-OPS", "read:01HLINE1/#")

	e, problems, err := TokenEntryWithGroups("anna", nil,
		[]string{"offline_access", "01HGRP-OPS"}, NewGroupIndex(f))
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Grants) != 1 {
		t.Fatalf("grants = %v, want the defined group's only", e.Grants)
	}
	var unknown *UnknownGroupError
	if len(problems) != 1 || !errors.As(problems[0], &unknown) || unknown.ID != "offline_access" {
		t.Fatalf("problems = %v, want one UnknownGroupError for offline_access", problems)
	}
}

func TestGroupNoticesReportsEachIDOnce(t *testing.T) {
	var n GroupNotices
	if !n.First("a") || n.First("a") || !n.First("b") {
		t.Fatal("First must be true exactly once per id")
	}
}
