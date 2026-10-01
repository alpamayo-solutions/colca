package uns

import (
	"strings"
	"testing"
)

func TestParseGrantAcceptsNodeRelativeZones(t *testing.T) {
	for _, tc := range []struct {
		grant   string
		element string
	}{
		{"read:$node/#", "$node"},
		{"read:$node", "$node"},
		{"write:$node/#", "$node"},
		{"cmd:$node/#:operate,param", "$node"},
		{"read:$node/CraftCanFiller/#", "$node/CraftCanFiller"},
		{"cmd:$node/Line 1/Press:configure", "$node/Line 1/Press"},
	} {
		g, err := ParseGrant(tc.grant)
		if err != nil {
			t.Errorf("%s: %v", tc.grant, err)
			continue
		}
		if g.Element != tc.element {
			t.Errorf("%s: element %q, want %q", tc.grant, g.Element, tc.element)
		}
		again, err := FormatGrant(g)
		if err != nil {
			t.Errorf("%s: format: %v", tc.grant, err)
			continue
		}
		if back, err := ParseGrant(again); err != nil || back.Element != g.Element {
			t.Errorf("%s: round trip %q → %+v, %v", tc.grant, again, back, err)
		}
	}
}

func TestParseGrantRefusesMalformedNodeRelativeZones(t *testing.T) {
	for _, grant := range []string{
		"read:$mount/#",         // not a zone colca knows
		"read:$nodes/#",         // a prefix of $node is not $node
		"read:$node//x/#",       // empty segment
		"read:$node/x/+/#",      // wildcard
		"read:$node/a#/#",       // wildcard inside a name
		"read:$node/$node/#",    // nested
		"admin:$node/#",         // zone-scoped admin stays reserved
		"cmd:$node/a:b/#:param", // ':' would split the grant
	} {
		if _, err := ParseGrant(grant); err == nil {
			t.Errorf("%s: accepted, want refused", grant)
		}
	}
}

// edgeScope is a leaf node mounted under a hub: it holds a machine at
// "CraftCanFiller" and an invoices element at "Invoices".
func edgeScope() Scope {
	return mapScope{
		paths: map[string]string{"01HMACHINE": "CraftCanFiller", "01HINVOICES": "Invoices"},
		above: map[string]bool{"01HHUB": true, "01HMOUNT": true},
	}
}

func TestANodeRelativeGrantCoversTheNodeThePersonSignedInAt(t *testing.T) {
	sc := edgeScope()
	anna, err := TokenEntry("anna", []string{"read:$node/#"})
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{
		"colca/v1/_Metric/01X/CraftCanFiller/temp",
		"colca/v1/_Constant/01X/Invoices/holder",
	} {
		if !Authorize(sc, anna, ActReadRecord, topic) {
			t.Errorf("read:$node/# must cover %s at the node anna signed in at", topic)
		}
	}
	if !Authorize(sc, anna, ActSub, "colca/#") {
		t.Error("read:$node/# must cover a whole-node subscription")
	}
}

func TestANodeRelativePathCoversOnlyThatSubtree(t *testing.T) {
	sc := edgeScope()
	anna, err := TokenEntry("anna", []string{"read:$node/CraftCanFiller/#", "cmd:$node/CraftCanFiller/#:param"})
	if err != nil {
		t.Fatal(err)
	}
	if !Authorize(sc, anna, ActReadRecord, "colca/v1/_Metric/01X/CraftCanFiller/temp") {
		t.Error("read:$node/CraftCanFiller/# must cover the machine's signals")
	}
	if Authorize(sc, anna, ActReadRecord, "colca/v1/_Constant/01X/Invoices/holder") {
		t.Error("read:$node/CraftCanFiller/# must not cover a sibling subtree")
	}
	if Authorize(sc, anna, ActReadRecord, "colca/v1/_Metric/01X/CraftCanFillerX/temp") {
		t.Error("a relative path covers its subtree, not every path sharing its prefix")
	}
	if Authorize(sc, anna, ActSub, "colca/#") {
		t.Error("a relative path must not cover a whole-node subscription")
	}
	if !Authorize(sc, anna, ActCmd, "colca/v1/_CmdParam/01X/CraftCanFiller/set-speed") {
		t.Error("cmd:$node/CraftCanFiller/#:param must cover a param command on the machine")
	}
	if Authorize(sc, anna, ActCmd, "colca/v1/_CmdParam/01X/Invoices/x") {
		t.Error("cmd:$node/CraftCanFiller/#:param must not cover a sibling")
	}
}

// A person rebuilt from a command that came down from the parent signed in at
// another node: $node names that node, so it grants nothing here.
func TestANodeRelativeGrantGrantsNothingToAPersonWhoSignedInElsewhere(t *testing.T) {
	sc := edgeScope()
	ole, err := TokenEntry("ole", []string{"read:$node/#", "cmd:$node/#:configure,operate,param"})
	if err != nil {
		t.Fatal(err)
	}
	ole.SignedInHere = false
	if Authorize(sc, ole, ActReadRecord, "colca/v1/_Metric/01X/CraftCanFiller/temp") {
		t.Error("$node must not resolve for a person who signed in at another node")
	}
	if AuthorizeCmdAt(sc, ole, "configure", "CraftCanFiller") || AuthorizeCmdAt(sc, ole, "configure", "") {
		t.Error("cmd:$node must not authorize an executor position for a person who signed in elsewhere")
	}
}

func TestATokenIsSignedInAtTheNodeThatVerifiedIt(t *testing.T) {
	e, err := TokenEntry("anna", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !e.SignedInHere {
		t.Error("a token entry is built by this node's door, so the person signed in here")
	}
}

// Only people sign in; a registry identity's placement already is its zone.
func TestARegistryEntryMayNotHoldANodeRelativeGrant(t *testing.T) {
	e := entry("m1", "read:$node/#")
	err := e.Validate()
	if err == nil || !strings.Contains(err.Error(), "node-relative") {
		t.Fatalf("Validate = %v, want a refusal naming node-relative grants", err)
	}
	// Even if one slipped in, it resolves for people only.
	e.Grants = []string{"read:$node/#"}
	if Authorize(edgeScope(), e, ActReadRecord, "colca/v1/_Metric/01X/CraftCanFiller/temp") {
		t.Error("a node-relative grant must not resolve for a machine")
	}
}
