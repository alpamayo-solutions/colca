package uns

import (
	"strings"
	"testing"
)

// entry is a test helper: a machine entry with the given mount and grants.
func entry(mount string, grants ...string) *Entry {
	return &Entry{
		ULID:   "01MACHINE0000000000000000A",
		Pubkey: strings.Repeat("ab", 32),
		Kind:   KindMachine,
		Mount:  mount,
		Grants: grants,
	}
}

func TestParseGrant(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
		verb    string
		prefix  string
		classes []string
	}{
		{in: "read:werk1/#", verb: "read", prefix: "werk1"},
		{in: "read:werk1", verb: "read", prefix: "werk1"},
		{in: "read:werk1/linie3/#", verb: "read", prefix: "werk1/linie3"},
		{in: "read:#", verb: "read", prefix: "#"},
		{in: "cmd:werk1/linie3/#:param,operate", verb: "cmd", prefix: "werk1/linie3", classes: []string{"param", "operate"}},
		{in: "cmd:z:admin", verb: "cmd", prefix: "z", classes: []string{"admin"}},
		{in: "write:werk1/#", wantErr: true},      // write is identity, never a grant (§5.1)
		{in: "read:", wantErr: true},              // empty prefix
		{in: "cmd:werk1/#", wantErr: true},        // cmd without classes
		{in: "cmd:werk1/#:", wantErr: true},       // empty classes
		{in: "cmd:werk1/#:reboot", wantErr: true}, // unknown class
		{in: "grant:werk1/#", wantErr: true},      // unknown verb
		{in: "", wantErr: true},
	}
	for _, c := range cases {
		g, err := ParseGrant(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseGrant(%q): expected error, got %+v", c.in, g)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseGrant(%q): unexpected error %v", c.in, err)
			continue
		}
		if g.Verb != c.verb || g.Prefix != c.prefix {
			t.Errorf("ParseGrant(%q) = %+v, want verb=%q prefix=%q", c.in, g, c.verb, c.prefix)
		}
		if len(c.classes) != len(g.Classes) {
			t.Errorf("ParseGrant(%q) classes = %v, want %v", c.in, g.Classes, c.classes)
		}
	}
}

func TestEntryValidate(t *testing.T) {
	good := entry("werk1/linie3/cnc5")
	if err := good.Validate(); err != nil {
		t.Fatalf("valid entry rejected: %v", err)
	}
	cases := []struct {
		name string
		mut  func(e *Entry)
	}{
		{"empty ulid", func(e *Entry) { e.ULID = "" }},
		{"short pubkey", func(e *Entry) { e.Pubkey = strings.Repeat("ab", 31) }},
		{"non-hex pubkey", func(e *Entry) { e.Pubkey = strings.Repeat("zz", 32) }},
		{"bad kind", func(e *Entry) { e.Kind = "gateway" }},
		{"node without mount", func(e *Entry) { e.Kind = KindNode; e.Mount = "" }},
		{"underscore mount", func(e *Entry) { e.Mount = "_observer" }},
		{"leading slash mount", func(e *Entry) { e.Mount = "/werk1" }},
		{"trailing slash mount", func(e *Entry) { e.Mount = "werk1/" }},
		{"empty mount segment", func(e *Entry) { e.Mount = "werk1//x" }},
		{"bad grant", func(e *Entry) { e.Grants = []string{"write:z/#"} }},
	}
	for _, c := range cases {
		e := entry("werk1/linie3/cnc5")
		c.mut(e)
		if err := e.Validate(); err == nil {
			t.Errorf("%s: expected validation error", c.name)
		}
	}
	// A machine observer (empty mount) is valid.
	obs := entry("", "read:werk1/#")
	if err := obs.Validate(); err != nil {
		t.Fatalf("observer entry rejected: %v", err)
	}
}

func TestCmdClass(t *testing.T) {
	cases := map[string]string{
		"_CmdParam":    "param",
		"_CmdOperate":  "operate",
		"_CmdMaintain": "maintain",
		"_CmdAdmin":    "admin",
		"_CmdFoo":      "admin", // unknown command contracts demand the highest class (§5.1)
	}
	for contract, want := range cases {
		if got := CmdClass(contract); got != want {
			t.Errorf("CmdClass(%s) = %q, want %q", contract, got, want)
		}
	}
}

func TestAuthorizeReadRecord(t *testing.T) {
	cases := []struct {
		name  string
		e     *Entry
		topic string
		want  bool
	}{
		{"own zone default", entry("werk1/linie3/cnc5"), "colca/v1/_Metric/01X/werk1/linie3/cnc5/temp", true},
		{"own zone root", entry("werk1"), "colca/v1/_Metric/01X/werk1", true},
		{"outside zone", entry("werk1/linie3/cnc5"), "colca/v1/_Metric/01X/werk1/linie4/x", false},
		{"prefix is not a zone boundary", entry("werk1"), "colca/v1/_Metric/01X/werk10/x", false},
		{"explicit wide grant", entry("werk1/linie3/cnc5", "read:werk2/#"), "colca/v1/_Metric/01X/werk2/a/b", true},
		{"read all", entry("", "read:#"), "colca/v1/_Metric/01X/anything/at/all", true},
		{"observer without grants", entry(""), "colca/v1/_Metric/01X/werk1/x", false},
		{"non-UNS record", entry("werk1"), "factory/raw", false},
		{"malformed UNS topic", entry("werk1"), "colca/v1/_Metric/01X", false},
	}
	for _, c := range cases {
		if got := Authorize(c.e, ActReadRecord, c.topic); got != c.want {
			t.Errorf("%s: Authorize(ReadRecord, %q) = %v, want %v", c.name, c.topic, got, c.want)
		}
	}
}

func TestAuthorizeSub(t *testing.T) {
	cases := []struct {
		name   string
		e      *Entry
		filter string
		want   bool
	}{
		{"own zone wildcard", entry("werk1/linie3"), "colca/v1/+/+/werk1/linie3/#", true},
		{"own zone deeper", entry("werk1/linie3"), "colca/v1/_Metric/+/werk1/linie3/cnc5/+", true},
		{"exact zone no wildcards", entry("werk1/linie3"), "colca/v1/_Metric/01X/werk1/linie3", true},
		{"sibling zone", entry("werk1/linie3"), "colca/v1/+/+/werk1/linie4/#", false},
		{"parent zone", entry("werk1/linie3"), "colca/v1/+/+/werk1/#", false},
		{"granted wide", entry("werk1/linie3", "read:werk1/#"), "colca/v1/+/+/werk1/#", true},
		{"grant narrower than filter", entry("", "read:werk1/linie3/#"), "colca/v1/+/+/werk1/#", false},
		{"hash all denied", entry("werk1/linie3"), "#", false},
		{"hash all with read-all", entry("", "read:#"), "#", true},
		{"uns hash denied", entry("werk1/linie3"), "colca/#", false},
		{"header-only filter", entry("werk1/linie3"), "colca/v1", false},
		{"empty fixed prefix via plus", entry("werk1/linie3"), "colca/v1/+/+/#", false},
		{"non-UNS filter", entry("werk1/linie3"), "factory/raw/+", true},
		{"non-UNS exact", entry(""), "local/announce", true},
		{"plus first segment reaches uns rule", entry("werk1/linie3"), "+/v1/+/+/werk1/linie3/#", true},
	}
	for _, c := range cases {
		if got := Authorize(c.e, ActSub, c.filter); got != c.want {
			t.Errorf("%s: Authorize(Sub, %q) = %v, want %v", c.name, c.filter, got, c.want)
		}
	}
}

// Time-sync design §2.2/§4: every authenticated machine session may subscribe
// the beacon filter regardless of its zone grants — a mount-less observer
// with NO grants at all still gets it, which a plain zone/read:# check would
// deny.
func TestAuthorizeSubTimeSyncBypassesZoneGrants(t *testing.T) {
	cases := []struct {
		name   string
		e      *Entry
		filter string
		want   bool
	}{
		{"zone-scoped machine, canonical filter", entry("werk1/linie3"), "colca/v1/_TimeSync/+", true},
		{"mountless machine with zero grants", entry(""), "colca/v1/_TimeSync/+", true},
		{"concrete node ulid, no wildcard", entry("werk1/linie3"), "colca/v1/_TimeSync/n-edge1", true},
		{"bare contract, no trailing segment", entry(""), "colca/v1/_TimeSync", true},
		{"broad colca/# wildcard is judged normally, not bypassed", entry("werk1/linie3"), "colca/#", false},
		{"broad colca/# wildcard WITH read:# still allowed (normal rule, not the bypass)", entry("", "read:#"), "colca/#", true},
		{"different contract at the same position is not the bypass", entry("werk1/linie3"), "colca/v1/_Metric/+", false},
	}
	for _, c := range cases {
		if got := Authorize(c.e, ActSub, c.filter); got != c.want {
			t.Errorf("%s: Authorize(Sub, %q) = %v, want %v", c.name, c.filter, got, c.want)
		}
	}
}

func TestAuthorizeCmd(t *testing.T) {
	withGrant := entry("hmi", "cmd:werk1/linie3/#:param,operate")
	cases := []struct {
		name  string
		e     *Entry
		topic string
		want  bool
	}{
		{"granted class in zone", withGrant, "colca/v1/_CmdParam/01T/werk1/linie3/cnc5/setpoint", true},
		{"second granted class", withGrant, "colca/v1/_CmdOperate/01T/werk1/linie3/start", true},
		{"ungranted class", withGrant, "colca/v1/_CmdMaintain/01T/werk1/linie3/calibrate", false},
		{"unknown cmd needs admin", withGrant, "colca/v1/_CmdFoo/01T/werk1/linie3/x", false},
		{"outside zone", withGrant, "colca/v1/_CmdParam/01T/werk2/x", false},
		{"no cmd grant at all", entry("werk1/linie3"), "colca/v1/_CmdParam/01T/werk1/linie3/x", false},
		{"not a command", withGrant, "colca/v1/_Metric/01T/werk1/linie3/x", false},
		{"admin class granted", entry("", "cmd:werk1/#:admin"), "colca/v1/_CmdFoo/01T/werk1/x", true},
	}
	for _, c := range cases {
		if got := Authorize(c.e, ActCmd, c.topic); got != c.want {
			t.Errorf("%s: Authorize(Cmd, %q) = %v, want %v", c.name, c.topic, got, c.want)
		}
	}
}
