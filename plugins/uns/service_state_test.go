package uns

import "testing"

func TestServiceStateBelongsToTheServiceAtItsPosition(t *testing.T) {
	sc := testScope(map[string]string{"el-press": "line1/press3"})
	unplaced := &Entry{ULID: "01SVC", Kind: KindLocal, Name: "notifications"}
	placed := &Entry{ULID: "01CON", Kind: KindLocal, Name: "connector", Element: "el-press"}
	unresolved := &Entry{ULID: "01LOST", Kind: KindLocal, Name: "lost", Element: "el-gone"}
	topic := func(path string) string { return Prefix() + ServiceStateContract + "/n1/" + path }

	cases := []struct {
		name  string
		entry *Entry
		path  string
		owns  bool
	}{
		{"unplaced, own key", unplaced, "notifications/evaluation/01A", true},
		{"unplaced, the bare position", unplaced, "notifications", false},
		{"unplaced, another service's key", unplaced, "projector/k", false},
		{"unplaced, a name that only starts like its own", unplaced, "notificationsx/k", false},
		{"placed, own key", placed, "line1/press3/connector/k", true},
		{"placed, another position in its write zone", placed, "line1/press3/other/k", false},
		{"placed at an element that does not resolve", unresolved, "lost/k", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, act := range []Action{ActPub, ActReadRecord} {
				if got := Authorize(sc, c.entry, act, topic(c.path)); got != c.owns {
					t.Fatalf("Authorize(%v) = %v, want %v", act, got, c.owns)
				}
			}
		})
	}
}

// Element read and write grants never reach it, however wide: not a person's,
// not a machine's.
func TestElementGrantsNeverReachServiceState(t *testing.T) {
	topic := Prefix() + ServiceStateContract + "/n1/notifications/k"
	for _, e := range []*Entry{
		{ULID: "kc-anna", Kind: KindHuman, Grants: []string{"read:#", "write:#", "cmd:#:admin"}},
		{ULID: "01MACH", Kind: KindExternal, Pubkey: hex64, Grants: []string{"read:#", "write:#"}},
	} {
		for _, act := range []Action{ActReadRecord, ActPub} {
			if Authorize(ns, e, act, topic) {
				t.Fatalf("%s %v reached service state", e.ULID, act)
			}
		}
		if Authorize(ns, e, ActSub, Prefix()+ServiceStateContract+"/#") {
			t.Fatalf("%s may subscribe to service state", e.ULID)
		}
	}
}

func TestServiceStateIsStateOffTheBusAndLocal(t *testing.T) {
	c := ClassOf(ServiceStateContract)
	if c != ClassServiceState || !IsState(c) || OnBus(c) || FlowsUp(c) || IsOwnedState(c) || BundleMayDeclare(c) {
		t.Fatalf("class %v: state=%v bus=%v up=%v", c, IsState(c), OnBus(c), FlowsUp(c))
	}
	if StreamFor(c) != "service_state" || IsUplinkStream("service_state") {
		t.Fatal("service state must land on service_state, which never rises")
	}
	if err := Validate(ServiceStateContract, nil); err != nil {
		t.Fatalf("a tombstone must be valid: %v", err)
	}
	for payload, ok := range map[string]bool{
		`{"version":1,"any":{"thing":[1]}}`: true,
		`{"version":3}`:                     true,
		`{}`:                                false,
		`{"version":0}`:                     false,
		`{"version":-1}`:                    false,
		`{"version":"1"}`:                   false,
		`{"version":1.5}`:                   false,
		`[1]`:                               false,
		`null`:                              false,
	} {
		if err := Validate(ServiceStateContract, []byte(payload)); (err == nil) != ok {
			t.Errorf("Validate(%s) = %v, want ok=%v", payload, err, ok)
		}
	}
}
