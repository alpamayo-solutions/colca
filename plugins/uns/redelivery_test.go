package uns

import (
	"fmt"
	"testing"
)

// live builds a _CmdParam payload that expires well after nowMS.
func live(nowMS int64) []byte {
	return []byte(fmt.Sprintf(`{"correlation_id":"c1","expires_at":%d}`, nowMS+60_000))
}

// The target identity is topic level 4, which no hop rewrites; the route
// after it is re-prefixed at every hop. The same command is owed to the same
// machine at the leaf and at the root.
func TestOwedCommandSelectsByTargetIdentityAtEveryHop(t *testing.T) {
	const now = 1_000_000
	for _, topic := range []string{
		"colca/v1/_CmdParam/m1/m1/speed",
		"colca/v1/_CmdParam/m1/site1/edge1/m1/speed",
	} {
		if !OwedCommand(topic, live(now), "m1", now) {
			t.Errorf("m1 is not owed its own live command on %q", topic)
		}
	}
}

func TestOwedCommandRefusesAnotherIdentitysCommand(t *testing.T) {
	const now = 1_000_000
	// m2 is never handed m1's command, even though an observer with read:#
	// passes every ACL check on this topic.
	if OwedCommand("colca/v1/_CmdParam/m1/m1/speed", live(now), "m2", now) {
		t.Fatal("m2 was offered a command addressed to m1")
	}
}

func TestOwedCommandRefusesAnExpiredCommand(t *testing.T) {
	const now = 1_000_000
	expired := []byte(`{"correlation_id":"c1","expires_at":999999}`)
	if OwedCommand("colca/v1/_CmdParam/m1/m1/speed", expired, "m1", now) {
		t.Fatal("an expired command was offered for redelivery")
	}
	// The same topic and identity with a live expires_at is owed, so the
	// test does not pass for a predicate that refuses everything.
	if !OwedCommand("colca/v1/_CmdParam/m1/m1/speed", live(now), "m1", now) {
		t.Fatal("the live control case was refused, so the expiry assertion proves nothing")
	}
}

func TestOwedCommandRefusesWhatIsNotACommand(t *testing.T) {
	const now = 1_000_000
	// Only commands are owed, even if a caller points the predicate at
	// another stream.
	if OwedCommand("colca/v1/_Metric/m1/m1/speed", live(now), "m1", now) {
		t.Fatal("a metric was offered as a command")
	}
}

func TestOwedCommandRefusesAnUnnamedSubscriber(t *testing.T) {
	const now = 1_000_000
	// An empty identity matches nothing, or a caller that lost its identity
	// would drain another machine's commands.
	if OwedCommand("colca/v1/_CmdParam//m1/speed", live(now), "", now) {
		t.Fatal("an empty identity matched a command")
	}
}

func TestOwedCommandRefusesAnUngrammaticalTopic(t *testing.T) {
	const now = 1_000_000
	if OwedCommand("nonsense", live(now), "m1", now) {
		t.Fatal("an unparseable topic was offered for redelivery")
	}
}

// The cursor lives in the identity's own namespace, so one machine cannot ack
// away another's delivery floor.
func TestCommandCursorLivesInTheIdentitysOwnNamespace(t *testing.T) {
	e := &Entry{ULID: "m1", Kind: KindExternal}
	got := e.CommandCursor()
	if want := "m1/cmd"; got != want {
		t.Fatalf("CommandCursor() = %q, want %q", got, want)
	}
	if pfx := e.CursorPrefix(); got[:len(pfx)] != pfx {
		t.Fatalf("CommandCursor() = %q escapes the entry's cursor prefix %q", got, pfx)
	}
}

// The delivery floor is written only by the node. A machine acking it to
// head would silently drop every command it is owed, and a reader cursor
// named "cmd" would otherwise collide with it.
func TestOwnsCursorRefusesTheIdentitysOwnDeliveryFloor(t *testing.T) {
	e := &Entry{ULID: "m1", Kind: KindExternal}
	if e.OwnsCursor(e.CommandCursor()) {
		t.Fatal("a machine was allowed to move its own command delivery floor")
	}
	// An ordinary cursor in the same namespace is the machine's to move, so
	// the refusal above is about the reserved name only.
	if !e.OwnsCursor(e.CursorPrefix() + "my-reader") {
		t.Fatal("a machine was refused an ordinary cursor of its own")
	}
	if e.OwnsCursor("m2/my-reader") {
		t.Fatal("a machine was allowed another identity's cursor")
	}
	if (*Entry)(nil).OwnsCursor("m1/my-reader") {
		t.Fatal("a nil entry owned a cursor")
	}
}
