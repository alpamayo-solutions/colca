package uns

import (
	"fmt"
	"testing"
)

// live builds a _CmdParam payload that expires well after nowMS.
func live(nowMS int64) []byte {
	return []byte(fmt.Sprintf(`{"correlation_id":"c1","expires_at":%d}`, nowMS+60_000))
}

// The command's TARGET identity is topic level 4 (segment index 3), and it is
// the one segment no hop rewrites on the way down. Everything after it is the
// route, which every hop strips its own mount prefix from — so at the leaf the
// same command reads colca/v1/_CmdParam/{machine}/{machine}/{name} while at the
// root it read colca/v1/_CmdParam/{machine}/site1/edge1/{machine}/{name}. Both
// are owed by the same machine, which is what makes level 4 the right question
// and the route the wrong one.
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
	// m2 subscribing must never be handed m1's command — the reason the
	// selection is by identity and not by what the subscriber may READ. An
	// observer holding read:# passes every ACL check on this topic and is
	// still owed nothing here.
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
	// The denominator: the SAME topic and identity, differing only in
	// expires_at, must be owed — otherwise this test would pass just as well
	// against a predicate that refuses everything.
	if !OwedCommand("colca/v1/_CmdParam/m1/m1/speed", live(now), "m1", now) {
		t.Fatal("the live control case was refused, so the expiry assertion proves nothing")
	}
}

func TestOwedCommandRefusesWhatIsNotACommand(t *testing.T) {
	const now = 1_000_000
	// The commands stream holds only commands, so this can only fire if a
	// caller points the predicate at another stream. It must fail closed
	// rather than trust its caller's aim.
	if OwedCommand("colca/v1/_Metric/m1/m1/speed", live(now), "m1", now) {
		t.Fatal("a metric was offered as a command")
	}
}

func TestOwedCommandRefusesAnUnnamedSubscriber(t *testing.T) {
	const now = 1_000_000
	// Same fail-closed rule as Ancestry.Covers: an empty identity must not
	// read as "matches anything". Without this guard a caller that lost its
	// identity would drain another machine's commands onto the bus.
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

// The cursor lives in the identity's OWN namespace, the same boundary /fetch
// and /ack use to refuse one identity moving another's cursor. If it did not,
// a machine could ack away the delivery floor of another.
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

// The delivery floor lives inside the machine's own cursor
// namespace so that nobody ELSE can move it — but the node is its only
// legitimate writer. A machine that acked its own floor to head through /ack
// would silently discard every command it is owed: no error, no metric, no
// replay. It is also a plain collision: a machine using /fetch and /ack with a
// cursor it happens to call "cmd" would otherwise share the broker's floor.
func TestOwnsCursorRefusesTheIdentitysOwnDeliveryFloor(t *testing.T) {
	e := &Entry{ULID: "m1", Kind: KindExternal}
	if e.OwnsCursor(e.CommandCursor()) {
		t.Fatal("a machine was allowed to move its own command delivery floor")
	}
	// The denominator: an ordinary cursor in the SAME namespace is the
	// machine's to move, so the refusal above is about this one reserved name
	// rather than the prefix check failing outright and refusing everything.
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
