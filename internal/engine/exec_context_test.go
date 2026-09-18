package engine

import (
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The executor learns the acting identity from the engine only: the verified
// entry from a door, nobody from the admin door, and for a downlinked command the
// person reconstituted from the groups on the record.
func TestExecutorReceivesTheActingEntry(t *testing.T) {
	rec := &recordingExec{contract: "_CmdEdit"}
	e := execEngine(t, Executors(rec))
	const topic = "colca/v1/_CmdEdit/n-edge1/apply"

	// Human door: the verified token entry, groups and all.
	anna, problems, err := uns.TokenEntryWithGroups("kc-sub-anna", []string{"cmd:#:configure"}, []string{"operators"}, nil)
	if err != nil || len(problems) != 0 {
		t.Fatal(err, problems)
	}
	if _, err := e.IngestHuman(anna, topic, cmdPayload("c-human")); err != nil {
		t.Fatal(err)
	}
	// Admin door: a token, not an identity.
	if _, err := e.IngestAdmin(topic, cmdPayload("c-admin")); err != nil {
		t.Fatal(err)
	}
	if len(rec.actors) != 2 {
		t.Fatalf("executor calls = %v", rec.calls)
	}
	if got := rec.actors[0]; got == nil || got.Kind != uns.KindHuman || got.ULID != "kc-sub-anna" ||
		len(got.Groups) != 1 || got.Groups[0] != "operators" {
		t.Fatalf("human door actor = %+v, want anna with her groups", got)
	}
	if rec.actors[1] != nil {
		t.Fatalf("admin door actor = %+v, want none", rec.actors[1])
	}

	// Downlink: this node holds an `operators` group definition of its own;
	// the record carries anna's sub and group ids, never her grants.
	if _, err := e.EntityStore().PublishBatch(uns.CommandContext{}, []uns.StateRecord{{
		Topic: "colca/v1/_Group/n-edge1/operators", Payload: []byte(`{"id":"operators","grants":["cmd:#:configure"]}`),
	}}); err != nil {
		t.Fatal(err)
	}
	ts := time.Now().UnixMilli()
	if _, err := e.IngestDownlinkAttributed(topic, cmdPayload("c-down"), ts, Attribution{
		WrittenBy: "n-hub", ActorID: "kc-sub-anna", ActorLabel: "anna", ActorKind: "human",
		ActorGroups: []string{"operators"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(rec.actors) != 3 {
		t.Fatalf("downlink did not execute: %v", rec.calls)
	}
	if got := rec.actors[2]; got == nil || got.Kind != uns.KindHuman || got.ULID != "kc-sub-anna" ||
		len(got.Grants) != 1 || got.Grants[0] != "cmd:#:configure" || len(got.Groups) != 1 {
		t.Fatalf("downlink actor = %+v, want anna reconstituted with this node's operators grants", got)
	}

	// A downlinked command without attested groups, or from a service, is
	// nobody at the executor: it must not pass as a person.
	for name, attribution := range map[string]Attribution{
		"human without groups": {WrittenBy: "n-hub", ActorID: "kc-sub-bob", ActorKind: "human"},
		"service":              {WrittenBy: "n-hub", ActorID: "api", ActorKind: "service", ActorGroups: []string{"operators"}},
	} {
		if _, err := e.IngestDownlinkAttributed(topic, cmdPayload("c-"+name), ts, attribution); err != nil {
			t.Fatal(name, err)
		}
		if got := rec.actors[len(rec.actors)-1]; got != nil {
			t.Fatalf("%s: downlink actor = %+v, want none", name, got)
		}
	}
}

// A local service may attest a person's group ids when it cannot forward their
// token. The command is then judged as that person at the door and at the
// executor: the service's configure grant never applies and an unknown group adds
// nothing.
func TestALocalServiceAttestingAPersonIsJudgedAsThatPerson(t *testing.T) {
	ids := fakeIDs{entries: map[string]*uns.Entry{
		"svc-api": {ULID: "svc-api", Kind: uns.KindLocal, Name: "api"},
	}}
	e := newEngineWithIDs(t, ids)
	rec := &recordingExec{contract: "_CmdEdit"}
	e.SetExecutor(Executors(rec))
	if _, err := e.EntityStore().PublishBatch(uns.CommandContext{}, []uns.StateRecord{{
		Topic: "colca/v1/_Group/n-edge1/operators", Payload: []byte(`{"id":"operators","grants":["cmd:#:configure"]}`),
	}}); err != nil {
		t.Fatal(err)
	}
	const topic = "colca/v1/_CmdEdit/n-edge1/apply"
	attest := func(groups ...string) Attribution {
		return Attribution{ActorID: "kc-sub-anna", ActorLabel: "anna", ActorKind: "human", ActorGroups: groups}
	}

	// The service as itself: no implicit _CmdEdit any more.
	if _, err := e.IngestClient("svc-api", topic, cmdPayload("c-self")); err == nil {
		t.Fatal("a local service issued _CmdEdit under its own identity")
	}
	// Attesting a group this node holds: judged as anna, executed as anna.
	if _, err := e.IngestLocalAttributed("svc-api", topic, cmdPayload("c-anna"), attest("operators")); err != nil {
		t.Fatalf("attested person with a covering group refused: %v", err)
	}
	if len(rec.actors) != 1 || !rec.actors[0].IsHuman() || rec.actors[0].ULID != "kc-sub-anna" ||
		len(rec.actors[0].Grants) != 1 {
		t.Fatalf("executor actor = %+v, want anna with the operators grants", rec.actors)
	}
	// Attesting a group this node does not hold buys nothing: refused at
	// the door, never reaching the executor.
	if _, err := e.IngestLocalAttributed("svc-api", topic, cmdPayload("c-ghost"), attest("ghosts")); err == nil {
		t.Fatal("an unresolvable group authorized a command")
	}
	if len(rec.actors) != 1 {
		t.Fatalf("a refused command reached the executor: %v", rec.calls)
	}
}

// At the human door the same person with a covering configure grant is refused
// _CmdConfigure and admitted _CmdEdit.
func TestTheHumanDoorRefusesConfigureAndAdmitsEdit(t *testing.T) {
	rec := &recordingExec{contract: "_CmdEdit"}
	e := execEngine(t, Executors(rec))
	anna := humanEntry(t, "cmd:#:configure")
	if _, err := e.IngestHuman(anna, "colca/v1/_CmdConfigure/n-edge1/definition/upsert", cmdPayload("c-cfg")); err == nil {
		t.Fatal("a person published _CmdConfigure on the human door")
	}
	if _, err := e.IngestHuman(anna, "colca/v1/_CmdEdit/n-edge1/apply", cmdPayload("c-wb")); err != nil {
		t.Fatalf("the same person's _CmdEdit was refused: %v", err)
	}
	if len(rec.actors) != 1 || !rec.actors[0].IsHuman() {
		t.Fatalf("executor actors = %+v, want anna once", rec.actors)
	}
}
