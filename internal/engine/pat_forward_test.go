package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// putPAT defines a personal access token on e's node, as the definition that
// descends the tree, and returns the token.
func putPAT(t *testing.T, e *Engine, id string, grants []string) string {
	t.Helper()
	token := "pk_pat_" + id + "_secret"
	digest := sha256.Sum256([]byte(token))
	payload, _ := json.Marshal(uns.PersonalAccessToken{
		ID: id, HashedSecret: hex.EncodeToString(digest[:]), OwnerSub: "kc-sub-anna",
		OwnerEmail: "anna@plant.test", Scopes: []string{"mcp"}, Grants: grants,
	})
	if _, err := e.EntityStore().PublishBatch(uns.CommandContext{}, []uns.StateRecord{{
		Topic: "colca/v1/" + uns.PersonalAccessTokenContract + "/n-edge1/" + id, Payload: payload,
	}}); err != nil {
		t.Fatal(err)
	}
	return token
}

// A person who signed in with a personal access token carries no groups: her
// grants are on the token. Her command names the token (ActorCredential), and
// the node it comes down to authorizes her against its own definition of that
// token, never wider than it, and not at all once the token is gone there.
func TestACommandAdmittedWithAPATRunsAsThatPersonWhereItComesDown(t *testing.T) {
	rec := &recordingExec{contract: "_CmdEdit"}
	e := execEngine(t, Executors(rec))
	pats := uns.NewPersonalAccessTokenIndex(e.EntityStore())
	e.SetPersonalAccessTokens(pats)
	const topic = "colca/v1/_CmdEdit/n-edge1/apply"
	token := putPAT(t, e, "01M0ZPAT0000000000000000A1", []string{"cmd:#:configure"})

	// At the admitting door the verified entry names the token, and so does
	// the stored command.
	_, anna, err := pats.Authenticate(token, uns.ScopeAPI, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.IngestHuman(anna, topic, cmdPayload("c-door")); err != nil {
		t.Fatal(err)
	}
	stored := storedCommand(t, e, topic)
	if stored.ActorCredential != anna.Credential || anna.Credential == "" {
		t.Fatalf("stored credential = %q, want %q", stored.ActorCredential, anna.Credential)
	}
	if stored.Door != doorHuman {
		t.Fatalf("door = %q: the token's grants alone must authorize it, so forwarding can check again", stored.Door)
	}
	if refusal := e.ForwardRefusal(stored); refusal != "" {
		t.Fatalf("forwarding refused a PAT person's authorized command: %s", refusal)
	}

	// Come down from the parent: rebuilt from this node's definition.
	attributed := Attribution{
		WrittenBy: "n-hub", ActorID: "kc-sub-anna", ActorLabel: "anna@plant.test", ActorKind: "human",
		ActorCredential: anna.Credential,
	}
	ts := time.Now().UnixMilli()
	if _, err := e.IngestDownlinkAttributed(topic, cmdPayload("c-down"), ts, attributed, nil); err != nil {
		t.Fatal(err)
	}
	got := rec.actors[len(rec.actors)-1]
	if got == nil || got.ULID != "kc-sub-anna" || len(got.Grants) != 1 || got.Grants[0] != "cmd:#:configure" ||
		got.SignedInHere {
		t.Fatalf("downlink actor = %+v, want anna with the token's grants, signed in elsewhere", got)
	}
	if source := uns.AnnotationSource(got); source != "user/kc-sub-anna" {
		t.Fatalf("annotation source = %q", source)
	}

	// Another person naming her token, or a token this node no longer
	// defines, is nobody: the executor refuses a _CmdEdit without an actor.
	for name, attribution := range map[string]Attribution{
		"another person": {WrittenBy: "n-hub", ActorID: "kc-sub-bob", ActorKind: "human", ActorCredential: anna.Credential},
		"unknown token":  {WrittenBy: "n-hub", ActorID: "kc-sub-anna", ActorKind: "human", ActorCredential: "01M0ZPAT0000000000000000ZZ:" + hex.EncodeToString(make([]byte, 32))},
	} {
		if _, err := e.IngestDownlinkAttributed(topic, cmdPayload("c-"+name), ts, attribution, nil); err != nil {
			t.Fatal(name, err)
		}
		if got := rec.actors[len(rec.actors)-1]; got != nil {
			t.Fatalf("%s: downlink actor = %+v, want none", name, got)
		}
	}
}
