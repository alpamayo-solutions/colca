package tests

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// definePATAtRoot writes a personal access token definition at the root, the
// way PREKIT's API does, waits until it has reached edge 1, and returns the
// token and its definition's topic.
func definePATAtRoot(t *testing.T, tp *topo, id, owner string, grants ...string) (string, string) {
	t.Helper()
	token := "pk_pat_" + id + "_secret"
	digest := sha256.Sum256([]byte(token))
	topic := "colca/v1/_PersonalAccessToken/n-global/" + id
	api(t, tp.global, "POST", "/publish", map[string]any{
		"topic": topic,
		"payload": map[string]any{
			"id": id, "hashed_secret": hex.EncodeToString(digest[:]), "owner_sub": owner,
			"owner_email": owner + "@plant.test", "scopes": []string{"broker-mqtt"}, "grants": grants,
		},
	})
	awaitDefinitionAt(t, tp.edge1, topic, hex.EncodeToString(digest[:]))
	return token, topic
}

// awaitDefinitionAt waits until n holds the token definition at topic with
// the given digest.
func awaitDefinitionAt(t *testing.T, n *node.Node, topic, digest string) {
	t.Helper()
	id := topic[strings.LastIndex(topic, "/")+1:]
	waitFor(t, fmt.Sprintf("token %s with digest %.8s at %s", id, digest, n.Cfg.ULID), 20*time.Second, func() bool {
		for _, e := range kvAt(t, n, id) {
			rec := e.(map[string]any)
			payload, _ := rec["payload"].(map[string]any)
			if rec["topic"] == topic && payload["hashed_secret"] == digest {
				return true
			}
		}
		return false
	})
}

// editAck waits for the hub to see edge 1's answer to one _CmdEdit and returns
// its result code and message.
func editAck(t *testing.T, tp *topo, corr string) (int, string) {
	t.Helper()
	var code int
	var message string
	waitFor(t, "edit ack for "+corr+" at global", 20*time.Second, func() bool {
		for _, r := range fetchRecords(t, tp.global, "commands", unique("pat-ack"), "", 500) {
			rec := r.(map[string]any)
			topic, _ := rec["topic"].(string)
			payload, _ := rec["payload"].(map[string]any)
			if strings.HasPrefix(topic, "colca/v1/_Ack/n-edge1/") && payload["correlation_id"] == corr {
				code = int(payload["result_code"].(float64))
				message, _ = payload["message"].(string)
				return true
			}
		}
		return false
	})
	return code, message
}

// A person signed in at the hub with a personal access token edits on edge 1:
// edge 1 rebuilds her from its own copy of the token's definition, so its
// edit executor sees a person, not nobody; once the token is revoked it does
// not.
func TestAPATPersonsEditRunsAsThemOnTheEdgeThatOwnsIt(t *testing.T) {
	tp := startTopo(t)
	awaitElement(t, tp.global, "site1/edge1/m1")
	grants := []string{"cmd:" + authtest.ElementID("edge1") + "/#:configure", "read:" + authtest.ElementID("edge1") + "/#"}
	token, topic := definePATAtRoot(t, tp, "01M0ZPAT0000000000000000T1", "pat-anna", grants...)
	c := humanPublisher(t, tp.global, "pat-anna", token)

	send := func() (int, string) {
		corr := unique("pat-edit")
		// No operation: the executor answers 422 for it, but only after it
		// has a person to authorize -- without one it answers 403 first.
		payload := fmt.Sprintf(`{"correlation_id":%q,"expires_at":%d}`, corr, time.Now().Add(time.Hour).UnixMilli())
		if reason := publishHuman5(t, c, "colca/v1/_CmdEdit/n-edge1/site1/edge1/apply", payload); reason != 0 {
			t.Fatalf("PUBACK reason = %#x, want success", reason)
		}
		return editAck(t, tp, corr)
	}

	code, message := send()
	if code == 403 || strings.Contains(message, "requires a human actor") {
		t.Fatalf("edge 1 found no person behind a PAT edit: %d %s", code, message)
	}
	if code != 422 || !strings.Contains(message, "operation_id") {
		t.Fatalf("edit ack = %d %q, want the executor's own 422 for the missing operation", code, message)
	}

	// Changed at the root, and so at edge 1 (the token re-issued under the
	// same id): the command names the token edge 1 no longer holds, and edge 1
	// finds nobody behind it.
	other := sha256.Sum256([]byte("another secret"))
	api(t, tp.global, "POST", "/publish", map[string]any{"topic": topic, "payload": map[string]any{
		"id": "01M0ZPAT0000000000000000T1", "hashed_secret": hex.EncodeToString(other[:]), "owner_sub": "pat-anna",
		"owner_email": "pat-anna@plant.test", "scopes": []string{"broker-mqtt"}, "grants": grants,
	}})
	awaitDefinitionAt(t, tp.edge1, topic, hex.EncodeToString(other[:]))
	if code, message := send(); code != 403 || !strings.Contains(message, "requires a human actor") {
		t.Fatalf("a changed PAT's edit ack = %d %q, want 403 without a person", code, message)
	}
}
