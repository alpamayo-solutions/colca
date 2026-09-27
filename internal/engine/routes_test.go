package engine

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// announce stores a service's _ServiceDetails with the commands it executes.
func announce(t *testing.T, e *Engine, service string, active bool, routes ...commandRoute) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"id": service, "name": service, "service_type": "dataops", "colca_node_id": "n-edge1",
		"is_active": active, "commands": routes,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := service + "/_service"
	if _, _, err := e.Store().Append("entities", []store.Record{{
		Topic: "colca/v1/_ServiceDetails/n-edge1/" + path, Payload: payload, TS: 1, KVPath: path, KVNode: "n-edge1",
	}}); err != nil {
		t.Fatal(err)
	}
}

func param(path string) string { return "colca/v1/_CmdParam/n-edge1/" + path }

func lastAck(t *testing.T, delivered *[]delivery) CommandOutcome {
	t.Helper()
	if len(*delivered) == 0 {
		t.Fatal("no ack delivered")
	}
	var outcome CommandOutcome
	if err := json.Unmarshal([]byte((*delivered)[len(*delivered)-1].Payload), &outcome); err != nil {
		t.Fatal(err)
	}
	return outcome
}

// A verb nobody announces, at an element where others are announced, is answered
// 404 with the verbs that element takes, and is not stored. A repeat gets the same
// answer, and the ack reaches the sender.
func TestAnUnannouncedVerbIsAnswered404(t *testing.T) {
	e, delivered := ledgerEngine(t)
	announce(t, e, "dataops-line", true, commandRoute{"_CmdParam", "line1/operator/setDensity"})
	announce(t, e, "dataops-performance", true, commandRoute{"_CmdParam", "line1/operator/setSandoff"})
	anna := humanEntry(t, "cmd:#:param")
	stored := e.Store().NextOffset("commands")

	for _, verb := range []string{"setProduct", "setdensity"} {
		res, err := e.IngestHuman(anna, param("line1/operator/"+verb), cmdPayload("c-"+verb))
		if err != nil || !res.Answered || res.Persisted || res.Command == nil || res.Command.ResultCode != 404 {
			t.Fatalf("%s: %+v, %v", verb, res, err)
		}
		ack := lastAck(t, delivered)
		if ack.ResultCode != 404 || ack.CorrelationID != "c-"+verb ||
			!strings.Contains(ack.Message, "line1/operator takes setDensity, setSandoff") {
			t.Fatalf("%s: ack %+v", verb, ack)
		}
		topic := (*delivered)[len(*delivered)-1].Topic
		if topic != "colca/v1/_Ack/n-edge1/line1/operator/"+verb {
			t.Fatalf("ack topic %s", topic)
		}
		if recipient, _ := e.AckRecipient(topic, []byte((*delivered)[len(*delivered)-1].Payload)); recipient != anna.ULID {
			t.Fatalf("ack goes to %q", recipient)
		}
	}
	if got := e.Store().NextOffset("commands"); got != stored {
		t.Fatalf("an unannounced command was stored: next %d, want %d", got, stored)
	}

	*delivered = nil
	res, err := e.IngestHuman(anna, param("line1/operator/setProduct"), cmdPayload("c-setProduct"))
	if err != nil || res.Command == nil || res.Command.ResultCode != 404 || len(*delivered) != 1 {
		t.Fatalf("repeat: %+v %v delivered %v", res, err, *delivered)
	}
}

// Announced commands, commands at elements nobody announces for (a machine or a
// service that does not announce), and commands of a service that is down pass
// on to their executor as before.
func TestAnnouncedAndUnclaimedCommandsPassOn(t *testing.T) {
	e, delivered := ledgerEngine(t)
	announce(t, e, "dataops-line", false, commandRoute{"_CmdParam", "line1/operator/setDensity"})
	announce(t, e, "bqc", true, commandRoute{"_CmdParam", "line1/bqc/+"})
	anna := humanEntry(t, "cmd:#:param")
	for _, path := range []string{"line1/operator/setDensity", "line1/bqc/setTarget", "line1/m2/speedSetpoint", "site/setConfiguration"} {
		res, err := e.IngestHuman(anna, param(path), cmdPayload("c-"+path))
		if err != nil || !res.Persisted || res.Answered {
			t.Fatalf("%s: %+v, %v", path, res, err)
		}
	}
	for _, d := range *delivered {
		if strings.Contains(d.Topic, "/_Ack/") {
			t.Fatalf("the node answered a command it should pass on: %v", d)
		}
	}
	// Another contract at an announced path is not announced.
	res, err := e.IngestHuman(humanEntry(t, "cmd:#:operate"), "colca/v1/_CmdOperate/n-edge1/line1/operator/setDensity", cmdPayload("op"))
	if err != nil || !res.Answered || !strings.Contains(res.Command.Message, "setDensity (_CmdParam)") {
		t.Fatalf("other contract: %+v %v", res, err)
	}
}

// A command payload the node refuses gets a 400 ack when its correlation id can
// be read, NaN included.
func TestARefusedCommandPayloadIsAnswered400(t *testing.T) {
	e, delivered := ledgerEngine(t)
	str := map[string]any{"type": "string", "minLength": 1}
	e.SetContracts(writeBundle(t, map[string]any{
		"_CmdParam": obj("cmd", false, []string{"correlation_id", "expires_at"}, map[string]any{
			"correlation_id": str, "expires_at": map[string]any{"type": "number"}, "command": map[string]any{"type": "object"},
		}),
	}))
	anna := humanEntry(t, "cmd:#:param")
	for id, payload := range map[string]string{
		"null-command": `{"correlation_id":"null-command","expires_at":` + jsonNumber(futureMS()) + `,"command":null}`,
		"not-object":   `{"correlation_id":"not-object","expires_at":` + jsonNumber(futureMS()) + `,"command":[1]}`,
		"nan":          `{"correlation_id":"nan","expires_at":` + jsonNumber(futureMS()) + `,"command":{"value":NaN}}`,
		"deep": `{"correlation_id":"deep","expires_at":` + jsonNumber(futureMS()) + `,"command":{"station":` +
			strings.Repeat("[", 500) + strings.Repeat("]", 500) + `,"direction":1}}`,
	} {
		*delivered = nil
		if _, err := e.IngestHuman(anna, param("line1/operator/setDensity"), []byte(payload)); err == nil {
			t.Fatalf("%s: accepted", id)
		}
		ack := lastAck(t, delivered)
		if ack.ResultCode != 400 || ack.CorrelationID != id || !strings.HasPrefix(ack.Message, "command refused: ") {
			t.Fatalf("%s: ack %+v", id, ack)
		}
	}
	*delivered = nil
	if _, err := e.IngestHuman(anna, param("line1/operator/setDensity"), []byte(`{"command":NaN}`)); err == nil || len(*delivered) != 0 {
		t.Fatalf("no correlation id, nobody to answer: %v %v", err, *delivered)
	}
}

// A payload nested deeper than MaxPayloadDepth is refused at publish, with the
// floor rules as with a bundle, and the refusal names the limit.
func TestAPayloadNestedTooDeepIsRefused(t *testing.T) {
	nested := func(levels int) []byte {
		return []byte(`{"correlation_id":"c` + strconv.Itoa(levels) + `","expires_at":` + jsonNumber(futureMS()) + `,"command":` +
			strings.Repeat(`{"a":`, levels-1) + `1` + strings.Repeat("}", levels-1) + `}`)
	}
	e, delivered := ledgerEngine(t)
	anna := humanEntry(t, "cmd:#:param")
	if _, err := e.IngestHuman(anna, param("line1/operator/setDensity"), nested(MaxPayloadDepth)); err != nil {
		t.Fatalf("%d levels refused: %v", MaxPayloadDepth, err)
	}
	*delivered = nil
	_, err := e.IngestHuman(anna, param("line1/operator/setDensity"), nested(MaxPayloadDepth+1))
	if err == nil || !strings.Contains(err.Error(), "deeper than 32 levels") {
		t.Fatalf("%d levels: %v", MaxPayloadDepth+1, err)
	}
	if ack := lastAck(t, delivered); ack.ResultCode != 400 || !strings.Contains(ack.Message, "deeper than 32 levels") {
		t.Fatalf("ack %+v", ack)
	}
}

func TestNestedDeeperThan(t *testing.T) {
	for _, c := range []struct {
		payload string
		want    bool
	}{
		{`{"a":[1]}`, false},
		{`{"a":[[1]]}`, true},
		{`{"a":"[[[[[["}`, false},
		{`{"a":"\\\"[[[["}`, false},
		{`[[`, false},
		{`[[[`, true},
	} {
		if got := nestedDeeperThan([]byte(c.payload), 2); got != c.want {
			t.Errorf("%s: %v", c.payload, got)
		}
	}
}

func jsonNumber(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestRouteMatches(t *testing.T) {
	for _, c := range []struct {
		pattern, path string
		want          bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b/C", false},
		{"a/+/c", "a/x/c", true},
		{"a/+", "a/x/c", false},
		{"a/#", "a/x/c", true},
		{"a/#", "a", true},
		{"#", "a/b", true},
		{"a/b", "a/b/c", false},
	} {
		if got := routeMatches(c.pattern, c.path); got != c.want {
			t.Errorf("routeMatches(%q, %q) = %v", c.pattern, c.path, got)
		}
	}
}

// With commands.strict, a command at an element where nobody announced anything
// is answered 404 as well; announced commands still pass.
func TestStrictCommandsAnswerEveryUnannouncedCommand(t *testing.T) {
	e, delivered := ledgerEngine(t)
	e.cfg.Commands.Strict = true
	announce(t, e, "dataops-line", true, commandRoute{"_CmdParam", "line1/operator/setDensity"})
	anna := humanEntry(t, "cmd:#:param")
	stored := e.Store().NextOffset("commands")

	for _, path := range []string{"line1/m2/head3/setHeight", "unknown-site/x/setY", "setZ"} {
		res, err := e.IngestHuman(anna, param(path), cmdPayload("c-"+path))
		if err != nil || !res.Answered || res.Persisted || res.Command == nil || res.Command.ResultCode != 404 {
			t.Fatalf("%s: %+v, %v", path, res, err)
		}
		if ack := lastAck(t, delivered); ack.ResultCode != 404 || !strings.Contains(ack.Message, "nothing is announced at") {
			t.Fatalf("%s: ack %+v", path, ack)
		}
	}
	if got := e.Store().NextOffset("commands"); got != stored {
		t.Fatalf("an unannounced command was stored: next %d, want %d", got, stored)
	}

	res, err := e.IngestHuman(anna, param("line1/operator/setDensity"), cmdPayload("c-ok"))
	if err != nil || res.Answered || !res.Persisted {
		t.Fatalf("an announced command did not pass: %+v, %v", res, err)
	}
}

// A command no grant of its sender covers is refused, answered with a 403 ack that
// reaches the sender, and not stored.
func TestADeniedCommandIsAnswered403(t *testing.T) {
	e, delivered := ledgerEngine(t)
	anna := humanEntry(t, "cmd:#:operate")
	stored := e.Store().NextOffset("commands")

	if _, err := e.IngestHuman(anna, "colca/v1/_CmdMaintain/n-edge1/line1/bqc/reset", cmdPayload("c-denied")); err == nil {
		t.Fatal("a command without a covering grant was accepted")
	}
	var acks []delivery
	for _, d := range *delivered {
		if strings.Contains(d.Topic, "/_Ack/") {
			acks = append(acks, d)
		}
	}
	if len(acks) != 1 || acks[0].Topic != "colca/v1/_Ack/n-edge1/line1/bqc/reset" {
		t.Fatalf("acks %v", acks)
	}
	ack := lastAck(t, &acks)
	if ack.ResultCode != 403 || ack.CorrelationID != "c-denied" || !strings.Contains(ack.Message, "_CmdMaintain line1/bqc/reset") {
		t.Fatalf("ack %+v", ack)
	}
	if recipient, _ := e.AckRecipient(acks[0].Topic, []byte(acks[0].Payload)); recipient != anna.ULID {
		t.Fatalf("ack goes to %q", recipient)
	}
	if got := e.Store().NextOffset("commands"); got != stored {
		t.Fatalf("a denied command was stored: next %d, want %d", got, stored)
	}
}
