package uns

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

// A tag binds to at most one signal. These tests cover the three ways a
// signal comes to hold a tag at the configure door: a declared bind_intent
// answered by a catalogue, an explicit data_tag on signal/upsert, and a
// take_over that moves a tag from a minted signal to a declared one.

const opcuaCatalogue = "colca/v1/_DataTags/n1/line1/opcua-1"

// intentNode is a node with the lifecycle trigger on and connector opcua-1
// (01JCONN) enrolled at line1, which has published nothing yet.
func intentNode(t *testing.T) *ConfigExec {
	t.Helper()
	c := newTriggerConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	return c
}

// arrive publishes a catalogue the way a connector does: the record is stored,
// then the node observes it.
func arrive(t *testing.T, c *ConfigExec, tags []map[string]any) {
	t.Helper()
	publishCatalogue(t, c, opcuaCatalogue, tags)
	c.Observe("_DataTags", opcuaCatalogue, mustJSON(map[string]any{"data_tags": tags}))
}

func intent(connector, variable string) map[string]any {
	return map[string]any{"bind_intent": map[string]any{"connector": connector, "variable": variable}}
}

// holdersOf lists the paths of the signals bound to tag.
func holdersOf(c *ConfigExec, tag string) []string {
	var out []string
	for path, s := range signalsAt(c) {
		if s.DataTag == tag {
			out = append(out, path)
		}
	}
	return out
}

func upsertSignal(t *testing.T, c *ConfigExec, path string, signal map[string]any, takeOver bool) (int, string) {
	t.Helper()
	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/upsert", body(t, map[string]any{
		"signals": []map[string]any{{"path": path, "signal": signal, "take_over": takeOver}},
	}))
	return code, msg
}

// The reported case: the declared signal sits away from the connector's
// mount, so name adoption cannot find it. Its bind_intent binds it when the
// catalogue arrives, and no second signal is minted for the tag.
func TestALateCatalogueBindsTheIntendedSignalWhereverItSits(t *testing.T) {
	c := intentNode(t)
	place(t, c, "01HPRESS", "line1/Press")
	declareSignal(t, c, "line1/Press/DrumTemperature", "01SDECLARED", "01HPRESS", intent("opcua-1", "tag-t1"))

	arrive(t, c, tags("t1", "t2"))

	if got := holdersOf(c, "t1"); len(got) != 1 || got[0] != "line1/Press/DrumTemperature" {
		t.Fatalf("t1 is held by %v, want only the declared signal", got)
	}
	record := signalRecordAt(t, c, "line1/Press/DrumTemperature")
	if _, kept := record["bind_intent"]; kept {
		t.Fatalf("the answered intent is still on the record: %+v", record)
	}
	if record["id"] != "01SDECLARED" || record["data_type"] != "float" || record["is_autobound"] != nil {
		t.Fatalf("bound record = %+v, want the declared signal, typed by the tag, not marked as minted", record)
	}
	if _, minted := signalsAt(c)["line1/tag-t1"]; minted {
		t.Fatal("a signal was minted for a tag a declared signal waits for")
	}
	if got := signalRecordAt(t, c, "line1/tag-t2"); got["is_autobound"] != true {
		t.Fatalf("the undeclared tag's minted signal = %+v, want it marked is_autobound", got)
	}
}

// An intent is explicit, so it wins over a guess: a declared signal standing
// at the tag's own name under the mount stays unbound when another signal
// names the tag in its bind_intent.
func TestAnIntentBindsBeforeNameAdoption(t *testing.T) {
	c := intentNode(t)
	declareSignal(t, c, "line1/tag-t1", "01SBYNAME", "01HLINE1", nil)
	declareSignal(t, c, "line1/Temperature", "01SBYINTENT", "01HLINE1", intent("opcua-1", "tag-t1"))

	arrive(t, c, tags("t1"))

	if got := holdersOf(c, "t1"); len(got) != 1 || got[0] != "line1/Temperature" {
		t.Fatalf("t1 is held by %v, want only the signal whose intent names it", got)
	}
	if got := signalRecordAt(t, c, "line1/tag-t1")["data_tag"]; got != nil {
		t.Fatalf("the name match was bound too: data_tag = %v", got)
	}
}

// A signal waiting for one tag is never adopted by name for another: its
// intent names the only tag that binds it.
func TestAWaitingSignalIsNotAdoptedForAnotherTag(t *testing.T) {
	c := intentNode(t)
	declareSignal(t, c, "line1/tag-t1", "01SWAITING", "01HLINE1", intent("opcua-1", "tag-t9"))

	arrive(t, c, tags("t1"))

	if got := signalRecordAt(t, c, "line1/tag-t1")["data_tag"]; got != nil {
		t.Fatalf("the waiting signal was bound to t1 by its name: data_tag = %v", got)
	}
	if got := holdersOf(c, "t1"); len(got) != 1 {
		t.Fatalf("t1 is held by %v, want one minted signal", got)
	}
}

// The intent may name the connector by ULID and the tag by its source.
func TestAnIntentNamesTheConnectorByULIDAndTheTagBySource(t *testing.T) {
	c := intentNode(t)
	declareSignal(t, c, "line1/Speed", "01SSPEED", "01HLINE1", intent("01JCONN", "ns=2;s=Speed"))

	arrive(t, c, []map[string]any{{"id": "t1", "name": "Speed", "source": "ns=2;s=Speed", "data_type": "float"}})

	if got := holdersOf(c, "t1"); len(got) != 1 || got[0] != "line1/Speed" {
		t.Fatalf("t1 is held by %v, want the signal whose intent names its source", got)
	}
}

// An intent for another connector, or for a tag this catalogue does not hold,
// binds nothing and keeps waiting.
func TestAnIntentForAnotherConnectorKeepsWaiting(t *testing.T) {
	c := intentNode(t)
	declareSignal(t, c, "line1/Other", "01SOTHER", "01HLINE1", intent("modbus-1", "tag-t1"))

	arrive(t, c, tags("t1"))

	record := signalRecordAt(t, c, "line1/Other")
	if record["data_tag"] != nil || record["bind_intent"] == nil {
		t.Fatalf("record = %+v, want it unbound and still waiting", record)
	}
}

// Two signals waiting for one tag contradict each other. Neither is bound, and
// no third signal is minted beside them.
func TestTwoIntentsForOneTagBindNeitherAndMintNothing(t *testing.T) {
	c := intentNode(t)
	declareSignal(t, c, "line1/A", "01SA", "01HLINE1", intent("opcua-1", "tag-t1"))
	declareSignal(t, c, "line1/B", "01SB", "01HLINE1", intent("opcua-1", "tag-t1"))
	publishCatalogue(t, c, opcuaCatalogue, tags("t1"))

	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	if code != 200 || !strings.Contains(msg, `"ambiguous":1`) {
		t.Fatalf("autobind = %d %q, want the contradiction reported", code, msg)
	}
	if got := holdersOf(c, "t1"); len(got) != 0 {
		t.Fatalf("t1 is held by %v, want nothing bound", got)
	}
	if len(signalsAt(c)) != 2 {
		t.Fatalf("signals = %+v, want only the two declared", signalsAt(c))
	}
}

// The same catalogue again changes nothing: the intent was answered and
// dropped, and the tag stays with the signal that took it.
func TestARepublishAfterAnIntentBindIsIdempotent(t *testing.T) {
	c := intentNode(t)
	declareSignal(t, c, "line1/Temperature", "01SDECLARED", "01HLINE1", intent("opcua-1", "tag-t1"))
	arrive(t, c, tags("t1"))
	before := signalsAt(c)

	arrive(t, c, tags("t1"))
	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	if code != 200 || !strings.Contains(msg, `"created":0`) {
		t.Fatalf("autobind = %d %q, want nothing created", code, msg)
	}
	if after := signalsAt(c); len(after) != len(before) || after["line1/Temperature"].DataTag != "t1" {
		t.Fatalf("signals after republish = %+v, want %+v", after, before)
	}
}

// A connector that is already running gets its intents answered on any
// publish, not only its first: after a restart the trigger has seen nothing,
// finds tags bound, and still binds the free tag a declared signal waits for.
func TestAnIntentIsAnsweredOnARepublishWithBoundTags(t *testing.T) {
	c := intentNode(t)
	declareSignal(t, c, "line1/tag-t1", "01SBOUND", "01HLINE1", map[string]any{"data_tag": "t1"})
	declareSignal(t, c, "line1/Pressure", "01SWAITING", "01HLINE1", intent("opcua-1", "tag-t2"))

	arrive(t, c, tags("t1", "t2"))

	if got := holdersOf(c, "t2"); len(got) != 1 || got[0] != "line1/Pressure" {
		t.Fatalf("t2 is held by %v, want the waiting signal", got)
	}
}

// A declaration whose tag the node already holds binds at once, so it does not
// wait for a catalogue publish that may never come.
func TestAnUpsertWithAnIntentBindsAtOnceWhenTheTagIsThere(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HLINE1", "line1")
	bindEntry(t, c, "01JCONN", "opcua-1", "01HLINE1")
	publishCatalogue(t, c, opcuaCatalogue, tags("t1"))

	declareSignal(t, c, "line1/Temperature", "01SDECLARED", "01HLINE1", intent("opcua-1", "tag-t1"))

	record := signalRecordAt(t, c, "line1/Temperature")
	if record["data_tag"] != "t1" || record["bind_intent"] != nil {
		t.Fatalf("record = %+v, want it bound to t1 with the intent dropped", record)
	}
}

// A redeclaration that does not speak bind_intent keeps it; an explicit null
// clears it.
func TestAnIntentSurvivesARedeclarationThatDoesNotSpeakIt(t *testing.T) {
	c := intentNode(t)
	declareSignal(t, c, "line1/Temperature", "01SDECLARED", "01HLINE1", intent("opcua-1", "tag-t1"))

	if code, msg := upsertSignal(t, c, "line1/Temperature", map[string]any{"id": "01SDECLARED", "name": "Temperature"}, false); code != 200 {
		t.Fatalf("redeclare = %d %q", code, msg)
	}
	if signalRecordAt(t, c, "line1/Temperature")["bind_intent"] == nil {
		t.Fatal("a redeclaration that did not mention the intent dropped it")
	}
	if code, msg := upsertSignal(t, c, "line1/Temperature", map[string]any{"id": "01SDECLARED", "name": "Temperature", "bind_intent": nil}, false); code != 200 {
		t.Fatalf("clear = %d %q", code, msg)
	}
	if _, kept := signalRecordAt(t, c, "line1/Temperature")["bind_intent"]; kept {
		t.Fatal("an explicit null did not clear the intent")
	}
}

// ── one tag, one signal ──────────────────────────────────────────────────

// mintedHolder lets autobind mint a signal for t1 at line1/tag-t1, then
// declares 01SDECLARED at line1/Press/Temp. This is how a node ended up with
// two signals for one tag before the rule.
func mintedHolder(t *testing.T) *ConfigExec {
	t.Helper()
	c := intentNode(t)
	arrive(t, c, tags("t1"))
	place(t, c, "01HPRESS", "line1/Press")
	declareSignal(t, c, "line1/Press/Temp", "01SDECLARED", "01HPRESS", nil)
	return c
}

// Binding a tag another signal holds is refused, naming the holder by id and
// path, and the refusal offers take_over because autobind minted the holder.
func TestUpsertRefusesATagAnotherSignalHolds(t *testing.T) {
	c := mintedHolder(t)
	holder := signalsAt(c)["line1/tag-t1"].ID

	code, msg := upsertSignal(t, c, "line1/Press/Temp", map[string]any{"id": "01SDECLARED", "name": "Temp", "data_tag": "t1"}, false)

	if code != 409 {
		t.Fatalf("upsert = %d %q, want 409", code, msg)
	}
	for _, want := range []string{"tag t1 is already bound to signal " + holder + " at line1/tag-t1", "created by autobind", "take_over"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("refusal %q does not say %q", msg, want)
		}
	}
	if got := holdersOf(c, "t1"); len(got) != 1 || got[0] != "line1/tag-t1" {
		t.Fatalf("t1 is held by %v after a refusal", got)
	}
}

// take_over moves the tag to the declared signal and retires the minted one.
func TestTakeOverMovesTheTagAndRetiresTheMintedSignal(t *testing.T) {
	c := mintedHolder(t)

	code, msg := upsertSignal(t, c, "line1/Press/Temp", map[string]any{"id": "01SDECLARED", "name": "Temp", "data_tag": "t1"}, true)

	if code != 200 {
		t.Fatalf("take over = %d %q", code, msg)
	}
	if got := holdersOf(c, "t1"); len(got) != 1 || got[0] != "line1/Press/Temp" {
		t.Fatalf("t1 is held by %v, want only the declared signal", got)
	}
	if _, kept := signalsAt(c)["line1/tag-t1"]; kept {
		t.Fatal("the minted signal was not retired")
	}
	// A republish does not mint it again: the tag is bound.
	arrive(t, c, tags("t1"))
	if got := holdersOf(c, "t1"); len(got) != 1 {
		t.Fatalf("t1 is held by %v after a republish", got)
	}
}

// A signal minted before is_autobound existed carries only what autobind
// writes, and take_over may retire it too.
func TestTakeOverRetiresASignalMintedBeforeTheMarker(t *testing.T) {
	c := intentNode(t)
	place(t, c, "01HPRESS", "line1/Press")
	declareSignal(t, c, "line1/tag-t1", "01SOLD", "01HLINE1", map[string]any{
		"data_tag": "t1", "is_published": true, "data_type": "float",
	})
	declareSignal(t, c, "line1/Press/Temp", "01SDECLARED", "01HPRESS", nil)

	code, msg := upsertSignal(t, c, "line1/Press/Temp", map[string]any{"id": "01SDECLARED", "name": "Temp", "data_tag": "t1"}, true)

	if code != 200 {
		t.Fatalf("take over = %d %q", code, msg)
	}
	if got := holdersOf(c, "t1"); len(got) != 1 || got[0] != "line1/Press/Temp" {
		t.Fatalf("t1 is held by %v", got)
	}
}

// A declared holder is not autobind's to give away: take_over is refused and
// the refusal says why.
func TestTakeOverRefusesADeclaredHolder(t *testing.T) {
	c := intentNode(t)
	place(t, c, "01HPRESS", "line1/Press")
	declareSignal(t, c, "line1/Temp", "01SHOLDER", "01HLINE1", map[string]any{"data_tag": "t1", "is_logged": true})
	declareSignal(t, c, "line1/Press/Temp", "01SDECLARED", "01HPRESS", nil)

	for _, takeOver := range []bool{false, true} {
		code, msg := upsertSignal(t, c, "line1/Press/Temp", map[string]any{"id": "01SDECLARED", "name": "Temp", "data_tag": "t1"}, takeOver)
		if code != 409 || !strings.Contains(msg, "01SHOLDER was not created by autobind") {
			t.Fatalf("take_over=%v: upsert = %d %q, want 409 saying the holder was declared", takeOver, code, msg)
		}
	}
	if got := holdersOf(c, "t1"); len(got) != 1 || got[0] != "line1/Temp" {
		t.Fatalf("t1 is held by %v, want the holder untouched", got)
	}
}

// A minted holder with something positioned below it is not retired either:
// retiring it would orphan what stands there.
func TestTakeOverRefusesAHolderWithChildren(t *testing.T) {
	c := mintedHolder(t)
	holder := signalsAt(c)["line1/tag-t1"].ID
	declareSignal(t, c, "line1/tag-t1/Alarm", "01SCHILD", "01HLINE1", nil)

	code, msg := upsertSignal(t, c, "line1/Press/Temp", map[string]any{"id": "01SDECLARED", "name": "Temp", "data_tag": "t1"}, true)

	if code != 409 || !strings.Contains(msg, holder+" still holds line1/tag-t1/Alarm") {
		t.Fatalf("upsert = %d %q, want 409 naming what stands below the holder", code, msg)
	}
	if _, kept := signalsAt(c)["line1/tag-t1"]; !kept {
		t.Fatal("the holder was retired despite the refusal")
	}
}

// Two signals already bound to one tag, from before the rule, do not block
// the redeclaration of either: the rule refuses new bindings, not old ones.
func TestAStandingDuplicateDoesNotBlockARedeclaration(t *testing.T) {
	c := intentNode(t)
	f := c.store.(*fakeStore)
	for path, id := range map[string]string{"line1/A": "01SA", "line1/B": "01SB"} {
		if _, err := f.seed(c.signalTopic(path), mustJSON(map[string]any{"id": id, "name": path[6:], "data_tag": "t1"})); err != nil {
			t.Fatal(err)
		}
	}

	if code, msg := upsertSignal(t, c, "line1/B", map[string]any{"id": "01SB", "name": "B", "unit": "bar"}, false); code != 200 {
		t.Fatalf("redeclare = %d %q, want the standing binding left alone", code, msg)
	}
}

// Writing a signal at the position its tag's holder stands replaces the
// holder, so nothing is left holding the tag twice and nothing is refused.
func TestAnUpsertThatReplacesTheHolderInPlaceIsNotRefused(t *testing.T) {
	c := intentNode(t)
	arrive(t, c, tags("t1"))

	if code, msg := upsertSignal(t, c, "line1/tag-t1", map[string]any{"id": "01SDECLARED", "name": "tag-t1"}, false); code != 200 {
		t.Fatalf("upsert = %d %q", code, msg)
	}
	if got := holdersOf(c, "t1"); len(got) != 1 || signalsAt(c)[got[0]].ID != "01SDECLARED" {
		t.Fatalf("t1 is held by %v", got)
	}
}

// ── the rule at commit ───────────────────────────────────────────────────

func signalRecord(node, path string, payload map[string]any) KVRecord {
	return KVRecord{Topic: "colca/v1/_Signal/" + node + "/" + path, Path: path, NodeID: node, Payload: mustJSON(payload)}
}

func signalWrite(node, path string, payload map[string]any) StateRecord {
	if payload == nil {
		return StateRecord{Topic: "colca/v1/_Signal/" + node + "/" + path}
	}
	return StateRecord{Topic: "colca/v1/_Signal/" + node + "/" + path, Payload: mustJSON(payload)}
}

func TestCheckTagBindings(t *testing.T) {
	current := []KVRecord{
		signalRecord("n1", "a", map[string]any{"id": "sa", "name": "a", "data_tag": "t1"}),
		signalRecord("n1", "b", map[string]any{"id": "sb", "name": "b"}),
		// Two signals on t9, from before the rule.
		signalRecord("n1", "c", map[string]any{"id": "sc", "name": "c", "data_tag": "t9"}),
		signalRecord("n1", "d", map[string]any{"id": "sd", "name": "d", "data_tag": "t9"}),
	}
	for name, tc := range map[string]struct {
		batch  []StateRecord
		holder string
	}{
		"a free tag binds": {
			batch: []StateRecord{signalWrite("n1", "b", map[string]any{"id": "sb", "data_tag": "t2"})},
		},
		"a held tag is refused": {
			batch:  []StateRecord{signalWrite("n1", "b", map[string]any{"id": "sb", "data_tag": "t1"})},
			holder: "sa",
		},
		"the holder rewritten keeps its tag": {
			batch: []StateRecord{signalWrite("n1", "a", map[string]any{"id": "sa", "data_tag": "t1", "unit": "bar"})},
		},
		"the holder moving keeps its tag": {
			batch: []StateRecord{
				signalWrite("n1", "a", nil),
				signalWrite("n1", "x/a", map[string]any{"id": "sa", "data_tag": "t1"}),
			},
		},
		"retiring the holder in the same batch frees the tag": {
			batch: []StateRecord{
				signalWrite("n1", "a", nil),
				signalWrite("n1", "b", map[string]any{"id": "sb", "data_tag": "t1"}),
			},
		},
		"two signals bound to one tag in one batch are refused": {
			batch: []StateRecord{
				signalWrite("n1", "b", map[string]any{"id": "sb", "data_tag": "t5"}),
				signalWrite("n1", "e", map[string]any{"id": "se", "data_tag": "t5"}),
			},
			holder: "se",
		},
		"a standing duplicate is left alone": {
			batch: []StateRecord{signalWrite("n1", "d", map[string]any{"id": "sd", "data_tag": "t9", "unit": "bar"})},
		},
		"a third signal on a duplicated tag is refused": {
			batch:  []StateRecord{signalWrite("n1", "b", map[string]any{"id": "sb", "data_tag": "t9"})},
			holder: "sc",
		},
		"other contracts are not signals": {
			batch: []StateRecord{{Topic: "colca/v1/_Constant/n1/b", Payload: mustJSON(map[string]any{"id": "sb", "data_tag": "t1"})}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := CheckTagBindings(current, tc.batch)
			if tc.holder == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			var held *TagHeldError
			if !errors.As(err, &held) || held.Holder != tc.holder {
				t.Fatalf("err = %v, want the tag held by %s", err, tc.holder)
			}
		})
	}
}

// Two commands binding one tag to two signals at once: the executor lock
// serializes them, and exactly one wins.
func TestConcurrentBindsOfOneTagLeaveOneHolder(t *testing.T) {
	c := intentNode(t)
	const n = 8
	for i := 0; i < n; i++ {
		declareSignal(t, c, "line1/S"+string(rune('a'+i)), "01S"+string(rune('A'+i)), "01HLINE1", nil)
	}
	var wg sync.WaitGroup
	codes := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			code, _, _ := c.Execute(asHuman, "_CmdConfigure", "signal/upsert", body(t, map[string]any{
				"signals": []map[string]any{{"path": "line1/S" + string(rune('a'+i)), "signal": map[string]any{
					"id": "01S" + string(rune('A'+i)), "name": "S", "data_tag": "t1",
				}}},
			}))
			codes[i] = code
		}(i)
	}
	wg.Wait()
	won := 0
	for _, code := range codes {
		switch code {
		case 200:
			won++
		case 409:
		default:
			t.Fatalf("codes = %v, want only 200 and 409", codes)
		}
	}
	if won != 1 || len(holdersOf(c, "t1")) != 1 {
		t.Fatalf("codes = %v, holders = %v, want exactly one", codes, holdersOf(c, "t1"))
	}
}

// Two signals already sharing a tag, from before the rule: take_over on the
// declared one retires the minted one, and a plain redeclaration does not.
func TestTakeOverResolvesATagTwoSignalsAlreadyShare(t *testing.T) {
	c := intentNode(t)
	f := c.store.(*fakeStore)
	for path, record := range map[string]map[string]any{
		"line1/tag-t1":     {"id": "01SMINTED", "name": "tag-t1", "data_tag": "t1", "is_published": true, "is_autobound": true},
		"line1/Press/Temp": {"id": "01SDECLARED", "name": "Temp", "data_tag": "t1", "is_logged": true},
	} {
		if _, err := f.seed(c.signalTopic(path), mustJSON(record)); err != nil {
			t.Fatal(err)
		}
	}
	declared := map[string]any{"id": "01SDECLARED", "name": "Temp", "data_tag": "t1", "is_logged": true}

	if code, msg := upsertSignal(t, c, "line1/Press/Temp", declared, false); code != 200 {
		t.Fatalf("redeclare = %d %q", code, msg)
	}
	if got := holdersOf(c, "t1"); len(got) != 2 {
		t.Fatalf("a redeclaration without take_over resolved the duplicate: %v", got)
	}
	if code, msg := upsertSignal(t, c, "line1/Press/Temp", declared, true); code != 200 {
		t.Fatalf("take over = %d %q", code, msg)
	}
	if got := holdersOf(c, "t1"); len(got) != 1 || got[0] != "line1/Press/Temp" {
		t.Fatalf("t1 is held by %v, want only the declared signal", got)
	}
}
