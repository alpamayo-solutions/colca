package uns

import (
	"strings"
	"testing"
)

// autobindConnector runs signal/autobind for the test connector and fails on
// anything but 200. It returns the message.
func autobindConnector(t *testing.T, c *ConfigExec) string {
	t.Helper()
	code, msg, _ := c.Execute(asHuman, "_CmdConfigure", "signal/autobind", body(t, map[string]any{"connector": "01JCONN"}))
	if code != 200 {
		t.Fatalf("autobind = %d %q", code, msg)
	}
	return msg
}

// PREKIT declares a signal at its PascalCase spelling of the tag name
// (Events/ExecutionContext for execution_context). Autobind binds that signal
// instead of minting a second one at Events/execution_context, and the tag's
// meta still reaches it.
func TestAutobindAdoptsADeclaredSignalAtPrekitsSpelling(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HEVENTS", "Events")
	bindEntry(t, c, "01JCONN", "tcdb-api", "01HEVENTS")
	seedSemanticTag(t, c, "01STAGCTX", "execution_context")
	declareSignal(t, c, "Events/ExecutionContext", "01SDECLARED", "01HEVENTS", map[string]any{"name": "ExecutionContext"})
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/Events/tcdb-api", []map[string]any{
		{"id": "t1", "name": "execution_context", "data_type": "json", "meta": map[string]any{
			"semantic_type": "execution_context", "description": "Current execution context",
		}},
	})

	autobindConnector(t, c)

	got := signalsAt(c)
	if len(got) != 1 {
		t.Fatalf("signals = %+v, want only the declared one", got)
	}
	if s := got["Events/ExecutionContext"]; s.ID != "01SDECLARED" || s.DataTag != "t1" {
		t.Fatalf("Events/ExecutionContext = %+v, want the declared 01SDECLARED bound to t1", s)
	}
	record := signalRecordAt(t, c, "Events/ExecutionContext")
	if record["semantic_type_id"] != "01STAGCTX" || record["description"] != "Current execution context" || record["data_type"] != "json" {
		t.Fatalf("the tag's meta did not reach the adopted signal: %+v", record)
	}
}

// A declared signal whose name field is the tag name is bound wherever it sits
// under the element, when it is the only one there with that name.
func TestAutobindAdoptsADeclaredSignalByItsName(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HEVENTS", "Events")
	bindEntry(t, c, "01JCONN", "tcdb-api", "01HEVENTS")
	declareSignal(t, c, "Events/Context", "01SDECLARED", "01HEVENTS", map[string]any{"name": "execution_context"})
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/Events/tcdb-api", []map[string]any{
		{"id": "t1", "name": "execution_context", "data_type": "json"},
	})

	autobindConnector(t, c)

	got := signalsAt(c)
	if len(got) != 1 || got["Events/Context"].ID != "01SDECLARED" || got["Events/Context"].DataTag != "t1" {
		t.Fatalf("signals = %+v, want only Events/Context bound to t1", got)
	}
}

// Two declared signals under the element sharing the tag's name are ambiguous:
// neither is bound and the tag gets a signal of its own, as before.
func TestAutobindLeavesAmbiguousNameMatchesAlone(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HEVENTS", "Events")
	bindEntry(t, c, "01JCONN", "tcdb-api", "01HEVENTS")
	declareSignal(t, c, "Events/A", "01SA", "01HEVENTS", map[string]any{"name": "execution_context"})
	declareSignal(t, c, "Events/B", "01SB", "01HEVENTS", map[string]any{"name": "execution_context"})
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/Events/tcdb-api", []map[string]any{
		{"id": "t1", "name": "execution_context", "data_type": "json"},
	})

	autobindConnector(t, c)

	got := signalsAt(c)
	if got["Events/A"].DataTag != "" || got["Events/B"].DataTag != "" {
		t.Fatalf("an ambiguous declaration was bound: %+v", got)
	}
	if got["Events/execution_context"].DataTag != "t1" {
		t.Fatalf("the tag got no signal of its own: %+v", got)
	}
}

// A signal at PREKIT's spelling that already holds another tag is never
// taken; the tag gets its own signal at autobind's spelling.
func TestAutobindDoesNotStealABoundSignalAtPrekitsSpelling(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HEVENTS", "Events")
	bindEntry(t, c, "01JCONN", "tcdb-api", "01HEVENTS")
	declareSignal(t, c, "Events/ExecutionContext", "01SOTHER", "01HEVENTS", map[string]any{
		"name": "execution_context", "data_tag": "t-elsewhere",
	})
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/Events/tcdb-api", []map[string]any{
		{"id": "t1", "name": "execution_context", "data_type": "json"},
	})

	autobindConnector(t, c)

	got := signalsAt(c)
	if s := got["Events/ExecutionContext"]; s.ID != "01SOTHER" || s.DataTag != "t-elsewhere" {
		t.Fatalf("a bound signal was taken: %+v", got)
	}
	if s := got["Events/execution_context"]; s.DataTag != "t1" || s.ID == "01SOTHER" {
		t.Fatalf("the tag got no signal of its own: %+v", got)
	}
}

// With no declared signal that matches, autobind mints at its own spelling and
// leaves an unrelated declaration unbound.
func TestAutobindWithoutAMatchMintsAsBefore(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HEVENTS", "Events")
	bindEntry(t, c, "01JCONN", "tcdb-api", "01HEVENTS")
	declareSignal(t, c, "Events/Shift", "01SSHIFT", "01HEVENTS", nil)
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/Events/tcdb-api", []map[string]any{
		{"id": "t1", "name": "execution_context", "data_type": "json"},
	})

	autobindConnector(t, c)

	got := signalsAt(c)
	if len(got) != 2 || got["Events/Shift"].DataTag != "" || got["Events/execution_context"].DataTag != "t1" {
		t.Fatalf("signals = %+v, want Events/Shift unbound and Events/execution_context minted for t1", got)
	}
}

// One declared signal is bound to one tag even when two tags of the catalogue
// match it; the second tag gets a signal of its own.
func TestADeclaredSignalIsAdoptedOnce(t *testing.T) {
	c := newConfigExec(t)
	place(t, c, "01HEVENTS", "Events")
	bindEntry(t, c, "01JCONN", "tcdb-api", "01HEVENTS")
	declareSignal(t, c, "Events/ExecutionContext", "01SDECLARED", "01HEVENTS", nil)
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/Events/tcdb-api", []map[string]any{
		{"id": "t1", "name": "execution_context", "data_type": "json"},
		{"id": "t2", "name": "executionContext", "data_type": "json"},
	})

	autobindConnector(t, c)

	got := signalsAt(c)
	if len(got) != 2 {
		t.Fatalf("signals = %+v, want the declared one and one minted", got)
	}
	if s := got["Events/ExecutionContext"]; s.ID != "01SDECLARED" || s.DataTag != "t1" {
		t.Fatalf("Events/ExecutionContext = %+v, want the declared one bound to the first tag", s)
	}
	if got["Events/executionContext"].DataTag != "t2" {
		t.Fatalf("the second tag got no signal of its own: %+v", got)
	}
}

// Re-publishing the catalogue, by verb or through the lifecycle trigger,
// changes nothing once the declared signal is adopted.
func TestAdoptingAtPrekitsSpellingIsIdempotent(t *testing.T) {
	c := newTriggerConfigExec(t)
	place(t, c, "01HEVENTS", "Events")
	bindEntry(t, c, "01JCONN", "tcdb-api", "01HEVENTS")
	declareSignal(t, c, "Events/ExecutionContext", "01SDECLARED", "01HEVENTS", nil)
	tags := []map[string]any{{"id": "t1", "name": "execution_context", "data_type": "json"}}
	publishCatalogue(t, c, "colca/v1/_DataTags/n1/Events/tcdb-api", tags)
	catalogue := mustJSON(map[string]any{"data_tags": tags})

	c.Observe("_DataTags", "colca/v1/_DataTags/n1/Events/tcdb-api", catalogue)
	c.Observe("_DataTags", "colca/v1/_DataTags/n1/Events/tcdb-api", catalogue)
	if msg := autobindConnector(t, c); !strings.Contains(msg, `"created":0,"skipped":1`) {
		t.Fatalf("autobind after adoption = %q, want nothing created", msg)
	}

	got := signalsAt(c)
	if len(got) != 1 || got["Events/ExecutionContext"].ID != "01SDECLARED" || got["Events/ExecutionContext"].DataTag != "t1" {
		t.Fatalf("signals = %+v, want only the declared one, bound to t1", got)
	}
}

func TestPascalSegmentFollowsPrekit(t *testing.T) {
	for in, want := range map[string]string{
		"execution_context": "ExecutionContext",
		"fillLevel":         "FillLevel",
		"HMIConfig":         "HMIConfig",
		"hmi_config":        "HmiConfig",
		"Line 1":            "Line1",
		"drum-temp":         "DrumTemp",
		"__x__":             "X",
		"":                  "",
	} {
		if got := pascalSegment(in); got != want {
			t.Errorf("pascalSegment(%q) = %q, want %q", in, got, want)
		}
	}
}
