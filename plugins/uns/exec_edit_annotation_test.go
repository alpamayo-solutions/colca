package uns

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// annotationIDVectorPath is the golden derive_annotation_id dataset, shared
// with the Python tests (tests/test_annotation_payload.py).
const annotationIDVectorPath = "../../contracts/src/colca_data_contracts/vectors/annotation_id.json"

type annotationIDVectorCase struct {
	AnnotationTypeID string   `json:"annotation_type_id"`
	Source           string   `json:"source"`
	TimeStart        float64  `json:"time_start"`
	SignalIDs        []string `json:"signal_ids"`
	ID               string   `json:"id"`
}

type annotationIDVectorFile struct {
	Cases []annotationIDVectorCase `json:"cases"`
}

func loadAnnotationIDVectors(t *testing.T) annotationIDVectorFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(annotationIDVectorPath))
	if err != nil {
		t.Fatalf("golden annotation_id vectors missing: %v", err)
	}
	var v annotationIDVectorFile
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("golden annotation_id vectors unparseable: %v", err)
	}
	if len(v.Cases) == 0 {
		t.Fatal("golden annotation_id vectors carry no cases")
	}
	return v
}

// TestDeriveAnnotationIDMatchesTheGoldenVectors checks deriveAnnotationID
// against vectors computed by colca_data_contracts.payload.derive_annotation_id.
func TestDeriveAnnotationIDMatchesTheGoldenVectors(t *testing.T) {
	cases := loadAnnotationIDVectors(t).Cases
	// The file must cover every signal-set shape (none, one, several, and
	// several in different orders), or the sort is never tested.
	sizes := map[int]bool{}
	orders := map[string]map[string]bool{}
	for _, c := range cases {
		sizes[len(c.SignalIDs)] = true
		set := append([]string(nil), c.SignalIDs...)
		sort.Strings(set)
		key := fmt.Sprintf("%s|%s|%v|%s", c.AnnotationTypeID, c.Source, c.TimeStart, strings.Join(set, ","))
		if orders[key] == nil {
			orders[key] = map[string]bool{}
		}
		orders[key][strings.Join(c.SignalIDs, ",")] = true
	}
	several := false
	for size := range sizes {
		if size > 1 {
			several = true
		}
	}
	if !sizes[0] || !sizes[1] || !several {
		t.Fatalf("golden vectors must carry cases with zero, one and several signals, got sizes %v", sizes)
	}
	twoOrders := false
	for _, o := range orders {
		if len(o) > 1 {
			twoOrders = true
		}
	}
	if !twoOrders {
		t.Fatal("no golden vector case gives the same signal set in two orders")
	}
	for _, c := range cases {
		given := append([]string(nil), c.SignalIDs...)
		if got := deriveAnnotationID(c.AnnotationTypeID, c.Source, c.TimeStart, c.SignalIDs); got != c.ID {
			t.Errorf("deriveAnnotationID(%q, %q, %v, %q) = %q, vectors want %q",
				c.AnnotationTypeID, c.Source, c.TimeStart, c.SignalIDs, got, c.ID)
		}
		// The derivation sorts a copy; the caller's slice keeps its order.
		if strings.Join(given, ",") != strings.Join(c.SignalIDs, ",") {
			t.Errorf("deriveAnnotationID reordered the caller's signal_ids: %q -> %q", given, c.SignalIDs)
		}
	}
}

// TestDeriveAnnotationIDIsDeterministicAndDistinct mirrors the Python tests,
// so both copies meet the same behavioural claims, not just the same values.
func TestDeriveAnnotationIDIsDeterministicAndDistinct(t *testing.T) {
	one := []string{"sig-1"}
	baseline := deriveAnnotationID("annotation-type-1", "dataops/part-cycle", 1710000000.0, one)
	if deriveAnnotationID("annotation-type-1", "dataops/part-cycle", 1710000000.0, one) != baseline {
		t.Fatal("deriveAnnotationID is not deterministic for identical inputs")
	}
	for name, got := range map[string]string{
		"annotation_type_id": deriveAnnotationID("annotation-type-2", "dataops/part-cycle", 1710000000.0, one),
		"source":             deriveAnnotationID("annotation-type-1", "dataops/other-producer", 1710000000.0, one),
		"time_start":         deriveAnnotationID("annotation-type-1", "dataops/part-cycle", 1710000001.0, one),
		// a different machine is a different annotation.
		"signal_ids":       deriveAnnotationID("annotation-type-1", "dataops/part-cycle", 1710000000.0, []string{"sig-2"}),
		"signal_ids=none":  deriveAnnotationID("annotation-type-1", "dataops/part-cycle", 1710000000.0, nil),
		"signal_ids=wider": deriveAnnotationID("annotation-type-1", "dataops/part-cycle", 1710000000.0, []string{"sig-1", "sig-2"}),
	} {
		if got == baseline {
			t.Fatalf("varying %s alone did not change the derived id", name)
		}
	}
	// The signals are a SET: order is not identity.
	sortedIn := deriveAnnotationID("annotation-type-1", "user/u-franz", 1700000000.0, []string{"a", "b", "c"})
	unsortedIn := deriveAnnotationID("annotation-type-1", "user/u-franz", 1700000000.0, []string{"c", "a", "b"})
	if sortedIn != unsortedIn {
		t.Fatalf("the same signal set in two orders derived two ids: %s vs %s", sortedIn, unsortedIn)
	}
	if nilSet, empty := deriveAnnotationID("t", "s", 1.0, nil), deriveAnnotationID("t", "s", 1.0, []string{}); nilSet != empty {
		t.Fatalf("a nil and an empty signal set must be the same identity, got %s vs %s", nilSet, empty)
	}
}

func floatPtr(v float64) *float64 { return &v }

// annotationCreateIntent is a valid create intent, ready to have one field
// tweaked per test.
func annotationCreateIntent() editIntent {
	return editIntent{
		Type: "annotation", Action: "create",
		AnnotationTypeID: "annotation-type-1", Source: "dataops/part-cycle",
		TimeStart: floatPtr(1710000000.0), SignalIDs: []string{"sig-1", "sig-2"},
	}
}

func TestComposeAnnotationCreateDerivesTheDocumentedID(t *testing.T) {
	exec := NewEditExec(newStore("n-edge1"), nil)
	code, msg, result, records := exec.composeAnnotation(annotationCreateIntent())
	if code != 200 || result != "ok" {
		t.Fatalf("create = %d %q %q, want 200 ok: %s", code, result, msg, msg)
	}
	if len(records) != 1 {
		t.Fatalf("create composed %d records, want exactly 1", len(records))
	}
	wantID := deriveAnnotationID("annotation-type-1", "dataops/part-cycle", 1710000000.0, []string{"sig-1", "sig-2"})
	wantTopic := "colca/v1/_Annotation/n-edge1/_colca/annotations/" + wantID
	if records[0].Topic != wantTopic {
		t.Fatalf("create topic = %q, want %q", records[0].Topic, wantTopic)
	}
	var payload map[string]any
	if err := json.Unmarshal(records[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["annotation_id"] != wantID {
		t.Fatalf("payload annotation_id = %v, want %q", payload["annotation_id"], wantID)
	}
	if payload["annotation_type_id"] != "annotation-type-1" || payload["source"] != "dataops/part-cycle" {
		t.Fatalf("composed payload lost caller fields: %+v", payload)
	}
	if payload["deleted"] != false {
		t.Fatalf("a create must not carry deleted=true, got %+v", payload["deleted"])
	}
	signalIDs, ok := payload["signal_ids"].([]any)
	if !ok || len(signalIDs) != 2 || signalIDs[0] != "sig-1" || signalIDs[1] != "sig-2" {
		t.Fatalf("signal_ids did not round-trip: %+v", payload["signal_ids"])
	}
	if _, present := payload["time_end"]; present {
		t.Fatalf("time_end must be omitted when the intent did not supply one, got %+v", payload["time_end"])
	}
}

// TestComposeAnnotationRefusesACallerSuppliedIDOnCreate checks that a create
// naming an id is refused with nothing queued, not given the derived id.
func TestComposeAnnotationRefusesACallerSuppliedIDOnCreate(t *testing.T) {
	intent := annotationCreateIntent()
	intent.AnnotationID = "attacker-chosen-id"
	code, msg, result, records := NewEditExec(newStore("n-edge1"), nil).composeAnnotation(intent)
	if code != 409 || result != "conflict" {
		t.Fatalf("create with a supplied id = %d %q %q, want 409 conflict", code, result, msg)
	}
	if records != nil {
		t.Fatalf("a refused create queued %d records, want zero", len(records))
	}
	if !strings.Contains(msg, "attacker-chosen-id") {
		t.Fatalf("refusal message %q does not name the offending id", msg)
	}
}

func TestComposeAnnotationRequiresItsFields(t *testing.T) {
	base := annotationCreateIntent()
	cases := []struct {
		name   string
		mutate func(*editIntent)
	}{
		{"annotation_type_id", func(i *editIntent) { i.AnnotationTypeID = "" }},
		{"time_start", func(i *editIntent) { i.TimeStart = nil }},
		{"source", func(i *editIntent) { i.Source = "" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			intent := base
			c.mutate(&intent)
			code, msg, result, records := NewEditExec(newStore("n-edge1"), nil).composeAnnotation(intent)
			if code != 422 || result != "invalid" {
				t.Fatalf("missing %s = %d %q %q, want 422 invalid", c.name, code, result, msg)
			}
			if records != nil {
				t.Fatalf("a refused create queued %d records, want zero", len(records))
			}
		})
	}
}

func TestComposeAnnotationRejectsAnUnknownAction(t *testing.T) {
	intent := annotationCreateIntent()
	intent.Action = "upsert"
	code, _, result, records := NewEditExec(newStore("n-edge1"), nil).composeAnnotation(intent)
	if code != 422 || result != "invalid" || records != nil {
		t.Fatalf("unknown action = %d %q, records=%d, want 422 invalid with zero records", code, result, len(records))
	}
}

// TestComposeAnnotationUpdateAndDeleteUseTheSuppliedIDVerbatim checks that
// update and delete use the id the caller names. Re-deriving would only be right
// if type, source and start were unchanged, which the executor cannot know.
func TestComposeAnnotationUpdateAndDeleteUseTheSuppliedIDVerbatim(t *testing.T) {
	const existingID = "01J000000000000000000ANNOT"

	t.Run("update requires an id", func(t *testing.T) {
		intent := annotationCreateIntent()
		intent.Action = "update"
		code, _, result, records := NewEditExec(newStore("n-edge1"), nil).composeAnnotation(intent)
		if code != 422 || result != "invalid" || records != nil {
			t.Fatalf("update without an id = %d %q, records=%d, want 422 invalid with zero records", code, result, len(records))
		}
	})

	t.Run("update composes a full record at the given id", func(t *testing.T) {
		intent := annotationCreateIntent()
		intent.Action = "update"
		intent.AnnotationID = existingID
		intent.TimeEnd = floatPtr(1710000030.0)
		code, _, _, records := NewEditExec(newStore("n-edge1"), nil).composeAnnotation(intent)
		if code != 200 || len(records) != 1 {
			t.Fatalf("update = %d, records=%d, want 200 with 1 record", code, len(records))
		}
		if !strings.HasSuffix(records[0].Topic, "/_colca/annotations/"+existingID) {
			t.Fatalf("update topic = %q, want it to target %s verbatim", records[0].Topic, existingID)
		}
		var payload map[string]any
		if err := json.Unmarshal(records[0].Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["deleted"] != false {
			t.Fatalf("update must not mark deleted, got %+v", payload["deleted"])
		}
		if payload["time_end"] != 1710000030.0 {
			t.Fatalf("update lost time_end: %+v", payload["time_end"])
		}
	})

	t.Run("delete requires an id", func(t *testing.T) {
		intent := annotationCreateIntent()
		intent.Action = "delete"
		code, _, result, records := NewEditExec(newStore("n-edge1"), nil).composeAnnotation(intent)
		if code != 422 || result != "invalid" || records != nil {
			t.Fatalf("delete without an id = %d %q, records=%d, want 422 invalid with zero records", code, result, len(records))
		}
	})

	t.Run("delete appends deleted:true at the existing id", func(t *testing.T) {
		intent := annotationCreateIntent()
		intent.Action = "delete"
		intent.AnnotationID = existingID
		code, _, _, records := NewEditExec(newStore("n-edge1"), nil).composeAnnotation(intent)
		if code != 200 || len(records) != 1 {
			t.Fatalf("delete = %d, records=%d, want 200 with 1 record", code, len(records))
		}
		if !strings.HasSuffix(records[0].Topic, "/_colca/annotations/"+existingID) {
			t.Fatalf("delete topic = %q, want it to target %s verbatim", records[0].Topic, existingID)
		}
		var payload map[string]any
		if err := json.Unmarshal(records[0].Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["deleted"] != true {
			t.Fatalf("delete must mark deleted=true, got %+v", payload["deleted"])
		}
		if payload["annotation_id"] != existingID {
			t.Fatalf("delete payload annotation_id = %v, want %q", payload["annotation_id"], existingID)
		}
	})
}

// annotationWireIntent renders an annotation intent as the JSON a client puts
// in the Edit envelope.
func annotationWireIntent(overrides map[string]any) map[string]any {
	intent := map[string]any{
		"type": "annotation", "action": "create",
		"annotation_type_id": "annotation-type-1", "source": "dataops/part-cycle",
		"time_start": 1710000000.0, "signal_ids": []string{"sig-1"},
	}
	for k, v := range overrides {
		intent[k] = v
	}
	return intent
}

// TestEditAnnotationCommitsThroughTheEventDoorNeverKV runs the real _CmdEdit
// envelope: the executor derives the id, the record lands on the annotations
// stream, and it is never KV-projected. An ordinary entity is seeded first to
// show KVGet finds records when they exist.
func TestEditAnnotationCommitsThroughTheEventDoorNeverKV(t *testing.T) {
	f := newStore("n-edge1")
	presenceTopic := "colca/v1/_Constant/n-edge1/line1/pin"
	seedEditEntity(t, f, "_Constant", "line1/pin", map[string]any{
		"id": "const-pin", "name": "pin", "data_type": "int64", "value": 1,
	})
	if _, ok := f.KVGet(presenceTopic); !ok {
		t.Fatalf("presence pin: KVGet(%s) found nothing before the annotation exists, so its later absence would prove nothing", presenceTopic)
	}

	exec := NewEditExec(f, nil)
	payload := editBody(t, "op-annotation-create", map[string]uint64{}, annotationWireIntent(nil))

	code, msg, result, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || result != "ok" || len(writes) != 1 {
		t.Fatalf("create = %d %q %q writes=%+v", code, result, msg, writes)
	}
	if writes[0].Stream != "annotations" {
		t.Fatalf("annotation write landed on stream %q, want %q", writes[0].Stream, "annotations")
	}
	wantID := deriveAnnotationID("annotation-type-1", "dataops/part-cycle", 1710000000.0, []string{"sig-1"})
	parsed, err := Parse(writes[0].Topic)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Path != "_colca/annotations/"+wantID {
		t.Fatalf("annotation topic path = %q, want it to name the derived id %q", parsed.Path, wantID)
	}
	if f.eventCalls != 1 {
		t.Fatalf("PublishEvent was called %d times, want exactly 1 (the annotation record)", f.eventCalls)
	}
	if f.batchCalls != 1 {
		t.Fatalf("PublishBatch was called %d times, want exactly 1 (the durable receipt only)", f.batchCalls)
	}
	if _, ok := f.KVGet(writes[0].Topic); ok {
		t.Fatalf("%s appeared in KV — an annotation must never be KV-projected", writes[0].Topic)
	}

	// Replaying the same operation_id returns the cached outcome without
	// appending the event or the receipt again, even though they are two
	// separate writes.
	code, _, _, replayWrites := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 200 || len(replayWrites) != 1 || replayWrites[0].Topic != writes[0].Topic {
		t.Fatalf("replay = %d writes=%+v, want the identical cached write", code, replayWrites)
	}
	if f.eventCalls != 1 || f.batchCalls != 1 {
		t.Fatalf("replay re-executed the write: eventCalls=%d batchCalls=%d, want both unchanged at 1",
			f.eventCalls, f.batchCalls)
	}
}

// TestEditAnnotationRefusalWritesNothing checks that a 409 at compose time
// reaches neither write path.
func TestEditAnnotationRefusalWritesNothing(t *testing.T) {
	f := newStore("n-edge1")
	exec := NewEditExec(f, nil)
	payload := editBody(t, "op-annotation-bad-create", map[string]uint64{},
		annotationWireIntent(map[string]any{"annotation_id": "attacker-chosen-id"}),
	)

	code, msg, result, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", payload)
	if code != 409 || result != "conflict" || len(writes) != 0 {
		t.Fatalf("refused create = %d %q %q writes=%+v, want 409 conflict with zero writes", code, result, msg, writes)
	}
	if f.eventCalls != 0 {
		t.Fatalf("a refused command called PublishEvent %d times, want zero", f.eventCalls)
	}
	if f.batchCalls != 0 {
		t.Fatalf("a refused command called PublishBatch %d times, want zero", f.batchCalls)
	}
}

// TestEditAnnotationDeleteAppendsAtAnExistingIDThroughTheEventDoor covers create
// then delete: the delete names the created id and also lands on the
// annotations stream, never in KV.
func TestEditAnnotationDeleteAppendsAtAnExistingIDThroughTheEventDoor(t *testing.T) {
	f := newStore("n-edge1")
	exec := NewEditExec(f, nil)
	createPayload := editBody(t, "op-annotation-create-2", map[string]uint64{}, annotationWireIntent(nil))
	code, _, _, createWrites := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", createPayload)
	if code != 200 || len(createWrites) != 1 {
		t.Fatalf("setup create = %d writes=%+v", code, createWrites)
	}
	parsed, err := Parse(createWrites[0].Topic)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimPrefix(parsed.Path, "_colca/annotations/")

	deletePayload := editBody(t, "op-annotation-delete", map[string]uint64{}, annotationWireIntent(map[string]any{
		"action": "delete", "annotation_id": id,
	}))
	code, msg, result, deleteWrites := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", deletePayload)
	if code != 200 || result != "ok" || len(deleteWrites) != 1 {
		t.Fatalf("delete = %d %q %q writes=%+v", code, result, msg, deleteWrites)
	}
	if deleteWrites[0].Topic != createWrites[0].Topic {
		t.Fatalf("delete topic %q did not target the created id's topic %q", deleteWrites[0].Topic, createWrites[0].Topic)
	}
	if deleteWrites[0].Stream != "annotations" {
		t.Fatalf("delete write landed on stream %q, want %q", deleteWrites[0].Stream, "annotations")
	}
	if f.eventCalls != 2 {
		t.Fatalf("PublishEvent was called %d times across create+delete, want exactly 2", f.eventCalls)
	}
	if _, ok := f.KVGet(deleteWrites[0].Topic); ok {
		t.Fatalf("%s appeared in KV after a delete append — must never be KV-projected", deleteWrites[0].Topic)
	}
}
