package uns

import (
	"strings"
	"testing"
)

func TestParseAndClass(t *testing.T) {
	p, err := Parse("colca/v1/_Metric/m1/site1/edge1/m1/temp")
	if err != nil {
		t.Fatal(err)
	}
	if p.Contract != "_Metric" || p.NodeID != "m1" || p.Path != "site1/edge1/m1/temp" {
		t.Fatalf("%+v", p)
	}
	if p.Prefix != "colca" || p.Version != "v1" {
		t.Fatalf("%+v", p)
	}
	cases := map[string]struct {
		class  Class
		stream string
	}{
		"_Metric": {ClassData, "metrics"}, "_EnrolledIdentity": {ClassEntity, "entities"},
		"_AlarmStateChange":         {ClassAlarm, "alarms"},
		"_NotificationDispatched":   {ClassAlarm, "alarms"},
		"_Annotation":               {ClassAnnotation, "annotations"},
		"_AlarmNotificationConfig":  {ClassEntity, "entities"},
		"_NotificationConfigStatus": {ClassEntity, "entities"},
		"_Node":               {ClassEntity, "entities"}, "_ServiceDetails": {ClassEntity, "entities"},
		"_ExternalReference": {ClassEntity, "entities"},
		"_SystemElement":     {ClassEntity, "entities"}, "_CmdParam": {ClassCmd, "commands"},
		"_CmdAdmin": {ClassCmd, "commands"}, "_Ack": {ClassAck, "commands"},
		// demo topology: every _Cmd* contract is a command, never ClassNone
		"_CmdOperate": {ClassCmd, "commands"}, "_CmdMaintain": {ClassCmd, "commands"},
		"_Signal":             {ClassEntity, "entities"},
		"_Constant":           {ClassEntity, "entities"},
		"_EditOperation": {ClassEntity, "entities"},
		"_MetadataType":       {ClassDefinition, "definitions"},
		"_AnnotationType":     {ClassDefinition, "definitions"},
		"_DataModel":          {ClassDefinition, "definitions"},
		"_ExternalSystem":     {ClassDefinition, "definitions"},
		"_SemanticTag":        {ClassDefinition, "definitions"},
		"_AuditEvent":         {ClassAudit, "audit"},
		// _StreamGap (design §6.4) is its own class. StreamFor deliberately
		// answers "" for it — unlike every other class it has no single fixed
		// stream, it targets whichever stream it describes (Parsed.Path, see
		// TestStreamGapTargetsDescribedStream).
		"_StreamGap": {ClassGap, ""},
	}
	for c, want := range cases {
		if ClassOf(c) != want.class || StreamFor(ClassOf(c)) != want.stream {
			t.Fatalf("%s → %v/%s", c, ClassOf(c), StreamFor(ClassOf(c)))
		}
	}
	if ClassOf("_Unknown") != ClassNone || StreamFor(ClassNone) != "" {
		t.Fatalf("unknown contract must be ClassNone with empty stream")
	}
	if _, err := Parse("colca/v1/nounderscore/m1/x"); err == nil {
		t.Fatal("want grammar error")
	}
	if _, err := Parse("colca/v1/_Metric/m1"); err == nil {
		t.Fatal("want error: missing path")
	}
	if IsUns("other/topic") {
		t.Fatal("non-UNS must be false")
	}
	if !IsUns("colca/v1/_Metric/m1/m1/temp") {
		t.Fatal("UNS topic must be true")
	}
}

// Time-sync design §2.2: _TimeSync is the only contract whose wire topic has
// no hierarchy path at all — Parse's one length exception, gated on the
// contract so it can never accidentally widen to any other class.
func TestTimeSyncTopicShape(t *testing.T) {
	p, err := Parse("colca/v1/_TimeSync/n-edge1")
	if err != nil {
		t.Fatal(err)
	}
	if p.Contract != "_TimeSync" || p.NodeID != "n-edge1" || p.Path != "" {
		t.Fatalf("%+v", p)
	}
	if p.Prefix != "colca" || p.Version != "v1" {
		t.Fatalf("%+v", p)
	}
	if ClassOf("_TimeSync") != ClassTimeSync || StreamFor(ClassOf("_TimeSync")) != "" {
		t.Fatalf("_TimeSync class/stream: %v/%q", ClassOf("_TimeSync"), StreamFor(ClassOf("_TimeSync")))
	}
	if got := TimeSyncTopic("n-edge1"); got != "colca/v1/_TimeSync/n-edge1" {
		t.Fatalf("TimeSyncTopic = %q", got)
	}
	// A padded (5+-segment) _TimeSync topic still parses normally through the
	// ordinary >=5 path — the relaxation only ever ADDS the 4-segment shape.
	p2, err := Parse("colca/v1/_TimeSync/n-edge1/extra")
	if err != nil || p2.Path != "extra" {
		t.Fatalf("padded _TimeSync topic: %+v, err=%v", p2, err)
	}

	// The relaxation is contract-gated, not length-only: any OTHER contract
	// at 4 segments must still be a grammar error (same case already pinned
	// generically in TestParseAndClass's "missing path" check for _Metric).
	if _, err := Parse("colca/v1/_Metric/n-edge1"); err == nil {
		t.Fatal("4-segment _Metric must still error — the relaxation is _TimeSync-only")
	}
}

// Design §6.4: "topic colca/v1/_StreamGap/{node-ulid}/{stream} — level 4 is the
// pruning node, ordinary uns grammar" — no Parse special case needed, and the
// stream the marker describes is exactly Parsed.Path.
func TestStreamGapTargetsDescribedStream(t *testing.T) {
	for _, stream := range []string{"metrics", "entities", "commands", "audit"} {
		topic := "colca/v1/_StreamGap/n-edge1/" + stream
		p, err := Parse(topic)
		if err != nil {
			t.Fatalf("%s: %v", topic, err)
		}
		if p.Contract != "_StreamGap" || p.NodeID != "n-edge1" || p.Path != stream {
			t.Fatalf("%s: got %+v", topic, p)
		}
		if ClassOf(p.Contract) != ClassGap {
			t.Fatalf("%s: class = %v, want ClassGap", topic, ClassOf(p.Contract))
		}
	}
}

func TestAuditEventIsAnUpwardAppendOnlyEvent(t *testing.T) {
	if !IsAudit(ClassOf("_AuditEvent")) {
		t.Fatal("_AuditEvent must have the audit domain class")
	}
	if IsState(ClassAudit) || IsOwnedState(ClassAudit) || IsDefinition(ClassAudit) || IsCommand(ClassAudit) {
		t.Fatal("audit must be an append-only event, not state or a downward command/definition")
	}
	if got := StreamFor(ClassAudit); got != "audit" {
		t.Fatalf("audit stream = %q", got)
	}
	if err := Validate("_AuditEvent", nil); err == nil {
		t.Fatal("an audit event cannot be tombstoned")
	}
	payload := []byte(`{"event_id":"evt-1","source":"api","action":"authorize","outcome":"denied","actor_kind":"human","occurred_at":1}`)
	if err := Validate("_AuditEvent", payload); err != nil {
		t.Fatalf("valid audit event rejected: %v", err)
	}
}

// engine.retainFor(c) == (c == ClassData || c == ClassEntity), and
// persistTS's KV-projection gate uses the identical condition — so pinning
// that ClassGap is neither ClassData nor ClassEntity here pins BOTH "never
// retained" and "never KV-projected" at the source: the class enum itself.
// (engine_test.go additionally exercises the real retainFor function.)
func TestStreamGapNeverRetainedOrKVProjected(t *testing.T) {
	if ClassGap == ClassData || ClassGap == ClassEntity {
		t.Fatal("ClassGap must not be ClassData or ClassEntity — it is an event, not state (design §6.4)")
	}
}

func TestMountInsertStrip(t *testing.T) {
	in := "colca/v1/_Metric/m1/m1/temp"
	out := MountInsert(in, "edge1")
	if out != "colca/v1/_Metric/m1/edge1/m1/temp" {
		t.Fatal(out)
	}
	back, ok := MountStrip(out, "edge1")
	if !ok || back != in {
		t.Fatalf("%s %v", back, ok)
	}
	if _, ok := MountStrip("colca/v1/_Metric/m1/other/x", "edge1"); ok {
		t.Fatal("strip must fail for foreign mount")
	}

	// demo topology: 3-level chain machine → edge1 → site1 → global
	global := MountInsert(MountInsert(in, "edge1"), "site1")
	if global != "colca/v1/_Metric/m1/site1/edge1/m1/temp" {
		t.Fatal(global)
	}

	// downlink: mount-strip once per hop on the way down
	cmdGlobal := "colca/v1/_CmdParam/m1/site1/edge1/m1/set-speed"
	atSite, ok := MountStrip(cmdGlobal, "site1")
	if !ok || atSite != "colca/v1/_CmdParam/m1/edge1/m1/set-speed" {
		t.Fatalf("%s %v", atSite, ok)
	}
	atEdge, ok := MountStrip(atSite, "edge1")
	if !ok || atEdge != "colca/v1/_CmdParam/m1/m1/set-speed" {
		t.Fatalf("%s %v", atEdge, ok)
	}
	if _, ok := MountStrip(cmdGlobal, "edge1"); ok {
		t.Fatal("foreign mount must not be delivered")
	}
}

func TestValidate(t *testing.T) {
	ok := [][2]string{
		{"_Metric", `{"v": 3.14}`},
		{"_Metric", `{"v": 3.14, "ts": 123}`},
		{"_CmdParam", `{"correlation_id":"abc","expires_at": 99999999999, "params":{"speed":5}}`},
		{"_Ack", `{"correlation_id":"abc","result_code":200,"message":"ok"}`},
		{"_EnrolledIdentity", `{"ulid":"n-edge1","element":"01HEDGE1","kind":"node","grants":[],"status":"active","pubkey":"aa"}`},
		// A data-model record names itself by "id", not by "ulid" — that is
		// the field grants and bindings reference it through.
		{"_SystemElement", `{"id":"01HLINE1","name":"Linie 1"}`},
		{"_Signal", `{"id":"01HSIG1","name":"Temperatur"}`},
		{"_Constant", `{"id":"01HCONST1","name":"Target speed","data_type":"int64","value":18000}`},
		{"_EditOperation", `{"id":"op-1","digest":"abc","message":"created","result":"ok","topics":["colca/v1/_Constant/n1/a"]}`},
		{"_StreamGap", `{"stream":"metrics","from_offset":57,"to_offset":49999,"first_ts":1755100000000,"last_ts":1755700000000,"overridden_cursors":["uplink"]}`},
		{"_StreamGap", `{"stream":"commands","from_offset":1,"to_offset":2,"first_ts":1,"last_ts":2,"overridden_cursors":["downlink:child-01","uplink"]}`},
		{"_TimeSync", `{"now_ms": 1755000000000}`},
	}
	for _, c := range ok {
		if err := Validate(c[0], []byte(c[1])); err != nil {
			t.Fatalf("%s should validate: %v", c[0], err)
		}
	}
	bad := [][2]string{
		{"_Metric", `{"v":"notanumber"}`},
		{"_Metric", `{}`},
		{"_CmdParam", `{"correlation_id":"abc"}`}, // missing expires_at
		{"_Ack", `{"result_code":200}`},           // missing correlation_id
		{"_Unknown", `{}`},                        // unknown contract
		{"_Metric", `not json`},
		// _StreamGap: each case is missing exactly one required field (§6.4:
		// {stream, from_offset, to_offset, first_ts, last_ts, overridden_cursors}).
		{"_StreamGap", `{"from_offset":1,"to_offset":2,"first_ts":1,"last_ts":2,"overridden_cursors":["uplink"]}`},              // missing stream
		{"_StreamGap", `{"stream":"metrics","to_offset":2,"first_ts":1,"last_ts":2,"overridden_cursors":["uplink"]}`},           // missing from_offset
		{"_StreamGap", `{"stream":"metrics","from_offset":1,"first_ts":1,"last_ts":2,"overridden_cursors":["uplink"]}`},         // missing to_offset
		{"_StreamGap", `{"stream":"metrics","from_offset":1,"to_offset":2,"last_ts":2,"overridden_cursors":["uplink"]}`},        // missing first_ts
		{"_StreamGap", `{"stream":"metrics","from_offset":1,"to_offset":2,"first_ts":1,"overridden_cursors":["uplink"]}`},       // missing last_ts
		{"_StreamGap", `{"stream":"metrics","from_offset":1,"to_offset":2,"first_ts":1,"last_ts":2}`},                           // missing overridden_cursors
		{"_StreamGap", `{"stream":"metrics","from_offset":1,"to_offset":2,"first_ts":1,"last_ts":2,"overridden_cursors":[]}`},   // empty overridden_cursors
		{"_StreamGap", `{"stream":"metrics","from_offset":1,"to_offset":2,"first_ts":1,"last_ts":2,"overridden_cursors":[""]}`}, // empty cursor name
		{"_StreamGap", `{"stream":"metrics","from_offset":1,"to_offset":2,"first_ts":1,"last_ts":2,"overridden_cursors":[1]}`},  // non-string cursor name
		{"_TimeSync", `{}`},                // missing now_ms
		{"_TimeSync", `{"now_ms":"nope"}`}, // now_ms not a number
		// The identity fields do not cross over: an element carrying only the
		// registry's field name is unaddressable, and vice versa.
		{"_SystemElement", `{"ulid":"01HLINE1","name":"Linie 1"}`},
		{"_Signal", `{"ulid":"01HSIG1"}`},
		{"_Constant", `{"id":"01HCONST1","name":"Target speed","data_type":"int64"}`},
		{"_Constant", `{"id":"01HCONST1","name":"Target speed","data_type":"int64","value":1.5}`},
		{"_Constant", `{"id":"01HCONST1","name":"Target speed","data_type":"date","value":"2026-08-20"}`},
		{"_EditOperation", `{"id":"op-1","message":"created","result":"ok","topics":["colca/v1/_Constant/n1/a"]}`},
		{"_EnrolledIdentity", `{"id":"n-edge1"}`},
	}
	for _, c := range bad {
		if err := Validate(c[0], []byte(c[1])); err == nil {
			t.Fatalf("%s/%s must be rejected", c[0], c[1])
		}
	}
}

// Retention design §7.1/§7.3: the empty payload is the tombstone and it is
// valid EXACTLY for the KV-projecting state classes (data/entity) — there it
// retires the path. For events (commands, acks, gap markers) and unknown
// contracts deletion is not meaningful and the empty payload stays rejected.
func TestValidateEmptyPayloadTombstoneRule(t *testing.T) {
	for _, contract := range []string{"_Metric", "_EnrolledIdentity", "_Node", "_SystemElement", "_Signal", "_Constant", "_EditOperation"} {
		if err := Validate(contract, nil); err != nil {
			t.Errorf("empty payload on KV-projecting %s must validate (tombstone): %v", contract, err)
		}
		if err := Validate(contract, []byte{}); err != nil {
			t.Errorf("zero-length payload on KV-projecting %s must validate (tombstone): %v", contract, err)
		}
	}
	for _, contract := range []string{"_CmdParam", "_Ack", "_StreamGap", "_TimeSync", "_Unknown"} {
		if err := Validate(contract, nil); err == nil {
			t.Errorf("empty payload on %s must be rejected — deletion is not meaningful for events", contract)
		}
	}
}

// A definition is its own stream and its own class (definition-stream design
// §2/§4): state, so an empty payload retracts it, and nowhere near the command
// path.
func TestDefinitionsAreStateInTheirOwnStream(t *testing.T) {
	if got := ClassOf("_Group"); got != ClassDefinition {
		t.Fatalf("ClassOf(_Group) = %v, want ClassDefinition", got)
	}
	if got := StreamFor(ClassDefinition); got != "definitions" {
		t.Fatalf("StreamFor(ClassDefinition) = %q, want definitions", got)
	}
	if !IsState(ClassDefinition) {
		t.Fatal("a definition is state: KV-projected, retained, retractable")
	}
	// The tombstone: an empty payload retracts a definition, the way it retires
	// a data or entity path — and the way it is still refused for events.
	if err := Validate("_Group", nil); err != nil {
		t.Fatalf("a definition tombstone must be valid: %v", err)
	}
	if err := Validate("_CmdParam", nil); err == nil {
		t.Fatal("an empty command is still not a tombstone — events have nothing to retire")
	}
	// A definition names itself by "id", like the other data-model records.
	if err := Validate("_Group", []byte(`{"id":"01HGRP","name":"Ops"}`)); err != nil {
		t.Fatalf("_Group must validate on the builtin floor: %v", err)
	}
	if err := Validate("_Group", []byte(`{"name":"Ops"}`)); err == nil {
		t.Fatal("a definition with no id is unreachable and must be rejected")
	}
}

// Design §3.1. Both alarm-subject contracts leave the sample lane. Order
// inside a stream never changes, so this is the only thing that lets an alarm
// overtake a metrics backlog.
func TestAlarmContractsRouteToTheAlarmsStream(t *testing.T) {
	for _, contract := range []string{"_AlarmStateChange", "_NotificationDispatched"} {
		class := ClassOf(contract)
		if class != ClassAlarm {
			t.Fatalf("ClassOf(%s) = %v, want ClassAlarm", contract, class)
		}
		if got := StreamFor(class); got != "alarms" {
			t.Fatalf("StreamFor(ClassOf(%s)) = %q, want %q", contract, got, "alarms")
		}
	}
	// The config contracts stay put: only the two event contracts move.
	for _, contract := range []string{"_AlarmNotificationConfig", "_NotificationConfigStatus"} {
		if got := ClassOf(contract); got != ClassEntity {
			t.Fatalf("ClassOf(%s) = %v, want ClassEntity — the silence rail rides entities", contract, got)
		}
	}
}

// Design §3. An alarm is an EVENT. Getting this wrong is what made every alarm
// transition leak a KV entry at a path nothing ever overwrites and Prune never
// deletes — on the authoring node and on every ancestor.
func TestAlarmIsAnEventNotState(t *testing.T) {
	if IsState(ClassAlarm) {
		t.Fatal("IsState(ClassAlarm): alarm events would KV-project at a never-reused path and be retained forever")
	}
	if IsOwnedState(ClassAlarm) {
		t.Fatal("IsOwnedState(ClassAlarm): every ancestor would KV-project them too")
	}
	if IsAudit(ClassAlarm) {
		t.Fatal("IsAudit(ClassAlarm): audit means security event, not alarm event")
	}
	for name, got := range map[string]bool{
		"IsCommand":              IsCommand(ClassAlarm),
		"IsDefinition":           IsDefinition(ClassAlarm),
		"IsCommandAuthoredState": IsCommandAuthoredState(ClassAlarm),
		"IsNodeLocal":            IsNodeLocal(ClassAlarm),
		"NeedsStateRefresh":      NeedsStateRefresh(ClassAlarm),
	} {
		if got {
			t.Fatalf("%s(ClassAlarm) = true, want false", name)
		}
	}
	if !IsKnown(ClassAlarm) {
		t.Fatal("IsKnown(ClassAlarm) = false: the validated namespace would reject every alarm")
	}
}

// Design §3. Alarms rise, and only on their own stream — a child offering one
// on `metrics` is refused at the parent's door.
func TestAlarmRisesOnItsOwnStreamOnly(t *testing.T) {
	if !FlowsUp(ClassAlarm) {
		t.Fatal("FlowsUp(ClassAlarm) = false: an alarm would never reach a parent")
	}
	p, err := Parse("colca/v1/_AlarmStateChange/n-edge1/_colca/alarm-events/a1/e1")
	if err != nil {
		t.Fatal(err)
	}
	if !MatchesUplinkStream(ClassAlarm, p, "alarms") {
		t.Fatal("an alarm was refused on its own stream")
	}
	if MatchesUplinkStream(ClassAlarm, p, "metrics") {
		t.Fatal("an alarm replicated upward on metrics was accepted")
	}
}

func TestAlarmManifestName(t *testing.T) {
	class, ok := ClassFromManifest("alarm")
	if !ok || class != ClassAlarm {
		t.Fatalf("ClassFromManifest(\"alarm\") = (%v, %v), want (ClassAlarm, true)", class, ok)
	}
}

// Dataops-evaluator design §8. An annotation instance leaves KV-projected
// state entirely, mirroring the alarm precedent for the same volume reason: a
// part-cycle producer at 1 part/30 s is ~1M annotations/year/machine.
func TestAnnotationContractRoutesToTheAnnotationsStream(t *testing.T) {
	class := ClassOf("_Annotation")
	if class != ClassAnnotation {
		t.Fatalf("ClassOf(_Annotation) = %v, want ClassAnnotation", class)
	}
	if got := StreamFor(class); got != "annotations" {
		t.Fatalf("StreamFor(ClassOf(_Annotation)) = %q, want %q", got, "annotations")
	}
}

// Design §8. An annotation is an EVENT, not state: create, update (setting
// time_end) and delete are all appends carrying the same deterministic id,
// applied last-write-wins in stream order — never KV-projected, never
// retained.
func TestAnnotationIsAnEventNotState(t *testing.T) {
	if IsState(ClassAnnotation) {
		t.Fatal("IsState(ClassAnnotation): an annotation would KV-project at a never-reused path and be retained forever")
	}
	if IsOwnedState(ClassAnnotation) {
		t.Fatal("IsOwnedState(ClassAnnotation): every ancestor would KV-project them too")
	}
	if IsAudit(ClassAnnotation) {
		t.Fatal("IsAudit(ClassAnnotation): audit means security event, not an annotation instance")
	}
	for name, got := range map[string]bool{
		"IsCommand":              IsCommand(ClassAnnotation),
		"IsDefinition":           IsDefinition(ClassAnnotation),
		"IsCommandAuthoredState": IsCommandAuthoredState(ClassAnnotation),
		"IsNodeLocal":            IsNodeLocal(ClassAnnotation),
		"NeedsStateRefresh":      NeedsStateRefresh(ClassAnnotation),
	} {
		if got {
			t.Fatalf("%s(ClassAnnotation) = true, want false", name)
		}
	}
	if !IsKnown(ClassAnnotation) {
		t.Fatal("IsKnown(ClassAnnotation) = false: the validated namespace would reject every annotation")
	}
}

// Design §8. Annotations rise, and only on their own stream — a child
// offering one on `metrics` is refused at the parent's door.
func TestAnnotationRisesOnItsOwnStreamOnly(t *testing.T) {
	if !FlowsUp(ClassAnnotation) {
		t.Fatal("FlowsUp(ClassAnnotation) = false: an annotation would never reach a parent")
	}
	p, err := Parse("colca/v1/_Annotation/n-edge1/m1/press1/01JANNOTATIONULID")
	if err != nil {
		t.Fatal(err)
	}
	if !MatchesUplinkStream(ClassAnnotation, p, "annotations") {
		t.Fatal("an annotation was refused on its own stream")
	}
	if MatchesUplinkStream(ClassAnnotation, p, "metrics") {
		t.Fatal("an annotation replicated upward on metrics was accepted")
	}
}

func TestAnnotationManifestName(t *testing.T) {
	class, ok := ClassFromManifest("annotation")
	if !ok || class != ClassAnnotation {
		t.Fatalf("ClassFromManifest(\"annotation\") = (%v, %v), want (ClassAnnotation, true)", class, ok)
	}
}

// A semantic type answers what an entity IS. It rides the same rails as
// _MetadataType and _DataModel: ClassDefinition, addressed by id.
func TestSemanticTagIsADefinition(t *testing.T) {
	if got := ClassOf("_SemanticTag"); got != ClassDefinition {
		t.Fatalf("ClassOf(_SemanticTag) = %v, want ClassDefinition", got)
	}
	if err := Validate("_SemanticTag", []byte(`{"name":"temperature"}`)); err == nil {
		t.Fatal("Validate accepted a _SemanticTag with no id; definitions are addressed by id")
	}
	if err := Validate("_SemanticTag", []byte(`{"id":"01JSEMTAG","name":"temperature"}`)); err != nil {
		t.Fatalf("Validate rejected a well-formed _SemanticTag: %v", err)
	}
}

// Design §3.1: a cursor names a position in one identified peer's stream, so
// the peer's identity is part of its key. Two parents must never share a
// cursor — that is the whole defect this fixes.
func TestParentScopedCursorNames(t *testing.T) {
	const a = "aa11"
	const b = "bb22"
	for _, tc := range []struct {
		name string
		fn   func(string) string
		want string
	}{
		{"uplink", UplinkCursor, "up:" + a},
		{"downlink", DownlinkCursor, "down:" + a},
		{"downlink-def", DownlinkDefCursor, "down-def:" + a},
	} {
		if got := tc.fn(a); got != tc.want {
			t.Fatalf("%s(%q) = %q, want %q", tc.name, a, got, tc.want)
		}
		if tc.fn(a) == tc.fn(b) {
			t.Fatalf("%s collides across parents: %q", tc.name, tc.fn(a))
		}
	}
}

// The child-side uplink cursor and the PARENT-side downlink cursor are
// different facts about different nodes. They must not be able to collide:
// the parent-side name is keyed by child ULID, the child-side by parent
// pubkey, and a value that happened to be both would otherwise alias.
func TestChildAndParentSideCursorNamesDoNotAlias(t *testing.T) {
	const shared = "01JSVC"
	if DownlinkCursor(shared) == DownlinkCursorPrefix+shared {
		t.Fatal("child-side downlink cursor aliases the parent-side cursor for the same string")
	}
}

func TestResourceIsEntityClass(t *testing.T) {
	if got := ClassOf("_Resource"); got != ClassEntity {
		t.Fatalf("ClassOf(_Resource) = %v, want ClassEntity", got)
	}
	if got := StreamFor(ClassOf("_Resource")); got != "entities" {
		t.Fatalf("stream = %q, want entities", got)
	}
	if !IsState(ClassOf("_Resource")) {
		t.Fatal("_Resource must be state: KV-projected, retained, tombstonable")
	}
}

func TestResourceValidation(t *testing.T) {
	good := []byte(`{"id":"r1","system_element_id":"el1","filename":"manual.pdf",
		"content_type":"application/pdf","size_bytes":1834722,
		"sha256":"` + strings.Repeat("a", 64) + `"}`)
	if err := Validate("_Resource", good); err != nil {
		t.Fatalf("a complete resource must validate: %v", err)
	}
	// The denominator above makes each rejection below meaningful.
	for name, payload := range map[string]string{
		"no id":          `{"system_element_id":"el1","sha256":"` + strings.Repeat("a", 64) + `","filename":"m.pdf","content_type":"application/pdf","size_bytes":1}`,
		"no element":     `{"id":"r1","sha256":"` + strings.Repeat("a", 64) + `","filename":"m.pdf","content_type":"application/pdf","size_bytes":1}`,
		"no sha":         `{"id":"r1","system_element_id":"el1","filename":"m.pdf","content_type":"application/pdf","size_bytes":1}`,
		"short sha":      `{"id":"r1","system_element_id":"el1","sha256":"abc","filename":"m.pdf","content_type":"application/pdf","size_bytes":1}`,
		"upper-case sha": `{"id":"r1","system_element_id":"el1","sha256":"` + strings.Repeat("A", 64) + `","filename":"m.pdf","content_type":"application/pdf","size_bytes":1}`,
		"non-hex sha":    `{"id":"r1","system_element_id":"el1","sha256":"` + strings.Repeat("z", 64) + `","filename":"m.pdf","content_type":"application/pdf","size_bytes":1}`,
		"no filename":    `{"id":"r1","system_element_id":"el1","sha256":"` + strings.Repeat("a", 64) + `","content_type":"application/pdf","size_bytes":1}`,
		"negative size":  `{"id":"r1","system_element_id":"el1","sha256":"` + strings.Repeat("a", 64) + `","filename":"m.pdf","content_type":"application/pdf","size_bytes":-1}`,
	} {
		if err := Validate("_Resource", []byte(payload)); err == nil {
			t.Fatalf("%s: want a validation error", name)
		}
	}
}

func TestResourceTombstoneValidates(t *testing.T) {
	if err := Validate("_Resource", nil); err != nil {
		t.Fatalf("an empty payload is a tombstone and must validate: %v", err)
	}
}

func TestResourceBlobReadsTheDigest(t *testing.T) {
	payload := []byte(`{"id":"r1","system_element_id":"el1","filename":"m.pdf",
		"content_type":"application/pdf","size_bytes":1,"sha256":"` + strings.Repeat("a", 64) + `"}`)
	sha, ok := ResourceBlob(payload)
	if !ok || sha != strings.Repeat("a", 64) {
		t.Fatalf("ResourceBlob = %q, %v", sha, ok)
	}
	if _, ok := ResourceBlob([]byte(`{"id":"r1"}`)); ok {
		t.Fatal("an incomplete record has no usable digest")
	}
}

func TestResourceIDReadsTheID(t *testing.T) {
	payload := []byte(`{"id":"r1","system_element_id":"el1","filename":"m.pdf",
		"content_type":"application/pdf","size_bytes":1,"sha256":"` + strings.Repeat("a", 64) + `"}`)
	id, ok := ResourceID(payload)
	if !ok || id != "r1" {
		t.Fatalf("ResourceID = %q, %v", id, ok)
	}
	if _, ok := ResourceID([]byte(`{"id":"r1"}`)); ok {
		t.Fatal("an incomplete record has no usable id")
	}
}

func TestLiveBlobDigestsReadsOnlyResources(t *testing.T) {
	sha := strings.Repeat("a", 64)
	records := []KVRecord{
		{Topic: "colca/v1/_Resource/n1/press3/r1", Payload: []byte(`{"id":"r1","system_element_id":"el1",
			"filename":"m.pdf","content_type":"application/pdf","size_bytes":1,"sha256":"` + sha + `"}`)},
		// A record whose payload does not parse as a resource — whatever
		// contract it actually carries, LiveBlobDigests contributes nothing
		// for it. Scoping the scan to _Resource is the caller's job
		// (EntityStore.KVScanAll(ResourceContract)); this function only ever
		// asks "does this payload name a digest", not "what contract is this".
		{Topic: "colca/v1/_Signal/n1/press3/temp", Payload: []byte(`{"id":"s1"}`)},
		// A _Resource record whose payload fails validation (missing fields):
		// unreadable, so it cannot be shown to reference anything either.
		{Topic: "colca/v1/_Resource/n1/press3/broken", Payload: []byte(`{"id":"r2"}`)},
	}
	live := LiveBlobDigests(records)
	if _, ok := live[sha]; !ok {
		t.Fatal("a live resource's digest must be reported")
	}
	if len(live) != 1 {
		t.Fatalf("live = %v, want exactly the one readable resource digest", live)
	}
}
