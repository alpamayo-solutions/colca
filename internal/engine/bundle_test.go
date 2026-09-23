package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// writeBundle builds a loadable fixture bundle with a correct digest.
func writeBundle(t *testing.T, contractEntries map[string]any) *contracts.Table {
	t.Helper()
	body := map[string]any{
		"bundle_version": "test",
		"source":         map[string]any{"package": "colca-data-contracts", "git_sha": "fixture"},
		"contracts":      contractEntries,
	}
	canon, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canon)
	full := map[string]any{"digest": hex.EncodeToString(sum[:]), "generated_at": "2026-08-17T00:00:00Z"}
	for k, v := range body {
		full[k] = v
	}
	raw, _ := json.Marshal(full)
	p := filepath.Join(t.TempDir(), "bundle.json")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	tbl, err := contracts.Load(p, "")
	if err != nil {
		t.Fatalf("fixture bundle failed to load: %v", err)
	}
	return tbl
}

func obj(class string, tombstone bool, required []string, props map[string]any) map[string]any {
	schema := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		schema["required"] = required
	}
	return map[string]any{"class": class, "tombstone": tombstone, "schema": schema}
}

// mirrorBundle replicates the builtin floor's five core contracts.
func mirrorBundle(t *testing.T) *contracts.Table {
	numeric := map[string]any{"type": "number"}
	str := map[string]any{"type": "string", "minLength": 1}
	return writeBundle(t, map[string]any{
		"_Metric":        obj("data", true, []string{"v"}, map[string]any{"v": numeric}),
		"_SystemElement": obj("entity", true, []string{"id"}, map[string]any{"id": str}),
		"_Signal":        obj("entity", true, []string{"id"}, map[string]any{"id": str}),
		"_Ack":           obj("ack", false, []string{"correlation_id", "result_code"}, map[string]any{"correlation_id": str, "result_code": numeric}),
		"_CmdParam":      obj("cmd", false, []string{"correlation_id", "expires_at"}, map[string]any{"correlation_id": str, "expires_at": numeric}),
	})
}

// A bundle generated to mirror the builtin floor gives the same verdicts on a
// corpus of valid and invalid payloads.
func TestFloorParityCorpus(t *testing.T) {
	floor := newEngine(t)
	bundled := newEngine(t)
	bundled.SetContracts(mirrorBundle(t))

	corpus := []struct {
		contract string
		payload  string
	}{
		{"_Metric", `{"v": 3}`},
		{"_Metric", `{"v": "not-a-number"}`},
		{"_Metric", `{}`},
		{"_Metric", ``}, // tombstone: data class admits it
		{"_SystemElement", `{"id": "x"}`},
		{"_SystemElement", `{"id": ""}`},
		{"_SystemElement", `{"ulid": "x"}`}, // the registry's field name is not this contract's
		{"_SystemElement", ``},
		{"_Ack", `{"correlation_id": "c", "result_code": 200}`},
		{"_Ack", `{"correlation_id": "c"}`},
		{"_Ack", ``}, // events: tombstone rejected
		{"_CmdParam", `{"correlation_id": "c", "expires_at": 99}`},
		{"_CmdParam", `{"correlation_id": "c"}`},
		{"_CmdParam", ``},
		{"_Unknown", `{"x": 1}`},
	}
	for _, c := range corpus {
		fv := floor.validateContract(c.contract, []byte(c.payload))
		bv := bundled.validateContract(c.contract, []byte(c.payload))
		if (fv == nil) != (bv == nil) {
			t.Errorf("verdict drift for %s %q: floor=%v bundle=%v", c.contract, c.payload, fv, bv)
		}
	}
}

// bundleEngine builds an engine with a bundle declaring a data contract and a
// command contract the builtin floor does not know.
func bundleEngine(t *testing.T) *Engine {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ids := testIDs()
	// Unknown _Cmd* contracts map to the admin hazard class until the plugin names
	// one, so the new command contract needs the highest grant. Pinned below.
	ids.entries["writer"] = &uns.Entry{ULID: "writer", Kind: uns.KindExternal, Element: "el-writer", Grants: []string{"cmd:#:admin"}}
	ids.entries["paramonly"] = &uns.Entry{ULID: "paramonly", Kind: uns.KindExternal, Element: "el-paramonly", Grants: []string{"cmd:#:param"}}
	e := New(s, &config.Config{ULID: "n-edge1"}, ids, nil, nil, nil)
	placeTestElements(t, e)
	numeric := map[string]any{"type": "number"}
	str := map[string]any{"type": "string", "minLength": 1}
	e.SetContracts(writeBundle(t, map[string]any{
		"_Reading":  obj("data", true, []string{"value", "signal_id"}, map[string]any{"value": map[string]any{}, "signal_id": str}),
		"_CmdWrite": obj("cmd", false, []string{"correlation_id", "expires_at"}, map[string]any{"correlation_id": str, "expires_at": numeric}),
		"_Metric":   obj("data", true, []string{"v"}, map[string]any{"v": numeric}),
	}))
	return e
}

// A bundle-declared contract is routed by its manifest class and validated by
// its schema.
func TestBundleDeclaredContractRoutesAndValidates(t *testing.T) {
	e := bundleEngine(t)

	res, err := e.IngestClient("m1", "colca/v1/_Reading/n-edge1/m1/temp", []byte(`{"value": 3, "signal_id": "s1"}`))
	if err != nil {
		t.Fatalf("new data contract must ingest under the bundle: %v", err)
	}
	if res.Stream != "metrics" {
		t.Fatalf("class data must route to the metrics stream, got %s", res.Stream)
	}

	// Schema enforced: missing signal_id rejected with the validation reason.
	_, err = e.IngestClient("m1", "colca/v1/_Reading/n-edge1/m1/temp", []byte(`{"value": 3}`))
	if err == nil || ReasonOf(err) != "validation" {
		t.Fatalf("schema reject must carry reason=validation, got %v (reason %q)", err, ReasonOf(err))
	}

	// New command class: cmd grant + class from the bundle.
	res, err = e.IngestClient("writer", "colca/v1/_CmdWrite/m1/m1/set", []byte(`{"correlation_id": "c", "expires_at": 9e12}`))
	if err != nil {
		t.Fatalf("bundle-declared command must ingest: %v", err)
	}
	if res.Stream != "commands" {
		t.Fatalf("class cmd must route to the commands stream, got %s", res.Stream)
	}

	// An unknown _Cmd* contract needs the admin class; cmd:#:param does not cover it.
	_, err = e.IngestClient("paramonly", "colca/v1/_CmdWrite/m1/m1/set", []byte(`{"correlation_id": "c", "expires_at": 9e12}`))
	if err == nil || ReasonOf(err) != "cmd_denied" {
		t.Fatalf("param grant must not cover a new cmd contract (hazard defaults to admin), got %v", err)
	}

	// Unknown contracts stay rejected: with a bundle loaded, the bundle is the whole
	// authority.
	if _, err := e.IngestClient("m1", "colca/v1/_Bogus/m1/x", []byte(`{}`)); err == nil {
		t.Fatal("unknown contract must be rejected under a bundle")
	}
	// Floor contracts missing from the bundle are unknown too; the two are never
	// merged.
	if _, err := e.IngestClient("m1", "colca/v1/_Signal/m1/x", []byte(`{"ulid": "u"}`)); err == nil {
		t.Fatal("a contract absent from the bundle must be unknown — bundle replaces the floor entirely")
	}
}

// With a bundle the manifest flag decides whether a contract accepts tombstones;
// the refresh door rejects empty payloads regardless.
func TestBundleTombstoneFlag(t *testing.T) {
	e := bundleEngine(t)

	// data + tombstone:true → empty payload retires the path.
	if _, err := e.IngestClient("m1", "colca/v1/_Reading/n-edge1/m1/temp", nil); err != nil {
		t.Fatalf("tombstone on a tombstonable contract must be accepted: %v", err)
	}
	// cmd + tombstone:false → rejected.
	if _, err := e.IngestClient("writer", "colca/v1/_CmdWrite/m1/m1/set", nil); err == nil {
		t.Fatal("tombstone on a non-tombstonable contract must be rejected")
	}
	// Refresh: empty rejected regardless of the flag.
	if _, _, err := e.IngestRefresh("colca/v1/_Reading/m1/temp", nil, 0); err == nil ||
		!strings.Contains(err.Error(), "must not be empty") {
		t.Fatalf("refresh must reject empty payloads regardless of tombstone flag, got %v", err)
	}
}

// Builtin-only contracts stay answered by the binary even with a bundle.
func TestBuiltinOnlyContractsBypassTheBundle(t *testing.T) {
	e := bundleEngine(t)
	if e.ClassOf("_StreamGap") != uns.ClassGap {
		t.Fatal("_StreamGap must keep its builtin class under a bundle")
	}
	if e.ClassOf("_TimeSync") != uns.ClassTimeSync {
		t.Fatal("_TimeSync must keep its builtin class under a bundle")
	}
	// And _EnrolledIdentity is still enrollment-door-only at the doors.
	if _, err := e.IngestClient("m1", "colca/v1/_EnrolledIdentity/m1/_colca/identities/u", []byte(`{"ulid": "u"}`)); err == nil {
		t.Fatal("_EnrolledIdentity must stay enrollment-door-only under a bundle")
	}
}

// Every door rejection carries a typed reason for the PUBACK mapping.
func TestRejectErrorsCarryReasons(t *testing.T) {
	e := newEngine(t)
	cases := []struct {
		reason string
		run    func() error
	}{
		{"grammar", func() error { _, err := e.IngestClient("m1", "colca/v1/bad", nil); return err }},
		{"registry_contract", func() error {
			_, err := e.IngestClient("m1", "colca/v1/_EnrolledIdentity/m1/_colca/identities/u", []byte(`{"ulid":"u"}`))
			return err
		}},
		{"node_id", func() error {
			_, err := e.IngestClient("m1", "colca/v1/_Metric/m2/temp", []byte(`{"v":1}`))
			return err
		}},
		{"validation", func() error {
			_, err := e.IngestClient("m1", "colca/v1/_Metric/n-edge1/m1/temp", []byte(`{"nope":1}`))
			return err
		}},
		{"write_denied", func() error {
			_, err := e.IngestClient("hmi", "colca/v1/_Metric/n-edge1/hmi/t", []byte(`{"v":1}`))
			return err
		}},
		{"cmd_denied", func() error {
			_, err := e.IngestClient("m1", "colca/v1/_CmdParam/m1/m1/go", []byte(`{"correlation_id":"c","expires_at":9e12}`))
			return err
		}},
	}
	for _, c := range cases {
		err := c.run()
		if err == nil || ReasonOf(err) != c.reason {
			t.Errorf("want reason %q, got %v (reason %q)", c.reason, err, ReasonOf(err))
		}
	}
}

// Under the real generated bundle the door refuses a _SystemElement whose id is
// not a ULID, naming the pattern, and accepts a ULID. A longer id would otherwise
// stall the projector's 26-character column.
func TestRealBundleRefusesNonULIDEntityIDsAtTheDoor(t *testing.T) {
	tbl, err := contracts.Load(contractstest.GeneratedBundlePath(t), "")
	if err != nil {
		t.Fatalf("real generated bundle failed to load: %v", err)
	}
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	e := New(s, &config.Config{ULID: "n-edge1"}, testIDs(), nil, nil, nil)
	e.SetContracts(tbl)

	// POST /publish returns the schema error verbatim, so the publisher sees which
	// field broke which pattern.
	const pattern = "^[0-9A-HJKMNP-TV-Z]{26}$"
	topic := "colca/v1/_SystemElement/n-edge1/site1"
	const ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	_, err = e.IngestAdmin(topic, []byte(`{"id":"`+ulid+`EXTRA","name":"site1"}`))
	if err == nil || !strings.Contains(err.Error(), "'/id'") || !strings.Contains(err.Error(), pattern) {
		t.Fatalf("a 31-character id must be refused at the door naming /id and the ULID pattern, got: %v", err)
	}
	if _, err := e.IngestAdmin(topic, []byte(`{"id":"`+ulid+`","name":"site1"}`)); err != nil {
		t.Fatalf("a ULID id must be accepted: %v", err)
	}
	// The references share the definition: a parent_id that is not a ULID is
	// refused the same way, so a bad reference cannot enter through the back.
	_, err = e.IngestAdmin("colca/v1/_SystemElement/n-edge1/site1/line1",
		[]byte(`{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAW","name":"line1","parent_id":"el-site1"}`))
	if err == nil || !strings.Contains(err.Error(), "'/parent_id'") || !strings.Contains(err.Error(), pattern) {
		t.Fatalf("a non-ULID parent_id must be refused naming /parent_id and the pattern, got: %v", err)
	}
}

// A bundle-declared alarm contract routes to the alarms stream and projects no
// KV. Every alarm topic carries a new event id and pruning never deletes KV keys,
// so a KV entry per transition would stay forever, here and at every ancestor.
func TestAlarmClassRoutesToAlarmsAndProjectsNoKV(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	e := New(s, &config.Config{ULID: "n-edge1"}, testIDs(), nil, nil, nil)
	placeTestElements(t, e)
	str := map[string]any{"type": "string", "minLength": 1}
	e.SetContracts(writeBundle(t, map[string]any{
		"_AlarmStateChange": obj("alarm", false,
			[]string{"event_id", "alarm_id", "to_status"},
			map[string]any{"event_id": str, "alarm_id": str, "to_status": str}),
		"_Metric": obj("data", true, []string{"v"}, map[string]any{"v": map[string]any{"type": "number"}}),
	}))

	res, err := e.IngestClient("m1",
		"colca/v1/_AlarmStateChange/n-edge1/m1/alarm-events/a1/e1",
		[]byte(`{"event_id":"e1","alarm_id":"a1","to_status":"firing"}`))
	if err != nil {
		t.Fatalf("a bundle-declared alarm contract must ingest: %v", err)
	}
	if res.Stream != "alarms" {
		t.Fatalf("alarm ingest landed on %q, want \"alarms\"", res.Stream)
	}
	// Scoped to the alarm: placeTestElements leaves _SystemElement entries,
	// which are entity state and belong in the KV.
	for _, entry := range mustKVScan(t, s, "") {
		if strings.Contains(entry.Topic, "_AlarmStateChange") {
			t.Fatalf("alarm ingest projected a KV entry at %q — its path carries "+
				"the event id, so nothing ever overwrites it and Prune never "+
				"deletes KV keys: it would live forever, here and at every ancestor",
				entry.Topic)
		}
	}
	if n := s.NextOffset("metrics"); n != 1 {
		t.Fatalf("the metrics stream advanced to %d — the alarm went to the "+
			"sample lane, where it would queue behind every buffered metric", n)
	}
}

// Under the real generated bundle a producer's annotation carries its element
// and the annotations it belongs to: the record is stored as published, and a
// malformed id in either field is refused at the door.
func TestRealBundleStoresAnnotationPlacementAndRelations(t *testing.T) {
	tbl, err := contracts.Load(contractstest.GeneratedBundlePath(t), "")
	if err != nil {
		t.Fatalf("real generated bundle failed to load: %v", err)
	}
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	e := New(s, &config.Config{ULID: "n-edge1"}, testIDs(), nil, nil, nil)
	e.SetContracts(tbl)

	const id = "01M2AB5YWM56SH9B2EQ3S5VNXF"
	topic := "colca/v1/_Annotation/n-edge1/production/line1/" + id
	headPass := `{"annotation_id":"` + id + `","annotation_type_id":"01M2AB5YWSZTAFBKYYYA5C0TE7",` +
		`"time_start":1710000000.0,"signal_ids":["01BX5ZZKBKACTAV9WEVGEMMVRZ"],` +
		`"system_element_id":"01BX5ZZKBKACTAV9WEVGEMMVRA","related_annotation_ids":["01M2AB5YWM56SH9B2EQ3S5VNXG"]}`
	res, err := e.IngestAdmin(topic, []byte(headPass))
	if err != nil {
		t.Fatalf("an annotation with placement and relations must be accepted: %v", err)
	}
	stored, _, err := s.Read("annotations", res.Offset, 1, nil)
	if err != nil || len(stored) != 1 {
		t.Fatalf("read back = %d records, %v", len(stored), err)
	}
	var got map[string]any
	if err := json.Unmarshal(stored[0].Payload, &got); err != nil {
		t.Fatal(err)
	}
	related, _ := got["related_annotation_ids"].([]any)
	if got["system_element_id"] != "01BX5ZZKBKACTAV9WEVGEMMVRA" || len(related) != 1 || related[0] != "01M2AB5YWM56SH9B2EQ3S5VNXG" {
		t.Fatalf("stored annotation lost its placement or relations: %s", stored[0].Payload)
	}

	for field, bad := range map[string]string{
		"system_element_id":      `"line1"`,
		"related_annotation_ids": `["panel-1"]`,
	} {
		var payload map[string]json.RawMessage
		_ = json.Unmarshal([]byte(headPass), &payload)
		payload[field] = json.RawMessage(bad)
		raw, _ := json.Marshal(payload)
		if _, err := e.IngestAdmin(topic, raw); err == nil || !strings.Contains(err.Error(), "/"+field) {
			t.Fatalf("a malformed %s must be refused naming the field, got: %v", field, err)
		}
	}
}
