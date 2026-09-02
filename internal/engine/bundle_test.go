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

// Floor parity (spec §12 level 1): a bundle generated to MIRROR the floor
// yields identical verdicts for a corpus of valid and invalid payloads —
// the cutover is behavior-preserving where it claims to be.
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

// bundleEngine builds an engine with a bundle declaring a NEW data contract
// and a NEW command contract that no broker release ever heard of.
func bundleEngine(t *testing.T) *Engine {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ids := testIDs()
	// NOTE the admin hazard class: uns.CmdClass maps UNKNOWN _Cmd* contracts
	// to "admin" (conservative by construction) — a bundle-declared new
	// command contract therefore demands the highest grant class until the
	// plugin names its hazard class. Pinned below.
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

// A bundle-declared NEW contract lands by rollout alone (§4.1 [delta]):
// routed to its manifest class's stream, validated by its schema.
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

	// The conservative hazard rule: an unknown _Cmd* contract demands the
	// ADMIN class — cmd:#:param does not cover it.
	_, err = e.IngestClient("paramonly", "colca/v1/_CmdWrite/m1/m1/set", []byte(`{"correlation_id": "c", "expires_at": 9e12}`))
	if err == nil || ReasonOf(err) != "cmd_denied" {
		t.Fatalf("param grant must not cover a new cmd contract (hazard defaults to admin), got %v", err)
	}

	// Unknown contracts stay rejected — with a bundle, the bundle is the
	// whole authority.
	if _, err := e.IngestClient("m1", "colca/v1/_Bogus/m1/x", []byte(`{}`)); err == nil {
		t.Fatal("unknown contract must be rejected under a bundle")
	}
	// The floor's stand-in contracts NOT in this bundle are unknown too (no
	// merge semantics, §7.1).
	if _, err := e.IngestClient("m1", "colca/v1/_Signal/m1/x", []byte(`{"ulid": "u"}`)); err == nil {
		t.Fatal("a contract absent from the bundle must be unknown — bundle replaces the floor entirely")
	}
}

// Tombstones under the bundle: driven by the manifest flag (§10.1); the
// refresh door rejects empty regardless (retention §6.5).
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

// Every door rejection carries a typed reason for the PUBACK mapping (§8.1).
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

// Alarm-stream design §3, the behaviour the class change exists for: a
// bundle-declared "alarm" contract routes to `alarms` and projects NO KV.
//
// The KV half is the load-bearing assertion. An alarm topic carries the event
// id, so its KV key is never reused, and Prune deletes only stream keys
// (`b/…`), never KV keys (`k\x00…`) — as ClassData every transition left one
// permanent entry behind, on the authoring node and on every ancestor after
// replication.
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
