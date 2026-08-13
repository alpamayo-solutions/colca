package engine

import (
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func newEngine(t *testing.T) *Engine {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := &config.Config{ULID: "n-edge1", Clients: []config.Client{{ULID: "m1", Token: "tok", Mount: "m1"}}}
	return New(s, cfg, nil) // nil = no local MQTT delivery in unit tests
}

func TestClientPublishMountAndKV(t *testing.T) {
	e := newEngine(t)
	res, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", []byte(`{"v":7}`))
	if err != nil {
		t.Fatal(err)
	}
	if res.Stream != "metrics" || res.Offset != 1 {
		t.Fatalf("%+v", res)
	}
	recs, _, _ := e.Store().Read("metrics", 1, 10, nil)
	if recs[0].Topic != "colca/v1/_Metric/m1/m1/temp" {
		t.Fatalf("mount rewrite failed: %s", recs[0].Topic)
	}
	kv := e.Store().KVScan("m1/temp")
	if len(kv) != 1 || kv[0].NodeID != "m1" {
		t.Fatalf("kv: %+v", kv)
	}
}

func TestClientIdentityRule(t *testing.T) {
	e := newEngine(t)
	_, err := e.IngestClient("m1", "colca/v1/_Metric/OTHER/temp", []byte(`{"v":1}`))
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("level-4 rule not enforced: %v", err)
	}
	_, err = e.IngestClient("m1", "colca/v1/_CmdParam/m1/x", []byte(`{"correlation_id":"c","expires_at":1}`))
	if err == nil {
		t.Fatal("clients must not publish commands")
	}
}

func TestValidationReject(t *testing.T) {
	e := newEngine(t)
	_, err := e.IngestClient("m1", "colca/v1/_Metric/m1/temp", []byte(`{"v":"bad"}`))
	if err == nil {
		t.Fatal("invalid payload must be rejected")
	}
	if e.Store().NextOffset("metrics") != 1 {
		t.Fatal("rejected payload must not be persisted")
	}
}

func TestAdminPublishCmdNoRewrite(t *testing.T) {
	e := newEngine(t)
	res, err := e.IngestAdmin("colca/v1/_CmdParam/m1/m1/set-speed", []byte(`{"correlation_id":"c1","expires_at":99999999999}`))
	if err != nil {
		t.Fatal(err)
	}
	if res.Stream != "commands" {
		t.Fatalf("%+v", res)
	}
	recs, _, _ := e.Store().Read("commands", 1, 10, nil)
	if recs[0].Topic != "colca/v1/_CmdParam/m1/m1/set-speed" {
		t.Fatal("admin publish must not be rewritten")
	}
}

func TestNonUnsIgnored(t *testing.T) {
	e := newEngine(t)
	res, err := e.IngestClient("m1", "other/random/topic", []byte("x"))
	if err != nil {
		t.Fatal("non-UNS must pass through without error")
	}
	if res.Persisted {
		t.Fatal("non-UNS must not be persisted")
	}
}
