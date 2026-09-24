package clockwork

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/door"
)

type fakeDoor struct {
	rows      []door.KVEntry
	published []any
	fail      bool
}

func (d *fakeDoor) Self(context.Context) (door.Self, error) {
	return door.Self{ULID: "service", Node: "node"}, nil
}
func (d *fakeDoor) KV(context.Context, string, ...string) ([]door.KVEntry, error) { return d.rows, nil }
func (d *fakeDoor) Publish(_ context.Context, _ string, p any) error {
	if d.fail {
		return errors.New("unavailable")
	}
	d.published = append(d.published, p)
	return nil
}

func TestCompletionRequiresFreshUpstreamAndDurableDrain(t *testing.T) {
	ctx := context.Background()
	d := &fakeDoor{rows: []door.KVEntry{{Topic: "colca/v1/_ClockDefinition/node/test", Payload: json.RawMessage(`{"id":"test","run_id":"run","revision":1,"real_anchor":100,"factory_anchor":10,"rate":1000,"stop_at":20}`)}}}
	calls := 0
	fail := true
	g := &Gate{Door: d, Name: "historian", Topic: d.rows[0].Topic, Dependencies: []string{"./dataops"}, Drain: func(context.Context, float64) (bool, error) {
		calls++
		if fail {
			return false, errors.New("database unavailable")
		}
		return true, nil
	}}
	step := func() error { g.lastCheck = time.Time{}; _, err := g.Once(ctx, 200); return err }
	if err := step(); err != nil || calls != 0 {
		t.Fatalf("missing upstream ran drain: %v %d", err, calls)
	}
	row := door.KVEntry{Topic: "colca/v1/_ServiceDetails/node/dataops/_service", Payload: json.RawMessage(`{"name":"dataops","is_active":true,"metadata":{"application_clock":{"ready":true,"run_id":"run","processed_at":20,"observed_at":100}}}`)}
	d.rows = append(d.rows, row)
	if err := step(); err != nil || calls != 0 {
		t.Fatalf("stale upstream ran drain: %v %d", err, calls)
	}
	d.rows[1].Payload = json.RawMessage(`{"name":"dataops","is_active":true,"metadata":{"application_clock":{"ready":true,"run_id":"run","processed_at":20,"observed_at":200}}}`)
	if err := step(); err != nil || calls != 0 {
		t.Fatal("status overtook its sample lane")
	}
	d.rows = append(d.rows, door.KVEntry{Topic: "colca/v1/_ClockProgress/node/dataops/_service", Payload: json.RawMessage(`{"run_id":"run","processed_at":20}`)})
	if step() == nil || len(d.published) != 0 {
		t.Fatal("failed database work acknowledged")
	}
	fail = false
	d.fail = true
	if step() == nil || g.completed != nil {
		t.Fatal("failed acknowledgement advanced progress")
	}
	d.fail = false
	if err := step(); err != nil || len(d.published) != 2 || g.completed == nil || *g.completed != 20 {
		t.Fatalf("successful drain not acknowledged: %v", err)
	}
	before := calls
	if err := step(); err != nil || calls != before {
		t.Fatal("completed window repeated")
	}
	g.lastReport = time.Time{}
	if err := step(); err != nil || len(d.published) != 4 || calls != before {
		t.Fatal("paused heartbeat did not preserve completed position")
	}
}

func TestDependencyConfiguration(t *testing.T) {
	if deps, err := Dependencies("", ""); err != nil || deps != nil {
		t.Fatal("optional clock required")
	}
	if deps, err := Dependencies("[]", "colca/v1/_ClockDefinition/node/test"); err != nil || deps == nil {
		t.Fatal("empty source dependencies disabled gate")
	}
	for _, raw := range []string{"null", "{}", "[1]", "[\"#\"]"} {
		if _, err := Dependencies(raw, "colca/v1/_ClockDefinition/node/test"); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestWaitingConsumerUsesAValidServiceCategory(t *testing.T) {
	d := &fakeDoor{}
	g := &Gate{Door: d, Name: "historian"}
	if err := g.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(d.published) != 1 {
		t.Fatal("registration unexpectedly published clock progress")
	}
	row := d.published[0].(map[string]any)
	if row["service_type"] != "other" {
		t.Fatal("historian is not a declared ServiceType; use other")
	}
}
