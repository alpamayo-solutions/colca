// A signal's current value and a placed service's catalogue follow their
// position when a configure command moves it, on a real node with its broker:
// the value is readable at the new path as soon as the command is
// acknowledged, nothing stays retained at the old path, the producer keeps
// producing, and the historian sees the row it already had.
package tests

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/historian"
	"github.com/alpamayo-solutions/colca/internal/node"
)

const carryNode = "n-carry"

func startCarryNode(t *testing.T) *node.Node {
	t.Helper()
	base := t.TempDir()
	n, err := node.Start(&config.Config{
		ULID: carryNode, DataDir: filepath.Join(base, "data"), LogLevel: "debug",
		KeyFile:   filepath.Join(base, "node.key"),
		API:       config.API{Addr: "127.0.0.1:0", LocalAddr: "127.0.0.1:0", Token: tok},
		MQTT:      config.Endpoint{Addr: "127.0.0.1:0"},
		MQTTLocal: config.Endpoint{Addr: "127.0.0.1:0"},
		Repl:      config.Endpoint{Addr: "127.0.0.1:0"},
		Contracts: config.Contracts{Bundle: bindingBundle(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	return n
}

// configure runs one _CmdConfigure verb at n and waits for its 200.
func configure(t *testing.T, n *node.Node, verb string, body map[string]any) {
	t.Helper()
	corr := cmdAdmin(t, n, "colca/v1/_CmdConfigure/"+carryNode+"/"+verb, body)
	awaitAdminAck(t, n, "colca/v1/_Ack/"+carryNode+"/"+verb, corr, 200)
}

// retainedAt is the payload n holds at topic, or nil.
func retainedAt(t *testing.T, n *node.Node, topic string) map[string]any {
	t.Helper()
	path := strings.SplitN(topic, "/", 5)[4]
	for _, raw := range kvAt(t, n, path) {
		entry := raw.(map[string]any)
		if entry["topic"] == topic {
			payload, _ := entry["payload"].(map[string]any)
			return payload
		}
	}
	return nil
}

// metricRecords are the records on n's metrics stream at topic, oldest first.
func metricRecords(t *testing.T, n *node.Node, topic string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range fetchRecords(t, n, "metrics", unique("carry"), "", 1000) {
		if rec := raw.(map[string]any); rec["topic"] == topic {
			out = append(out, rec)
		}
	}
	return out
}

// historianRow is the row colca-historian writes for one metrics record.
func historianRow(t *testing.T, rec map[string]any) historian.Row {
	t.Helper()
	row, err := historian.RowFrom(rec["topic"].(string), []byte(mustJSON(rec["payload"])), int64(rec["ts"].(float64)))
	if err != nil {
		t.Fatalf("historian row of %v: %v", rec, err)
	}
	return row
}

// readOnly connects a machine that may only read, so a retained message it
// gets on SUBSCRIBE is what any consumer would get.
func readOnly(t *testing.T, n *node.Node) func(topic string) []string {
	t.Helper()
	reader := authtest.NewMachine(t, unique("reader"))
	authtest.EnrollAt(t, n.Registry, n.Engine, reader, "observer", "read:#")
	return func(topic string) []string {
		c := observer(t, n.MQTTAddr, unique("obs"), reader)
		var got []string
		for _, m := range collectFor(subscribeAll(t, c, topic), 500*time.Millisecond) {
			if m.Retained() {
				got = append(got, string(m.Payload()))
			}
		}
		c.Disconnect(50)
		return got
	}
}

// The upgrade step that moves a pre-1.33 segment: the element and its signal
// both move, and the producer is a machine placed at the element.
func TestARehomedSignalKeepsItsValueAndItsProducer(t *testing.T) {
	n := startCarryNode(t)
	read := readOnly(t, n)

	element := authtest.Place(t, n.Engine, "Craftcanfiller")
	plc := authtest.NewMachine(t, "plc-1")
	if _, _, err := n.Registry.Enroll([]byte(`{"ulid":"plc-1","pubkey":"` + plc.Pubkey +
		`","kind":"external","name":"plc-1","element":"` + element + `","grants":["write:` + element + `/#"]}`)); err != nil {
		t.Fatal(err)
	}
	pc := machine(t, n.MQTTAddr, plc)
	publish := func(topic, payload string) {
		t.Helper()
		if tk := pc.Publish(topic, 1, true, payload); !tk.WaitTimeout(5*time.Second) || tk.Error() != nil {
			t.Fatalf("plc publish %s: %v", topic, tk.Error())
		}
	}

	const (
		signal     = "01JSIG0000000000000000LEVL"
		oldCat     = "colca/v1/_DataTags/" + carryNode + "/Craftcanfiller/plc-1"
		newCat     = "colca/v1/_DataTags/" + carryNode + "/CraftCanFiller/plc-1"
		oldMetric  = "colca/v1/_Metric/" + carryNode + "/Craftcanfiller/Filllevel"
		newMetric  = "colca/v1/_Metric/" + carryNode + "/CraftCanFiller/FillLevel"
		value      = `{"v":3.5,"value":3.5,"signal_id":"` + signal + `","timestamp":1790000000.25}`
		laterValue = `{"v":4.5,"value":4.5,"signal_id":"` + signal + `","timestamp":1790000060.25}`
	)
	publish(oldCat, `{"connector":"plc-1","data_tags":[{"id":"01JTAG-LEVEL","name":"level","data_type":"float"}]}`)
	waitFor(t, "the placed catalogue stored", 10*time.Second, func() bool { return retainedAt(t, n, oldCat) != nil })
	signalBody := func(path string) map[string]any {
		return map[string]any{"signals": []any{map[string]any{"path": path, "signal": map[string]any{
			"id": signal, "name": "fillLevel", "data_tag": "01JTAG-LEVEL", "system_element_id": element,
		}}}}
	}
	elementBody := func(path string) map[string]any {
		return map[string]any{"elements": []any{map[string]any{"path": path, "element": map[string]any{
			"id": element, "name": "CraftCanFiller",
		}}}}
	}
	configure(t, n, "signal/upsert", signalBody("Craftcanfiller/Filllevel"))
	publish(oldMetric, value)
	waitFor(t, "the producer's value stored", 10*time.Second, func() bool { return retainedAt(t, n, oldMetric) != nil })
	original := metricRecords(t, n, oldMetric)[0]

	// The rehome, as rehome_topic_segments sends it: the element, then its signal.
	configure(t, n, "element/upsert", elementBody("CraftCanFiller"))
	configure(t, n, "signal/upsert", signalBody("CraftCanFiller/FillLevel"))

	// Readable at the new path at once, through /kv and as the broker's
	// retained message; nothing is left at the old one.
	if got := retainedAt(t, n, newMetric); got == nil || got["v"] != 3.5 {
		t.Fatalf("value at the new path = %v, want the producer's 3.5", got)
	}
	if got := retainedAt(t, n, oldMetric); got != nil {
		t.Fatalf("value still retained at the old path: %v", got)
	}
	if got := read(newMetric); len(got) != 1 || !strings.Contains(got[0], `"v":3.5`) {
		t.Fatalf("retained on SUBSCRIBE at the new path = %v, want the producer's value", got)
	}
	if got := read(oldMetric); len(got) != 0 {
		t.Fatalf("retained on SUBSCRIBE at the old path = %v, want nothing", got)
	}
	if retainedAt(t, n, newCat) == nil || retainedAt(t, n, oldCat) != nil {
		t.Fatal("the placed catalogue did not follow its element")
	}

	// History: the carried record is the same measurement, and the old path's
	// tombstone is none, so the historian keeps exactly the row it had.
	carried := metricRecords(t, n, newMetric)
	if len(carried) != 1 {
		t.Fatalf("records at the new path = %v, want the one carried", carried)
	}
	if historianRow(t, carried[0]).Timestamp != historianRow(t, original).Timestamp ||
		historianRow(t, carried[0]).SignalID != signal || *historianRow(t, carried[0]).Number != 3.5 {
		t.Fatalf("carried row %+v differs from the original %+v", historianRow(t, carried[0]), historianRow(t, original))
	}
	tombstones := metricRecords(t, n, oldMetric)
	if len(tombstones) != 2 || tombstones[1]["payload"] != nil {
		t.Fatalf("old path records = %v, want the value and one tombstone", tombstones)
	}

	// The producer rule is untouched: the placed producer, whose catalogue
	// moved with it, keeps publishing; a stranger is refused.
	publish(newMetric, laterValue)
	waitFor(t, "the producer's next value at the new path", 10*time.Second, func() bool {
		got := retainedAt(t, n, newMetric)
		return got != nil && got["v"] == 4.5
	})
	if code, out := localPublish(t, n, "stray-service", newMetric, `{"v":1234}`); code != 403 || out["reason"] != "not_producer" {
		t.Fatalf("stranger publishing the moved signal: %d %v, want 403 not_producer", code, out)
	}

	// Re-running the step moves nothing.
	metricsHead := n.Engine.Store().NextOffset("metrics")
	configure(t, n, "element/upsert", elementBody("CraftCanFiller"))
	configure(t, n, "signal/upsert", signalBody("CraftCanFiller/FillLevel"))
	if got := n.Engine.Store().NextOffset("metrics"); got != metricsHead {
		t.Fatalf("a re-run wrote %d metrics records", got-metricsHead)
	}
	if got := retainedAt(t, n, newMetric); got == nil || got["v"] != 4.5 {
		t.Fatalf("a re-run changed the value: %v", got)
	}
}

// A rename and a reparent through _CmdConfigure, for an unbound signal whose
// value a local service wrote.
func TestASignalsValueFollowsARenameAndAReparent(t *testing.T) {
	n := startCarryNode(t)
	read := readOnly(t, n)
	line1 := authtest.Place(t, n.Engine, "line1")
	line2 := authtest.Place(t, n.Engine, "line2")

	const signal = "01JSIG0000000000000000TEMP"
	move := func(path, element string) {
		t.Helper()
		configure(t, n, "signal/upsert", map[string]any{"signals": []any{map[string]any{
			"path": path, "signal": map[string]any{"id": signal, "name": "temp", "system_element_id": element},
		}}})
	}
	metric := func(path string) string { return "colca/v1/_Metric/" + carryNode + "/" + path }

	move("line1/temp", line1)
	if code, out := localPublish(t, n, "probe", metric("line1/temp"),
		`{"v":21.5,"value":21.5,"signal_id":"`+signal+`","timestamp":1790000000}`); code != 200 {
		t.Fatalf("value publish: %d %v", code, out)
	}

	move("line2/temp", line2) // reparent
	if got := retainedAt(t, n, metric("line2/temp")); got == nil || got["v"] != 21.5 {
		t.Fatalf("after the reparent the new path holds %v", got)
	}
	move("line2/temperature", line2) // rename
	if got := retainedAt(t, n, metric("line2/temperature")); got == nil || got["v"] != 21.5 {
		t.Fatalf("after the rename the new path holds %v", got)
	}
	for _, old := range []string{"line1/temp", "line2/temp"} {
		if got := retainedAt(t, n, metric(old)); got != nil {
			t.Fatalf("value still retained at %s: %v", old, got)
		}
		if got := read(metric(old)); len(got) != 0 {
			t.Fatalf("retained on SUBSCRIBE at %s = %v", old, got)
		}
	}
	if got := read(metric("line2/temperature")); len(got) != 1 {
		t.Fatalf("retained on SUBSCRIBE at the final path = %v", got)
	}

	// Every record on the stream is the same measurement or a tombstone, so
	// the historian has one row for it, as before the moves.
	rows := map[string]bool{}
	for _, raw := range fetchRecords(t, n, "metrics", unique("rows"), "", 1000) {
		rec := raw.(map[string]any)
		if rec["payload"] == nil {
			continue
		}
		var payload map[string]any
		_ = json.Unmarshal([]byte(mustJSON(rec["payload"])), &payload)
		if payload["signal_id"] != signal {
			continue
		}
		row := historianRow(t, rec)
		rows[row.SignalID+"@"+row.Timestamp.String()] = true
	}
	if len(rows) != 1 {
		t.Fatalf("historian rows for the signal = %v, want one", rows)
	}
}
