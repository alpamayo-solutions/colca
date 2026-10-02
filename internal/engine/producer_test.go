package engine

import (
	"errors"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// producerEngine is a node with a PLC connector and a stray service, both
// unplaced local services, a dataops service placed at m1, and the machine m1
// with a write grant over m1. The PLC's catalogue holds TAG-SETPOINT, dataops'
// holds TAG-OEE, and one signal is bound to each.
func producerEngine(t *testing.T) *Engine {
	t.Helper()
	ids := testIDs()
	ids.entries["plc"] = &uns.Entry{ULID: "plc", Kind: uns.KindLocal, Name: "plc"}
	ids.entries["probe"] = &uns.Entry{ULID: "probe", Kind: uns.KindLocal, Name: "probe"}
	ids.entries["dataops"] = &uns.Entry{ULID: "dataops", Kind: uns.KindLocal, Name: "dataops", Element: "el-m1"}
	e := newEngineWithIDs(t, ids)
	e.SetContracts(writeBundle(t, map[string]any{
		"_Metric":        obj("data", false, nil, map[string]any{}),
		"_Signal":        obj("entity", true, nil, map[string]any{}),
		"_SystemElement": obj("entity", true, nil, map[string]any{}),
		"_DataTags":      obj("entity", true, nil, map[string]any{}),
	}))
	mustIngest(t, e, "plc", "colca/v1/_DataTags/n-edge1/plc", `{"data_tags":[{"id":"TAG-SETPOINT"}]}`)
	mustIngest(t, e, "dataops", "colca/v1/_DataTags/n-edge1/m1/dataops", `{"data_tags":[{"id":"TAG-OEE"}]}`)
	mustAdmin(t, e, "colca/v1/_Signal/n-edge1/m1/setpoint", `{"id":"01SIGSETPOINT","data_tag":"TAG-SETPOINT"}`)
	mustAdmin(t, e, "colca/v1/_Signal/n-edge1/m1/oee", `{"id":"01SIGOEE","data_tag":"TAG-OEE"}`)
	mustAdmin(t, e, "colca/v1/_Signal/n-edge1/m1/manual", `{"id":"01SIGMANUAL"}`)
	return e
}

func mustIngest(t *testing.T, e *Engine, identity, topic, payload string) {
	t.Helper()
	if _, err := e.IngestClient(identity, topic, []byte(payload)); err != nil {
		t.Fatalf("%s publishing %s: %v", identity, topic, err)
	}
}

func mustAdmin(t *testing.T, e *Engine, topic, payload string) {
	t.Helper()
	if _, err := e.IngestAdmin(topic, []byte(payload)); err != nil {
		t.Fatalf("admin publishing %s: %v", topic, err)
	}
}

// assertNotProducer checks a refusal is the producer rule's, typed as a denial
// (403 on HTTP, not authorized on MQTT), and that nothing was stored.
func assertNotProducer(t *testing.T, e *Engine, identity, topic string) {
	t.Helper()
	before := e.Store().NextOffset("metrics")
	_, err := e.IngestClient(identity, topic, []byte(`{"v":1234.0}`))
	if err == nil {
		t.Fatalf("%s published %s, which it does not produce", identity, topic)
	}
	assertRejectReason(t, err, metrics.ReasonNotProducer)
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("refusal is not a denial, so HTTP would answer 422: %v", err)
	}
	if after := e.Store().NextOffset("metrics"); after != before {
		t.Fatalf("a refused metric was stored: metrics offset %d -> %d", before, after)
	}
}

func TestABoundSignalsMetricIsAcceptedOnlyFromItsProducer(t *testing.T) {
	e := producerEngine(t)

	// The producer first, so a broken lookup cannot make the refusals pass.
	mustIngest(t, e, "plc", "colca/v1/_Metric/n-edge1/m1/setpoint", `{"v":3.5}`)

	// A local service whose write zone is the whole node, and a machine with an
	// explicit write grant over m1: both may write there, neither produces it.
	assertNotProducer(t, e, "probe", "colca/v1/_Metric/n-edge1/m1/setpoint")
	assertNotProducer(t, e, "m1", "colca/v1/_Metric/n-edge1/m1/setpoint")
	// Producing one signal does not make a service the producer of another.
	assertNotProducer(t, e, "dataops", "colca/v1/_Metric/n-edge1/m1/setpoint")
}

func TestADataopsOutputIsAcceptedFromDataopsOnly(t *testing.T) {
	e := producerEngine(t)
	mustIngest(t, e, "dataops", "colca/v1/_Metric/n-edge1/m1/oee", `{"v":0.8}`)
	assertNotProducer(t, e, "plc", "colca/v1/_Metric/n-edge1/m1/oee")
	assertNotProducer(t, e, "probe", "colca/v1/_Metric/n-edge1/m1/oee")
}

// A signal bound to nothing, or a path with no signal, keeps the write-zone
// rule: any identity whose zone covers it may publish.
func TestAnUnboundSignalKeepsTheWriteZoneRule(t *testing.T) {
	e := producerEngine(t)
	mustIngest(t, e, "probe", "colca/v1/_Metric/n-edge1/m1/manual", `{"v":1}`)
	mustIngest(t, e, "m1", "colca/v1/_Metric/n-edge1/m1/manual", `{"v":2}`)
	mustIngest(t, e, "probe", "colca/v1/_Metric/n-edge1/m1/no-signal-here", `{"v":3}`)
}

// The admin token stays able to correct or retire a bound signal's value.
func TestTheAdminTokenMayStillWriteABoundSignalsMetric(t *testing.T) {
	e := producerEngine(t)
	mustAdmin(t, e, "colca/v1/_Metric/n-edge1/m1/setpoint", `{"v":0}`)
}

// The index follows the catalogue: a tag the producer drops stops being its
// signal's source for everyone, and a tag it adds back makes it the source
// again.
func TestTheProducerFollowsTheCatalogue(t *testing.T) {
	e := producerEngine(t)
	const topic = "colca/v1/_Metric/n-edge1/m1/setpoint"
	mustIngest(t, e, "plc", topic, `{"v":1}`)

	mustIngest(t, e, "plc", "colca/v1/_DataTags/n-edge1/plc", `{"data_tags":[{"id":"TAG-OTHER"}]}`)
	assertNotProducer(t, e, "plc", topic)

	mustIngest(t, e, "plc", "colca/v1/_DataTags/n-edge1/plc", `{"data_tags":[{"id":"TAG-SETPOINT"}]}`)
	mustIngest(t, e, "plc", topic, `{"v":2}`)

	// A retired catalogue holds nothing.
	mustIngest(t, e, "plc", "colca/v1/_DataTags/n-edge1/plc", ``)
	assertNotProducer(t, e, "plc", topic)
}

// Copying the producer's tag id into another catalogue does not make its
// author a producer; while two catalogues claim a tag, nobody is.
func TestATagClaimedByTwoCataloguesHasNoProducer(t *testing.T) {
	e := producerEngine(t)
	const topic = "colca/v1/_Metric/n-edge1/m1/setpoint"
	mustIngest(t, e, "probe", "colca/v1/_DataTags/n-edge1/probe", `{"data_tags":[{"id":"TAG-SETPOINT"}]}`)
	assertNotProducer(t, e, "probe", topic)
	assertNotProducer(t, e, "plc", topic)
}

// A batch applies the rule per record: a refused record does not stop the
// producer's.
func TestABatchAppliesTheProducerRulePerRecord(t *testing.T) {
	e := producerEngine(t)
	results := e.IngestClientBatch("probe", []BatchRecord{
		{Topic: "colca/v1/_Metric/n-edge1/m1/setpoint", Payload: []byte(`{"v":1234.0}`)},
		{Topic: "colca/v1/_Metric/n-edge1/m1/manual", Payload: []byte(`{"v":1}`)},
	})
	assertRejectReason(t, results[0].Err, metrics.ReasonNotProducer)
	if results[1].Err != nil || !results[1].Persisted {
		t.Fatalf("the unbound record in the same batch was refused: %+v", results[1])
	}
}

// The index loads what the store already holds, so a node that restarts knows
// its producers before any catalogue is republished.
func TestTheProducerIsKnownAfterARestart(t *testing.T) {
	e := producerEngine(t)
	restarted := New(e.Store(), e.cfg, e.ids, nil, nil, nil)
	restarted.SetContracts(e.contracts)
	mustIngest(t, restarted, "plc", "colca/v1/_Metric/n-edge1/m1/setpoint", `{"v":1}`)
	assertNotProducer(t, restarted, "probe", "colca/v1/_Metric/n-edge1/m1/setpoint")
}
