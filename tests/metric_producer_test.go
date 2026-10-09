// A bound signal's values come from its producer only, on a real parent and
// child: a connector and a dataops service publish their catalogues at the
// child, autobind binds them, and another local service tries to publish over
// the connector's setpoint through both local doors.
package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	pahov5 "github.com/eclipse/paho.golang/paho"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// localPublish posts one record to the node's local HTTP door as the named
// service and returns the status and the decoded answer.
func localPublish(t *testing.T, n *node.Node, service, topic, payload string) (int, map[string]any) {
	t.Helper()
	body := `{"topic":` + mustJSON(topic) + `,"payload":` + payload + `}`
	req, err := http.NewRequest(http.MethodPost, "http://"+n.LocalAPIAddr+"/publish", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Colca-Service", service)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("local publish %s as %s: %v", topic, service, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// localMQTT5 connects an MQTT 5 client to the node's local door as the named
// service, so a refusal comes back as a PUBACK reason code.
func localMQTT5(t *testing.T, n *node.Node, service string) *pahov5.Client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", n.MQTTLocalAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c := pahov5.NewClient(pahov5.ClientConfig{Conn: packets.NewThreadSafeConn(conn)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := c.Connect(ctx, &pahov5.Connect{
		ClientID: fmt.Sprintf("%s-%d", service, time.Now().UnixNano()),
		Username: service, UsernameFlag: true, KeepAlive: 30, CleanStart: true,
		Properties: &pahov5.ConnectProperties{},
	})
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("local mqtt connect as %s: ack=%v err=%v", service, ack, err)
	}
	t.Cleanup(func() { _ = c.Disconnect(&pahov5.Disconnect{ReasonCode: 0}) })
	return c
}

// metricValues returns the values a node stored for the _Metric at path,
// oldest first.
func metricValues(t *testing.T, n *node.Node, path string) []float64 {
	t.Helper()
	var out []float64
	for _, r := range fetchRecords(t, n, "metrics", unique("vals"), path, 500) {
		rec := r.(map[string]any)
		topic, _ := rec["topic"].(string)
		if !strings.HasPrefix(topic, "colca/v1/_Metric/") || !strings.HasSuffix(topic, "/"+path) {
			continue
		}
		var payload struct {
			V float64 `json:"v"`
		}
		if json.Unmarshal([]byte(mustJSON(rec["payload"])), &payload) == nil {
			out = append(out, payload.V)
		}
	}
	return out
}

// signalPathFor returns the path of the child's signal bound to tag.
func signalPathFor(t *testing.T, n *node.Node, tag string) string {
	t.Helper()
	for topic, payload := range signalsAt(t, n, "_Signal") {
		if payload["data_tag"] == tag {
			return strings.SplitN(topic, "/", 5)[4]
		}
	}
	t.Fatalf("no signal at %s is bound to %s", n.Cfg.ULID, tag)
	return ""
}

func TestOnlyTheProducerPublishesABoundSignalsMetric(t *testing.T) {
	base := t.TempDir()
	keys := map[string]*identity.Identity{}
	for _, n := range []string{"n-parent", "n-child"} {
		id, err := identity.Generate(filepath.Join(base, n+".key"))
		if err != nil {
			t.Fatal(err)
		}
		keys[n] = id
	}
	bundle := bindingBundle(t)
	mk := func(ulid string, parent *config.Parent) *config.Config {
		return &config.Config{
			ULID: ulid, DataDir: filepath.Join(base, ulid+"-data"), LogLevel: "debug",
			KeyFile:   filepath.Join(base, ulid+".key"),
			API:       config.API{Addr: "127.0.0.1:0", LocalAddr: "127.0.0.1:0", Token: tok},
			MQTT:      config.Endpoint{Addr: "127.0.0.1:0"},
			MQTTLocal: config.Endpoint{Addr: "127.0.0.1:0"},
			Repl:      config.Endpoint{Addr: "127.0.0.1:0"},
			Parent:    parent,
			Contracts: config.Contracts{Bundle: bundle},
		}
	}
	parent, err := node.Start(mk("n-parent", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Stop()
	authtestEnrollNode(t, parent, "n-child", keys["n-child"].PublicHex(), "child1")
	child, err := node.Start(mk("n-child", &config.Parent{
		URL: "https://" + parent.ReplAddr, Pubkey: keys["n-parent"].PublicHex(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Stop()
	// The child attaches once its parent issued its certificate; a command
	// issued before that attachment is not meant for it.
	waitForPrefix(t, "n-child", child)

	// The PLC connector: a machine at mount plc with its catalogue.
	plc := authtest.NewMachine(t, "plc-1")
	enrollMachineAt(t, child, "plc-1", plc.Pubkey, "plc")
	pc := machine(t, child.MQTTAddr, plc)
	if tk := pc.Publish("colca/v1/_DataTags/n-child/plc/plc-1", 1, true,
		`{"data_tags":[{"id":"01JTAG-SETPOINT","name":"abfuelldruck","data_type":"float"}]}`); !tk.WaitTimeout(5*time.Second) || tk.Error() != nil {
		t.Fatalf("plc catalogue publish: %v", tk.Error())
	}
	// dataops: an unplaced local service with its outputs' catalogue.
	if code, out := localPublish(t, child, "dataops", "colca/v1/_DataTags/n-child/dataops",
		`{"data_tags":[{"id":"01JTAG-OEE","name":"oee","data_type":"float"}]}`); code != 200 {
		t.Fatalf("dataops catalogue publish: %d %v", code, out)
	}
	waitFor(t, "both catalogues stored at the child", 10*time.Second, func() bool {
		return len(signalsAt(t, child, "_DataTags")) == 2
	})

	// Autobind creates the signals, issued at the parent as an operator would. A
	// local service is named by the ULID the node minted for it.
	dataops, ok := child.Registry.ByName("dataops")
	if !ok {
		t.Fatal("dataops did not register at the child")
	}
	for _, connector := range []string{"plc-1", dataops.ULID} {
		corr := cmdAdmin(t, parent, "colca/v1/_CmdConfigure/n-child/child1/signal/autobind",
			map[string]any{"connector": connector, "under": "plc"})
		awaitAdminAck(t, parent, "colca/v1/_Ack/n-child/child1/signal/autobind", corr, 200)
	}
	setpoint := signalPathFor(t, child, "01JTAG-SETPOINT")
	oee := signalPathFor(t, child, "01JTAG-OEE")
	setpointTopic := "colca/v1/_Metric/n-child/" + setpoint
	oeeTopic := "colca/v1/_Metric/n-child/" + oee

	// The producers publish; checked first so the refusals below mean something.
	if tk := pc.Publish(setpointTopic, 1, false, `{"v":3.5}`); !tk.WaitTimeout(5*time.Second) || tk.Error() != nil {
		t.Fatalf("plc metric publish: %v", tk.Error())
	}
	if code, out := localPublish(t, child, "dataops", oeeTopic, `{"v":0.8}`); code != 200 {
		t.Fatalf("dataops output publish: %d %v", code, out)
	}
	waitFor(t, "the producers' values stored at the child", 10*time.Second, func() bool {
		return len(metricValues(t, child, setpoint)) == 1 && len(metricValues(t, child, oee)) == 1
	})

	// A foreign local service: unplaced, so its write zone is the whole node.
	const probe = "hygentile-rig-probe"
	if code, out := localPublish(t, child, probe, setpointTopic, `{"v":1234.0}`); code != 403 || out["reason"] != "not_producer" {
		t.Fatalf("foreign HTTP publish over the setpoint: %d %v, want 403 not_producer", code, out)
	}
	if code, out := localPublish(t, child, probe, oeeTopic, `{"v":1234.0}`); code != 403 || out["reason"] != "not_producer" {
		t.Fatalf("foreign HTTP publish over a dataops output: %d %v, want 403 not_producer", code, out)
	}
	if code := publishHuman5(t, localMQTT5(t, child, probe), setpointTopic, `{"v":1234.0}`); code != 0x87 {
		t.Fatalf("foreign MQTT publish over the setpoint: PUBACK 0x%02X, want 0x87 (not authorized)", code)
	}
	// Producing one signal does not make dataops a source for another.
	if code, out := localPublish(t, child, "dataops", setpointTopic, `{"v":1234.0}`); code != 403 {
		t.Fatalf("dataops publish over the setpoint: %d %v, want 403", code, out)
	}

	// The operator's admin token may still correct a value.
	api(t, child, "POST", "/publish", map[string]any{"topic": setpointTopic, "payload": map[string]any{"v": 4.0}})

	// A signal bound to nothing keeps the write-zone rule.
	if code, out := localPublish(t, child, probe, "colca/v1/_Metric/n-child/plc/manual-entry", `{"v":7}`); code != 200 {
		t.Fatalf("a publish on an unbound path was refused: %d %v", code, out)
	}

	got := metricValues(t, child, setpoint)
	if len(got) != 2 || got[0] != 3.5 || got[1] != 4.0 {
		t.Fatalf("setpoint values at the child = %v, want [3.5 4] (the producer's, then the admin's)", got)
	}

	// The refusals are counted under their own reason.
	resp, err := http.Get("http://" + child.LocalAPIAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	scrape, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(scrape), `colca_rejected_publishes_total{reason="not_producer"} 4`) {
		t.Fatalf("not_producer refusals not counted as 4:\n%s", grepLines(string(scrape), "colca_rejected_publishes_total"))
	}

	// Replication is untouched: the child's admitted values reach the parent,
	// which holds no catalogue for them and must not refuse them.
	waitFor(t, "the child's setpoint values replicated to the parent", 20*time.Second, func() bool {
		// The parent holds them under the mount it gave the child.
		return len(metricValues(t, parent, "child1/"+setpoint)) == 2 && len(metricValues(t, parent, "child1/"+oee)) == 1
	})
}

func grepLines(text, needle string) string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, needle) {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}
