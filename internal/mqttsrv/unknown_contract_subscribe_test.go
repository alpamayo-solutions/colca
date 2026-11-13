package mqttsrv

import (
	"context"
	"net"
	"testing"
	"time"

	pahov5 "github.com/eclipse/paho.golang/paho"
	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
	"github.com/alpamayo-solutions/colca/internal/metrics"
)

// The filters an older machine or a misspelt client may send: the contract
// level names something this node does not define, so nothing is ever
// published there.
var unknownContractFilters = []string{
	"colca/v1/_TimeSync/+",
	"colca/v1/_TimeSync/#",
	"+/v1/_TimeSync/n1/#",
	"colca/v1/_NoSuchContract/#",
}

// What the same identities may still subscribe to; without these the refusals
// above would also pass on a door that refuses everything.
var knownContractFilters = []string{"colca/v1/_Metric/#", "colca/#"}

func assertSubDenialsCounted(t *testing.T, m *metrics.Metrics, before float64, door string) {
	t.Helper()
	const denyLine = `colca_acl_denials_total{action="sub"}`
	want := before + float64(len(unknownContractFilters))
	if got := scrapeMetric(t, m, denyLine); got != want {
		t.Fatalf("%s door: %s = %v, want %v: every refused filter is counted", door, denyLine, got, want)
	}
}

func assertMQTT3Subscriptions(t *testing.T, door string, c paho.Client) {
	t.Helper()
	sub := func(filter string) byte {
		tok := c.Subscribe(filter, 1, func(paho.Client, paho.Message) {})
		if !tok.WaitTimeout(5 * time.Second) {
			t.Fatalf("%s door: no SUBACK for %s", door, filter)
		}
		return tok.(*paho.SubscribeToken).Result()[filter]
	}
	for _, filter := range unknownContractFilters {
		if code := sub(filter); code != 0x80 {
			t.Errorf("%s door: SUBACK for %s = 0x%02X, want 0x80: a read grant on everything "+
				"does not cover a contract the node does not define", door, filter, code)
		}
	}
	for _, filter := range knownContractFilters {
		if code := sub(filter); code == 0x80 {
			t.Errorf("%s door: SUBACK for %s = 0x80, want a grant", door, filter)
		}
	}
}

// A subscription whose filter names a contract the node does not define is
// refused on the machine door even for an identity that may read everything:
// granted, it would leave the subscriber waiting for messages that cannot come.
func TestAFilterNamingAnUnknownContractIsRefusedOnTheMachineDoor(t *testing.T) {
	w := newWorld(t)
	before := scrapeMetric(t, w.m, `colca_acl_denials_total{action="sub"}`)
	c := connect(t, w.srv.Addr(), "obs-wide-reader", w.obs)
	assertMQTT3Subscriptions(t, "machine", c)
	assertSubDenialsCounted(t, w.m, before, "machine")
	if !c.IsConnected() {
		t.Fatal("the session ended after the refused subscriptions")
	}
}

// The people's door answers the same way to a person who may read everything.
func TestAFilterNamingAnUnknownContractIsRefusedOnTheHumanDoor(t *testing.T) {
	w := newHumanWorld(t)
	before := scrapeMetric(t, w.m, `colca_acl_denials_total{action="sub"}`)
	tok := w.iss.Mint("anna", []string{"read:#"}, time.Now().Add(5*time.Minute))
	c, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "anna", tok)
	if err != nil {
		t.Fatalf("human connect: %v", err)
	}
	defer c.Disconnect(100)
	assertMQTT3Subscriptions(t, "human", c)
	assertSubDenialsCounted(t, w.m, before, "human")
}

// A registered local service reads its whole node and is still refused; over
// MQTT 5 the SUBACK says Not authorized.
func TestAFilterNamingAnUnknownContractIsRefusedOnTheLocalDoor(t *testing.T) {
	w := startServerWithLocalDoor(t)
	before := scrapeMetric(t, w.m, `colca_acl_denials_total{action="sub"}`)
	conn, err := net.Dial("tcp", w.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	client := pahov5.NewClient(pahov5.ClientConfig{Conn: conn})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ack, err := client.Connect(ctx, &pahov5.Connect{ClientID: "probe", Username: "probe",
		UsernameFlag: true, CleanStart: true, KeepAlive: 30})
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("connect: %v, %v", ack, err)
	}
	t.Cleanup(func() { _ = client.Disconnect(&pahov5.Disconnect{}) })

	sub := func(filter string) byte {
		// paho returns an error next to the SUBACK when a reason is a failure.
		suback, _ := client.Subscribe(ctx, &pahov5.Subscribe{
			Subscriptions: []pahov5.SubscribeOptions{{Topic: filter, QoS: 1}}})
		if suback == nil || len(suback.Reasons) != 1 {
			t.Fatalf("local door: no SUBACK for %s", filter)
		}
		return suback.Reasons[0]
	}
	for _, filter := range unknownContractFilters {
		if code := sub(filter); code != 0x87 {
			t.Errorf("local door: SUBACK for %s = 0x%02X, want 0x87 (not authorized)", filter, code)
		}
	}
	for _, filter := range knownContractFilters {
		if code := sub(filter); code > 2 {
			t.Errorf("local door: SUBACK for %s = 0x%02X, want a grant", filter, code)
		}
	}
	assertSubDenialsCounted(t, w.m, before, "local")
}

// Which contracts exist is the engine's answer, the one a publish gets: on a
// node with the schema bundle a contract only the bundle defines is granted,
// and the retired beacon stays refused.
func TestTheContractsInForceDecideWhichFiltersNameAnUnknownContract(t *testing.T) {
	w := newWorld(t)
	c := connect(t, w.srv.Addr(), "obs-bundle", w.obs)
	sub := func(filter string) byte {
		tok := c.Subscribe(filter, 1, func(paho.Client, paho.Message) {})
		if !tok.WaitTimeout(5 * time.Second) {
			t.Fatalf("no SUBACK for %s", filter)
		}
		return tok.(*paho.SubscribeToken).Result()[filter]
	}
	const bundleOnly = "colca/v1/_DataTags/#"
	if code := sub(bundleOnly); code != 0x80 {
		t.Fatalf("SUBACK for %s without a bundle = 0x%02X, want 0x80: the built-in rules do not define it",
			bundleOnly, code)
	}

	tbl, err := contracts.Load(contractstest.GeneratedBundlePath(t), "")
	if err != nil {
		t.Fatal(err)
	}
	w.eng.SetContracts(tbl)
	if code := sub(bundleOnly); code == 0x80 {
		t.Fatalf("SUBACK for %s with the bundle = 0x80, want a grant", bundleOnly)
	}
	if code := sub("colca/v1/_TimeSync/+"); code != 0x80 {
		t.Fatalf("SUBACK for the retired beacon with the bundle = 0x%02X, want 0x80", code)
	}
}
