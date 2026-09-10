package mqttsrv

import (
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// HasSubscriberFor reports whether that machine is listening, not whether anyone
// is. The observer holds read:# and subscribes to the machine's command topic;
// counting it would mark an absent machine's commands delivered. Tested against the
// real broker because engine tests stub this lookup.
func TestHasSubscriberForDistinguishesWhoIsListening(t *testing.T) {
	w := newWorld(t)
	topic := "colca/v1/_CmdParam/m1/m1/set-speed"

	obs := connect(t, w.srv.Addr(), "obs-1", w.obs)
	tok := obs.Subscribe("colca/v1/_CmdParam/#", 1, func(paho.Client, paho.Message) {})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("observer subscribe: %v", tok.Error())
	}

	// Denominator: the observer is subscribed, so the lookup does find subscriptions.
	if !w.srv.HasSubscriberFor(topic, "observer") {
		t.Fatal("the observer's own subscription was not found — the lookup itself is broken")
	}
	if w.srv.HasSubscriberFor(topic, "m1") {
		t.Fatal("an observer's subscription was taken for the machine's own")
	}

	// And once the machine itself subscribes, the same call flips.
	m1 := connect(t, w.srv.Addr(), "m1-1", w.m1)
	tok = m1.Subscribe("colca/v1/_CmdParam/+/m1/#", 1, func(paho.Client, paho.Message) {})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("machine subscribe: %v", tok.Error())
	}
	if !w.srv.HasSubscriberFor(topic, "m1") {
		t.Fatal("the machine's own subscription was not found")
	}
	if w.srv.HasSubscriberFor(topic, "") {
		t.Fatal("an empty identity matched a subscription")
	}
}
