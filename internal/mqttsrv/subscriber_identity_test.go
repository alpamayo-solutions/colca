package mqttsrv

import (
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// The subscriber lookup that decides a command's delivery
// floor must report whether THAT MACHINE is listening, not whether anyone is.
//
// mochi's Subscribers() returns matching subscriptions keyed by client ID, so
// an implementation that merely counted them would answer "delivered" for any
// third party. The observer here holds read:# and legitimately subscribes to
// the machine's command topic for diagnostics; if that were taken for the
// machine's own subscription, an absent machine's commands would be marked
// delivered and never replayed — silently, since no counter would rise either.
//
// Pinned here, against the real broker, because the engine's unit tests stub
// this lookup and therefore cannot see it lose the identity.
func TestHasSubscriberForDistinguishesWhoIsListening(t *testing.T) {
	w := newWorld(t)
	topic := "colca/v1/_CmdParam/m1/m1/set-speed"

	obs := connect(t, w.srv.Addr(), "obs-1", w.obs)
	tok := obs.Subscribe("colca/v1/_CmdParam/#", 1, func(paho.Client, paho.Message) {})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("observer subscribe: %v", tok.Error())
	}

	// The observer IS subscribed — the denominator, without which the "not m1"
	// assertion below would pass just as well against a lookup that finds
	// nothing at all (a wrong topic filter, a wrong index).
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
