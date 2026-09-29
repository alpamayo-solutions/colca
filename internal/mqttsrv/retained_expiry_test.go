package mqttsrv

import (
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
)

// A retained message older than a day stays retained: the set is the KV
// projection, and a node up for longer must still hand its whole namespace to a
// subscriber that arrives late.
func TestRetainedMessagesDoNotAgeOut(t *testing.T) {
	w := newWorld(t)
	const topic = "m1/structure/old"
	w.srv.S.Topics.RetainMessage(packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Retain: true},
		TopicName:       topic,
		Payload:         []byte(`{}`),
		Created:         time.Now().Add(-48 * time.Hour).Unix(),
		ProtocolVersion: 5,
	})

	// mochi sweeps expired retained messages once a second.
	time.Sleep(2500 * time.Millisecond)
	if _, ok := w.srv.S.Topics.Retained.Get(topic); !ok {
		t.Fatal("a retained message written two days ago was dropped")
	}
	if got := w.srv.S.Options.Capabilities.MaximumMessageExpiryInterval; got != 0 {
		t.Fatalf("MaximumMessageExpiryInterval = %d, want 0 (no cap)", got)
	}
}
