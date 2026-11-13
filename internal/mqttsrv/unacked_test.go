package mqttsrv

import (
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
)

const (
	stalledClientsLine = `colca_mqtt_stalled_clients`
	unackedDroppedLine = `colca_mqtt_unacked_dropped_total`
	stalledLog         = "mqtt client does not acknowledge"
)

// silentClient is a connected subscriber that reads every message and
// acknowledges none of them until told to.
type silentClient struct {
	paho.Client
	received atomic.Int64
	acking   atomic.Bool
}

func connectSilent(t *testing.T, w *world, clientID string, m *authtest.Machine, filter string) *silentClient {
	t.Helper()
	s := &silentClient{}
	opts := paho.NewClientOptions().
		AddBroker("ssl://" + w.srv.Addr()).
		SetTLSConfig(m.TLSConfig()).
		SetClientID(clientID).
		SetUsername(m.ULID).
		SetProtocolVersion(4).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(5 * time.Second).
		SetAutoAckDisabled(true).
		SetOrderMatters(false).
		SetDefaultPublishHandler(func(_ paho.Client, msg paho.Message) {
			s.received.Add(1)
			if s.acking.Load() {
				msg.Ack()
			}
		})
	s.Client = paho.NewClient(opts)
	if tok := s.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connect %s: %v", clientID, tok.Error())
	}
	t.Cleanup(func() { s.Disconnect(100) })
	if tok := s.Subscribe(filter, 1, nil); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe %s: %v", filter, tok.Error())
	}
	return s
}

func waitQueued(t *testing.T, w *world, clientID string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for queued(t, w, clientID) != want {
		if time.Now().After(deadline) {
			t.Fatalf("the broker holds %d messages for %s, want %d", queued(t, w, clientID), clientID, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The finding this guards: a connected client that reads but never
// acknowledges held up to 65,535 messages, like an abandoned session before
// offline sessions were bounded. Once it acknowledged nothing for the stall
// time it is cut back to what an offline session may keep, nothing more is
// queued for it, the drops are counted, and one line says so. A client that
// acknowledges is left alone, and delivery resumes with the first
// acknowledgement.
func TestAConnectedClientThatNeverAcknowledgesIsBounded(t *testing.T) {
	const limit = 50
	stalledLogs := countLogs(t, stalledLog)
	w := newWorldWithConfig(t, func(cfg *config.Config) { cfg.MQTTLimits.MaxOfflineMessagesPerClient = limit })
	silent := connectSilent(t, w, "silent", w.obs, offlineTopic)
	box := &inbox{}
	healthy := persistentConnect(t, w, "healthy", w.m1, box)
	if tok := healthy.Subscribe("colca/v1/_Metric/+/m1/#", 1, nil); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	const first = 4 * limit
	publishNumbered(w, 0, first, 8)
	waitQueued(t, w, "silent", first)

	// Over the bound, but not for long enough: nothing is dropped yet.
	now := time.Now()
	w.srv.sweepUnackedClients(now)
	w.srv.sweepUnackedClients(now.Add(unackedStallAfter - time.Second))
	if got := queued(t, w, "silent"); got != first {
		t.Fatalf("the broker holds %d messages before the stall time passed, want all %d", got, first)
	}
	if got := scrapeMetric(t, w.m, stalledClientsLine); got != 0 {
		t.Fatalf("%s = %v before the stall time passed", stalledClientsLine, got)
	}

	w.srv.sweepUnackedClients(now.Add(unackedStallAfter))
	if got := queued(t, w, "silent"); got != limit {
		t.Fatalf("the broker holds %d messages for the client that never acknowledged, want the limit %d", got, limit)
	}
	if got := scrapeMetric(t, w.m, stalledClientsLine); got != 1 {
		t.Fatalf("%s = %v, want 1", stalledClientsLine, got)
	}
	if got := scrapeMetric(t, w.m, unackedDroppedLine); got != first-limit {
		t.Fatalf("%s = %v, want %d", unackedDroppedLine, got, first-limit)
	}

	// While it is stalled nothing more is queued for it, however much is published.
	const more = 300
	publishNumbered(w, first, more, 8)
	deadline := time.Now().Add(10 * time.Second)
	for len(box.payloads(offlineTopic)) < first+more {
		if time.Now().After(deadline) {
			t.Fatalf("the client that acknowledges received %d of %d messages", len(box.payloads(offlineTopic)), first+more)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := queued(t, w, "silent"); got != limit {
		t.Fatalf("the broker holds %d messages for the stalled client after %d more, want the limit %d", got, more, limit)
	}
	if got := scrapeMetric(t, w.m, unackedDroppedLine); got != first-limit+more {
		t.Fatalf("%s = %v, want %d", unackedDroppedLine, got, first-limit+more)
	}
	w.srv.sweepUnackedClients(now.Add(2 * unackedStallAfter))
	if got := stalledLogs.Load(); got != 1 {
		t.Fatalf("the stall was logged %d times, want once", got)
	}
	if got := queued(t, w, "healthy"); got != 0 {
		t.Fatalf("the broker holds %d messages for the client that acknowledges", got)
	}

	// Its first acknowledgement ends the stall: what is published next reaches
	// it. The kept messages are resent only on a reconnect, so the
	// acknowledgement of one is handed to the hook the way mochi does for a
	// PUBACK.
	silent.acking.Store(true)
	before := silent.received.Load()
	cl, _ := w.srv.S.Clients.Get("silent")
	oldest := cl.State.Inflight.GetAll(false)[0]
	cl.State.Inflight.Delete(oldest.PacketID)
	w.srv.hook.OnQosComplete(cl, oldest)
	if got := scrapeMetric(t, w.m, stalledClientsLine); got != 0 {
		t.Fatalf("%s = %v after an acknowledgement, want 0", stalledClientsLine, got)
	}
	publishNumbered(w, first+more, 10, 8)
	deadline = time.Now().Add(10 * time.Second)
	for silent.received.Load() < before+10 {
		if time.Now().After(deadline) {
			t.Fatalf("after its acknowledgement the client received %d of 10 new messages", silent.received.Load()-before)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A subscriber that acknowledges is never cut back, however much it holds at
// one moment: a retained replay may be larger than the bound.
func TestAClientThatAcknowledgesIsNotCutBack(t *testing.T) {
	const limit = 50
	w := newWorldWithConfig(t, func(cfg *config.Config) { cfg.MQTTLimits.MaxOfflineMessagesPerClient = limit })
	connectSilent(t, w, "slow", w.obs, offlineTopic)
	publishNumbered(w, 0, 4*limit, 8)
	waitQueued(t, w, "slow", 4*limit)

	now := time.Now()
	w.srv.sweepUnackedClients(now)
	cl, _ := w.srv.S.Clients.Get("slow")
	// One acknowledgement in each stall time is a client that is still working.
	for i := 1; i <= 3; i++ {
		pk := cl.State.Inflight.GetAll(false)[0]
		cl.State.Inflight.Delete(pk.PacketID)
		w.srv.hook.OnQosComplete(cl, pk)
		w.srv.sweepUnackedClients(now.Add(time.Duration(i) * unackedStallAfter))
	}
	if got := queued(t, w, "slow"); got != 4*limit-3 {
		t.Fatalf("the broker holds %d messages for a client that acknowledges, want %d", got, 4*limit-3)
	}
	if got := scrapeMetric(t, w.m, unackedDroppedLine); got != 0 {
		t.Fatalf("%s = %v, want 0", unackedDroppedLine, got)
	}
}
