package mqttsrv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
)

const (
	offlineSessionsLine = `colca_mqtt_offline_sessions`
	offlineMessagesLine = `colca_mqtt_offline_queued_messages`
	offlineBytesLine    = `colca_mqtt_offline_queued_bytes`
	offlineDroppedLine  = `colca_mqtt_offline_dropped_total`
	offlineFullLog      = "offline mqtt session's queue is full"
)

// inbox collects what a client receives, by topic.
type inbox struct {
	mu   sync.Mutex
	seen map[string][]string
}

func (b *inbox) handler(_ paho.Client, m paho.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.seen == nil {
		b.seen = map[string][]string{}
	}
	b.seen[m.Topic()] = append(b.seen[m.Topic()], string(m.Payload()))
}

func (b *inbox) payloads(topic string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.seen[topic]...)
}

// persistentConnect connects with a session that outlives the connection
// (MQTT 3.1.1, clean session off). Everything received goes to box, also what
// the broker resends before the client subscribes again.
func persistentConnect(t *testing.T, w *world, clientID string, m *authtest.Machine, box *inbox) paho.Client {
	t.Helper()
	opts := paho.NewClientOptions().
		AddBroker("ssl://" + w.srv.Addr()).
		SetTLSConfig(m.TLSConfig()).
		SetClientID(clientID).
		SetUsername(m.ULID).
		SetProtocolVersion(4).
		SetCleanSession(false).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(5 * time.Second)
	if box != nil {
		opts.SetDefaultPublishHandler(box.handler)
	}
	c := paho.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connect %s: %v", clientID, tok.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// leaveSubscribed subscribes a persistent session to filter at QoS 1 and
// disconnects it, then waits until the broker counts it as offline.
func leaveSubscribed(t *testing.T, w *world, clientID string, m *authtest.Machine, filter string, offlineAfter int) {
	t.Helper()
	c := persistentConnect(t, w, clientID, m, nil)
	if tok := c.Subscribe(filter, 1, nil); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe %s: %v", filter, tok.Error())
	}
	c.Disconnect(100)
	waitMetric(t, w, offlineSessionsLine, float64(offlineAfter))
}

func waitMetric(t *testing.T, w *world, line string, want float64) {
	t.Helper()
	waitMetricWithin(t, w, line, want, 10*time.Second)
}

func waitMetricWithin(t *testing.T, w *world, line string, want float64, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		got := scrapeMetric(t, w.m, line)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s = %v, want %v", line, got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

const offlineTopic = "colca/v1/_Metric/n1/m1/temp"

// publishNumbered publishes n messages on offlineTopic whose payload is their
// number padded to size bytes.
func publishNumbered(w *world, from, n, size int) {
	for i := from; i < from+n; i++ {
		payload := []byte(fmt.Sprintf("%0*d", size, i))
		w.srv.DeliverLocal(offlineTopic, payload, false)
	}
}

func queued(t *testing.T, w *world, clientID string) int {
	t.Helper()
	cl, ok := w.srv.S.Clients.Get(clientID)
	if !ok {
		t.Fatalf("the broker holds no session %s", clientID)
	}
	return cl.State.Inflight.Len()
}

func liveHeapMiB() float64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.HeapAlloc) / (1 << 20)
}

// countingLogs counts log records by message prefix while the test runs.
type countingLogs struct {
	slog.Handler
	prefix string
	n      *atomic.Int64
}

func (c countingLogs) Handle(ctx context.Context, r slog.Record) error {
	if strings.HasPrefix(r.Message, c.prefix) {
		c.n.Add(1)
	}
	return c.Handler.Handle(ctx, r)
}
func (c countingLogs) WithAttrs(a []slog.Attr) slog.Handler {
	return countingLogs{c.Handler.WithAttrs(a), c.prefix, c.n}
}
func (c countingLogs) WithGroup(g string) slog.Handler {
	return countingLogs{c.Handler.WithGroup(g), c.prefix, c.n}
}

func countLogs(t *testing.T, prefix string) *atomic.Int64 {
	t.Helper()
	n := &atomic.Int64{}
	was := slog.Default()
	// Not the previous handler: the standard one writes through the log package,
	// which routes back to the default logger once that is replaced.
	slog.SetDefault(slog.New(countingLogs{slog.NewTextHandler(io.Discard, nil), prefix, n}))
	t.Cleanup(func() { slog.SetDefault(was) })
	return n
}

// The finding this guards: a session left behind subscribed to everything kept
// 65,535 messages in memory (75 MiB at 200-byte payloads) and logged a line a
// minute. With the default bounds it keeps the first 4,096, memory stays flat
// however much the plant publishes, the drops are counted, and one line says so.
func TestAnAbandonedWideSubscriberCannotGrowTheNodesMemory(t *testing.T) {
	fullLogs := countLogs(t, offlineFullLog)
	w := newWorld(t)
	leaveSubscribed(t, w, "wide", w.obs, "colca/#", 1)
	already := queued(t, w, "wide")

	const published, size = 200_000, 200
	limit := config.MQTTLimits{}.EffectiveMaxOfflineMessagesPerClient()
	before := liveHeapMiB()
	publishNumbered(w, 0, published, size)
	grew := liveHeapMiB() - before
	t.Logf("live heap grew by %.1f MiB for %d messages of %d bytes published to the offline session", grew, published, size)

	if got := queued(t, w, "wide"); got != limit {
		t.Fatalf("the offline session holds %d messages, want the limit %d", got, limit)
	}
	// Unbounded, these messages took 75 MiB; 4,096 of them take about 5.
	if grew > 16 {
		t.Fatalf("live heap grew by %.1f MiB while %d messages were published to an offline session, want < 16 MiB",
			grew, published)
	}
	waitMetric(t, w, offlineMessagesLine, float64(limit))
	wantBytes := float64(limit-already) * float64(size+len(offlineTopic)+offlineMessageOverhead)
	if got := scrapeMetric(t, w.m, offlineBytesLine); got < wantBytes || got > wantBytes+float64(already)*8192 {
		t.Fatalf("%s = %v, want about %v", offlineBytesLine, got, wantBytes)
	}
	if got, want := scrapeMetric(t, w.m, offlineDroppedLine), float64(published-(limit-already)); got != want {
		t.Fatalf("%s = %v, want %v", offlineDroppedLine, got, want)
	}
	if got := fullLogs.Load(); got != 1 {
		t.Fatalf("the full queue was logged %d times for %d dropped messages, want once", got, published-limit)
	}
}

// What the session kept is the start of what it missed: when the client comes
// back it gets those messages, the account ends, and live delivery goes on.
func TestAReturningClientGetsTheMessagesItsSessionKept(t *testing.T) {
	const limit = 50
	w := newWorldWithConfig(t, func(cfg *config.Config) { cfg.MQTTLimits.MaxOfflineMessagesPerClient = limit })
	leaveSubscribed(t, w, "away", w.obs, offlineTopic, 1)
	if got := queued(t, w, "away"); got != 0 {
		t.Fatalf("precondition: the session already holds %d messages", got)
	}
	publishNumbered(w, 0, 3*limit, 8)

	box := &inbox{}
	persistentConnect(t, w, "away", w.obs, box)
	deadline := time.Now().Add(10 * time.Second)
	for len(box.payloads(offlineTopic)) < limit {
		if time.Now().After(deadline) {
			t.Fatalf("the returning client received %d of the %d kept messages", len(box.payloads(offlineTopic)), limit)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Well inside the sweep interval: the reconnect itself ends the account.
	waitMetricWithin(t, w, offlineSessionsLine, 0, sweepInterval/4)
	waitMetric(t, w, offlineMessagesLine, 0)
	waitMetric(t, w, offlineBytesLine, 0)

	publishNumbered(w, 1000, 1, 8)
	deadline = time.Now().Add(10 * time.Second)
	for len(box.payloads(offlineTopic)) < limit+1 {
		if time.Now().After(deadline) {
			t.Fatal("a message published after the reconnect never arrived")
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := map[int]bool{}
	for _, p := range box.payloads(offlineTopic) {
		n, err := strconv.Atoi(p)
		if err != nil {
			t.Fatalf("payload %q", p)
		}
		got[n] = true
	}
	for i := 0; i < limit; i++ {
		if !got[i] {
			t.Fatalf("message %d is missing: the session keeps the first %d and drops later ones; got %d messages",
				i, limit, len(got))
		}
	}
	if !got[1000] || len(got) != limit+1 {
		t.Fatalf("received %d distinct messages, want the %d kept and the live one", len(got), limit)
	}
}

// A connected subscriber is not bounded by the offline queue: it gets every
// message while an offline session beside it is full.
func TestALiveSubscriberGetsEverythingWhileAnOfflineSessionIsFull(t *testing.T) {
	const limit, published = 20, 1000
	w := newWorldWithConfig(t, func(cfg *config.Config) { cfg.MQTTLimits.MaxOfflineMessagesPerClient = limit })
	leaveSubscribed(t, w, "away", w.obs, offlineTopic, 1)

	box := &inbox{}
	live := persistentConnect(t, w, "live", w.obs, box)
	if tok := live.Subscribe(offlineTopic, 1, nil); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	publishNumbered(w, 0, published, 8)
	deadline := time.Now().Add(20 * time.Second)
	for len(box.payloads(offlineTopic)) < published {
		if time.Now().After(deadline) {
			t.Fatalf("the live subscriber received %d of %d messages", len(box.payloads(offlineTopic)), published)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := queued(t, w, "away"); got != limit {
		t.Fatalf("the offline session holds %d messages, want %d", got, limit)
	}
	if got := scrapeMetric(t, w.m, offlineDroppedLine); got != published-limit {
		t.Fatalf("%s = %v, want %d: only the offline session's messages are dropped", offlineDroppedLine, got, published-limit)
	}
}

// Few large messages are bounded by bytes, per session and over all sessions.
func TestOfflineQueuesAreBoundedInBytesPerSessionAndInTotal(t *testing.T) {
	const size = 8 << 10
	cost := int64(size + len(offlineTopic) + offlineMessageOverhead)
	w := newWorldWithConfig(t, func(cfg *config.Config) {
		cfg.MQTTLimits.MaxOfflineBytesPerClient = config.ByteSize(3*cost + 100) //nolint:gosec // test constant
		cfg.MQTTLimits.MaxOfflineBytes = config.ByteSize(5*cost + 100)          //nolint:gosec // test constant
	})
	leaveSubscribed(t, w, "a", w.obs, offlineTopic, 1)
	leaveSubscribed(t, w, "b", w.obs, offlineTopic, 2)
	publishNumbered(w, 0, 40, size)

	a, b := queued(t, w, "a"), queued(t, w, "b")
	if a > 3 || b > 3 || a+b != 5 {
		t.Fatalf("the sessions hold %d and %d messages, want at most 3 each and 5 together", a, b)
	}
	if got := scrapeMetric(t, w.m, offlineBytesLine); got != float64(5*cost) {
		t.Fatalf("%s = %v, want %d", offlineBytesLine, got, 5*cost)
	}
	if got := scrapeMetric(t, w.m, offlineDroppedLine); got != 2*40-5 {
		t.Fatalf("%s = %v, want %d", offlineDroppedLine, got, 2*40-5)
	}
}

func subscribedTo(w *world, clientID string) bool {
	_, ok := w.srv.S.Topics.Subscribers(offlineTopic).Subscriptions[clientID]
	return ok
}

// A session nobody returns to ends at the session expiry: its queue, its
// subscriptions and its account go, and the end is counted.
func TestAnAbandonedSessionExpiresWithItsQueueAndSubscriptions(t *testing.T) {
	w := newWorldWithConfig(t, func(cfg *config.Config) {
		cfg.MQTTLimits.MaxSessionExpiry = config.Duration(time.Second)
	})
	leaveSubscribed(t, w, "gone", w.obs, offlineTopic, 1)
	publishNumbered(w, 0, 10, 8)
	if got := queued(t, w, "gone"); got != 10 {
		t.Fatalf("the offline session holds %d messages, want 10", got)
	}
	if !subscribedTo(w, "gone") {
		t.Fatal("precondition: the offline session is not in the topic index")
	}

	waitMetric(t, w, `colca_mqtt_offline_sessions_ended_total{reason="expired"}`, 1)
	if _, ok := w.srv.S.Clients.Get("gone"); ok {
		// mochi deletes the client right after the hook; give it that step.
		time.Sleep(100 * time.Millisecond)
		if _, ok := w.srv.S.Clients.Get("gone"); ok {
			t.Fatal("the expired session is still held")
		}
	}
	if subscribedTo(w, "gone") {
		t.Fatal("the expired session's subscription is still in the topic index")
	}
	waitMetric(t, w, offlineSessionsLine, 0)
	waitMetric(t, w, offlineBytesLine, 0)
}

// The session of an identity that was revoked or retired cannot be returned
// to, so it does not wait for its expiry.
func TestTheOfflineSessionOfARemovedIdentityIsRemoved(t *testing.T) {
	w := newWorld(t)
	leaveSubscribed(t, w, "retired", w.obs, offlineTopic, 1)
	leaveSubscribed(t, w, "kept", w.m1, "colca/v1/_Metric/+/m1/#", 2)
	publishNumbered(w, 0, 10, 8)
	if got := queued(t, w, "retired"); got != 10 {
		t.Fatalf("the offline session holds %d messages, want 10", got)
	}

	w.srv.sweepOfflineSessions()
	if _, ok := w.srv.S.Clients.Get("retired"); !ok {
		t.Fatal("the session of an enrolled identity was removed")
	}

	if _, _, err := w.reg.Revoke(w.obs.ULID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	w.srv.sweepOfflineSessions()
	if _, ok := w.srv.S.Clients.Get("retired"); ok {
		t.Fatal("the session of the revoked identity is still held")
	}
	if subscribedTo(w, "retired") {
		t.Fatal("the revoked identity's subscription is still in the topic index")
	}
	if _, ok := w.srv.S.Clients.Get("kept"); !ok || !subscribedTo(w, "kept") {
		t.Fatal("the session of another, enrolled identity was removed with it")
	}
	if got := scrapeMetric(t, w.m, `colca_mqtt_offline_sessions_ended_total{reason="identity_removed"}`); got != 1 {
		t.Fatalf("sessions ended for a removed identity = %v, want 1", got)
	}
	waitMetric(t, w, offlineSessionsLine, 1)
}
