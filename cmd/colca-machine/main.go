// Command colca-machine is the demo machine simulator: an MQTT client that
// behaves like a machine attached to an edge node.
//
// It publishes a temperature metric under its node's ULID (NODE_ULID) every
// PUBLISH_INTERVAL_MS, subscribes to its commands and acks each with 200, or
// 498 when expires_at has passed. Expiry is judged against the time learned
// from the node's _TimeSync beacon; after a reconnect, decisions wait for a
// beacon or TIME_SYNC_HOLD_MS. SIM_CLOCK_OFFSET_MS skews its clock for tests.
//
// It survives broker restarts and malformed messages and exits cleanly on
// SIGTERM.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const (
	// Result codes acked back to the issuer of a command.
	codeOK      = 200
	codeExpired = 498

	connectRetryInterval = 2 * time.Second
	connectTimeout       = 5 * time.Second
	connectProgressEvery = 5 * time.Second
	subscribeTimeout     = 10 * time.Second
	publishTimeout       = 5 * time.Second
	disconnectQuiesceMS  = 250

	// defaultHoldMS is how long expiry decisions wait for a beacon after a
	// reconnect.
	defaultHoldMS = 10000
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// envInt64 reads k as a signed integer; anything unparseable or absent falls
// back to def rather than killing the container.
func envInt64(log *slog.Logger, k string, def int64) int64 {
	raw := env(k, strconv.FormatInt(def, 10))
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		log.Warn("invalid "+k+" — using default", "value", raw, "default", def)
		return def
	}
	return v
}

// newSimClock is the process's only wall-clock read, skewed by offsetMS. The
// skew is constant, so durations between two readings stay correct.
func newSimClock(offsetMS int64) func() time.Time {
	return func() time.Time { return time.Now().Add(time.Duration(offsetMS) * time.Millisecond) }
}

// syncState is a snapshot of timeSync's state, so decide needs no lock.
type syncState struct {
	offsetMS int64
	holding  bool
	deadline time.Time
}

// decide reports whether an expiry decision may proceed: not while a hold is
// open and its deadline has not passed. syncedNow is the wall time plus the
// offset; deadlineHit says the deadline, not a beacon, ended the hold.
func decide(s syncState, wallNow time.Time) (ready bool, syncedNow time.Time, deadlineHit bool) {
	if s.holding && wallNow.Before(s.deadline) {
		return false, time.Time{}, false
	}
	return true, wallNow.Add(time.Duration(s.offsetMS) * time.Millisecond), s.holding
}

// timeSync keeps the offset from the latest _TimeSync beacon and the hold after
// a reconnect: expiry decisions wait, never fail, until a beacon arrives or the
// hold ends. now is the injectable clock.
type timeSync struct {
	now    func() time.Time
	holdMS int64

	mu      sync.Mutex
	st      syncState
	release chan struct{} // closed + replaced whenever a hold ends via a beacon
}

func newTimeSync(now func() time.Time, holdMS int64) *timeSync {
	return &timeSync{now: now, holdMS: holdMS, release: make(chan struct{})}
}

// Connect opens a hold, so a reconnect does not trust an old offset until a
// beacon lands or the hold ends. holdMS <= 0 means no hold.
func (t *timeSync) Connect() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.holdMS <= 0 {
		t.st.holding = false
		return
	}
	t.st.holding = true
	t.st.deadline = t.now().Add(time.Duration(t.holdMS) * time.Millisecond)
}

// Beacon records offset = now_ms - wall time (the latest sample wins) and ends
// any open hold.
func (t *timeSync) Beacon(nowMS int64) {
	t.mu.Lock()
	t.st.offsetMS = nowMS - t.now().UnixMilli()
	wasHolding := t.st.holding
	t.st.holding = false
	var ch chan struct{}
	if wasHolding {
		ch, t.release = t.release, make(chan struct{})
	}
	t.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// Snapshot returns the current decision-relevant state for decide.
func (t *timeSync) Snapshot() syncState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.st
}

// Await blocks until an expiry decision may proceed: a beacon arrives, the
// hold's deadline passes (deadlineHit is then true), or ctx is cancelled.
func (t *timeSync) Await(ctx context.Context) (syncedNow time.Time, offsetMS int64, deadlineHit bool) {
	for {
		t.mu.Lock()
		wallNow := t.now()
		ready, sn, dh := decide(t.st, wallNow)
		if ready {
			if dh {
				t.st.holding = false
			}
			off := t.st.offsetMS
			t.mu.Unlock()
			return sn, off, dh
		}
		deadline, ch := t.st.deadline, t.release
		t.mu.Unlock()

		// Both readings come from the same skewed clock, so their difference is the
		// real remaining time; time.Until would add the skew back.
		timer := time.NewTimer(deadline.Sub(wallNow))
		select {
		case <-ch:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.mu.Lock()
			off := t.st.offsetMS
			t.mu.Unlock()
			return wallNow, off, false
		}
		timer.Stop()
	}
}

func main() { os.Exit(run()) }

// beaconFilter matches the time beacon of whichever node this process is
// connected to.
func beaconFilter() string { return uns.Prefix() + "_TimeSync/+" }

func run() int {
	if err := uns.SetRootFromEnv(); err != nil {
		fmt.Fprintln(os.Stderr, "colca-machine:", err)
		return 2
	}
	ulid := env("MACHINE_ULID", "m1")
	keyPath := env("MACHINE_KEY", "/keys/"+ulid+"-machine.key")
	broker := env("BROKER_ADDR", "127.0.0.1:8883")
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("machine", ulid)

	// Topics carry the ULID of the node this machine is attached to, not the
	// machine's own. There is no default: a guessed node would refuse the traffic.
	nodeULID := env("NODE_ULID", "")
	if nodeULID == "" {
		log.Error("NODE_ULID is required: colca-machine publishes under the node's identity")
		return 1
	}
	interval := publishInterval(log)

	// The machine's clock, skewable for tests, and the offset and hold state fed by
	// the node's beacon.
	simOffsetMS := envInt64(log, "SIM_CLOCK_OFFSET_MS", 0)
	simNow := newSimClock(simOffsetMS)
	holdMS := envInt64(log, "TIME_SYNC_HOLD_MS", defaultHoldMS)
	ts := newTimeSync(simNow, holdMS)
	if simOffsetMS != 0 {
		log.Warn("SIM_CLOCK_OFFSET_MS active — this process's wall clock is deliberately skewed (simulator only)", "offset_ms", simOffsetMS)
	}

	// The machine's key is its credential; it is enrolled at the node before the
	// first connect.
	id, err := identity.Load(keyPath)
	if err != nil {
		log.Error("cannot load machine key — generate it with colca-keygen and enroll the pubkey", "key", keyPath, "err", err)
		return 1
	}
	cert, err := id.SelfSignedCert(ulid)
	if err != nil {
		log.Error("cannot build client certificate", "err", err)
		return 1
	}

	metricTopic := uns.Prefix() + "_Metric/" + nodeULID + "/" + ulid + "/temp"
	// Path-anchored (node-id level is a wildcard for readers): the machine's
	// default read grant covers its own zone, which is where its commands land.
	cmdFilter := uns.Prefix() + "_CmdParam/+/" + ulid + "/#"

	// SIGINT/SIGTERM cancel the context; every wait below selects on it, so the
	// process always leaves through the single clean shutdown path.
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	onCommand := func(c pahomqtt.Client, msg pahomqtt.Message) {
		handleCommand(ctx, log, c, ulid, nodeULID, ts, msg)
	}
	onBeacon := func(_ pahomqtt.Client, msg pahomqtt.Message) {
		handleBeacon(log, ts, msg)
	}

	opts := pahomqtt.NewClientOptions().
		AddBroker("ssl://" + broker).
		SetTLSConfig(&tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true, // #nosec G402 -- pinning model: the node's registry pins THIS key; no CA exists
			MinVersion:         tls.VersionTLS13,
		}).
		SetClientID(ulid).SetUsername(ulid).
		// Clean session on purpose. A kept session would queue commands in mochi's
		// memory, which a broker restart loses. With a clean session the node records
		// undelivered commands and replays them from the durable commands stream when
		// the machine subscribes again.
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(connectRetryInterval).
		SetConnectTimeout(connectTimeout).
		// Each inbound message gets its own goroutine, so a handler may wait on
		// the token of the ack it publishes without deadlocking the router.
		SetOrderMatters(false)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		log.Info("CONNECTED", "broker", broker, "client_id", ulid)
		// The hold is already open (see SetReconnectingHandler and the call before
		// Connect); refreshing it here is harmless.
		ts.Connect()

		// Send both SUBSCRIBEs before waiting on either, so a slow command SUBACK
		// cannot push the beacon subscribe past the hold.
		cmdTok := c.Subscribe(cmdFilter, 1, onCommand)
		// The node sends a beacon when this subscription arrives, which is what ends
		// the reconnect hold within hold_ms. The beacon is QoS 0.
		beaconTok := c.Subscribe(beaconFilter(), 0, onBeacon)

		if !cmdTok.WaitTimeout(subscribeTimeout) {
			log.Error("subscribe not confirmed", "filter", cmdFilter, "waited", subscribeTimeout)
			return
		}
		if err := cmdTok.Error(); err != nil {
			log.Error("subscribe failed", "filter", cmdFilter, "err", err)
			return
		}
		log.Info("subscribed", "filter", cmdFilter, "qos", 1)

		if !beaconTok.WaitTimeout(subscribeTimeout) {
			log.Error("subscribe not confirmed", "filter", beaconFilter(), "waited", subscribeTimeout)
			return
		}
		if err := beaconTok.Error(); err != nil {
			log.Error("subscribe failed", "filter", beaconFilter(), "err", err)
			return
		}
		log.Info("subscribed", "filter", beaconFilter(), "qos", 0)
	})
	opts.SetConnectionLostHandler(func(_ pahomqtt.Client, err error) {
		log.Warn("CONNECTION LOST — reconnecting", "broker", broker, "err", err)
	})
	opts.SetReconnectingHandler(func(_ pahomqtt.Client, _ *pahomqtt.ClientOptions) {
		log.Info("reconnecting", "broker", broker)
		// Open the hold before the reconnect starts. Inbound messages run on their own
		// goroutines with no ordering against onConnect, so a command could otherwise
		// be decided on an unsynced clock.
		ts.Connect()
	})
	// Commands and beacons only arrive once their subscription exists, so an
	// unrouted message is a real mismatch and worth a log line.
	opts.SetDefaultPublishHandler(func(_ pahomqtt.Client, msg pahomqtt.Message) {
		log.Warn("message with no matching route", "topic", msg.Topic())
	})

	client := pahomqtt.NewClient(opts)
	log.Info("connecting", "broker", broker, "client_id", ulid, "interval", interval)
	// SetReconnectingHandler does not fire for the first attempt, so open the hold
	// here too.
	ts.Connect()
	// Connect() is called exactly once: SetConnectRetry(true) makes paho retry
	// internally, so calling it again per iteration would stack connection
	// attempts. The token completes when the connection is up (or is aborted).
	tk := client.Connect()
	for !tk.WaitTimeout(connectProgressEvery) {
		select {
		case <-ctx.Done():
			log.Info("shutdown signal while connecting — exiting")
			client.Disconnect(disconnectQuiesceMS)
			return 0
		default:
		}
		log.Info("still waiting for broker", "broker", broker, "retry_interval", connectRetryInterval)
	}
	if err := tk.Error(); err != nil {
		select {
		case <-ctx.Done():
			log.Info("shutdown signal while connecting — exiting")
			return 0
		default:
		}
		log.Error("connect aborted", "broker", broker, "err", err)
		return 1
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	seq := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("shutdown signal — disconnecting", "published", seq)
			client.Disconnect(disconnectQuiesceMS)
			log.Info("stopped")
			return 0
		case <-ticker.C:
			seq++
			// Blocks until this seq's PUBACK arrives, so at most one metric is in flight
			// (see publishSeqSerialized).
			publishSeqSerialized(ctx, log, client.Publish, metricTopic, seq)
		}
	}
}

// publishSeqSerialized publishes one metric and blocks until it is confirmed or
// ctx is cancelled. The caller must not publish the next seq before it returns.
//
// This keeps metrics in order across reconnects: paho replays unacked messages
// from an unordered map while new publishes go out, so with more than one in
// flight an old seq could arrive after a newer one. publish is client.Publish,
// so tests can pass a fake.
func publishSeqSerialized(ctx context.Context, log *slog.Logger, publish func(topic string, qos byte, retained bool, payload interface{}) pahomqtt.Token, topic string, seq int) {
	v := 20 + 5*math.Sin(float64(seq)/10)
	// value and signal_id satisfy the _Metric contract; v is what the built-in
	// rules expect when no bundle is loaded.
	payload := fmt.Sprintf(`{"v": %.2f, "value": %.2f, "signal_id": %q, "seq": %d, "timestamp": %.3f}`, v, v, topic, seq, float64(time.Now().UnixNano())/1e9)
	log.Debug("publish metric", "topic", topic, "seq", seq, "v", v)
	waitForConfirm(ctx, log, publish(topic, 1, false, payload), "metric publish", "topic", topic, "seq", seq)
}

// waitForConfirm waits for tok, logging progress, until it completes or ctx is
// cancelled. It never gives up early: that would let a second publish go out
// while this one is unacked.
func waitForConfirm(ctx context.Context, log *slog.Logger, tok pahomqtt.Token, what string, attrs ...any) bool {
	for !tok.WaitTimeout(publishTimeout) {
		select {
		case <-ctx.Done():
			log.Info(what+" abandoned — shutdown signal while waiting for PUBACK", attrs...)
			return false
		default:
		}
		log.Warn(what+" not yet confirmed — still waiting", append(append([]any{}, attrs...), "waited_at_least", publishTimeout)...)
	}
	if err := tok.Error(); err != nil {
		log.Warn(what+" failed", append(attrs, "err", err)...)
		return false
	}
	log.Debug(what+" confirmed", attrs...)
	return true
}

// publishInterval reads PUBLISH_INTERVAL_MS; anything unparseable or <= 0 falls
// back to the 1000 ms default rather than killing the container.
func publishInterval(log *slog.Logger) time.Duration {
	const def = 1000
	raw := env("PUBLISH_INTERVAL_MS", strconv.Itoa(def))
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		log.Warn("invalid PUBLISH_INTERVAL_MS — using default", "value", raw, "default_ms", def)
		ms = def
	}
	return time.Duration(ms) * time.Millisecond
}

// handleCommand decides 200 or 498 and acks the command, never panicking on a
// bad message. The expiry decision waits for the time-sync hold; each message
// has its own goroutine, so waiting does not stall the router.
func handleCommand(ctx context.Context, log *slog.Logger, c pahomqtt.Client, ulid, nodeULID string, ts *timeSync, msg pahomqtt.Message) {
	topic := msg.Topic()
	var cmd map[string]any
	if err := json.Unmarshal(msg.Payload(), &cmd); err != nil {
		log.Error("COMMAND ignored — payload is not JSON", "topic", topic, "err", err, "payload", string(msg.Payload()))
		return
	}
	syncedNow, offsetMS, deadlineHit := ts.Await(ctx)
	if deadlineHit {
		log.Warn("time-sync hold deadline reached with no post-connect beacon; proceeding on the last known offset",
			"offset_ms", offsetMS)
	}
	code, message := result(cmd, syncedNow.UnixMilli())
	corr, _ := cmd["correlation_id"].(string)
	log.Info("COMMAND received",
		"topic", topic,
		"correlation_id", corr,
		"params", cmd["params"],
		"expires_at", cmd["expires_at"],
		"result_code", code,
		"message", message)

	if corr == "" {
		log.Warn("COMMAND not acked — missing correlation_id (an ack without one is unroutable)", "topic", topic)
		return
	}
	name := lastSegment(topic)
	if name == "" {
		log.Warn("COMMAND not acked — command topic has no name segment", "topic", topic, "correlation_id", corr)
		return
	}
	ackTopic := uns.Prefix() + "_Ack/" + nodeULID + "/" + ulid + "/" + name
	ack, err := json.Marshal(map[string]any{"correlation_id": corr, "result_code": code, "message": message})
	if err != nil {
		log.Error("COMMAND not acked — cannot encode ack", "topic", topic, "correlation_id", corr, "err", err)
		return
	}
	log.Info("ACK publishing", "topic", ackTopic, "correlation_id", corr, "result_code", code)
	confirm(log, c.Publish(ackTopic, 1, false, ack), "ack publish", "topic", ackTopic, "correlation_id", corr)
}

// result decides the ack code. expires_at is unix milliseconds; when it is
// absent or unusable the command is treated as NOT expired — a missing deadline
// must never turn into a rejection.
func result(cmd map[string]any, nowMS int64) (int, string) {
	exp, ok := expiresAt(cmd)
	switch {
	case !ok:
		return codeOK, "executed (no usable expires_at)"
	case exp < nowMS:
		return codeExpired, fmt.Sprintf("expired %d ms ago", nowMS-exp)
	default:
		return codeOK, "executed"
	}
}

// expiresAt extracts expires_at as unix milliseconds. JSON numbers decode to
// float64; a numeric string is accepted as well. Anything else (absent, null,
// object, non-numeric string) reports "no usable deadline".
func expiresAt(cmd map[string]any) (int64, bool) {
	switch v := cmd["expires_at"].(type) {
	case float64:
		return int64(v), true
	case string:
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			return ms, true
		}
	}
	return 0, false
}

// handleBeacon records the offset from a _TimeSync beacon ({"now_ms": ...}). A
// malformed payload is logged and dropped. A message flagged as a duplicate is
// a resend carrying an old now_ms and is ignored, so it cannot end a hold or
// skew the offset.
func handleBeacon(log *slog.Logger, ts *timeSync, msg pahomqtt.Message) {
	if msg.Duplicate() {
		log.Debug("_TimeSync beacon ignored — broker-flagged duplicate/resend, never a fresh sample", "topic", msg.Topic())
		return
	}
	var p struct {
		NowMS int64 `json:"now_ms"`
	}
	if err := json.Unmarshal(msg.Payload(), &p); err != nil {
		log.Warn("_TimeSync beacon ignored — payload is not JSON", "topic", msg.Topic(), "err", err, "payload", string(msg.Payload()))
		return
	}
	ts.Beacon(p.NowMS)
	log.Debug("time-sync beacon received", "topic", msg.Topic(), "now_ms", p.NowMS, "offset_ms", ts.Snapshot().offsetMS)
}

// lastSegment returns the command name, i.e. the last segment of the topic.
func lastSegment(topic string) string {
	if i := strings.LastIndex(topic, "/"); i >= 0 {
		return topic[i+1:]
	}
	return topic
}

// confirm waits (bounded) for a publish token and logs its outcome.
func confirm(log *slog.Logger, tk pahomqtt.Token, what string, attrs ...any) {
	if !tk.WaitTimeout(publishTimeout) {
		log.Warn(what+" not confirmed", append(attrs, "waited", publishTimeout)...)
		return
	}
	if err := tk.Error(); err != nil {
		log.Warn(what+" failed", append(attrs, "err", err)...)
		return
	}
	log.Debug(what+" confirmed", attrs...)
}
