// Command colca-machine is the demo machine simulator: a long-running MQTT
// client that behaves like a real machine attached to a Colca edge node.
//
// It publishes a sine-shaped temperature metric to colca/v1/_Metric/{ulid}/temp
// every PUBLISH_INTERVAL_MS, subscribes to colca/v1/_CmdParam/{ulid}/# and acks
// every command it receives to colca/v1/_Ack/{ulid}/{command-name} with result
// code 200, or 498 when the command's expires_at (unix milliseconds) already
// passed — expiry is decided by the machine at execution time, never by the
// queue.
//
// Expiry is decided against SYNCED time, not the machine's raw wall clock
// (the time sync move drain design
// §2.3): it subscribes to colca/v1/_TimeSync/+ and maintains offset_ms from the
// most recent beacon, so a machine whose own clock has drifted (dead RTC
// battery, no NTP reach) still decides expiry correctly. After every
// (re)connect, expiry decisions are BUFFERED — never rejected — until either
// the first beacon received after the connect instant, or TIME_SYNC_HOLD_MS
// elapses (default 10000; env, mirrors the node's time_sync.hold_ms), at
// which point it proceeds on the last-known offset (0 if it never synced)
// and logs a warning.
//
// Env SIM_CLOCK_OFFSET_MS (default 0, signed milliseconds) skews this
// process's own notion of wall time everywhere it reads the clock — a
// SIMULATOR-ONLY convenience for skew scenarios (chaos design §5's "skewed
// machine" cases); production colcad has no such knob.
//
// It is a container process in the demo topology: it must survive a broker
// restart (docker compose stop/start of an edge node), must never exit because
// of a malformed message, and must shut down cleanly on SIGTERM so that
// `docker compose down` never has to escalate to SIGKILL.
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

	// beaconFilter is the MQTT subscribe filter for the node's time-sync
	// beacon (design §2.2: colca/v1/_TimeSync/{node-ulid}, no hierarchy path).
	// The node-id level is a wildcard: this process only ever sees the beacon
	// from whichever node it happens to be connected to.
	beaconFilter = "colca/v1/_TimeSync/+"

	// defaultHoldMS is the time-sync design §2.5 default for hold_ms — how
	// long a post-(re)connect expiry decision is buffered before the machine
	// fails open on its last-known offset.
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

// newSimClock builds this process's single wall-clock read point (time-sync
// task's SIM_CLOCK_OFFSET_MS): every decision in timeSync/handleCommand reads
// time through this func, never through a bare time.Now() — the same
// injectable-clock pattern colcad's own time-sync state uses
// (internal/clock.Clock). offsetMS skews what "now" means everywhere the
// simulator reads the clock; it is constant for the process lifetime (read
// once at startup), which is what makes it safe to mix with real
// time.Timer/time.Since durations elsewhere (a constant additive skew always
// cancels out of a subtraction between two readings of this same clock).
func newSimClock(offsetMS int64) func() time.Time {
	return func() time.Time { return time.Now().Add(time.Duration(offsetMS) * time.Millisecond) }
}

// syncState is a snapshot of timeSync's decision-relevant fields — exists so
// decide (the pure function every time-sync test exercises) needs no lock and
// no goroutine: give it a snapshot and a wall reading, get a decision.
type syncState struct {
	offsetMS int64
	holding  bool
	deadline time.Time
}

// decide is the pure state machine at the heart of design §2.3 rule 2: ready
// is false exactly while a hold is open and its deadline has not passed yet
// — the caller must buffer the decision, never reject it. Once ready,
// syncedNow is wall_now + offset_ms (rule 1); deadlineHit reports whether it
// was the DEADLINE, not a beacon, that let the decision through — the
// caller's cue to log the design §2.4 warning.
func decide(s syncState, wallNow time.Time) (ready bool, syncedNow time.Time, deadlineHit bool) {
	if s.holding && wallNow.Before(s.deadline) {
		return false, time.Time{}, false
	}
	return true, wallNow.Add(time.Duration(s.offsetMS) * time.Millisecond), s.holding
}

// timeSync is the machine-side half of the time-sync rule (design §2.3): the
// offset learned from the most recent _TimeSync beacon, plus the
// post-(re)connect hold that buffers expiry decisions — never rejects them —
// until either the first beacon received after the connect instant, or
// hold_ms elapses (rule 2: fail open on deadline, with a warning).
//
// now is the injectable wall clock (mandatory for every decision here — no
// bare time.Now()): production passes newSimClock's SIM_CLOCK_OFFSET_MS-
// wrapped clock, tests pass a fake.
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

// Connect starts a new hold window (design §2.3 rule 2): a (re)connect must
// not trust a pre-reconnect offset for expiry decisions until either a fresh
// beacon lands or hold_ms elapses. holdMS <= 0 is the operator's explicit "no
// hold" (config.TimeSync.EffectiveHoldMS's own contract on the node side) —
// proceed immediately on the last-known offset.
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

// Beacon records offset = now_ms - wall_receipt (design §2.1/§2.3 rule 1:
// last sample wins, no smoothing) and releases any open hold immediately —
// this IS "the first beacon received after the connect instant" the rule
// calls for, since Connect resets the hold on every (re)connect before any
// beacon can arrive.
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

// Await blocks until an expiry decision may proceed (design §2.3 rule 2:
// "commands buffered, not rejected"): either a beacon lands (immediate
// release via the closed channel — no poll-interval delay, so the hold is
// milliseconds in practice per the design's own claim) or the hold's
// deadline passes (fail open on the last-known offset, deadlineHit=true so
// the caller logs the required warning). ctx cancellation (process shutdown)
// also returns, best-effort, so a draining process is never stuck here.
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

		// deadline.Sub(wallNow), NOT time.Until(deadline): both deadline and
		// wallNow were read through the SAME (possibly skewed) clock, so
		// their difference is the correct REAL remaining duration regardless
		// of any constant SIM_CLOCK_OFFSET_MS skew — time.Until would instead
		// subtract the unskewed time.Now(), silently reintroducing the skew
		// into the timer length.
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

func run() int {
	ulid := env("MACHINE_ULID", "m1")
	keyPath := env("MACHINE_KEY", "/keys/"+ulid+"-machine.key")
	broker := env("BROKER_ADDR", "127.0.0.1:1883")
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("machine", ulid)
	interval := publishInterval(log)

	// Time-sync design §2.1/§2.3: the machine's own wall-clock read point
	// (SIM_CLOCK_OFFSET_MS-skewable, simulator-only) and the offset/hold
	// state derived from the node's _TimeSync beacon.
	simOffsetMS := envInt64(log, "SIM_CLOCK_OFFSET_MS", 0)
	simNow := newSimClock(simOffsetMS)
	holdMS := envInt64(log, "TIME_SYNC_HOLD_MS", defaultHoldMS)
	ts := newTimeSync(simNow, holdMS)
	if simOffsetMS != 0 {
		log.Warn("SIM_CLOCK_OFFSET_MS active — this process's wall clock is deliberately skewed (simulator only)", "offset_ms", simOffsetMS)
	}

	// The machine's key IS its credential (auth design §6.1): pre-provisioned
	// at keyPath (gen-keys.sh) and enrolled at the node before first connect.
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

	metricTopic := "colca/v1/_Metric/" + ulid + "/temp"
	// Path-anchored (node-id level is a wildcard for readers): the machine's
	// default read grant covers its own zone, which is where its commands land.
	cmdFilter := "colca/v1/_CmdParam/+/" + ulid + "/#"

	// SIGINT/SIGTERM cancel the context; every wait below selects on it, so the
	// process always leaves through the single clean shutdown path.
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	onCommand := func(c pahomqtt.Client, msg pahomqtt.Message) {
		handleCommand(ctx, log, c, ulid, ts, msg)
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
		// The broker keeps the session, so QoS-1 commands issued while this
		// machine was away are delivered after a reconnect.
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(connectRetryInterval).
		SetConnectTimeout(connectTimeout).
		// Each inbound message gets its own goroutine, so a handler may wait on
		// the token of the ack it publishes without deadlocking the router.
		SetOrderMatters(false)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		log.Info("CONNECTED", "broker", broker, "client_id", ulid)
		// Time-sync design §2.3 rule 2: a fresh hold window starts on every
		// (re)connect, BEFORE any subscription — a command routed through
		// SetDefaultPublishHandler ahead of the SUBSCRIBE completing (see
		// below) must still see the hold already open.
		ts.Connect()

		// Both SUBSCRIBE packets are issued CONCURRENTLY — c.Subscribe
		// itself is non-blocking (it queues the packet and returns a
		// token immediately); only WAITING on that token blocks. Calling
		// Subscribe for cmd, THEN blocking on its token, THEN calling
		// Subscribe for beacon (the original shape) meant a slow or
		// contended broker's cmd SUBACK — bounded by the SAME
		// subscribeTimeout order of magnitude as time_sync's hold_ms —
		// could eat into the beacon subscribe's own share of the
		// reconnect-hold window before its packet was even SENT. Found
		// hardening the design §2.2 subscribe-triggered beacon fix: a CI run showed the reconnect-hold decision land on
		// the deadline despite the structural fix, and this sequential
		// coupling is the residual explanation once the primary
		// connect-vs-subscribe race was already closed. Issuing both
		// packets back-to-back before waiting on either token removes the
		// coupling: the beacon SUBSCRIBE is in flight at essentially the
		// same instant as the cmd one, not queued behind its full round
		// trip.
		cmdTok := c.Subscribe(cmdFilter, 1, onCommand)
		// Time-sync design §2.2: the node beacons on this session's
		// SUBSCRIBE to colca/v1/_TimeSync/+, so issuing it here — immediately,
		// concurrently with the cmd subscribe above, not queued behind it —
		// is what makes the reconnect hold (design §2.3 rule 2) land inside
		// hold_ms deterministically, not merely usually.
		//
		// QoS 0: the node now publishes the beacon
		// at QoS 0 (mqttsrv.go), so the effective delivered QoS is
		// min(0, sub.Qos) = 0 regardless of what's requested here — matching
		// it explicitly documents that this subscription deliberately wants
		// no at-least-once/redelivery semantics, not accidentally-QoS-0.
		beaconTok := c.Subscribe(beaconFilter, 0, onBeacon)

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
			log.Error("subscribe not confirmed", "filter", beaconFilter, "waited", subscribeTimeout)
			return
		}
		if err := beaconTok.Error(); err != nil {
			log.Error("subscribe failed", "filter", beaconFilter, "err", err)
			return
		}
		log.Info("subscribed", "filter", beaconFilter, "qos", 0)
	})
	opts.SetConnectionLostHandler(func(_ pahomqtt.Client, err error) {
		log.Warn("CONNECTION LOST — reconnecting", "broker", broker, "err", err)
	})
	opts.SetReconnectingHandler(func(_ pahomqtt.Client, _ *pahomqtt.ClientOptions) {
		log.Info("reconnecting", "broker", broker)
	})
	// With a persistent session the broker may push queued commands before the
	// SUBSCRIBE of a fresh connection has been registered as a route; such a
	// message would otherwise be dropped, so route it by hand.
	opts.SetDefaultPublishHandler(func(c pahomqtt.Client, msg pahomqtt.Message) {
		if isOwnCommand(msg.Topic(), ulid) {
			log.Debug("command arrived before the subscription route", "topic", msg.Topic())
			onCommand(c, msg)
			return
		}
		if isTimeSyncTopic(msg.Topic()) {
			log.Debug("beacon arrived before the subscription route", "topic", msg.Topic())
			onBeacon(c, msg)
			return
		}
		log.Debug("message with no matching route", "topic", msg.Topic())
	})

	client := pahomqtt.NewClient(opts)
	log.Info("connecting", "broker", broker, "client_id", ulid, "interval", interval)
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
			v := 20 + 5*math.Sin(float64(seq)/10)
			payload := fmt.Sprintf(`{"v": %.2f, "seq": %d}`, v, seq)
			log.Debug("publish metric", "topic", metricTopic, "seq", seq, "v", v)
			// Confirmed off the loop: while the broker is away the tokens stay
			// pending, and blocking here would stall both the metric cadence
			// and the shutdown path.
			go confirm(log, client.Publish(metricTopic, 1, false, payload), "metric publish", "topic", metricTopic, "seq", seq)
		}
	}
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

// handleCommand logs a received command prominently, decides 200 vs 498 and
// acks it. Every failure path returns instead of panicking: this process must
// outlive any message a node can send it.
//
// Time-sync design §2.3 rule 2: the expiry decision is BUFFERED (ts.Await)
// until a post-connect beacon lands or the hold deadline passes — never
// rejected. This runs in the message's own goroutine (SetOrderMatters(false)
// above), so blocking here for up to hold_ms never stalls the router.
func handleCommand(ctx context.Context, log *slog.Logger, c pahomqtt.Client, ulid string, ts *timeSync, msg pahomqtt.Message) {
	topic := msg.Topic()
	var cmd map[string]any
	if err := json.Unmarshal(msg.Payload(), &cmd); err != nil {
		log.Error("COMMAND ignored — payload is not JSON", "topic", topic, "err", err, "payload", string(msg.Payload()))
		return
	}
	syncedNow, offsetMS, deadlineHit := ts.Await(ctx)
	if deadlineHit {
		log.Warn("time-sync hold deadline reached with no post-connect beacon — proceeding on last-known offset (design §2.3 rule 2)",
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
	ackTopic := "colca/v1/_Ack/" + ulid + "/" + name
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

// isOwnCommand reports whether topic is a _CmdParam addressed into this
// machine's zone (local coordinates: colca/v1/_CmdParam/{target}/{mount}/...,
// where the mount equals the machine's ulid in the demo topology).
func isOwnCommand(topic, ulid string) bool {
	parts := strings.Split(topic, "/")
	return len(parts) >= 5 && parts[2] == "_CmdParam" && parts[4] == ulid
}

// isTimeSyncTopic reports whether topic is the node's time-sync beacon
// (design §2.2: colca/v1/_TimeSync/{node-ulid}, no hierarchy path — exactly 4
// segments, unlike every other uns contract).
func isTimeSyncTopic(topic string) bool {
	parts := strings.Split(topic, "/")
	return len(parts) == 4 && parts[2] == "_TimeSync"
}

// handleBeacon parses a _TimeSync beacon (design §2.2: {"now_ms": <int64>})
// and records the offset sample. A malformed payload is logged and dropped —
// this process must never crash on a message a node can send it, same rule
// as handleCommand.
//
// A message flagged Duplicate() is a broker-side resend, never a fresh
// sample, and is dropped outright: it must never be
// allowed to satisfy the post-connect hold or update the offset, since a
// resend necessarily carries an OLD now_ms racing a stale wall-clock
// reading — exactly the "clock behind" corruption design §1.1 exists to
// prevent. The beacon now publishes at QoS 0 (mqttsrv.go), which already
// removes the one delivery path that could produce a resend in this
// codebase's own broker (mochi only queues/resends QoS>0); this check is
// defense in depth against any other source of a Duplicate-flagged message
// and costs nothing, since _TimeSync is "only the latest matters" by design.
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
