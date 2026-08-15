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
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() { os.Exit(run()) }

func run() int {
	ulid := env("MACHINE_ULID", "m1")
	keyPath := env("MACHINE_KEY", "/keys/"+ulid+"-machine.key")
	broker := env("BROKER_ADDR", "127.0.0.1:1883")
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("machine", ulid)
	interval := publishInterval(log)

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
		handleCommand(log, c, ulid, msg)
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
		// Subscribing here (and not once after Connect) re-establishes the
		// subscription after every reconnect — otherwise the machine goes deaf
		// as soon as the edge node restarts.
		tk := c.Subscribe(cmdFilter, 1, onCommand)
		if !tk.WaitTimeout(subscribeTimeout) {
			log.Error("subscribe not confirmed", "filter", cmdFilter, "waited", subscribeTimeout)
			return
		}
		if err := tk.Error(); err != nil {
			log.Error("subscribe failed", "filter", cmdFilter, "err", err)
			return
		}
		log.Info("subscribed", "filter", cmdFilter, "qos", 1)
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
func handleCommand(log *slog.Logger, c pahomqtt.Client, ulid string, msg pahomqtt.Message) {
	topic := msg.Topic()
	var cmd map[string]any
	if err := json.Unmarshal(msg.Payload(), &cmd); err != nil {
		log.Error("COMMAND ignored — payload is not JSON", "topic", topic, "err", err, "payload", string(msg.Payload()))
		return
	}
	code, message := result(cmd, time.Now().UnixMilli())
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
