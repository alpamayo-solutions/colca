// Command colca-machine is the demo machine simulator: an MQTT client that
// behaves like a machine attached to an edge node.
//
// It publishes a temperature metric under its node's ULID (NODE_ULID) every
// PUBLISH_INTERVAL_MS, subscribes to its commands and acks each with 200, or
// 498 when expires_at has passed. Expiry is judged on the machine's own wall
// clock, which the host's NTP keeps in step with the node's.
//
// It only talks to the node whose public key it pins: NODE_PUBKEY holds the hex
// key, or NODE_PUBKEY_FILE names the file colca-keygen printed it to.
// INSECURE_ANY_NODE_KEY=1 turns the check off for local tests.
//
// It survives broker restarts and malformed messages and exits cleanly on
// SIGTERM.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/hex"
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
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// nodeKeyPin returns the node's public key from NODE_PUBKEY or NODE_PUBKEY_FILE,
// or "" when neither is set.
func nodeKeyPin() (string, error) {
	if v := strings.TrimSpace(os.Getenv("NODE_PUBKEY")); v != "" {
		return parsePubHex(v)
	}
	path := os.Getenv("NODE_PUBKEY_FILE")
	if path == "" {
		return "", nil
	}
	raw, err := os.ReadFile(path) //nolint:gosec // a path from the machine's own environment
	if err != nil {
		return "", fmt.Errorf("NODE_PUBKEY_FILE: %w", err)
	}
	return parsePubHex(strings.TrimSpace(string(raw)))
}

func parsePubHex(v string) (string, error) {
	key, err := hex.DecodeString(v)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return "", fmt.Errorf("%q is not a hex ed25519 public key", v)
	}
	return hex.EncodeToString(key), nil
}

// tlsConfig presents the machine's certificate and accepts only the node whose key
// is nodePub; an empty nodePub accepts any node. Certificates are self-signed, so
// the pinned key is the trust, not a CA.
func tlsConfig(cert tls.Certificate, nodePub string) *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true, //nolint:gosec // trust is the pinned node key, checked in VerifyConnection
		MinVersion:         tls.VersionTLS13,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if nodePub == "" {
				return nil
			}
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("node presented no certificate")
			}
			pub, err := identity.PeerPubHex(cs.PeerCertificates[0].Raw)
			if err != nil {
				return err
			}
			if pub != nodePub {
				return fmt.Errorf("node key mismatch: got %s, want %s", pub, nodePub)
			}
			return nil
		},
	}
}

func main() { os.Exit(run()) }

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

	nodePub, err := nodeKeyPin()
	if err != nil {
		log.Error("cannot read the node's public key", "err", err)
		return 1
	}
	if nodePub == "" {
		if env("INSECURE_ANY_NODE_KEY", "") != "1" {
			log.Error("NODE_PUBKEY or NODE_PUBKEY_FILE is required: the machine only talks to the node whose key it pins")
			return 1
		}
		log.Warn("INSECURE_ANY_NODE_KEY=1: accepting any node key, use this only for local tests")
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
		handleCommand(log, c.Publish, ulid, nodeULID, time.Now, msg)
	}

	opts := pahomqtt.NewClientOptions().
		AddBroker("ssl://" + broker).
		SetTLSConfig(tlsConfig(cert, nodePub)).
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
		cmdTok := c.Subscribe(cmdFilter, 1, onCommand)
		if !cmdTok.WaitTimeout(subscribeTimeout) {
			log.Error("subscribe not confirmed", "filter", cmdFilter, "waited", subscribeTimeout)
			return
		}
		if err := cmdTok.Error(); err != nil {
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
	// Commands only arrive once their subscription exists, so an unrouted message
	// is a real mismatch and worth a log line.
	opts.SetDefaultPublishHandler(func(_ pahomqtt.Client, msg pahomqtt.Message) {
		log.Warn("message with no matching route", "topic", msg.Topic())
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
			// Blocks until this seq's PUBACK arrives, so at most one metric is in flight
			// (see publishSeqSerialized).
			publishSeqSerialized(ctx, log, client.Publish, metricTopic, seq)
		}
	}
}

// publishFunc is the signature of paho's client.Publish.
type publishFunc func(topic string, qos byte, retained bool, payload interface{}) pahomqtt.Token

// publishSeqSerialized publishes one metric and blocks until it is confirmed or
// ctx is cancelled. The caller must not publish the next seq before it returns.
//
// This keeps metrics in order across reconnects: paho replays unacked messages
// from an unordered map while new publishes go out, so with more than one in
// flight an old seq could arrive after a newer one. publish is client.Publish,
// so tests can pass a fake.
func publishSeqSerialized(ctx context.Context, log *slog.Logger, publish publishFunc, topic string, seq int) {
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
// bad message. now is the machine's wall clock, the only time expiry is judged
// on. publish is client.Publish, so tests can pass a fake.
func handleCommand(log *slog.Logger, publish publishFunc, ulid, nodeULID string, now func() time.Time, msg pahomqtt.Message) {
	topic := msg.Topic()
	var cmd map[string]any
	if err := json.Unmarshal(msg.Payload(), &cmd); err != nil {
		log.Error("COMMAND ignored — payload is not JSON", "topic", topic, "err", err, "payload", string(msg.Payload()))
		return
	}
	code, message := result(cmd, now().UnixMilli())
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
	confirm(log, publish(ackTopic, 1, false, ack), "ack publish", "topic", ackTopic, "correlation_id", corr)
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
