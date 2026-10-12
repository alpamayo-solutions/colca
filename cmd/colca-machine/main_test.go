package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/identity"
)

// fakeCommand is a minimal pahomqtt.Message for driving handleCommand.
type fakeCommand struct {
	topic   string
	payload []byte
}

func (m fakeCommand) Duplicate() bool   { return false }
func (m fakeCommand) Qos() byte         { return 1 }
func (m fakeCommand) Retained() bool    { return false }
func (m fakeCommand) Topic() string     { return m.topic }
func (m fakeCommand) MessageID() uint16 { return 0 }
func (m fakeCommand) Payload() []byte   { return m.payload }
func (m fakeCommand) Ack()              {}

var _ pahomqtt.Message = fakeCommand{}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var epoch = time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

// A command without a usable expires_at always executes.
func TestResultNoUsableExpiryAlwaysExecutes(t *testing.T) {
	cases := []map[string]any{
		{},
		{"expires_at": nil},
		{"expires_at": "not-a-number"},
		{"expires_at": map[string]any{}},
	}
	for _, cmd := range cases {
		code, msg := result(cmd, epoch.UnixMilli())
		if code != codeOK {
			t.Errorf("cmd %+v: code = %d, want %d (%s)", cmd, code, codeOK, msg)
		}
	}
}

func TestResultExpiryBoundary(t *testing.T) {
	now := epoch.UnixMilli()
	if code, _ := result(map[string]any{"expires_at": float64(now)}, now); code != codeOK {
		t.Errorf("exp == now must not be expired, got %d", code)
	}
	if code, _ := result(map[string]any{"expires_at": float64(now - 1)}, now); code != codeExpired {
		t.Errorf("exp < now must be expired, got %d", code)
	}
	if code, _ := result(map[string]any{"expires_at": float64(now + 1)}, now); code != codeOK {
		t.Errorf("exp > now must not be expired, got %d", code)
	}
}

// The machine judges expires_at on its own clock and nothing else: the same
// command is executed by a machine whose clock is before the deadline and
// answered 498 by one whose clock is past it. No time from the node enters the
// decision.
func TestCommandExpiryIsJudgedOnTheMachinesOwnClock(t *testing.T) {
	deadline := epoch.Add(time.Minute)
	command, err := json.Marshal(map[string]any{
		"correlation_id": "c-1", "expires_at": deadline.UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		now  time.Time
		want int
	}{
		"clock before the deadline executes":  {epoch, codeOK},
		"clock at the deadline executes":      {deadline, codeOK},
		"clock past the deadline answers 498": {deadline.Add(time.Millisecond), codeExpired},
	} {
		t.Run(name, func(t *testing.T) {
			var ackTopic string
			var ack map[string]any
			publish := func(topic string, _ byte, _ bool, payload interface{}) pahomqtt.Token {
				ackTopic = topic
				if err := json.Unmarshal(payload.([]byte), &ack); err != nil {
					t.Errorf("ack is not JSON: %v", err)
				}
				tok := newFakeToken()
				tok.complete()
				return tok
			}
			handleCommand(discardLogger(), publish, "m1", "n1", func() time.Time { return tc.now },
				fakeCommand{topic: "colca/v1/_CmdParam/n1/m1/setpoint", payload: command})
			if ackTopic != "colca/v1/_Ack/n1/m1/setpoint" {
				t.Fatalf("ack topic = %q, want colca/v1/_Ack/n1/m1/setpoint", ackTopic)
			}
			if got := int(ack["result_code"].(float64)); got != tc.want {
				t.Fatalf("result_code = %d, want %d", got, tc.want)
			}
		})
	}
}

// fakeToken is a minimal pahomqtt.Token whose completion the test controls.
type fakeToken struct{ done chan struct{} }

func newFakeToken() *fakeToken { return &fakeToken{done: make(chan struct{})} }

func (t *fakeToken) Wait() bool { <-t.done; return true }
func (t *fakeToken) WaitTimeout(d time.Duration) bool {
	select {
	case <-t.done:
		return true
	case <-time.After(d):
		return false
	}
}
func (t *fakeToken) Done() <-chan struct{} { return t.done }
func (t *fakeToken) Error() error          { return nil }
func (t *fakeToken) complete()             { close(t.done) }

var _ pahomqtt.Token = (*fakeToken)(nil)

// TestSeqPublishesAreSerialized checks that the publish loop never starts a seq
// while an earlier one is unconfirmed, which keeps paho's replay after a
// reconnect from reordering metrics. Both subtests use the real publish
// signature with tokens the test completes.
func TestSeqPublishesAreSerialized(t *testing.T) {
	const n = 3

	t.Run("serialized: publishSeqSerialized keeps at most one seq inflight", func(t *testing.T) {
		var inflight, maxInflight int32
		tokCh := make(chan *fakeToken, n)
		publish := func(topic string, qos byte, retained bool, payload interface{}) pahomqtt.Token {
			cur := atomic.AddInt32(&inflight, 1)
			for {
				m := atomic.LoadInt32(&maxInflight)
				if cur <= m || atomic.CompareAndSwapInt32(&maxInflight, m, cur) {
					break
				}
			}
			tok := newFakeToken()
			tokCh <- tok
			return tok
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			for seq := 1; seq <= n; seq++ {
				publishSeqSerialized(context.Background(), discardLogger(), publish, "topic", seq)
			}
		}()

		for i := 0; i < n; i++ {
			select {
			case tok := <-tokCh:
				atomic.AddInt32(&inflight, -1)
				tok.complete()
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for seq %d's publish call — the serialized loop appears stuck", i+1)
			}
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("producer loop never finished after every token was completed")
		}

		if maxInflight > 1 {
			t.Fatalf("max concurrent inflight seq publishes = %d, want <= 1 (serialization broken)", maxInflight)
		}
	})

	// Fire-and-forget publishing lets every seq be in flight at once, which is what
	// allows paho's replay to reorder them.
	t.Run("mutation check: fire-and-forget (pre-fix pattern) allows >1 inflight", func(t *testing.T) {
		var callCount int32
		var mu sync.Mutex
		var tokens []*fakeToken
		publish := func(topic string, qos byte, retained bool, payload interface{}) pahomqtt.Token {
			atomic.AddInt32(&callCount, 1)
			tok := newFakeToken()
			mu.Lock()
			tokens = append(tokens, tok)
			mu.Unlock()
			return tok
		}

		log := discardLogger()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for seq := 1; seq <= n; seq++ {
				v := 20 + 5*math.Sin(float64(seq)/10)
				payload := fmt.Sprintf(`{"v": %.2f, "seq": %d}`, v, seq)
				go confirm(log, publish("topic", 1, false, payload), "metric publish", "topic", "topic", "seq", seq)
			}
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("fire-and-forget producer loop never finished — it should never block on a token")
		}

		if got := atomic.LoadInt32(&callCount); got != n {
			t.Fatalf("expected all %d seqs published without blocking, got %d calls", n, got)
		}
		// The loop finished with no token completed, so all n were in flight at once.
		mu.Lock()
		pending := len(tokens)
		mu.Unlock()
		if pending != n {
			t.Fatalf("mutation check failed: expected %d simultaneously-inflight (uncompleted) tokens when the "+
				"loop finished, got %d — this test cannot distinguish serialized from fire-and-forget, strengthen it", n, pending)
		}
		for _, tok := range tokens {
			tok.complete() // let the background confirm() goroutines finish instead of leaking past the test
		}
	})
}

func TestTLSConfigAcceptsOnlyThePinnedNode(t *testing.T) {
	node := newTestIdentity(t, "node")
	other := newTestIdentity(t, "other")
	state := func(id *identity.Identity) tls.ConnectionState {
		cert, err := id.SelfSignedCert("peer")
		if err != nil {
			t.Fatal(err)
		}
		leaf, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		return tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
	}
	machineCert, err := newTestIdentity(t, "machine").SelfSignedCert("m1")
	if err != nil {
		t.Fatal(err)
	}

	pinned := tlsConfig(machineCert, node.PublicHex())
	if err := pinned.VerifyConnection(state(node)); err != nil {
		t.Fatalf("the pinned node was rejected: %v", err)
	}
	if err := pinned.VerifyConnection(state(other)); err == nil {
		t.Fatal("a node with another key was accepted")
	}
	if err := pinned.VerifyConnection(tls.ConnectionState{}); err == nil {
		t.Fatal("a node without a certificate was accepted")
	}
	if err := tlsConfig(machineCert, "").VerifyConnection(state(other)); err != nil {
		t.Fatalf("without a pin any node is accepted, got %v", err)
	}
}

func TestNodeKeyPin(t *testing.T) {
	key := newTestIdentity(t, "node").PublicHex()

	t.Setenv("NODE_PUBKEY", strings.ToUpper(key))
	if got, err := nodeKeyPin(); err != nil || got != key {
		t.Fatalf("NODE_PUBKEY: got %q, %v", got, err)
	}

	t.Setenv("NODE_PUBKEY", "")
	path := filepath.Join(t.TempDir(), "node.pub")
	if err := os.WriteFile(path, []byte(key+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NODE_PUBKEY_FILE", path)
	if got, err := nodeKeyPin(); err != nil || got != key {
		t.Fatalf("NODE_PUBKEY_FILE: got %q, %v", got, err)
	}

	t.Setenv("NODE_PUBKEY", "not-a-key")
	if _, err := nodeKeyPin(); err == nil {
		t.Fatal("a malformed key was accepted")
	}

	t.Setenv("NODE_PUBKEY", "")
	t.Setenv("NODE_PUBKEY_FILE", "")
	if got, err := nodeKeyPin(); err != nil || got != "" {
		t.Fatalf("with nothing set: got %q, %v", got, err)
	}
}

func newTestIdentity(t *testing.T, name string) *identity.Identity {
	t.Helper()
	id, err := identity.Generate(filepath.Join(t.TempDir(), name+".key"))
	if err != nil {
		t.Fatal(err)
	}
	return id
}
