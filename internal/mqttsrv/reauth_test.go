package mqttsrv

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	pahov5 "github.com/eclipse/paho.golang/paho"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
)

// noChallenge is paho's AuthHandler for a node that never sends a challenge.
type noChallenge struct{}

func (noChallenge) Authenticate(a *pahov5.Auth) *pahov5.Auth { return a }
func (noChallenge) Authenticated()                           {}

// v5Session is an MQTT 5 client on the human TCP door.
type v5Session struct {
	c        *pahov5.Client
	id       string
	connack  *pahov5.Connack
	messages chan *pahov5.Publish
	// disconnected receives the node's DISCONNECT.
	disconnected chan *pahov5.Disconnect
}

func dialV5(t *testing.T, w *humanWorld, username, token, method string) (*v5Session, error) {
	t.Helper()
	return dialV5As(t, w, fmt.Sprintf("h-v5-%d", time.Now().UnixNano()), username, token, method)
}

// dialV5As connects under a chosen client id, so a second connection can take
// over the first one's session.
func dialV5As(t *testing.T, w *humanWorld, id, username, token, method string) (*v5Session, error) {
	t.Helper()
	conn, err := tls.Dial("tcp", w.srv.HumanTCPAddr(), insecureTLS())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	s := &v5Session{
		id:           id,
		messages:     make(chan *pahov5.Publish, 16),
		disconnected: make(chan *pahov5.Disconnect, 1),
	}
	s.c = pahov5.NewClient(pahov5.ClientConfig{
		Conn:        packets.NewThreadSafeConn(conn),
		AuthHandler: noChallenge{},
		OnPublishReceived: []func(pahov5.PublishReceived) (bool, error){func(p pahov5.PublishReceived) (bool, error) {
			s.messages <- p.Packet
			return true, nil
		}},
		OnServerDisconnect: func(d *pahov5.Disconnect) { s.disconnected <- d },
		OnClientError:      func(error) {},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.connack, err = s.c.Connect(ctx, &pahov5.Connect{
		ClientID:     s.id,
		UsernameFlag: true,
		Username:     username,
		PasswordFlag: true,
		Password:     []byte(token),
		KeepAlive:    30,
		CleanStart:   true,
		Properties:   &pahov5.ConnectProperties{AuthMethod: method},
	})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	t.Cleanup(func() { _ = s.c.Disconnect(&pahov5.Disconnect{}) })
	return s, nil
}

func (s *v5Session) reauth(t *testing.T, method, token string) *pahov5.AuthResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := s.c.Authenticate(ctx, &pahov5.Auth{
		ReasonCode: packets.AuthReauthenticate,
		Properties: &pahov5.AuthProperties{AuthMethod: method, AuthData: []byte(token)},
	})
	if err != nil {
		t.Fatalf("AUTH: %v", err)
	}
	return resp
}

func (s *v5Session) subscribe(t *testing.T, filter string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := s.c.Subscribe(ctx, &pahov5.Subscribe{Subscriptions: []pahov5.SubscribeOptions{{Topic: filter, QoS: 1}}})
	if err != nil || ack.Reasons[0] >= 0x80 {
		t.Fatalf("subscribe %s: %v %+v", filter, err, ack)
	}
}

// expectDisconnect waits for the node's DISCONNECT and checks its reason code.
func (s *v5Session) expectDisconnect(t *testing.T, code byte) {
	t.Helper()
	select {
	case d := <-s.disconnected:
		if d.ReasonCode != code {
			t.Fatalf("DISCONNECT reason 0x%02x, want 0x%02x", d.ReasonCode, code)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DISCONNECT, want reason 0x%02x", code)
	}
}

func readM1(t *testing.T, w *humanWorld, exp time.Time) string {
	t.Helper()
	return w.iss.Mint("anna", []string{"read:" + authtest.ElementID("m1") + "/#"}, exp)
}

// A token renewed over AUTH keeps the connection and its subscriptions, moves
// the session's expiry to the new token, and the sweeper then judges the new one.
func TestHumanReauthenticationRenewsTheSessionInPlace(t *testing.T) {
	w := newHumanWorld(t)
	first := time.Now().Add(2 * time.Minute)
	s, err := dialV5(t, w, "anna", readM1(t, w, first), AuthMethodToken)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if s.connack.Properties == nil || s.connack.Properties.AuthMethod != AuthMethodToken {
		t.Fatalf("CONNACK does not name the authentication method: %+v", s.connack.Properties)
	}
	s.subscribe(t, "colca/v1/_Metric/+/m1/#")

	second := time.Now().Add(10 * time.Minute)
	resp := s.reauth(t, AuthMethodToken, readM1(t, w, second))
	if !resp.Success || resp.ReasonCode != 0 {
		t.Fatalf("re-authentication = %+v, want success", resp)
	}
	session, ok := w.srv.hook.humans.get(s.id)
	if !ok || session.exp.Unix() != second.Unix() {
		t.Fatalf("session expiry = %v, want the new token's %v", session.exp, second)
	}

	// The subscription made under the first token still delivers.
	w.srv.DeliverLocal("colca/v1/_Metric/m1/m1/temp", []byte(`{"v":1}`), false)
	select {
	case <-s.messages:
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery after re-authentication")
	}

	// Past the first token's expiry the session stands; past the second it is kicked.
	w.srv.sweepInvalidHumanSessions(first.Add(time.Second))
	if _, ok := w.srv.hook.humans.get(s.id); !ok {
		t.Fatal("the sweeper judged the replaced token")
	}
	w.srv.sweepInvalidHumanSessions(second.Add(time.Second))
	s.expectDisconnect(t, 0x87)
}

// The node's AUTH answer names the method, which MQTT 5 requires and mqtt.js
// checks. paho's client drops that property, so this reads the packets directly.
func TestHumanReauthenticationAnswerNamesTheMethod(t *testing.T) {
	w := newHumanWorld(t)
	conn, err := tls.Dial("tcp", w.srv.HumanTCPAddr(), insecureTLS())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	future := time.Now().Add(5 * time.Minute)
	connect := &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, KeepAlive: 30, CleanStart: true,
		ClientID:     fmt.Sprintf("h-raw-%d", time.Now().UnixNano()),
		UsernameFlag: true, Username: "anna",
		PasswordFlag: true, Password: []byte(readM1(t, w, future)),
		Properties: &packets.Properties{AuthMethod: AuthMethodToken},
	}
	if _, err := connect.WriteTo(conn); err != nil {
		t.Fatalf("CONNECT: %v", err)
	}
	if cp, err := packets.ReadPacket(conn); err != nil || cp.Type != packets.CONNACK {
		t.Fatalf("CONNACK: %v %v", cp, err)
	}
	auth := &packets.Auth{ReasonCode: packets.AuthReauthenticate, Properties: &packets.Properties{
		AuthMethod: AuthMethodToken, AuthData: []byte(readM1(t, w, future.Add(time.Minute))),
	}}
	if _, err := auth.WriteTo(conn); err != nil {
		t.Fatalf("AUTH: %v", err)
	}
	cp, err := packets.ReadPacket(conn)
	if err != nil || cp.Type != packets.AUTH {
		t.Fatalf("answer: %v %v", cp, err)
	}
	answer := cp.Content.(*packets.Auth)
	if answer.ReasonCode != 0 || answer.Properties.AuthMethod != AuthMethodToken {
		t.Fatalf("AUTH answer = reason 0x%02x method %q, want 0x00 %q",
			answer.ReasonCode, answer.Properties.AuthMethod, AuthMethodToken)
	}
}

// Re-authentication that fails ends the connection with the reason.
func TestHumanReauthenticationRefusals(t *testing.T) {
	w := newHumanWorld(t)
	future := time.Now().Add(5 * time.Minute)
	for name, tc := range map[string]struct {
		method string
		token  string
		want   byte
	}{
		"another subject": {AuthMethodToken, w.iss.Mint("bert", nil, future), 0x87},
		"expired token":   {AuthMethodToken, w.iss.Mint("anna", nil, time.Now().Add(-5*time.Minute)), 0x87},
		"bad signature":   {AuthMethodToken, w.iss.MintOpt(tokentest.MintOpts{Sub: "anna", WrongKey: true}), 0x87},
		"not a token":     {AuthMethodToken, "garbage", 0x87},
		"another method":  {"SCRAM-SHA-1", w.iss.Mint("anna", nil, future), 0x8C},
	} {
		t.Run(name, func(t *testing.T) {
			s, err := dialV5(t, w, "anna", readM1(t, w, future), AuthMethodToken)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			resp := s.reauth(t, tc.method, tc.token)
			if resp.Success || resp.ReasonCode != tc.want {
				t.Fatalf("re-authentication = %+v, want refusal 0x%02x", resp, tc.want)
			}
			waitGone(t, w, s.id)
		})
	}

	t.Run("no method at connect", func(t *testing.T) {
		s, err := dialV5(t, w, "anna", readM1(t, w, future), "")
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		if s.connack.Properties != nil && s.connack.Properties.AuthMethod != "" {
			t.Fatalf("CONNACK names a method nobody asked for: %q", s.connack.Properties.AuthMethod)
		}
		resp := s.reauth(t, AuthMethodToken, readM1(t, w, future))
		if resp.Success || resp.ReasonCode != 0x82 {
			t.Fatalf("AUTH without a method at CONNECT = %+v, want protocol error 0x82", resp)
		}
	})

	t.Run("unknown method at connect", func(t *testing.T) {
		if _, err := dialV5(t, w, "anna", readM1(t, w, future), "SCRAM-SHA-1"); err == nil {
			t.Fatal("CONNECT with an unknown authentication method was accepted")
		}
	})
}

// waitGone waits until the node dropped the session.
func waitGone(t *testing.T, w *humanWorld, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := w.srv.hook.humans.get(id); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("session still open")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A back-channel logout ends the sessions of that login, and only those.
func TestHumanBackchannelLogoutEndsThatLoginsSessions(t *testing.T) {
	w := newHumanWorld(t)
	ended, err := dialV5(t, w, "anna",
		w.iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-a"}), AuthMethodToken)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	other, err := dialV5(t, w, "anna",
		w.iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-b"}), AuthMethodToken)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	if _, err := w.ver.BackchannelLogout(w.iss.Logout("sid-a", "anna")); err != nil {
		t.Fatalf("logout: %v", err)
	}
	ended.expectDisconnect(t, 0x98)
	if _, ok := w.srv.hook.humans.get(other.id); !ok {
		t.Fatal("another login of the same person was ended")
	}

	// The ended login can neither come back nor renew another connection with its token.
	if _, err := dialV5(t, w, "anna", w.iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-a"}), AuthMethodToken); err == nil {
		t.Fatal("a token of the logged-out session connected")
	}
	resp := other.reauth(t, AuthMethodToken, w.iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-a"}))
	if resp.Success {
		t.Fatal("a token of the logged-out session renewed a connection")
	}
}

// A connection that took over a client id is ended by a logout of its login.
// The connection it replaced disconnects after the new one authenticated, and
// that disconnect must not erase the new connection's session.
func TestHumanBackchannelLogoutEndsATakenOverSession(t *testing.T) {
	w := newHumanWorld(t)
	token := func() string { return w.iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-a"}) }
	first, err := dialV5As(t, w, "tab", "anna", token(), AuthMethodToken)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	second, err := dialV5As(t, w, "tab", "anna", token(), AuthMethodToken)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	first.expectDisconnect(t, 0x8E)
	// Let the replaced connection finish disconnecting on the node.
	time.Sleep(300 * time.Millisecond)

	if _, err := w.ver.BackchannelLogout(w.iss.Logout("sid-a", "anna")); err != nil {
		t.Fatalf("logout: %v", err)
	}
	second.expectDisconnect(t, 0x98)
}

// Connections that authenticate while a logout of their login arrives are all
// ended: none slips in between its token check and the kick.
func TestHumanBackchannelLogoutRacingConnects(t *testing.T) {
	w := newHumanWorld(t)
	const n = 24
	sessions := make(chan *v5Session, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			s, err := dialV5(t, w, "anna", w.iss.MintOpt(tokentest.MintOpts{Sub: "anna", Sid: "sid-r"}), AuthMethodToken)
			if err == nil {
				sessions <- s
			}
		}()
	}
	close(start)
	time.Sleep(2 * time.Millisecond)
	if _, err := w.ver.BackchannelLogout(w.iss.Logout("sid-r", "anna")); err != nil {
		t.Fatalf("logout: %v", err)
	}
	wg.Wait()
	close(sessions)
	for s := range sessions {
		s.expectDisconnect(t, 0x98)
	}
}
